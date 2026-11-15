//! RFC 9068-style access-token verification for an opt-in operator identity endpoint.
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use rsa::{
    RsaPublicKey, pkcs1::DecodeRsaPublicKey, pkcs8::DecodePublicKey, traits::PublicKeyParts,
};
use serde::{Deserialize, Deserializer};
use std::{
    collections::HashMap,
    sync::{RwLock, atomic::{AtomicBool, Ordering}},
    time::{SystemTime, UNIX_EPOCH},
};

/// A verified subject is an issuer-scoped opaque identifier, not proof of a human user.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedSubject {
    pub issuer: String,
    pub subject: String,
    pub client_id: String,
    pub token_id: String,
    pub expires_at: u64,
}

#[derive(Deserialize)]
struct AccessClaims {
    iss: String,
    sub: String,
    client_id: String,
    jti: String,
    exp: u64,
    iat: u64,
    #[serde(default, deserialize_with = "optional_numeric")]
    nbf: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifierConfig {
    issuer: String,
    audience: String,
    keys: Vec<VerifierKey>,
    max_token_lifetime_seconds: Option<u64>,
    #[serde(default)]
    check_token_revocation: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifierKey {
    kid: String,
    pem: String,
}

fn optional_numeric<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    u64::deserialize(deserializer).map(Some)
}

pub struct AccessTokenVerifier {
    issuer: String,
    validation: Validation,
    keys: RwLock<HashMap<String, DecodingKey>>,
    accepting: AtomicBool,
    max_token_lifetime_seconds: Option<u64>,
    check_token_revocation: bool,
    audience: String,
    introspection: Option<crate::operator_introspection::Introspection>,
}

impl AccessTokenVerifier {
    pub fn from_json(value: &str) -> Result<Self, &'static str> {
        if value.len() > 16384 {
            return Err("invalid verifier configuration");
        }
        let config: VerifierConfig =
            serde_json::from_str(value).map_err(|_| "invalid verifier configuration")?;
        let keys: Vec<_> = config
            .keys
            .iter()
            .map(|key| (key.kid.as_str(), key.pem.as_bytes()))
            .collect();
        if config
            .max_token_lifetime_seconds
            .is_some_and(|v| !(1..=31_536_000).contains(&v))
        {
            return Err("invalid verifier configuration");
        }
        let mut verifier = Self::new(&config.issuer, &config.audience, &keys)?;
        verifier.max_token_lifetime_seconds = config.max_token_lifetime_seconds;
        verifier.check_token_revocation = config.check_token_revocation;
        Ok(verifier)
    }
    /// Key material is trusted configuration; each entry is a unique key ID and RSA public PEM.
    pub fn new(issuer: &str, audience: &str, keys: &[(&str, &[u8])]) -> Result<Self, &'static str> {
        if issuer.is_empty()
            || issuer.len() > 2048
            || audience.is_empty()
            || audience.len() > 2048
            || keys.is_empty()
            || keys.len() > 16
        {
            return Err("invalid verifier configuration");
        }
        let mut decoding_keys = HashMap::new();
        for (id, pem) in keys {
            if id.is_empty()
                || id.len() > 256
                || pem.len() > 16384
                || decoding_keys.contains_key(*id)
            {
                return Err("invalid key configuration");
            }
            let text = std::str::from_utf8(pem).map_err(|_| "invalid RSA key")?;
            let public = RsaPublicKey::from_public_key_pem(text)
                .or_else(|_| RsaPublicKey::from_pkcs1_pem(text))
                .map_err(|_| "invalid RSA key")?;
            if !(2048..=8192).contains(&public.n().bits()) {
                return Err("RSA key size must be 2048 to 8192 bits");
            }
            let key = DecodingKey::from_rsa_pem(pem).map_err(|_| "invalid RSA key")?;
            decoding_keys.insert((*id).to_owned(), key);
        }
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.leeway = 0;
        validation.validate_nbf = true;
        Ok(Self {
            issuer: issuer.to_owned(),
            validation,
            keys: RwLock::new(decoding_keys),
            accepting: AtomicBool::new(true),
            max_token_lifetime_seconds: None,
            check_token_revocation: false,
            audience: audience.to_owned(),
            introspection: None,
        })
    }

    /// Replace only signing keys; identity and authorization policy stay fixed.
    pub fn reload_keys(&self, value: &str) -> Result<(), &'static str> {
        let candidate = Self::from_json(value)?;
        if candidate.issuer != self.issuer || candidate.audience != self.audience
            || candidate.max_token_lifetime_seconds != self.max_token_lifetime_seconds
            || candidate.check_token_revocation != self.check_token_revocation {
            return Err("operator verifier policy changed");
        }
        let keys = candidate.keys.into_inner().map_err(|_| "operator key lock unavailable")?;
        *self.keys.write().map_err(|_| "operator key lock unavailable")? = keys;
        self.accepting.store(true, Ordering::Release);
        Ok(())
    }
    pub fn reject_until_reloaded(&self) { self.accepting.store(false, Ordering::Release); }
    pub fn is_available(&self) -> bool { self.accepting.load(Ordering::Acquire) }

    pub fn set_introspection(&mut self, config: crate::operator_introspection::Introspection) {
        self.introspection = Some(config);
    }
    pub async fn introspect(
        &self,
        token: &str,
        identity: &VerifiedSubject,
    ) -> crate::operator_introspection::Outcome {
        match &self.introspection {
            Some(config) => config.check(token, identity, &self.audience).await,
            None => crate::operator_introspection::Outcome::Active(None),
        }
    }

    pub fn checks_token_revocation(&self) -> bool {
        self.check_token_revocation
    }

    pub fn verify(&self, token: &str) -> Option<VerifiedSubject> {
        if !self.accepting.load(Ordering::Acquire) || token.len() > 16384 || token.is_empty() {
            return None;
        }
        let header = decode_header(token).ok()?;
        if header.alg != Algorithm::RS256
            || !header.typ.as_deref().is_some_and(|t| {
                t.eq_ignore_ascii_case("at+jwt") || t.eq_ignore_ascii_case("application/at+jwt")
            })
            || header.crit.is_some()
            || header.cty.is_some()
            || header.jku.is_some()
            || header.jwk.is_some()
            || header.x5u.is_some()
            || header.x5c.is_some()
            || header.enc.is_some()
            || header.zip.is_some()
        {
            return None;
        }
        let key = self.keys.read().ok()?.get(header.kid.as_deref()?)?.clone();
        let claims = decode::<AccessClaims>(token, &key, &self.validation)
            .ok()?
            .claims;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        if claims.iss != self.issuer
            || claims.iat > now
            || claims.exp <= now
            || self.max_token_lifetime_seconds.is_some_and(|max| {
                claims
                    .exp
                    .checked_sub(claims.iat)
                    .is_none_or(|duration| duration > max)
            })
            || claims.nbf.is_some_and(|nbf| nbf > now)
            || [&claims.sub, &claims.client_id, &claims.jti]
                .iter()
                .any(|s| s.is_empty() || s.len() > 1024)
        {
            return None;
        }
        Some(VerifiedSubject {
            issuer: self.issuer.clone(),
            subject: claims.sub,
            client_id: claims.client_id,
            token_id: claims.jti,
            expires_at: claims.exp,
        })
    }
}
