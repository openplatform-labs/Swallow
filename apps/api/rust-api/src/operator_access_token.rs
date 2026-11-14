//! RFC 9068-style access-token verification for an opt-in operator identity endpoint.
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use rsa::{
    RsaPublicKey, pkcs1::DecodeRsaPublicKey, pkcs8::DecodePublicKey, traits::PublicKeyParts,
};
use serde::{Deserialize, Deserializer};
use std::{
    collections::HashMap,
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
    keys: HashMap<String, DecodingKey>,
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
        Self::new(&config.issuer, &config.audience, &keys)
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
            keys: decoding_keys,
        })
    }

    pub fn verify(&self, token: &str) -> Option<VerifiedSubject> {
        if token.len() > 16384 || token.is_empty() {
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
        let key = self.keys.get(header.kid.as_deref()?)?;
        let claims = decode::<AccessClaims>(token, key, &self.validation)
            .ok()?
            .claims;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        if claims.iss != self.issuer
            || claims.iat > now
            || claims.exp <= now
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
