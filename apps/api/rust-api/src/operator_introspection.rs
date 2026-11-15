//! Optional, fail-closed RFC 7662 check after local operator JWT verification.
use crate::operator_access_token::VerifiedSubject;
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{Client, Url, header};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Active(Option<u64>),
    Inactive,
    Unavailable,
}

pub struct Introspection {
    client: Client,
    endpoint: Url,
    authorization: String,
}
impl Introspection {
    /// Credentials and response contents must not appear in logs or errors.
    pub fn new(
        endpoint: &str,
        client_id: &str,
        secret: &str,
        allow_loopback_http: bool,
        timeout: Duration,
    ) -> Result<Self, ()> {
        let url = Url::parse(endpoint).map_err(|_| ())?;
        let loopback = matches!(
            url.host_str(),
            Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
        );
        if endpoint.len() > 2048
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.host_str().is_none()
            || !(url.scheme() == "https"
                || (allow_loopback_http && loopback && url.scheme() == "http"))
            || client_id.is_empty()
            || client_id.len() > 512
            || client_id.contains('\0')
            || secret.is_empty()
            || secret.len() > 16384
            || secret.contains(['\0', '\r', '\n'])
            || timeout < Duration::from_secs(1)
            || timeout > Duration::from_secs(30)
        {
            return Err(());
        }
        // OAuth client_secret_basic requires form encoding each credential before Base64.
        let encode = |value: &str| {
            form_urlencoded::Serializer::new(String::new())
                .append_pair("", value)
                .finish()[1..]
                .to_owned()
        };
        let authorization = format!(
            "Basic {}",
            STANDARD.encode(format!("{}:{}", encode(client_id), encode(secret)))
        );
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(|_| ())?;
        Ok(Self {
            client,
            endpoint: url,
            authorization,
        })
    }
    pub fn from_env() -> Result<Option<Self>, ()> {
        let names = [
            "SWALLOW_OPERATOR_INTROSPECTION_URL",
            "SWALLOW_OPERATOR_INTROSPECTION_CLIENT_ID",
            "SWALLOW_OPERATOR_INTROSPECTION_CLIENT_SECRET",
            "SWALLOW_OPERATOR_INTROSPECTION_CLIENT_SECRET_FILE",
            "SWALLOW_OPERATOR_INTROSPECTION_ALLOW_LOOPBACK_HTTP",
            "SWALLOW_OPERATOR_INTROSPECTION_TIMEOUT_SECONDS",
        ];
        if !names.iter().any(|name| std::env::var_os(name).is_some()) {
            return Ok(None);
        }
        let endpoint = std::env::var(names[0]).map_err(|_| ())?;
        let id = std::env::var(names[1]).map_err(|_| ())?;
        let secret = novu_rust_contracts::secret::from_env(names[2], names[3])
            .map_err(|_| ())?
            .ok_or(())?;
        let allow = match std::env::var(names[4]).as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err(()),
        };
        let timeout = match std::env::var(names[5]) {
            Ok(v) => v
                .parse::<u64>()
                .ok()
                .filter(|v| (1..=30).contains(v))
                .ok_or(())?,
            Err(std::env::VarError::NotPresent) => 5,
            Err(_) => return Err(()),
        };
        Self::new(&endpoint, &id, &secret, allow, Duration::from_secs(timeout)).map(Some)
    }
    pub async fn check(&self, token: &str, identity: &VerifiedSubject, audience: &str) -> Outcome {
        let body = form_urlencoded::Serializer::new(String::new())
            .append_pair("token", token)
            .append_pair("token_type_hint", "access_token")
            .finish();
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(header::AUTHORIZATION, &self.authorization)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::ACCEPT, "application/json")
            .body(body)
            .send()
            .await;
        let Ok(mut response) = response else {
            return Outcome::Unavailable;
        };
        if response.status() != reqwest::StatusCode::OK
            || response.content_length().is_some_and(|n| n > 16384)
        {
            return Outcome::Unavailable;
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    if bytes.len() + chunk.len() > 16384 {
                        return Outcome::Unavailable;
                    }
                    bytes.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                Err(_) => return Outcome::Unavailable,
            }
        }
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            return Outcome::Unavailable;
        };
        match value.get("active").and_then(Value::as_bool) {
            Some(false) => return Outcome::Inactive,
            Some(true) => {}
            None => return Outcome::Unavailable,
        }
        for (field, expected) in [
            ("iss", identity.issuer.as_str()),
            ("sub", identity.subject.as_str()),
            ("client_id", identity.client_id.as_str()),
            ("jti", identity.token_id.as_str()),
        ] {
            if value
                .get(field)
                .is_some_and(|v| v.as_str() != Some(expected))
            {
                return Outcome::Inactive;
            }
        }
        if let Some(aud) = value.get("aud") {
            let valid = aud.as_str() == Some(audience)
                || aud.as_array().is_some_and(|a| {
                    !a.is_empty()
                        && a.iter().all(Value::is_string)
                        && a.iter().any(|v| v.as_str() == Some(audience))
                });
            if !valid {
                return Outcome::Inactive;
            }
        }
        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(t) => t.as_secs(),
            Err(_) => return Outcome::Unavailable,
        };
        let expiration = match value.get("exp") {
            Some(value) => match value.as_u64() {
                Some(exp) if exp > now => Some(exp),
                _ => return Outcome::Inactive,
            },
            None => None,
        };
        if let Some(value) = value.get("nbf") {
            if !value.as_u64().is_some_and(|nbf| nbf <= now) {
                return Outcome::Inactive;
            }
        }
        Outcome::Active(expiration)
    }
}
