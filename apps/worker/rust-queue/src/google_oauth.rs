//! Google service-account OAuth; secrets and responses must not be logged.
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use ring::{
    rand::SystemRandom,
    signature::{RSA_PKCS1_SHA256, RsaKeyPair},
};
use serde::Deserialize;
use serde_json::json;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

const TOKEN_URI: &str = "https://www.googleapis.com/oauth2/v4/token";
const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/firebase.database https://www.googleapis.com/auth/firebase.messaging https://www.googleapis.com/auth/identitytoolkit https://www.googleapis.com/auth/userinfo.email";
const GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    InvalidCredentials,
    Transport,
    Provider(u16),
    InvalidResponse,
}
impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCredentials => f.write_str("invalid Google service account"),
            Self::Transport => f.write_str("Google OAuth transport failed"),
            Self::Provider(code) => write!(f, "Google OAuth returned HTTP {code}"),
            Self::InvalidResponse => f.write_str("invalid Google OAuth response"),
        }
    }
}
impl std::error::Error for AuthError {}

struct CachedToken {
    token: String,
    refresh_at: Instant,
}
pub struct ServiceAccount {
    email: String,
    key: RsaKeyPair,
    http: reqwest::Client,
    endpoint: String,
    cached: Mutex<Option<CachedToken>>,
}
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    token_type: Option<String>,
}
impl ServiceAccount {
    pub fn new(email: String, private_key_pem: &str) -> Result<Self, AuthError> {
        Self::with_endpoint(email, private_key_pem, TOKEN_URI)
    }
    pub(crate) fn with_endpoint(
        email: String,
        private_key_pem: &str,
        endpoint: &str,
    ) -> Result<Self, AuthError> {
        if email.is_empty() {
            return Err(AuthError::InvalidCredentials);
        }
        let body = private_key_pem
            .trim()
            .strip_prefix("-----BEGIN PRIVATE KEY-----")
            .and_then(|s| s.strip_suffix("-----END PRIVATE KEY-----"))
            .ok_or(AuthError::InvalidCredentials)?;
        let encoded: String = body.chars().filter(|c| !c.is_ascii_whitespace()).collect();
        let bytes = STANDARD
            .decode(encoded)
            .map_err(|_| AuthError::InvalidCredentials)?;
        let key = RsaKeyPair::from_pkcs8(&bytes).map_err(|_| AuthError::InvalidCredentials)?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| AuthError::InvalidCredentials)?;
        Ok(Self {
            email,
            key,
            http,
            endpoint: endpoint.into(),
            cached: Mutex::new(None),
        })
    }
    fn assertion(&self) -> Result<String, AuthError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::InvalidCredentials)?
            .as_secs();
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256"}"#);
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(
                &json!({"iss":self.email,"scope":SCOPE,"aud":TOKEN_URI,"iat":now,"exp":now+3600}),
            )
            .map_err(|_| AuthError::InvalidCredentials)?,
        );
        let data = format!("{header}.{payload}");
        let mut signature = vec![0; self.key.public().modulus_len()];
        self.key
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                data.as_bytes(),
                &mut signature,
            )
            .map_err(|_| AuthError::InvalidCredentials)?;
        Ok(format!("{data}.{}", URL_SAFE_NO_PAD.encode(signature)))
    }
    /// Serialize refreshes so concurrent callers share a still-valid access token.
    pub async fn access_token(&self) -> Result<String, AuthError> {
        let mut cached = self.cached.lock().await;
        if let Some(value) = cached.as_ref().filter(|v| v.refresh_at > Instant::now()) {
            return Ok(value.token.clone());
        }
        let assertion = self.assertion()?;
        let body =
            serde_urlencoded::to_string([("grant_type", GRANT), ("assertion", assertion.as_str())])
                .map_err(|_| AuthError::InvalidCredentials)?;
        let response = self
            .http
            .post(&self.endpoint)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(body)
            .send()
            .await
            .map_err(|_| AuthError::Transport)?;
        if !response.status().is_success() {
            return Err(AuthError::Provider(response.status().as_u16()));
        }
        let response: TokenResponse = response
            .json()
            .await
            .map_err(|_| AuthError::InvalidResponse)?;
        if response.access_token.is_empty()
            || response.expires_in == 0
            || response
                .token_type
                .as_deref()
                .is_some_and(|v| !v.eq_ignore_ascii_case("bearer"))
            || reqwest::header::HeaderValue::from_str(&format!("Bearer {}", response.access_token))
                .is_err()
        {
            return Err(AuthError::InvalidResponse);
        }
        let valid = Duration::from_secs(response.expires_in.saturating_sub(300));
        let refresh_at = Instant::now()
            .checked_add(valid)
            .ok_or(AuthError::InvalidResponse)?;
        let token = response.access_token;
        *cached = Some(CachedToken {
            token: token.clone(),
            refresh_at,
        });
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};
    use std::process::{Command, Stdio};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn test_key() -> String {
        let output = Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap()
    }
    fn decode_assertion(assertion: &str) -> (ValueParts, Vec<u8>, String) {
        let parts: Vec<_> = assertion.split('.').collect();
        assert_eq!(parts.len(), 3);
        let header = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        let claims = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        (
            ValueParts { header, claims },
            URL_SAFE_NO_PAD.decode(parts[2]).unwrap(),
            format!("{}.{}", parts[0], parts[1]),
        )
    }
    struct ValueParts {
        header: serde_json::Value,
        claims: serde_json::Value,
    }

    #[tokio::test]
    async fn caches_refreshes_and_signs_real_assertions() {
        let pem = test_key();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
        let service =
            ServiceAccount::with_endpoint("fixture@example.test".into(), &pem, &endpoint).unwrap();
        let public = service.key.public().as_ref().to_vec();
        let server = tokio::spawn(async move {
            for index in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut tmp = [0; 4096];
                loop {
                    let n = socket.read(&mut tmp).await.unwrap();
                    assert!(n > 0);
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&buf[..pos]);
                        let len = headers
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        if buf.len() >= pos + 4 + len {
                            break;
                        }
                    }
                }
                let raw = String::from_utf8(buf).unwrap();
                assert!(raw.starts_with("POST /token HTTP/1.1\r\n"));
                let form: std::collections::HashMap<String, String> =
                    serde_urlencoded::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
                assert_eq!(form["grant_type"], GRANT);
                let (parts, sig, data) = decode_assertion(&form["assertion"]);
                assert_eq!(parts.header, json!({"alg":"RS256"}));
                assert_eq!(parts.claims["iss"], "fixture@example.test");
                assert_eq!(parts.claims["aud"], TOKEN_URI);
                assert_eq!(parts.claims["scope"], SCOPE);
                assert_eq!(
                    parts.claims["exp"].as_u64().unwrap() - parts.claims["iat"].as_u64().unwrap(),
                    3600
                );
                UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, &public)
                    .verify(data.as_bytes(), &sig)
                    .unwrap();
                let response = format!(
                    "{{\"access_token\":\"token-{index}\",\"expires_in\":3600,\"token_type\":\"Bearer\"}}"
                );
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
            }
        });
        let (first, second) = tokio::join!(service.access_token(), service.access_token());
        assert_eq!(first.unwrap(), "token-0");
        assert_eq!(second.unwrap(), "token-0");
        service.cached.lock().await.as_mut().unwrap().refresh_at = Instant::now();
        assert_eq!(service.access_token().await.unwrap(), "token-1");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_oauth_failures_and_bad_tokens_without_caching() {
        let pem = test_key();
        assert!(matches!(
            ServiceAccount::new("fixture@example.test".into(), "bad key"),
            Err(AuthError::InvalidCredentials)
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
        let service =
            ServiceAccount::with_endpoint("fixture@example.test".into(), &pem, &endpoint).unwrap();
        let server = tokio::spawn(async move {
            for (status, body) in [
                (403, r#"{"error":"sensitive fixture"}"#),
                (200, r#"{"access_token":"","expires_in":3600}"#),
                (
                    200,
                    r#"{"access_token":"short","expires_in":300,"token_type":"Bearer"}"#,
                ),
                (
                    200,
                    r#"{"access_token":"valid","expires_in":3600,"token_type":"Bearer"}"#,
                ),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = [0; 4096];
                assert!(socket.read(&mut buf).await.unwrap() > 0);
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        assert_eq!(service.access_token().await, Err(AuthError::Provider(403)));
        assert_eq!(
            service.access_token().await,
            Err(AuthError::InvalidResponse)
        );
        assert_eq!(service.access_token().await.unwrap(), "short");
        assert_eq!(service.access_token().await.unwrap(), "valid");
        server.await.unwrap();
    }

    #[test]
    #[ignore = "requires built reference google-auth-library and node"]
    fn original_google_auth_claims_match() {
        use std::io::Write;
        let pem = test_key();
        let service = ServiceAccount::new("fixture@example.test".into(), &pem).unwrap();
        let (rust, signature, data) = decode_assertion(&service.assertion().unwrap());
        UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, service.key.public().as_ref())
            .verify(data.as_bytes(), &signature)
            .unwrap();
        let mut child = Command::new("node")
            .arg("tests/reference_google_oauth.cjs")
            .env(
                "NOVU_REFERENCE_SOURCE",
                std::env::var("NOVU_REFERENCE_SOURCE").unwrap(),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(pem.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut reference: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(reference["grant"], GRANT);
        assert_eq!(reference["url"], TOKEN_URI);
        assert_eq!(reference["header"], rust.header);
        let mut rust_claims = rust.claims;
        for key in ["iat", "exp"] {
            let original = reference["claims"][key].as_i64().unwrap();
            let actual = rust_claims[key].as_i64().unwrap();
            assert!((original - actual).abs() <= 3);
            reference["claims"].as_object_mut().unwrap().remove(key);
            rust_claims.as_object_mut().unwrap().remove(key);
        }
        assert_eq!(reference["claims"], rust_claims);
    }
}
