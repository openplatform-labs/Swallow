use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

const PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-private.pem");
const PUBLIC: &str = include_str!("fixtures/operator_jwt/test-only-public.pem");

fn config(kids: &[&str]) -> Value {
    json!({"issuer":"https://issuer.test","audience":"swallow",
        "keys":kids.iter().map(|kid| json!({"kid":kid,"pem":PUBLIC})).collect::<Vec<_>>()})
}
fn token(kid: &str) -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("at+jwt".into());
    header.kid = Some(kid.into());
    encode(&header, &json!({"iss":"https://issuer.test","aud":"swallow","sub":"alice",
        "client_id":"console","jti":"test","iat":now-1,"exp":now+300}),
        &EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
}

#[test]
fn key_rotation_rejects_invalid_config_and_preserves_policy() {
    let verifier = AccessTokenVerifier::from_json(&config(&["old"]).to_string()).unwrap();
    let old = token("old");
    let new = token("new");
    assert!(verifier.verify(&old).is_some());
    assert!(verifier.verify(&new).is_none());
    verifier.reload_keys(&config(&["old", "new"]).to_string()).unwrap();
    assert!(verifier.verify(&old).is_some());
    assert!(verifier.verify(&new).is_some());
    verifier.reload_keys(&config(&["new"]).to_string()).unwrap();
    assert!(verifier.verify(&old).is_none());
    assert!(verifier.verify(&new).is_some());
    assert!(verifier.reload_keys("{").is_err());
    let mut changed = config(&["old"]);
    changed["issuer"] = json!("https://other.test");
    assert!(verifier.reload_keys(&changed.to_string()).is_err());
    changed = config(&["old"]);
    changed["check_token_revocation"] = json!(true);
    assert!(verifier.reload_keys(&changed.to_string()).is_err());
    assert!(verifier.verify(&old).is_none());
    assert!(verifier.verify(&new).is_some());
    verifier.reject_until_reloaded();
    assert!(!verifier.is_available());
    assert!(verifier.verify(&new).is_none());
    assert!(verifier.reload_keys("{}").is_err());
    assert!(!verifier.is_available());
    verifier.reload_keys(&config(&["new"]).to_string()).unwrap();
    assert!(verifier.is_available());
    assert!(verifier.verify(&new).is_some());
}
