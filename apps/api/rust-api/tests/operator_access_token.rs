//! Test keys in this directory are public fixtures, never production credentials.
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use novu_rust_api::operator_access_token::{AccessTokenVerifier, VerifiedSubject};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

const PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-private.pem");
const OTHER_PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-other-private.pem");
const PUBLIC: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-public.pem");
const WEAK_PUBLIC: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-weak-public.pem");

fn token(claims: &Value, mutate: impl FnOnce(&mut Header)) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("at+jwt".into());
    header.kid = Some("one".into());
    mutate(&mut header);
    encode(
        &header,
        claims,
        &EncodingKey::from_rsa_pem(PRIVATE).unwrap(),
    )
    .unwrap()
}

#[test]
fn verifies_pinned_access_tokens_and_rejects_confusion() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let verifier =
        AccessTokenVerifier::new("https://issuer.test", "swallow-api", &[("one", PUBLIC)]).unwrap();
    let good = json!({"iss":"https://issuer.test","sub":"person-1","aud":["swallow-api","other"],"exp":now+120,"iat":now-1,"jti":"token-1","client_id":"console"});
    assert_eq!(
        verifier.verify(&token(&good, |_| {})),
        Some(VerifiedSubject {
            issuer: "https://issuer.test".into(),
            subject: "person-1".into(),
            client_id: "console".into(),
            token_id: "token-1".into(),
            expires_at: now + 120
        })
    );
    let cases = [
        ("iss", json!("https://other.test")),
        ("iss", json!(["https://issuer.test"])),
        ("aud", json!("other")),
        ("exp", json!(now - 1)),
        ("exp", json!(now)),
        ("nbf", json!(now + 60)),
        ("nbf", json!(null)),
        ("iat", json!(now + 60)),
        ("iat", json!("wrong")),
        ("sub", json!("")),
        ("jti", json!(null)),
        ("client_id", json!(null)),
    ];
    for (field, value) in cases {
        let mut changed = good.clone();
        changed[field] = value;
        assert!(
            verifier.verify(&token(&changed, |_| {})).is_none(),
            "{field}"
        );
    }
    for field in ["iss", "aud", "exp", "iat", "sub", "jti", "client_id"] {
        let mut missing = good.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            verifier.verify(&token(&missing, |_| {})).is_none(),
            "{field}"
        );
    }
    for mutate in [
        (|h: &mut Header| h.typ = Some("JWT".into())) as fn(&mut Header),
        |h| h.typ = None,
        |h| h.kid = Some("unknown".into()),
        |h| h.kid = None,
        |h| h.jku = Some("https://evil.test/key".into()),
        |h| h.crit = Some(vec!["unknown".into()]),
    ] {
        assert!(verifier.verify(&token(&good, mutate)).is_none());
    }
    let mut tampered = token(&good, |_| {});
    tampered.push('a');
    assert!(verifier.verify(&tampered).is_none());
    let mut h = Header::new(Algorithm::HS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("one".into());
    let wrong_alg = encode(&h, &good, &EncodingKey::from_secret(b"arbitrary")).unwrap();
    assert!(verifier.verify(&wrong_alg).is_none());
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("one".into());
    let wrong_key = encode(
        &h,
        &good,
        &EncodingKey::from_rsa_pem(OTHER_PRIVATE).unwrap(),
    )
    .unwrap();
    assert!(verifier.verify(&wrong_key).is_none());
    assert!(verifier.verify(&"x".repeat(16385)).is_none());
}

#[test]
fn rejects_invalid_or_duplicate_configuration() {
    let config = json!({"issuer":"https://issuer.test","audience":"api","keys":[{"kid":"one","pem":std::str::from_utf8(PUBLIC).unwrap()}]});
    assert!(AccessTokenVerifier::from_json(&config.to_string()).is_ok());
    assert!(AccessTokenVerifier::from_json("{}").is_err());
    assert!(AccessTokenVerifier::from_json(&" ".repeat(16385)).is_err());
    assert!(AccessTokenVerifier::new("issuer", "aud", &[("weak", WEAK_PUBLIC)]).is_err());
    assert!(AccessTokenVerifier::new("issuer", "aud", &[("private", PRIVATE)]).is_err());
    assert!(AccessTokenVerifier::new("", "aud", &[("one", PUBLIC)]).is_err());
    assert!(AccessTokenVerifier::new("issuer", "", &[("one", PUBLIC)]).is_err());
    assert!(AccessTokenVerifier::new("issuer", "aud", &[]).is_err());
    assert!(
        AccessTokenVerifier::new("issuer", "aud", &[("one", PUBLIC), ("one", PUBLIC)]).is_err()
    );
    assert!(AccessTokenVerifier::new("issuer", "aud", &[("one", b"invalid")]).is_err());
}
