//! Parsing for independently provisioned experimental console credentials.
use axum::http::{HeaderMap, header};
use sha2::{Digest, Sha256};

pub(super) fn credential_hash(headers: &HeaderMap) -> Option<String> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let token = value.to_str().ok()?.strip_prefix("SwallowConsole ")?;
    if token.len() != 64
        || !token
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return None;
    }
    Some(format!("{:x}", Sha256::digest(token.as_bytes())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn accepts_only_one_well_formed_console_authorization() {
        let mut headers = HeaderMap::new();
        assert!(credential_hash(&headers).is_none());
        let token = "a".repeat(64);
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("SwallowConsole {token}")).unwrap(),
        );
        assert_eq!(
            credential_hash(&headers),
            Some(format!("{:x}", Sha256::digest(token.as_bytes())))
        );
        headers.append(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("SwallowConsole {token}")).unwrap(),
        );
        assert!(credential_hash(&headers).is_none());
        for value in [
            format!("SwallowConsole {}, SwallowConsole {}", token, token),
            format!("SwallowConsole {}", "A".repeat(64)),
            format!("Bearer {token}"),
            format!("SwallowConsole {token} "),
            "SwallowConsole short".into(),
        ] {
            headers.clear();
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&value).unwrap(),
            );
            assert!(credential_hash(&headers).is_none());
        }
        headers.clear();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_bytes(b"SwallowConsole \x80").unwrap(),
        );
        assert!(credential_hash(&headers).is_none());
    }
}
