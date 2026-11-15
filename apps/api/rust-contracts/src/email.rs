//! Conservative ASCII subset of the reference validator.js email contract.
//! `None` means the address is outside this subset and must not be guessed.
pub fn validate(value: &str) -> Option<bool> {
    if !value.is_ascii() || value.contains('"') || value.contains('\\') {
        return None;
    }
    if value.len() > 254 {
        return Some(false);
    }
    let Some((local, domain)) = value.split_once('@') else {
        return Some(false);
    };
    if local.is_empty() || local.len() > 64 || domain.len() > 254 || domain.contains('@') {
        return Some(false);
    }
    const SPECIAL: &[u8] = b"!#$%&'*+-/=?^_`{|}~";
    if local.split('.').any(|part| {
        part.is_empty()
            || !part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || SPECIAL.contains(&b))
    }) {
        return Some(false);
    }
    let labels: Vec<&str> = domain.split('.').collect();
    if labels.len() < 2 {
        return Some(false);
    }
    let tld = labels.last().copied().unwrap_or_default();
    let valid_tld = (tld.len() >= 2 && tld.bytes().all(|b| b.is_ascii_alphabetic()))
        || (tld.len() >= 4
            && tld[..2].eq_ignore_ascii_case("xn")
            && tld.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'));
    Some(
        valid_tld
            && labels.iter().all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            }),
    )
}

/// Whether the disposition and CID can be represented by Resend.
pub fn supports_resend_attachment_disposition(disposition: Option<&str>, cid: Option<&str>) -> bool {
    if cid.is_some_and(|value| value.is_empty() || value.chars().count() >= 128) {
        return false;
    }
    match disposition {
        None => true,
        Some("attachment") => cid.is_none(),
        Some("inline") => cid.is_some_and(|v| !v.is_empty() && v.chars().count() < 128),
        Some(_) => false,
    }
}
