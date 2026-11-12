//! Verification of the HS256 subscriber tokens emitted by the reference API.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2_11::Sha256;

pub struct Claims {
    pub environment_id: String,
    pub subscriber_id: String,
    pub context_keys: Vec<String>,
}

pub fn sign(payload: &Value, secret: &[u8]) -> Option<String> {
    if secret.is_empty() {
        return None;
    }
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(payload).ok()?);
    let signed = format!("{header}.{body}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).ok()?;
    mac.update(signed.as_bytes());
    Some(format!(
        "{signed}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    ))
}

pub fn verify_hmac(keys: &[String], subscriber_id: &str, hash: &str) -> bool {
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return false;
    }
    let mut bytes = [0_u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let Ok(parsed) = u8::from_str_radix(&hash[i * 2..i * 2 + 2], 16) else {
            return false;
        };
        *byte = parsed;
    }
    keys.iter().filter(|key| !key.is_empty()).any(|key| {
        let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(key.as_bytes()) else {
            return false;
        };
        mac.update(subscriber_id.as_bytes());
        mac.verify_slice(&bytes).is_ok()
    })
}

pub fn verify(token: &str, secret: &[u8], now: u64) -> Option<Claims> {
    if secret.is_empty() || token.len() > 16_384 {
        return None;
    }
    let mut parts = token.split('.');
    let (header, payload, signature) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).ok()?).ok()?;
    if header.get("alg")?.as_str()? != "HS256" {
        return None;
    }
    let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).ok()?;
    mac.update(header_segment(token).as_bytes());
    mac.verify_slice(&signature).ok()?;
    let payload: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
    if payload.get("aud")?.as_str()? != "widget_user" {
        return None;
    }
    if let Some(exp) = payload.get("exp")
        && exp.as_f64().is_none_or(|exp| now as f64 >= exp)
    {
        return None;
    }
    if let Some(nbf) = payload.get("nbf")
        && nbf.as_f64().is_none_or(|nbf| (now as f64) < nbf)
    {
        return None;
    }
    let environment_id = payload.get("environmentId")?.as_str()?.to_owned();
    let subscriber_id = payload.get("subscriberId")?.as_str()?.to_owned();
    if environment_id.is_empty() || subscriber_id.is_empty() {
        return None;
    }
    let context_keys = match payload.get("contextKeys") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()?,
        _ => return None,
    };
    Some(Claims {
        environment_id,
        subscriber_id,
        context_keys,
    })
}

fn header_segment(token: &str) -> &str {
    // Three segments have already been checked above.
    token
        .rsplit_once('.')
        .map(|(signed, _)| signed)
        .unwrap_or("")
}
