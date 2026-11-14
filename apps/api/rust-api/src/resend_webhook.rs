//! Resend/Svix signed delivery evidence. Does not mutate job state.
use axum::{
    body::Bytes,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::{digest, hmac};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::Client;

#[derive(Clone)]
pub struct Config {
    environment: String,
    secret: Vec<u8>,
    lock_retry_jobs: bool,
    capture_correlation: bool,
}
impl Config {
    pub fn new(environment: String, secret: &str) -> Option<Self> {
        let decoded = STANDARD.decode(secret.strip_prefix("whsec_")?).ok()?;
        if environment.trim().is_empty() || decoded.len() < 16 || decoded.len() > 128 {
            return None;
        }
        Some(Self {
            environment,
            secret: decoded,
            lock_retry_jobs: false,
            capture_correlation: false,
        })
    }
    pub fn with_retry_job_lock(mut self, enabled: bool) -> Self {
        self.lock_retry_jobs = enabled;
        self
    }
    pub fn with_correlation_capture(mut self, enabled: bool) -> Self {
        self.capture_correlation = enabled;
        self
    }
    pub fn from_env() -> Option<Self> {
        let lock_retry_jobs = match std::env::var("NOVU_RESEND_WEBHOOK_LOCK_RETRY_JOBS").as_deref()
        {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return None,
        };
        let capture_correlation =
            match std::env::var("NOVU_RESEND_WEBHOOK_CAPTURE_CORRELATION").as_deref() {
                Ok("true") => true,
                Ok("false") | Err(std::env::VarError::NotPresent) => false,
                _ => return None,
            };
        Some(
            Self::new(
                std::env::var("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID").ok()?,
                &novu_rust_contracts::secret::from_env(
                    "NOVU_RESEND_WEBHOOK_SECRET",
                    "NOVU_RESEND_WEBHOOK_SECRET_FILE",
                )
                .ok()??,
            )?
            .with_retry_job_lock(lock_retry_jobs)
            .with_correlation_capture(capture_correlation),
        )
    }
}

fn verify(secret: &[u8], id: &str, timestamp: &str, body: &[u8], signatures: &str) -> bool {
    let mut signed = Vec::with_capacity(id.len() + timestamp.len() + body.len() + 2);
    signed.extend_from_slice(id.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(timestamp.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(body);
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    signatures.split_whitespace().take(10).any(|item| {
        item.strip_prefix("v1,")
            .and_then(|s| STANDARD.decode(s).ok())
            .is_some_and(|sig| hmac::verify(&key, &signed, &sig).is_ok())
    })
}

pub async fn receive(
    State(db): State<Arc<Client>>,
    Extension(config): Extension<Arc<Option<Config>>>,
    Path((organization, environment)): Path<(String, String)>,
    headers: HeaderMap,
    raw: Bytes,
) -> StatusCode {
    let Some(config) = config.as_ref() else {
        return StatusCode::UNAUTHORIZED;
    };
    let header = |key: &str| headers.get(key).and_then(|h| h.to_str().ok());
    let (Some(id), Some(timestamp), Some(signatures)) = (
        header("svix-id"),
        header("svix-timestamp"),
        header("svix-signature"),
    ) else {
        return StatusCode::UNAUTHORIZED;
    };
    if environment != config.environment
        || raw.len() > 1_048_576
        || id.is_empty()
        || id.len() > 256
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        || timestamp.len() > 20
        || signatures.len() > 4096
    {
        return StatusCode::UNAUTHORIZED;
    }
    let Ok(seconds) = timestamp.parse::<u64>() else {
        return StatusCode::UNAUTHORIZED;
    };
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return StatusCode::UNAUTHORIZED;
    };
    if seconds > now.as_secs().saturating_add(300)
        || now.as_secs().saturating_sub(seconds) > 300
        || !verify(&config.secret, id, timestamp, &raw, signatures)
    {
        return StatusCode::UNAUTHORIZED;
    }
    let Ok(value) = serde_json::from_slice::<Value>(&raw) else {
        return StatusCode::BAD_REQUEST;
    };
    let (Some(kind), Some(email)) = (
        value.get("type").and_then(Value::as_str),
        value.pointer("/data/email_id").and_then(Value::as_str),
    ) else {
        return StatusCode::BAD_REQUEST;
    };
    if ![
        "email.sent",
        "email.delivered",
        "email.delivery_delayed",
        "email.bounced",
        "email.failed",
        "email.complained",
        "email.opened",
        "email.clicked",
        "email.scheduled",
        "email.suppressed",
    ]
    .contains(&kind)
        || email.is_empty()
        || email.len() > 256
        || !email
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return StatusCode::BAD_REQUEST;
    }
    // A replay must match the complete signed payload, not just its ID and type.
    let fingerprint = digest::digest(&digest::SHA256, &raw);
    let recipient_hash = value
        .pointer("/data/to")
        .and_then(Value::as_array)
        .filter(|v| v.len() == 1)
        .and_then(|v| v[0].as_str())
        .filter(|v| !v.is_empty() && v.len() <= 320)
        .map(|address| {
            STANDARD.encode(digest::digest(&digest::SHA256, address.as_bytes()).as_ref())
        });
    let sql = if config.lock_retry_jobs {
        "WITH valid_env AS MATERIALIZED (SELECT id FROM public.novu_rust_environments WHERE id=$1 AND organization_id=$2), locked AS MATERIALIZED (SELECT id,public.novu_rust_lock_resend_webhook_jobs(id,$4) AS n FROM valid_env) INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,payload_fingerprint,recipient_hash) SELECT id,$3,$4,$5,$6,$7 FROM locked WHERE n>=0 ON CONFLICT(environment_id,event_id) DO UPDATE SET event_id=EXCLUDED.event_id WHERE public.novu_rust_resend_delivery_events.email_id=EXCLUDED.email_id AND public.novu_rust_resend_delivery_events.event_type=EXCLUDED.event_type AND (public.novu_rust_resend_delivery_events.payload_fingerprint IS NULL OR public.novu_rust_resend_delivery_events.payload_fingerprint=EXCLUDED.payload_fingerprint)"
    } else {
        "INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,payload_fingerprint,recipient_hash) SELECT e.id,$3,$4,$5,$6,$7 FROM novu_rust_environments e WHERE e.id=$1 AND e.organization_id=$2 ON CONFLICT(environment_id,event_id) DO UPDATE SET event_id=EXCLUDED.event_id WHERE novu_rust_resend_delivery_events.email_id=EXCLUDED.email_id AND novu_rust_resend_delivery_events.event_type=EXCLUDED.event_type AND (novu_rust_resend_delivery_events.payload_fingerprint IS NULL OR novu_rust_resend_delivery_events.payload_fingerprint=EXCLUDED.payload_fingerprint)"
    };
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
        &environment,
        &organization,
        &id,
        &email,
        &kind,
        &fingerprint.as_ref(),
        &recipient_hash,
    ];
    let result = if config.capture_correlation {
        // Accept only the canonical random UUID form produced by the worker.
        let correlation = value
            .pointer("/data/tags/swallow_dispatch")
            .and_then(Value::as_str)
            .and_then(|s| {
                uuid::Uuid::parse_str(s)
                    .ok()
                    .filter(|u| u.get_version_num() == 4 && u.to_string() == s)
                    .map(|u| u.to_string())
            });
        let tagged_sql = sql
            .replace(
                "payload_fingerprint,recipient_hash)",
                "payload_fingerprint,recipient_hash,correlation_id)",
            )
            .replace("$5,$6,$7 FROM", "$5,$6,$7,$8::text::uuid FROM");
        let mut tagged_params = params.to_vec();
        tagged_params.push(&correlation);
        db.execute(&tagged_sql, &tagged_params).await
    } else {
        db.execute(sql, params).await
    };
    match result {
        Ok(1) => StatusCode::NO_CONTENT,
        Ok(_) => StatusCode::CONFLICT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn svix_published_vector() {
        let secret = STANDARD.decode("plJ3nmyCDGBKInavdOK15jsl").unwrap();
        let raw = br#"{"event_type":"ping","data":{"success":true}}"#;
        assert!(verify(
            &secret,
            "msg_loFOjxBNrRLzqYUf",
            "1731705121",
            raw,
            "v1,rAvfW3dJ/X/qxhsaXPOyyCGmRKsaKWcsNccKXlIktD0="
        ));
        assert!(!verify(
            &secret,
            "msg_loFOjxBNrRLzqYUf",
            "1731705121",
            b"tampered",
            "v1,rAvfW3dJ/X/qxhsaXPOyyCGmRKsaKWcsNccKXlIktD0="
        ));
    }
}
