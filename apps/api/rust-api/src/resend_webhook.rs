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
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::Client;

#[derive(Clone)]
pub struct Config {
    environment: String,
    secrets: Vec<Vec<u8>>,
    reload: Option<Arc<Reload>>,
    lock_retry_jobs: bool,
    capture_correlation: bool,
    lock_correlation_jobs: bool,
}
struct Reload {
    path: PathBuf,
    permits: Arc<tokio::sync::Semaphore>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretBundle {
    current: String,
    previous: Option<String>,
}

impl Config {
    pub fn new(environment: String, secret: &str) -> Option<Self> {
        Self::new_with_secrets(environment, &[secret])
    }
    pub fn new_with_secrets(environment: String, secrets: &[&str]) -> Option<Self> {
        if environment.trim().is_empty() || secrets.is_empty() || secrets.len() > 2 {
            return None;
        }
        let mut decoded = Vec::with_capacity(secrets.len());
        for secret in secrets {
            let value = STANDARD.decode(secret.strip_prefix("whsec_")?).ok()?;
            if !(16..=128).contains(&value.len()) || decoded.contains(&value) {
                return None;
            }
            decoded.push(value);
        }
        Some(Self {
            environment,
            secrets: decoded,
            reload: None,
            lock_retry_jobs: false,
            capture_correlation: false,
            lock_correlation_jobs: false,
        })
    }
    fn read_bundle(path: &FsPath) -> Option<Vec<Vec<u8>>> {
        let raw = novu_rust_contracts::secret::read_regular_file(path).ok()?;
        let bundle: SecretBundle = serde_json::from_str(&raw).ok()?;
        let mut secrets = vec![bundle.current.as_str()];
        if let Some(previous) = bundle.previous.as_deref() {
            secrets.push(previous);
        }
        Some(Self::new_with_secrets("fixture".into(), &secrets)?.secrets)
    }
    /// Reload an atomic JSON bundle before signature verification; failures are closed.
    pub fn new_reloading(environment: String, path: PathBuf) -> Option<Self> {
        if environment.trim().is_empty() || path.as_os_str().is_empty() {
            return None;
        }
        Self::read_bundle(&path)?;
        Some(Self {
            environment,
            secrets: Vec::new(),
            reload: Some(Arc::new(Reload {
                path,
                permits: Arc::new(tokio::sync::Semaphore::new(16)),
            })),
            lock_retry_jobs: false,
            capture_correlation: false,
            lock_correlation_jobs: false,
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
    pub fn with_correlation_job_lock(mut self, enabled: bool) -> Self {
        self.lock_correlation_jobs = enabled;
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
        let lock_correlation_jobs =
            match std::env::var("NOVU_RESEND_WEBHOOK_LOCK_CORRELATION_JOBS").as_deref() {
                Ok("true") => true,
                Ok("false") | Err(std::env::VarError::NotPresent) => false,
                _ => return None,
            };
        if lock_correlation_jobs && (!lock_retry_jobs || !capture_correlation) {
            return None;
        }
        let environment = std::env::var("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID").ok()?;
        let mut config =
            if let Some(path) = std::env::var_os("NOVU_RESEND_WEBHOOK_SECRET_BUNDLE_FILE") {
                if [
                    "NOVU_RESEND_WEBHOOK_SECRET",
                    "NOVU_RESEND_WEBHOOK_SECRET_FILE",
                    "NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET",
                    "NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET_FILE",
                ]
                .iter()
                .any(|name| std::env::var_os(name).is_some())
                {
                    return None;
                }
                Self::new_reloading(environment, PathBuf::from(path))?
            } else {
                let current = novu_rust_contracts::secret::from_env(
                    "NOVU_RESEND_WEBHOOK_SECRET",
                    "NOVU_RESEND_WEBHOOK_SECRET_FILE",
                )
                .ok()??;
                let previous = novu_rust_contracts::secret::from_env(
                    "NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET",
                    "NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET_FILE",
                )
                .ok()?;
                let mut secrets = vec![current.as_str()];
                if let Some(previous) = &previous {
                    secrets.push(previous);
                }
                Self::new_with_secrets(environment, &secrets)?
            };
        config.lock_retry_jobs = lock_retry_jobs;
        config.capture_correlation = capture_correlation;
        config.lock_correlation_jobs = lock_correlation_jobs;
        Some(config)
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
    if config.lock_correlation_jobs && (!config.lock_retry_jobs || !config.capture_correlation) {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
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
    if seconds > now.as_secs().saturating_add(300) || now.as_secs().saturating_sub(seconds) > 300 {
        return StatusCode::UNAUTHORIZED;
    }
    let reloaded = if let Some(reload) = &config.reload {
        let Ok(permit) = reload.permits.clone().try_acquire_owned() else {
            return StatusCode::SERVICE_UNAVAILABLE;
        };
        let path = reload.path.clone();
        match tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                Config::read_bundle(&path)
            }),
        )
        .await
        {
            Ok(Ok(Some(secrets))) => Some(secrets),
            _ => return StatusCode::SERVICE_UNAVAILABLE,
        }
    } else {
        None
    };
    let mut valid = false;
    for secret in reloaded.as_deref().unwrap_or(&config.secrets) {
        valid |= verify(secret, id, timestamp, &raw, signatures);
    }
    if !valid {
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
        let mut tagged_sql = sql
            .replace(
                "payload_fingerprint,recipient_hash)",
                "payload_fingerprint,recipient_hash,correlation_id)",
            )
            .replace("$5,$6,$7 FROM", "$5,$6,$7,$8::text::uuid FROM");
        if config.lock_correlation_jobs {
            tagged_sql = tagged_sql.replace(
                "novu_rust_lock_resend_webhook_jobs(id,$4)",
                "novu_rust_lock_resend_webhook_jobs_with_correlation(id,$4,$8::text::uuid)",
            );
        }
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
