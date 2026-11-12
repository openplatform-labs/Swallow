//! Authenticated Twilio status evidence. This does not change delivery state.
use axum::{
    body::Bytes,
    extract::{Extension, OriginalUri, Path, State},
    http::{HeaderMap, StatusCode, Uri},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::Client;
use uuid::Uuid;

/// One explicitly configured Twilio account and its public HTTPS callback URL.
/// Keep the auth token out of logs and of the database.
pub struct Config {
    environment: String,
    account_sid: String,
    public_url: String,
    public_path: String,
    auth_token: Vec<u8>,
}

impl Config {
    pub fn new(
        environment: String,
        account_sid: String,
        public_url: String,
        auth_token: Vec<u8>,
    ) -> Option<Self> {
        let uri: Uri = public_url.parse().ok()?;
        if environment.is_empty()
            || !valid_sid(&account_sid, "AC")
            || auth_token.is_empty()
            || uri.scheme_str() != Some("https")
            || uri.authority().is_none_or(|v| v.as_str().contains('@'))
            || uri.query().is_some()
            || uri.path().is_empty()
        {
            return None;
        }
        let public_path = uri.path().to_owned();
        Some(Self {
            environment,
            account_sid,
            public_url,
            public_path,
            auth_token,
        })
    }
    pub fn from_env() -> Option<Self> {
        Self::new(
            std::env::var("NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID").ok()?,
            std::env::var("NOVU_TWILIO_WEBHOOK_ACCOUNT_SID").ok()?,
            std::env::var("NOVU_TWILIO_WEBHOOK_URL").ok()?,
            std::env::var("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN")
                .ok()?
                .into_bytes(),
        )
    }
}

fn valid_sid(value: &str, prefix: &str) -> bool {
    value.len() == 34
        && value.starts_with(prefix)
        && value[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

// Twilio's Node helper sorts keys and the unique values of repeated keys by
// JavaScript's UTF-16 ordering, then concatenates each name and value.
fn signed_data(url: &str, fields: &HashMap<String, Vec<String>>) -> Vec<u8> {
    let mut entries: Vec<_> = fields.iter().collect();
    entries.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
    let mut result = url.as_bytes().to_vec();
    for (key, values) in entries {
        let mut values = values.clone();
        values.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
        values.dedup();
        for value in values {
            result.extend_from_slice(key.as_bytes());
            result.extend_from_slice(value.as_bytes());
        }
    }
    result
}

pub async fn receive(
    State(db): State<Arc<Client>>,
    Extension(config): Extension<Arc<Option<Config>>>,
    Path((organization, environment)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    raw: Bytes,
) -> StatusCode {
    receive_inner(
        db,
        config,
        organization,
        environment,
        uri,
        headers,
        raw,
        None,
        None,
    )
    .await
}

pub async fn receive_job(
    State(db): State<Arc<Client>>,
    Extension(config): Extension<Arc<Option<Config>>>,
    Path((organization, environment, job)): Path<(String, String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    raw: Bytes,
) -> StatusCode {
    let Ok(id) = Uuid::parse_str(&job) else {
        return StatusCode::UNAUTHORIZED;
    };
    if id.to_string() != job {
        return StatusCode::UNAUTHORIZED;
    }
    receive_inner(
        db,
        config,
        organization,
        environment,
        uri,
        headers,
        raw,
        Some(id),
        None,
    )
    .await
}

pub async fn receive_dispatch(
    State(db): State<Arc<Client>>,
    Extension(config): Extension<Arc<Option<Config>>>,
    Path((organization, environment, job, dispatch)): Path<(String, String, String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    raw: Bytes,
) -> StatusCode {
    let (Ok(job_id), Ok(dispatch_id)) = (Uuid::parse_str(&job), Uuid::parse_str(&dispatch)) else {
        return StatusCode::UNAUTHORIZED;
    };
    if job_id.to_string() != job || dispatch_id.to_string() != dispatch {
        return StatusCode::UNAUTHORIZED;
    }
    receive_inner(
        db,
        config,
        organization,
        environment,
        uri,
        headers,
        raw,
        Some(job_id),
        Some(dispatch_id),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn receive_inner(
    db: Arc<Client>,
    config: Arc<Option<Config>>,
    organization: String,
    environment: String,
    uri: Uri,
    headers: HeaderMap,
    raw: Bytes,
    job: Option<Uuid>,
    dispatch: Option<Uuid>,
) -> StatusCode {
    let Some(config) = config.as_ref() else {
        return StatusCode::UNAUTHORIZED;
    };
    let suffix = match (job, dispatch) {
        (Some(job), Some(dispatch)) => format!("/jobs/{job}/attempts/{dispatch}"),
        (Some(job), None) => format!("/jobs/{job}"),
        _ => String::new(),
    };
    let expected_path = format!("{}{}", config.public_path, suffix);
    let expected_url = format!("{}{}", config.public_url, suffix);
    if environment != config.environment
        || uri.path() != expected_path
        || uri.query().is_some()
        || raw.len() > 65_536
    {
        return StatusCode::UNAUTHORIZED;
    }
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if !content_type.eq_ignore_ascii_case("application/x-www-form-urlencoded")
        || std::str::from_utf8(&raw).is_err()
    {
        return StatusCode::BAD_REQUEST;
    }
    let mut fields: HashMap<String, Vec<String>> = HashMap::new();
    let mut count = 0;
    for (key, value) in form_urlencoded::parse(&raw) {
        count += 1;
        if count > 128 || key.len() > 256 || value.len() > 4096 {
            return StatusCode::BAD_REQUEST;
        }
        fields
            .entry(key.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    let Some(signature) = headers
        .get("x-twilio-signature")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| STANDARD.decode(v).ok())
    else {
        return StatusCode::UNAUTHORIZED;
    };
    let key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, &config.auth_token);
    if hmac::verify(&key, &signed_data(&expected_url, &fields), &signature).is_err() {
        return StatusCode::UNAUTHORIZED;
    }
    let one = |name: &str| -> Option<&str> {
        let values = fields.get(name)?;
        if values.len() != 1 {
            return None;
        }
        Some(values[0].as_str())
    };
    let (Some(account), Some(message), Some(status)) =
        (one("AccountSid"), one("MessageSid"), one("MessageStatus"))
    else {
        return StatusCode::BAD_REQUEST;
    };
    let error = match fields.get("ErrorCode") {
        None => "",
        Some(values) if values.len() == 1 => values[0].as_str(),
        _ => return StatusCode::BAD_REQUEST,
    };
    if account != config.account_sid
        || !(valid_sid(message, "SM") || valid_sid(message, "MM"))
        || status.is_empty()
        || status.len() > 32
        || !status.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
        || error.len() > 12
        || !error.bytes().all(|b| b.is_ascii_digit())
    {
        return StatusCode::BAD_REQUEST;
    }
    if let (Some(job), Some(dispatch)) = (job, dispatch) {
        let rows = db.execute("WITH locked_job AS MATERIALIZED (SELECT id,environment_id FROM novu_queue_jobs WHERE id=$7 AND environment_id=$1 AND kind='experimental_twilio_sms' FOR UPDATE) INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,error_code,job_id,dispatch_id) SELECT e.id,$3,$4,$5,$6,j.id,d.id FROM novu_rust_environments e JOIN locked_job j ON j.environment_id=e.id JOIN novu_rust_twilio_dispatch_ids d ON d.job_id=j.id AND d.environment_id=e.id AND d.id=$8 WHERE e.id=$1 AND e.organization_id=$2 ON CONFLICT(environment_id,account_sid,message_sid,status,error_code) DO UPDATE SET job_id=EXCLUDED.job_id,dispatch_id=EXCLUDED.dispatch_id WHERE (novu_rust_twilio_delivery_events.job_id IS NULL OR novu_rust_twilio_delivery_events.job_id=EXCLUDED.job_id) AND (novu_rust_twilio_delivery_events.dispatch_id IS NULL OR novu_rust_twilio_delivery_events.dispatch_id=EXCLUDED.dispatch_id)", &[&environment,&organization,&account,&message,&status,&error,&job,&dispatch]).await;
        match rows {
            Ok(0) => StatusCode::NOT_FOUND,
            Ok(_) => StatusCode::NO_CONTENT,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    } else if let Some(job) = job {
        let rows=db.execute("WITH locked_job AS MATERIALIZED (SELECT id,environment_id FROM novu_queue_jobs WHERE id=$7 AND environment_id=$1 AND kind='experimental_twilio_sms' FOR UPDATE) INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,error_code,job_id) SELECT e.id,$3,$4,$5,$6,j.id FROM novu_rust_environments e JOIN locked_job j ON j.environment_id=e.id WHERE e.id=$1 AND e.organization_id=$2 AND (EXISTS(SELECT 1 FROM novu_rust_twilio_attempts a WHERE a.job_id=j.id AND a.environment_id=e.id) OR EXISTS(SELECT 1 FROM novu_rust_delivery_reconciliations r WHERE r.job_id=j.id AND r.environment_id=e.id AND r.target_index IS NULL AND r.decision='retry')) ON CONFLICT(environment_id,account_sid,message_sid,status,error_code) DO UPDATE SET job_id=EXCLUDED.job_id WHERE novu_rust_twilio_delivery_events.job_id IS NULL OR novu_rust_twilio_delivery_events.job_id=EXCLUDED.job_id", &[&environment,&organization,&account,&message,&status,&error,&job]).await;
        match rows {
            Ok(0) => StatusCode::NOT_FOUND,
            Ok(_) => StatusCode::NO_CONTENT,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    } else {
        match db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,error_code) SELECT e.id,$3,$4,$5,$6 FROM novu_rust_environments e WHERE e.id=$1 AND e.organization_id=$2 ON CONFLICT DO NOTHING", &[&environment,&organization,&account,&message,&status,&error]).await {
            Ok(_) => StatusCode::NO_CONTENT,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
