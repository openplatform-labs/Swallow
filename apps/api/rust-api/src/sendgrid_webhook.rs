//! Signed SendGrid delivery evidence, scoped to an environment.
use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use der::asn1::ObjectIdentifier;
use ring::signature::{ECDSA_P256_SHA256_ASN1, UnparsedPublicKey};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::Client;
use uuid::Uuid;

fn verify(key: &str, signature: &[u8], timestamp: &str, raw: &[u8]) -> bool {
    let Ok(der) = STANDARD.decode(key.trim()) else {
        return false;
    };
    let Ok(info) = spki::SubjectPublicKeyInfoRef::try_from(der.as_slice()) else {
        return false;
    };
    if info.algorithm.oid != ObjectIdentifier::new_unwrap("1.2.840.10045.2.1") {
        return false;
    }
    let Some(params) = info.algorithm.parameters else {
        return false;
    };
    if params.decode_as::<ObjectIdentifier>().ok()
        != Some(ObjectIdentifier::new_unwrap("1.2.840.10045.3.1.7"))
    {
        return false;
    }
    let Some(public) = info.subject_public_key.as_bytes() else {
        return false;
    };
    let mut signed = Vec::with_capacity(timestamp.len() + raw.len());
    signed.extend_from_slice(timestamp.as_bytes());
    signed.extend_from_slice(raw);
    UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, public)
        .verify(&signed, signature)
        .is_ok()
}

pub async fn receive(
    State(db): State<Arc<Client>>,
    Path((organization, environment, provider)): Path<(String, String, String)>,
    headers: HeaderMap,
    raw: Bytes,
) -> impl IntoResponse {
    let fail = || {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"message":"invalid webhook authentication"})),
        )
    };
    if provider != "sendgrid" || raw.len() > 1_048_576 {
        return fail();
    }
    let Some(timestamp) = headers
        .get("x-twilio-email-event-webhook-timestamp")
        .and_then(|v| v.to_str().ok())
    else {
        return fail();
    };
    let Some(signature) = headers
        .get("x-twilio-email-event-webhook-signature")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| STANDARD.decode(v).ok())
    else {
        return fail();
    };
    let Ok(timestamp_seconds) = timestamp.parse::<u64>() else {
        return fail();
    };
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return fail();
    };
    if timestamp_seconds > now.as_secs().saturating_add(300)
        || now.as_secs().saturating_sub(timestamp_seconds) > 7 * 24 * 60 * 60
    {
        return fail();
    }
    let keys=match db.query("SELECT k.public_key_base64 FROM novu_rust_sendgrid_webhook_keys k JOIN novu_rust_environments e ON e.id=k.environment_id WHERE k.environment_id=$1 AND e.organization_id=$2 AND k.active LIMIT 10", &[&environment,&organization]).await { Ok(keys)=>keys,Err(_)=>return (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"message":"webhook unavailable"}))) };
    if !keys
        .iter()
        .any(|row| verify(&row.get::<_, String>(0), &signature, timestamp, &raw))
    {
        return fail();
    }
    let Ok(value) = serde_json::from_slice::<Value>(&raw) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"message":"invalid webhook body"})),
        );
    };
    let events = match value {
        Value::Array(events) if events.len() <= 1000 => events,
        Value::Object(_) => vec![value],
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message":"invalid webhook body"})),
            );
        }
    };
    let mut records = Vec::new();
    for event in events {
        let Some(kind) = event.get("event").and_then(Value::as_str) else {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message":"invalid webhook event"})),
            );
        };
        if ![
            "delivered",
            "bounce",
            "dropped",
            "blocked",
            "deferred",
            "processed",
        ]
        .contains(&kind)
        {
            continue;
        }
        let valid = || -> Option<Value> {
            let id = event.get("id")?.as_str()?.trim();
            let event_id = event.get("sg_event_id")?.as_str()?;
            let address = event.get("email")?.as_str()?;
            let timestamp = event.get("timestamp")?.as_i64()?;
            if id.len() != 24
                || !id.bytes().all(|b| b.is_ascii_hexdigit())
                || event_id.is_empty()
                || event_id.len() > 256
                || address.is_empty()
                || address.len() > 320
                || timestamp < 0
            {
                return None;
            }
            // A missing, malformed or unmatched custom argument stays
            // unbound; it must never be treated as a different dispatch.
            let dispatch = event
                .get("swallow_dispatch_id")
                .and_then(Value::as_str)
                .and_then(|raw| Uuid::parse_str(raw).ok().filter(|id| id.to_string() == raw));
            let proof_hash = event
                .get("swallow_dispatch_proof")
                .and_then(Value::as_str)
                .filter(|proof| {
                    proof.len() == 64
                        && proof
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
                .map(|proof| format!("{:x}", Sha256::digest(proof.as_bytes())));
            let bounce_type = if kind == "bounce" {
                event
                    .get("type")
                    .and_then(Value::as_str)
                    .filter(|value| ["bounce", "blocked"].contains(value))
            } else {
                None
            };
            Some(
                json!({"event_id":event_id,"message_id":id,"event_type":kind,"event_timestamp":timestamp,"recipient_hash":format!("{:x}",Sha256::digest(address.as_bytes())),"dispatch_id":dispatch.map(|id| id.to_string()),"proof_hash":proof_hash,"bounce_type":bounce_type}),
            )
        };
        let Some(record) = valid() else {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message":"invalid webhook event"})),
            );
        };
        records.push(record);
    }
    let inserted=match db.query("WITH incoming AS MATERIALIZED (SELECT * FROM jsonb_to_recordset($2::jsonb) AS e(event_id text,message_id text,event_type text,event_timestamp bigint,recipient_hash text,dispatch_id uuid,proof_hash text,bounce_type text)), locked_jobs AS MATERIALIZED (SELECT j.id FROM novu_queue_jobs j WHERE j.environment_id=$1 AND j.kind='experimental_sendgrid_email' AND EXISTS(SELECT 1 FROM incoming e JOIN novu_rust_email_messages m ON m.environment_id=$1 AND m.internal_id=e.message_id WHERE m.email_job_id=j.id) ORDER BY j.id FOR UPDATE) INSERT INTO novu_rust_sendgrid_delivery_events(environment_id,event_id,message_internal_id,event_type,event_timestamp,recipient_hash,dispatch_id,dispatch_proof_verified,bounce_type) SELECT $1,e.event_id,e.message_id,e.event_type,e.event_timestamp,e.recipient_hash,d.id,(d.id IS NOT NULL),e.bounce_type FROM incoming e JOIN novu_rust_email_messages m ON m.environment_id=$1 AND m.internal_id=e.message_id LEFT JOIN novu_rust_sendgrid_dispatch_ids d ON d.id=e.dispatch_id AND d.environment_id=m.environment_id AND d.job_id=m.email_job_id AND d.proof_hash=e.proof_hash AND e.proof_hash IS NOT NULL CROSS JOIN (SELECT count(*) AS n FROM locked_jobs) locks WHERE locks.n >= 0 ON CONFLICT(environment_id,event_id) DO NOTHING RETURNING event_id", &[&environment,&json!(records)]).await { Ok(rows)=>rows.len(),Err(_)=>return (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"message":"webhook unavailable"}))) };
    (StatusCode::OK, Json(json!({"data":[],"recorded":inserted})))
}
