//! Experimental API-key scoped SendGrid webhook verification key management.
use super::{ApiError, authenticate, error_at};
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use der::asn1::ObjectIdentifier;
use ring::{
    agreement::{self, ECDH_P256, EphemeralPrivateKey, UnparsedPublicKey},
    rand::SystemRandom,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::{Client, error::SqlState};
use uuid::Uuid;

fn fail(status: StatusCode, path: &str) -> ApiError {
    let (message, label) = match status {
        StatusCode::BAD_REQUEST => ("Invalid request", "Bad Request"),
        StatusCode::FORBIDDEN => ("Forbidden", "Forbidden"),
        StatusCode::NOT_FOUND => ("Not found", "Not Found"),
        StatusCode::CONFLICT => ("Key conflict or active key limit reached", "Conflict"),
        _ => ("Internal server error", "Internal Server Error"),
    };
    error_at(status, message, label, path)
}
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.as_bytes()[0].is_ascii_alphanumeric()
        && id
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || b"._-".contains(&v))
}
fn valid_public_key(value: &str) -> bool {
    if value.len() > 1024 {
        return false;
    }
    let Ok(der) = STANDARD.decode(value) else {
        return false;
    };
    // Canonical encoding also prevents alternate textual representations of one key.
    if STANDARD.encode(&der) != value {
        return false;
    }
    let Ok(info) = spki::SubjectPublicKeyInfoRef::try_from(der.as_slice()) else {
        return false;
    };
    if info.algorithm.oid != ObjectIdentifier::new_unwrap("1.2.840.10045.2.1")
        || info
            .algorithm
            .parameters
            .and_then(|v| v.decode_as::<ObjectIdentifier>().ok())
            != Some(ObjectIdentifier::new_unwrap("1.2.840.10045.3.1.7"))
    {
        return false;
    }
    let Some(point) = info.subject_public_key.as_bytes() else {
        return false;
    };
    let Ok(ephemeral) = EphemeralPrivateKey::generate(&ECDH_P256, &SystemRandom::new()) else {
        return false;
    };
    agreement::agree_ephemeral(
        ephemeral,
        &UnparsedPublicKey::new(&ECDH_P256, point),
        |_| (),
    )
    .is_ok()
}
async fn identity(
    db: &Client,
    headers: &HeaderMap,
    path: &str,
) -> Result<(String, String), ApiError> {
    let env = authenticate(db, headers, path).await?;
    let key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("ApiKey "))
        .ok_or_else(|| fail(StatusCode::FORBIDDEN, path))?;
    Ok((env, format!("{:x}", Sha256::digest(key.as_bytes()))))
}
fn db_error(error: tokio_postgres::Error, path: &str) -> ApiError {
    if error.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) {
        fail(StatusCode::FORBIDDEN, path)
    } else {
        fail(StatusCode::INTERNAL_SERVER_ERROR, path)
    }
}
pub async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match identity(&db, &headers, &path).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut after: Option<String> = None;
    for (key, value) in form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        if key != "after"
            || after.is_some()
            || value.is_empty()
            || value.len() > 1020
            || value.contains('\0')
        {
            return fail(StatusCode::BAD_REQUEST, &path);
        }
        after = Some(value.into_owned());
    }
    let rows = match db
        .query(
            "SELECT key_id,active FROM public.novu_rust_sendgrid_keys_list($1,$2,$3)",
            &[&hash, &env, &after],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => return db_error(e, &path),
    };
    let more = rows.len() > 100;
    let data: Vec<Value> = rows
        .iter()
        .take(100)
        .map(|r| json!({"identifier":r.get::<_,String>(0),"active":r.get::<_,bool>(1)}))
        .collect();
    let next = if more {
        data.last().and_then(|v| v["identifier"].as_str())
    } else {
        None
    };
    (StatusCode::OK, Json(json!({"data":data,"nextCursor":next})))
}

pub async fn audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match identity(&db, &headers, &path).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    let mut after: Option<Uuid> = None;
    for (name, value) in form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        if name != "after" || after.is_some() {
            return fail(StatusCode::BAD_REQUEST, &path);
        }
        after = match Uuid::parse_str(&value) {
            Ok(value) => Some(value),
            Err(_) => return fail(StatusCode::BAD_REQUEST, &path),
        };
    }
    let rows = match db.query("SELECT id,key_id,action,user_id,recorded_at FROM public.novu_rust_sendgrid_key_audit_list($1,$2,$3)", &[&hash,&env,&after]).await {
        Ok(rows) => rows,
        Err(error) => return db_error(error, &path),
    };
    let more = rows.len() > 100;
    let data: Vec<Value> = rows
        .iter()
        .take(100)
        .map(|row| {
            json!({
                "id":row.get::<_,Uuid>(0),"identifier":row.get::<_,String>(1),
                "action":row.get::<_,String>(2),"userId":row.get::<_,String>(3),
                "recordedAt":row.get::<_,String>(4)
            })
        })
        .collect();
    let next = if more {
        data.last().and_then(|value| value["id"].as_str())
    } else {
        None
    };
    (StatusCode::OK, Json(json!({"data":data,"nextCursor":next})))
}
async fn change(
    db: &Client,
    headers: &HeaderMap,
    path: &str,
    action: &str,
    id: &str,
    public: Option<&str>,
) -> ApiError {
    let (env, hash) = match identity(db, headers, path).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    if !valid_id(id) {
        return fail(StatusCode::BAD_REQUEST, path);
    }
    let result = db
        .query_one(
            "SELECT public.novu_rust_sendgrid_key_change($1,$2,$3,$4,$5,$6)",
            &[&hash, &env, &action, &id, &public, &Uuid::new_v4()],
        )
        .await;
    match result {
        Ok(row) => match row.get::<_, String>(0).as_str() {
            "ok" => (
                if action == "create" {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                Json(json!({"data":{"identifier":id}})),
            ),
            "missing" => fail(StatusCode::NOT_FOUND, path),
            "conflict" | "limit" => fail(StatusCode::CONFLICT, path),
            _ => fail(StatusCode::INTERNAL_SERVER_ERROR, path),
        },
        Err(e) => db_error(e, path),
    }
}
pub async fn create(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiError {
    let path = uri.to_string();
    // Authenticate before parsing or reporting validation results.
    if let Err(e) = identity(&db, &headers, &path).await {
        return e;
    }
    let Ok(Value::Object(obj)) = serde_json::from_slice::<Value>(&body) else {
        return fail(StatusCode::BAD_REQUEST, &path);
    };
    let (Some(id), Some(public)) = (
        obj.get("identifier").and_then(Value::as_str),
        obj.get("publicKey").and_then(Value::as_str),
    ) else {
        return fail(StatusCode::BAD_REQUEST, &path);
    };
    if obj.len() != 2 || !valid_id(id) || !valid_public_key(public) {
        return fail(StatusCode::BAD_REQUEST, &path);
    }
    change(&db, &headers, &path, "create", id, Some(public)).await
}
pub async fn delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Path(id): Path<String>,
) -> ApiError {
    change(&db, &headers, &uri.to_string(), "delete", &id, None).await
}
