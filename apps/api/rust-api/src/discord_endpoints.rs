//! Experimental non-secret Discord destination bindings; not the Novu endpoint contract.
use super::{ApiError, authenticate, error_at};
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;

fn fail(status: StatusCode, msg: &str, path: &str) -> ApiError {
    let label = match status {
        StatusCode::BAD_REQUEST => "Bad Request",
        StatusCode::FORBIDDEN => "Forbidden",
        StatusCode::NOT_FOUND => "Not Found",
        StatusCode::CONFLICT => "Conflict",
        _ => "Internal Server Error",
    };
    error_at(status, msg, label, path)
}
async fn access(
    db: &Client,
    headers: &HeaderMap,
    path: &str,
    permission: &str,
) -> Result<(String, String), ApiError> {
    let env = authenticate(db, headers, path).await?;
    let key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("ApiKey "))
        .ok_or_else(|| fail(StatusCode::FORBIDDEN, "Forbidden", path))?;
    let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
    let allowed = db
        .query_one(
            "SELECT public.novu_rust_lock_discord_endpoint_grant($1,$2,$3)",
            &[&hash, &env, &permission],
        )
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                path,
            )
        })?
        .get::<_, bool>(0);
    if !allowed {
        return Err(fail(StatusCode::FORBIDDEN, "Forbidden", path));
    }
    Ok((env, hash))
}
fn valid_id(v: &str) -> bool {
    !v.is_empty() && v.len() <= 255 && !v.chars().any(char::is_control)
}
fn valid_identifier(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 255
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn destination(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 64
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn context(input: Option<&Value>) -> Option<Vec<String>> {
    match input {
        None | Some(Value::Null) => Some(vec![]),
        Some(value) => super::inbox_context::parse(value).map(|p| p.keys),
    }
}
const DTO: &str = "jsonb_build_object('identifier',e.identifier,'subscriberId',e.subscriber_id,'destinationId',e.destination_id,'contextKeys',to_jsonb(e.context_keys),'revision',e.revision::text)";

pub(super) async fn create(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let input: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return fail(StatusCode::BAD_REQUEST, "Invalid JSON", &path),
    };
    let Some(fields) = input.as_object().filter(|o| {
        o.keys().all(|k| {
            ["identifier", "subscriberId", "destinationId", "context"].contains(&k.as_str())
        })
    }) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    let (Some(id), Some(subscriber), Some(dest), Some(context)) = (
        fields
            .get("identifier")
            .and_then(Value::as_str)
            .filter(|s| valid_identifier(s)),
        fields
            .get("subscriberId")
            .and_then(Value::as_str)
            .filter(|s| valid_id(s)),
        fields
            .get("destinationId")
            .and_then(Value::as_str)
            .filter(|s| destination(s)),
        context(fields.get("context")),
    ) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    let audit = Uuid::new_v4();
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_discord_endpoint_grant($1,$2,'write') AS ok), target AS MATERIALIZED (SELECT s.internal_id FROM auth a JOIN public.novu_rust_subscribers s ON s.environment_id=$2 AND s.subscriber_id=$3 JOIN public.novu_rust_discord_destinations d ON d.environment_id=$2 AND d.destination_id=$4 WHERE a.ok FOR UPDATE OF s), e AS (INSERT INTO public.novu_rust_discord_workflow_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,destination_id,context_keys) SELECT $2,$5,$3,internal_id,$4,$6 FROM target RETURNING *), audit AS (INSERT INTO public.novu_rust_discord_endpoint_audit(id,environment_id,identifier,action,key_hash,user_id) SELECT $7,$2,$5,'create',$1,k.user_id FROM e JOIN public.novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT {DTO} FROM e CROSS JOIN audit"
    );
    match db
        .query_opt(
            &sql,
            &[&hash, &env, &subscriber, &dest, &id, &context, &audit],
        )
        .await
    {
        Ok(Some(row)) => (
            StatusCode::CREATED,
            Json(json!({"data":row.get::<_,Value>(0)})),
        ),
        Ok(None) => fail(
            StatusCode::NOT_FOUND,
            "Subscriber, destination or permission not found",
            &path,
        ),
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
            fail(StatusCode::CONFLICT, "Endpoint already exists", &path)
        }
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION) => {
            fail(
                StatusCode::NOT_FOUND,
                "Subscriber or destination not found",
                &path,
            )
        }
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

pub(super) async fn get(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_discord_endpoint_grant($1,$2,'read') AS ok) SELECT {DTO} FROM auth a JOIN public.novu_rust_discord_workflow_endpoints e ON e.environment_id=$2 AND e.identifier=$3 WHERE a.ok"
    );
    match db.query_opt(&sql, &[&hash, &env, &id]).await {
        Ok(Some(r)) => (StatusCode::OK, Json(json!({"data":r.get::<_,Value>(0)}))),
        Ok(None) => fail(StatusCode::NOT_FOUND, "Endpoint not found", &path),
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

pub(super) async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut after: Option<String> = None;
    for (key, value) in form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        if key != "after" || after.is_some() || !valid_identifier(&value) {
            return fail(StatusCode::BAD_REQUEST, "Invalid cursor", &path);
        }
        after = Some(value.into_owned());
    }
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_discord_endpoint_grant($1,$2,'read') AS ok) SELECT {DTO} FROM auth a JOIN public.novu_rust_discord_workflow_endpoints e ON e.environment_id=$2 WHERE a.ok AND ($3::text IS NULL OR e.identifier COLLATE \"C\">$3 COLLATE \"C\") ORDER BY e.identifier COLLATE \"C\" LIMIT 101"
    );
    match db.query(&sql, &[&hash, &env, &after]).await {
        Ok(rows) => {
            let more = rows.len() > 100;
            let mut data: Vec<Value> = rows.into_iter().take(100).map(|r| r.get(0)).collect();
            let next = if more {
                data.last()
                    .and_then(|v| v["identifier"].as_str())
                    .map(str::to_owned)
            } else {
                None
            };
            (
                StatusCode::OK,
                Json(json!({"data":std::mem::take(&mut data),"next":next})),
            )
        }
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

pub(super) async fn update(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let input: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return fail(StatusCode::BAD_REQUEST, "Invalid JSON", &path),
    };
    let Some(fields) = input.as_object().filter(|o| {
        o.keys()
            .all(|k| ["expectedRevision", "destinationId", "context"].contains(&k.as_str()))
    }) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    let (Some(revision), Some(dest), Some(context)) = (
        fields
            .get("expectedRevision")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok()),
        fields
            .get("destinationId")
            .and_then(Value::as_str)
            .filter(|s| destination(s)),
        context(fields.get("context")),
    ) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    let audit = Uuid::new_v4();
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_discord_endpoint_grant($1,$2,'write') AS ok), e AS (UPDATE public.novu_rust_discord_workflow_endpoints x SET destination_id=$4,context_keys=$5 FROM auth a WHERE a.ok AND x.environment_id=$2 AND x.identifier=$3 AND x.revision=$6 AND EXISTS(SELECT 1 FROM public.novu_rust_discord_destinations d WHERE d.environment_id=$2 AND d.destination_id=$4) RETURNING x.*), audit AS (INSERT INTO public.novu_rust_discord_endpoint_audit(id,environment_id,identifier,action,key_hash,user_id) SELECT $7,$2,$3,'update',$1,k.user_id FROM e JOIN public.novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT {DTO} FROM e CROSS JOIN audit"
    );
    match db
        .query_opt(
            &sql,
            &[&hash, &env, &id, &dest, &context, &revision, &audit],
        )
        .await
    {
        Ok(Some(r)) => (StatusCode::OK, Json(json!({"data":r.get::<_,Value>(0)}))),
        Ok(None) => fail(
            StatusCode::NOT_FOUND,
            "Endpoint, revision, destination or permission not found",
            &path,
        ),
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

pub(super) async fn delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    let revision = if params.len() == 1 && params[0].0 == "revision" {
        Uuid::parse_str(&params[0].1).ok()
    } else {
        None
    };
    let Some(revision) = revision else {
        return fail(StatusCode::BAD_REQUEST, "Revision required", &path);
    };
    let audit = Uuid::new_v4();
    let sql = "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_discord_endpoint_grant($1,$2,'write') AS ok), removed AS (DELETE FROM public.novu_rust_discord_workflow_endpoints e USING auth a WHERE a.ok AND e.environment_id=$2 AND e.identifier=$3 AND e.revision=$4 RETURNING e.identifier), audit AS (INSERT INTO public.novu_rust_discord_endpoint_audit(id,environment_id,identifier,action,key_hash,user_id) SELECT $5,$2,$3,'delete',$1,k.user_id FROM removed JOIN public.novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT 1 FROM audit";
    match db
        .query_opt(sql, &[&hash, &env, &id, &revision, &audit])
        .await
    {
        Ok(Some(_)) => (StatusCode::OK, Json(json!({"data":{"deleted":true}}))),
        Ok(None) => fail(
            StatusCode::NOT_FOUND,
            "Endpoint, revision or permission not found",
            &path,
        ),
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}
