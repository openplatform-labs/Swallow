//! Restricted, non-secret FCM integration metadata management.
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

fn fail(status: StatusCode, message: &str, path: &str) -> ApiError {
    let label = match status {
        StatusCode::BAD_REQUEST => "Bad Request",
        StatusCode::FORBIDDEN => "Forbidden",
        StatusCode::NOT_FOUND => "Not Found",
        StatusCode::CONFLICT => "Conflict",
        _ => "Internal Server Error",
    };
    error_at(status, message, label, path)
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
    let allowed: bool = db
        .query_one(
            "SELECT public.novu_rust_lock_fcm_integration_grant($1,$2,$3)",
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
        .get(0);
    if !allowed {
        return Err(fail(StatusCode::FORBIDDEN, "Forbidden", path));
    }
    Ok((env, hash))
}
fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}
const DTO: &str = "jsonb_build_object('internalId',i.internal_id,'identifier',i.identifier,'active',i.active,'createdAt',to_char(i.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'))";

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
    let mut after = None;
    for (name, value) in form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        if name != "after" || after.is_some() || value.is_empty() || value.len() > 128 {
            return fail(StatusCode::BAD_REQUEST, "Invalid pagination", &path);
        }
        after = Some(value.into_owned());
    }
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_fcm_integration_grant($1,$2,'read') AS ok), rows AS MATERIALIZED (SELECT i.* FROM public.novu_rust_fcm_integrations i CROSS JOIN auth a WHERE a.ok AND i.environment_id=$2 AND ($3::text IS NULL OR i.internal_id>$3) ORDER BY i.internal_id LIMIT 101) SELECT a.ok, COALESCE((SELECT jsonb_agg({DTO} ORDER BY i.internal_id) FROM (SELECT * FROM rows ORDER BY internal_id LIMIT 100) i),'[]'::jsonb),(SELECT count(*)>100 FROM rows) FROM auth a"
    );
    match db.query_one(&sql, &[&hash, &env, &after]).await {
        Ok(row) if row.get::<_, bool>(0) => {
            let data: Value = row.get(1);
            let more: bool = row.get(2);
            let next = if more {
                data.as_array()
                    .and_then(|items| items.last())
                    .and_then(|v| v.get("internalId"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            } else {
                None
            };
            (
                StatusCode::OK,
                Json(json!({"data":data,"hasMore":more,"next":next})),
            )
        }
        Ok(_) => fail(StatusCode::FORBIDDEN, "Permission revoked", &path),
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
    OriginalUri(uri): OriginalUri,
    Path(identifier): Path<String>,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_fcm_integration_grant($1,$2,'read') AS ok) SELECT a.ok,(SELECT {DTO} FROM public.novu_rust_fcm_integrations i WHERE a.ok AND i.environment_id=$2 AND i.identifier=$3) FROM auth a"
    );
    match db.query_one(&sql, &[&hash, &env, &identifier]).await {
        Ok(row) if !row.get::<_, bool>(0) => {
            fail(StatusCode::FORBIDDEN, "Permission revoked", &path)
        }
        Ok(row) => match row.get::<_, Option<Value>>(1) {
            Some(value) => (StatusCode::OK, Json(json!({"data":value}))),
            None => fail(StatusCode::NOT_FOUND, "Integration not found", &path),
        },
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}
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
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return fail(StatusCode::BAD_REQUEST, "Invalid JSON", &path),
    };
    let Some(obj) = value.as_object() else {
        return fail(StatusCode::BAD_REQUEST, "Invalid integration", &path);
    };
    if obj.keys().any(|k| k != "identifier" && k != "active") {
        return fail(StatusCode::BAD_REQUEST, "Unsupported field", &path);
    }
    let Some(identifier) = obj
        .get("identifier")
        .and_then(Value::as_str)
        .filter(|s| valid_identifier(s))
    else {
        return fail(StatusCode::BAD_REQUEST, "Invalid identifier", &path);
    };
    let active = match obj.get("active") {
        None => false,
        Some(Value::Bool(value)) => *value,
        _ => return fail(StatusCode::BAD_REQUEST, "Invalid active", &path),
    };
    let id = Uuid::new_v4().simple().to_string()[..24].to_owned();
    let audit = Uuid::new_v4();
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_fcm_integration_grant($1,$2,'write') AS ok), written AS (INSERT INTO public.novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) SELECT $2,$3,$4,$5 FROM auth WHERE ok RETURNING *), audited AS (INSERT INTO public.novu_rust_fcm_integration_audit(id,environment_id,integration_internal_id,identifier,action,new_active,key_hash,user_id) SELECT $6,$2,w.internal_id,w.identifier,'create',w.active,$1,k.user_id FROM written w JOIN public.novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT a.ok,(SELECT {DTO} FROM written i),(SELECT count(*) FROM audited) FROM auth a"
    );
    match db
        .query_one(&sql, &[&hash, &env, &id, &identifier, &active, &audit])
        .await
    {
        Ok(row) if !row.get::<_, bool>(0) => {
            fail(StatusCode::FORBIDDEN, "Permission revoked", &path)
        }
        Ok(row) => (
            StatusCode::CREATED,
            Json(json!({"data":row.get::<_,Value>(1)})),
        ),
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
            fail(StatusCode::CONFLICT, "Integration already exists", &path)
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
    OriginalUri(uri): OriginalUri,
    Path(identifier): Path<String>,
    bytes: Bytes,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return fail(StatusCode::BAD_REQUEST, "Invalid JSON", &path),
    };
    let Some(obj) = value.as_object() else {
        return fail(StatusCode::BAD_REQUEST, "Invalid integration", &path);
    };
    if obj.len() != 1 {
        return fail(StatusCode::BAD_REQUEST, "Only active can be updated", &path);
    };
    let Some(active) = obj.get("active").and_then(Value::as_bool) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid active", &path);
    };
    let audit = Uuid::new_v4();
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_fcm_integration_grant($1,$2,'write') AS ok), target AS MATERIALIZED (SELECT i.* FROM public.novu_rust_fcm_integrations i CROSS JOIN auth a WHERE a.ok AND i.environment_id=$2 AND i.identifier=$3 FOR UPDATE OF i), written AS (UPDATE public.novu_rust_fcm_integrations i SET active=$4 FROM target t WHERE i.environment_id=t.environment_id AND i.internal_id=t.internal_id RETURNING i.*), audited AS (INSERT INTO public.novu_rust_fcm_integration_audit(id,environment_id,integration_internal_id,identifier,action,old_active,new_active,key_hash,user_id) SELECT $5,$2,w.internal_id,w.identifier,'update',t.active,w.active,$1,k.user_id FROM written w JOIN target t ON t.internal_id=w.internal_id JOIN public.novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT a.ok,(SELECT {DTO} FROM written i),(SELECT count(*) FROM audited) FROM auth a"
    );
    match db
        .query_one(&sql, &[&hash, &env, &identifier, &active, &audit])
        .await
    {
        Ok(row) if !row.get::<_, bool>(0) => {
            fail(StatusCode::FORBIDDEN, "Permission revoked", &path)
        }
        Ok(row) => match row.get::<_, Option<Value>>(1) {
            Some(value) => (StatusCode::OK, Json(json!({"data":value}))),
            None => fail(StatusCode::NOT_FOUND, "Integration not found", &path),
        },
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}
