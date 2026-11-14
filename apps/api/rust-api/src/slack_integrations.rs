//! Environment-scoped non-secret Slack integration metadata; not Novu's integrations API.
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
use tokio_postgres::{Client, error::SqlState};
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
fn valid(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
}
// Existing database rows may have identifiers outside the creation subset.
fn valid_cursor(s: &str) -> bool {
    !s.is_empty() && s.len() <= 1020 && s.chars().count() <= 255 && !s.contains('\0')
}
async fn access(
    db: &Client,
    headers: &HeaderMap,
    path: &str,
    perm: &str,
) -> Result<(String, String), ApiError> {
    let env = authenticate(db, headers, path).await?;
    let key = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("ApiKey "))
        .ok_or_else(|| fail(StatusCode::FORBIDDEN, "Forbidden", path))?;
    let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
    let allowed: bool = db
        .query_one(
            "SELECT public.novu_rust_lock_slack_integration_grant($1,$2,$3)",
            &[&hash, &env, &perm],
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
fn identifier(body: &Bytes) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let id = obj.get("identifier")?.as_str()?;
    valid(id).then(|| id.to_owned())
}
async fn create(
    db: &Client,
    env: &str,
    hash: &str,
    integration: &str,
    connection: Option<&str>,
    path: &str,
) -> ApiError {
    if !valid(integration) || connection.is_some_and(|id| !valid(id)) {
        return fail(StatusCode::BAD_REQUEST, "Invalid identifier", path);
    }
    let audit = Uuid::new_v4();
    let write = if connection.is_some() {
        "INSERT INTO public.novu_rust_slack_endpoint_connections(environment_id,integration_identifier,identifier) SELECT $2,$3,$4 FROM auth WHERE ok ON CONFLICT DO NOTHING RETURNING 1"
    } else {
        "INSERT INTO public.novu_rust_slack_endpoint_integrations(environment_id,identifier) SELECT $2,$3 FROM auth WHERE ok ON CONFLICT DO NOTHING RETURNING 1"
    };
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_integration_grant($1,$2,'write') AS ok), written AS ({write}), audited AS (INSERT INTO public.novu_rust_slack_integration_audit(id,environment_id,integration_identifier,connection_identifier,action,key_hash,user_id) SELECT $5,$2,$3,$4,'create',$1,k.user_id FROM written w JOIN public.novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT a.ok,(SELECT count(*) FROM written),(SELECT count(*) FROM audited) FROM auth a"
    );
    match db
        .query_one(&sql, &[&hash, &env, &integration, &connection, &audit])
        .await
    {
        Ok(row) if !row.get::<_, bool>(0) => {
            fail(StatusCode::FORBIDDEN, "Permission revoked", path)
        }
        Ok(row) if row.get::<_, i64>(1) == 0 => fail(StatusCode::CONFLICT, "Already exists", path),
        Ok(_) => (
            StatusCode::CREATED,
            Json(
                json!({"data":{"integrationIdentifier":integration,"connectionIdentifier":connection}}),
            ),
        ),
        Err(e) if e.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) => {
            fail(StatusCode::NOT_FOUND, "Integration not found", path)
        }
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            path,
        ),
    }
}
async fn remove(
    db: &Client,
    env: &str,
    hash: &str,
    integration: &str,
    connection: Option<&str>,
    path: &str,
) -> ApiError {
    if !valid(integration) || connection.is_some_and(|id| !valid(id)) {
        return fail(StatusCode::BAD_REQUEST, "Invalid identifier", path);
    }
    let audit = Uuid::new_v4();
    let remove = if connection.is_some() {
        "DELETE FROM public.novu_rust_slack_endpoint_connections t USING auth a WHERE a.ok AND t.environment_id=$2 AND t.integration_identifier=$3 AND t.identifier=$4 RETURNING 1"
    } else {
        "DELETE FROM public.novu_rust_slack_endpoint_integrations t USING auth a WHERE a.ok AND t.environment_id=$2 AND t.identifier=$3 RETURNING 1"
    };
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_integration_grant($1,$2,'write') AS ok), removed AS ({remove}), audited AS (INSERT INTO public.novu_rust_slack_integration_audit(id,environment_id,integration_identifier,connection_identifier,action,key_hash,user_id) SELECT $5,$2,$3,$4,'delete',$1,k.user_id FROM removed r JOIN public.novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT a.ok,(SELECT count(*) FROM removed),(SELECT count(*) FROM audited) FROM auth a"
    );
    match db
        .query_one(&sql, &[&hash, &env, &integration, &connection, &audit])
        .await
    {
        Ok(row) if !row.get::<_, bool>(0) => {
            fail(StatusCode::FORBIDDEN, "Permission revoked", path)
        }
        Ok(row) if row.get::<_, i64>(1) == 0 => fail(StatusCode::NOT_FOUND, "Not found", path),
        Ok(_) => (
            StatusCode::OK,
            Json(
                json!({"data":{"integrationIdentifier":integration,"connectionIdentifier":connection}}),
            ),
        ),
        Err(e) if e.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) => {
            fail(StatusCode::CONFLICT, "Metadata is in use", path)
        }
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            path,
        ),
    }
}
async fn list(
    db: &Client,
    env: &str,
    hash: &str,
    integration: Option<&str>,
    uri: &axum::http::Uri,
) -> ApiError {
    let path = uri.to_string();
    if integration.is_some_and(|id| !valid(id)) {
        return fail(StatusCode::BAD_REQUEST, "Invalid identifier", &path);
    }
    let mut after = None;
    for (key, value) in form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        if key != "after" || after.is_some() || !valid_cursor(&value) {
            return fail(StatusCode::BAD_REQUEST, "Invalid pagination", &path);
        }
        after = Some(value.into_owned());
    }
    let source = if integration.is_some() {
        "SELECT t.identifier FROM public.novu_rust_slack_endpoint_connections t,auth a WHERE a.ok AND t.environment_id=$2 AND t.integration_identifier=$3 AND ($4::text IS NULL OR t.identifier COLLATE \"C\">$4 COLLATE \"C\") ORDER BY t.identifier COLLATE \"C\" LIMIT 101"
    } else {
        "SELECT t.identifier FROM public.novu_rust_slack_endpoint_integrations t,auth a WHERE a.ok AND t.environment_id=$2 AND $3::text IS NULL AND ($4::text IS NULL OR t.identifier COLLATE \"C\">$4 COLLATE \"C\") ORDER BY t.identifier COLLATE \"C\" LIMIT 101"
    };
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_integration_grant($1,$2,'read') AS ok), items AS MATERIALIZED ({source}) SELECT a.ok,COALESCE((SELECT jsonb_agg(identifier ORDER BY identifier COLLATE \"C\") FROM (SELECT identifier FROM items ORDER BY identifier COLLATE \"C\" LIMIT 100) q),'[]'::jsonb),(SELECT count(*)>100 FROM items),CASE WHEN $3::text IS NULL THEN true ELSE EXISTS(SELECT 1 FROM public.novu_rust_slack_endpoint_integrations WHERE environment_id=$2 AND identifier=$3) END FROM auth a"
    );
    match db
        .query_one(&sql, &[&hash, &env, &integration, &after])
        .await
    {
        Ok(row) if !row.get::<_, bool>(0) => {
            fail(StatusCode::FORBIDDEN, "Permission revoked", &path)
        }
        Ok(row) if !row.get::<_, bool>(3) => {
            fail(StatusCode::NOT_FOUND, "Integration not found", &path)
        }
        Ok(row) => {
            let data: Value = row.get(1);
            let more: bool = row.get(2);
            let next = if more {
                data.as_array()
                    .and_then(|a| a.last())
                    .and_then(Value::as_str)
            } else {
                None
            };
            (
                StatusCode::OK,
                Json(json!({"data":data,"hasMore":more,"next":next})),
            )
        }
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

async fn get_one(
    db: &Client,
    env: &str,
    hash: &str,
    integration: &str,
    connection: Option<&str>,
    path: &str,
) -> ApiError {
    if !valid(integration) || connection.is_some_and(|v| !valid(v)) {
        return fail(StatusCode::BAD_REQUEST, "Invalid identifier", path);
    }
    let exists = if connection.is_some() {
        "EXISTS(SELECT 1 FROM public.novu_rust_slack_endpoint_connections WHERE environment_id=$2 AND integration_identifier=$3 AND identifier=$4)"
    } else {
        "EXISTS(SELECT 1 FROM public.novu_rust_slack_endpoint_integrations WHERE environment_id=$2 AND identifier=$3 AND $4::text IS NULL)"
    };
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_integration_grant($1,$2,'read') AS ok) SELECT a.ok, CASE WHEN a.ok THEN {exists} ELSE false END FROM auth a"
    );
    match db
        .query_one(&sql, &[&hash, &env, &integration, &connection])
        .await
    {
        Ok(row) if !row.get::<_, bool>(0) => {
            fail(StatusCode::FORBIDDEN, "Permission revoked", path)
        }
        Ok(row) if !row.get::<_, bool>(1) => fail(StatusCode::NOT_FOUND, "Not found", path),
        Ok(_) => (
            StatusCode::OK,
            Json(
                json!({"data":{"integrationIdentifier":integration,"connectionIdentifier":connection}}),
            ),
        ),
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            path,
        ),
    }
}

pub(super) async fn get_integration(
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
    get_one(&db, &env, &hash, &id, None, &path).await
}
pub(super) async fn get_connection(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((integration, id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    get_one(&db, &env, &hash, &integration, Some(&id), &path).await
}
pub(super) async fn create_integration(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let Some(id) = identifier(&body) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid identifier", &path);
    };
    create(&db, &env, &hash, &id, None, &path).await
}
pub(super) async fn delete_integration(
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
    remove(&db, &env, &hash, &id, None, &path).await
}
pub(super) async fn list_integrations(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    list(&db, &env, &hash, None, &uri).await
}
pub(super) async fn create_connection(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(integration): Path<String>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let Some(id) = identifier(&body) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid identifier", &path);
    };
    create(&db, &env, &hash, &integration, Some(&id), &path).await
}
pub(super) async fn delete_connection(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((integration, id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    remove(&db, &env, &hash, &integration, Some(&id), &path).await
}
pub(super) async fn list_connections(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(integration): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    list(&db, &env, &hash, Some(&integration), &uri).await
}
