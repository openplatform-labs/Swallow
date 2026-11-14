//! Restricted Slack channel/user endpoint management; other endpoint types are unsupported.
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

fn failure(status: StatusCode, message: &str, path: &str) -> ApiError {
    let label = match status {
        StatusCode::FORBIDDEN => "Forbidden",
        StatusCode::NOT_FOUND => "Not Found",
        StatusCode::CONFLICT => "Conflict",
        StatusCode::NOT_IMPLEMENTED => "Not Implemented",
        StatusCode::BAD_REQUEST => "Bad Request",
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
        .ok_or_else(|| failure(StatusCode::FORBIDDEN, "Forbidden", path))?;
    let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
    let allowed = db
        .query_one(
            "SELECT public.novu_rust_lock_slack_endpoint_grant($1,$2,$3)",
            &[&hash, &env, &permission],
        )
        .await
        .map_err(|_| {
            failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                path,
            )
        })?
        .get::<_, bool>(0);
    if !allowed {
        return Err(failure(StatusCode::FORBIDDEN, "Forbidden", path));
    }
    Ok((env, hash))
}
const DTO: &str = "jsonb_build_object('identifier',e.identifier,'channel','chat','providerId','slack','integrationIdentifier',e.integration_identifier,'connectionIdentifier',e.connection_identifier,'subscriberId',e.subscriber_id,'contextKeys',to_jsonb(e.context_keys),'type',e.endpoint_type,'endpoint',e.endpoint,'createdAt',to_char(e.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),'updatedAt',to_char(e.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'))";
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
        Err(_) => return failure(StatusCode::BAD_REQUEST, "Invalid JSON", &path),
    };
    let Some(fields) = input.as_object() else {
        return failure(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    if fields.keys().any(|k| {
        ![
            "identifier",
            "subscriberId",
            "integrationIdentifier",
            "connectionIdentifier",
            "type",
            "endpoint",
        ]
        .contains(&k.as_str())
    }) {
        return failure(
            StatusCode::NOT_IMPLEMENTED,
            "Endpoint option not implemented",
            &path,
        );
    }
    let required = |key: &str| {
        fields
            .get(key)
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty() && v.len() <= 255)
    };
    let (Some(subscriber), Some(integration), Some(kind)) = (
        required("subscriberId"),
        required("integrationIdentifier"),
        required("type"),
    ) else {
        return failure(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    let target = match kind {
        "slack_channel" => "channelId",
        "slack_user" => "userId",
        _ => {
            return failure(
                StatusCode::NOT_IMPLEMENTED,
                "Endpoint type not implemented",
                &path,
            );
        }
    };
    let Some(endpoint) = fields
        .get("endpoint")
        .and_then(Value::as_object)
        .filter(|o| {
            o.len() == 1
                && o.get(target)
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty() && s.len() <= 255)
        })
    else {
        return failure(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    let identifier = match fields.get("identifier") {
        None => format!("sw-{}", Uuid::new_v4().simple()),
        Some(Value::String(s)) if !s.is_empty() && s.len() <= 255 => s.clone(),
        _ => {
            return failure(
                StatusCode::BAD_REQUEST,
                "Invalid endpoint identifier",
                &path,
            );
        }
    };
    let connection = match fields.get("connectionIdentifier") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if !s.is_empty() && s.len() <= 255 => Some(s.as_str()),
        _ => {
            return failure(
                StatusCode::BAD_REQUEST,
                "Invalid connection identifier",
                &path,
            );
        }
    };
    let endpoint = Value::Object(endpoint.clone());
    let audit = Uuid::new_v4();
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_endpoint_grant($1,$2,'write') AS ok), target AS MATERIALIZED (SELECT s.internal_id FROM auth a JOIN novu_rust_subscribers s ON s.environment_id=$2 AND s.subscriber_id=$3 JOIN novu_rust_slack_endpoint_integrations i ON i.environment_id=$2 AND i.identifier=$4 WHERE a.ok AND ($5::text IS NULL OR EXISTS(SELECT 1 FROM novu_rust_slack_endpoint_connections c WHERE c.environment_id=$2 AND c.integration_identifier=$4 AND c.identifier=$5)) FOR UPDATE OF s), e AS (INSERT INTO novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,connection_identifier,endpoint_type,endpoint) SELECT $2,$6,$3,internal_id,$4,$5,$7,$8 FROM target RETURNING *), audit AS (INSERT INTO public.novu_rust_slack_endpoint_audit(id,environment_id,identifier,action,key_hash,user_id) SELECT $9,$2,$6,'create',$1,k.user_id FROM e JOIN novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT {DTO} FROM e CROSS JOIN audit"
    );
    match db
        .query_opt(
            &sql,
            &[
                &hash,
                &env,
                &subscriber,
                &integration,
                &connection,
                &identifier,
                &kind,
                &endpoint,
                &audit,
            ],
        )
        .await
    {
        Ok(Some(row)) => (
            StatusCode::CREATED,
            Json(json!({"data":row.get::<_,Value>(0)})),
        ),
        Ok(None) => failure(
            StatusCode::NOT_FOUND,
            "Subscriber, integration, connection or permission not found",
            &path,
        ),
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
            failure(StatusCode::CONFLICT, "Endpoint already exists", &path)
        }
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION) => {
            failure(
                StatusCode::NOT_FOUND,
                "Subscriber, integration or connection not found",
                &path,
            )
        }
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}
pub(super) async fn get(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_endpoint_grant($1,$2,'read') AS ok) SELECT {DTO} FROM auth a JOIN novu_rust_slack_channel_endpoints e ON e.environment_id=$2 AND e.identifier=$3 WHERE a.ok"
    );
    match db.query_opt(&sql, &[&hash, &env, &identifier]).await {
        Ok(Some(row)) => (StatusCode::OK, Json(json!({"data":row.get::<_,Value>(0)}))),
        Ok(None) => failure(StatusCode::NOT_FOUND, "Endpoint not found", &path),
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}
pub(super) async fn update(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
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
        Err(_) => return failure(StatusCode::BAD_REQUEST, "Invalid JSON", &path),
    };
    let Some(fields) = input.as_object().filter(|v| v.len() == 1) else {
        return failure(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    let Some(endpoint) = fields
        .get("endpoint")
        .and_then(Value::as_object)
        .filter(|o| {
            o.len() == 1
                && ["channelId", "userId"].iter().any(|k| {
                    o.get(*k)
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty() && s.len() <= 255)
                })
        })
    else {
        return failure(StatusCode::BAD_REQUEST, "Invalid endpoint", &path);
    };
    let endpoint = Value::Object(endpoint.clone());
    let audit = Uuid::new_v4();
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_endpoint_grant($1,$2,'write') AS ok), e AS (UPDATE novu_rust_slack_channel_endpoints x SET endpoint=$4,updated_at=clock_timestamp() FROM auth a WHERE a.ok AND x.environment_id=$2 AND x.identifier=$3 RETURNING x.*), audit AS (INSERT INTO public.novu_rust_slack_endpoint_audit(id,environment_id,identifier,action,key_hash,user_id) SELECT $5,$2,$3,'update',$1,k.user_id FROM e JOIN novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT {DTO} FROM e CROSS JOIN audit"
    );
    match db
        .query_opt(&sql, &[&hash, &env, &identifier, &endpoint, &audit])
        .await
    {
        Ok(Some(row)) => (StatusCode::OK, Json(json!({"data":row.get::<_,Value>(0)}))),
        Ok(None) => failure(
            StatusCode::NOT_FOUND,
            "Endpoint not found or permission revoked",
            &path,
        ),
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::CHECK_VIOLATION) => failure(
            StatusCode::BAD_REQUEST,
            "Endpoint does not match type",
            &path,
        ),
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

pub(super) async fn delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(identifier): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> StatusCode {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e.0,
    };
    let audit = Uuid::new_v4();
    let sql = "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_endpoint_grant($1,$2,'write') AS ok), removed AS (DELETE FROM novu_rust_slack_channel_endpoints e USING auth a WHERE a.ok AND e.environment_id=$2 AND e.identifier=$3 RETURNING e.identifier), audit AS (INSERT INTO public.novu_rust_slack_endpoint_audit(id,environment_id,identifier,action,key_hash,user_id) SELECT $4,$2,$3,'delete',$1,k.user_id FROM removed JOIN novu_rust_api_keys k ON k.key_hash=$1 RETURNING 1) SELECT 1 FROM audit";
    match db.query_opt(sql, &[&hash, &env, &identifier, &audit]).await {
        Ok(Some(_)) => StatusCode::NO_CONTENT,
        Ok(None) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
