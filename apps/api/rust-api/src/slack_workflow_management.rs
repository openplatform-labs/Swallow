//! Experimental, separately authorized Slack workflow step management.
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
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("ApiKey "))
        .ok_or_else(|| fail(StatusCode::FORBIDDEN, "Forbidden", path))?;
    let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
    let allowed: bool = db
        .query_one(
            "SELECT public.novu_rust_lock_slack_workflow_grant($1,$2,$3)",
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
pub(super) async fn get(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(workflow): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    match db.query_opt("WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_workflow_grant($1,$2,'read') AS ok) SELECT w.chat_content,w.chat_step_active FROM public.novu_rust_workflows w CROSS JOIN auth a WHERE a.ok AND w.environment_id=$2 AND w.identifier=$3", &[&hash,&env,&workflow]).await {
      Ok(Some(row))=>(StatusCode::OK,Json(json!({"data":{"workflowIdentifier":workflow,"content":row.get::<_,Option<String>>(0),"active":row.get::<_,bool>(1)}}))),
      Ok(None)=>fail(StatusCode::NOT_FOUND,"Workflow not found or permission revoked",&path),
      Err(_)=>fail(StatusCode::INTERNAL_SERVER_ERROR,"Internal server error",&path),
    }
}
pub(super) async fn put(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(workflow): Path<String>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let input: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return fail(StatusCode::BAD_REQUEST, "Invalid JSON", &path),
    };
    let Some(object) = input.as_object().filter(|v| {
        (v.len() == 2 || (v.len() == 3 && v.contains_key("expected")))
            && v.contains_key("content")
            && v.contains_key("active")
    }) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid chat step", &path);
    };
    let content: Option<&str> = match &object["content"] {
        Value::Null => None,
        Value::String(s) if s.len() <= 40_000 && !s.contains('\0') => Some(s),
        _ => return fail(StatusCode::BAD_REQUEST, "Invalid chat content", &path),
    };
    let Some(active) = object["active"].as_bool() else {
        return fail(StatusCode::BAD_REQUEST, "Invalid chat step", &path);
    };
    let (conditional, expected_content, expected_active) = match object.get("expected") {
        None => (false, None, None),
        Some(Value::Object(e))
            if e.len() == 2 && e.contains_key("content") && e.contains_key("active") =>
        {
            let content = match &e["content"] {
                Value::Null => None,
                Value::String(s) if s.len() <= 40000 && !s.contains('\0') => Some(s.as_str()),
                _ => return fail(StatusCode::BAD_REQUEST, "Invalid expected step", &path),
            };
            match e["active"].as_bool() {
                Some(a) => (true, content, Some(a)),
                None => return fail(StatusCode::BAD_REQUEST, "Invalid expected step", &path),
            }
        }
        _ => return fail(StatusCode::BAD_REQUEST, "Invalid expected step", &path),
    };
    let id = Uuid::new_v4();
    let sql = "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_slack_workflow_grant($1,$2,'write') AS ok), target AS MATERIALIZED (SELECT w.environment_id,w.identifier,w.chat_step_active AS previous_active,octet_length(w.chat_content) AS previous_bytes,k.user_id FROM public.novu_rust_workflows w JOIN public.novu_rust_api_keys k ON k.key_hash=$1 CROSS JOIN auth a WHERE a.ok AND w.environment_id=$2 AND w.identifier=$3 AND (NOT $7::boolean OR (w.chat_content IS NOT DISTINCT FROM $8::text AND w.chat_step_active IS NOT DISTINCT FROM $9::boolean)) FOR UPDATE OF w), updated AS (UPDATE public.novu_rust_workflows w SET chat_content=$4,chat_step_active=$5 FROM target t WHERE w.environment_id=t.environment_id AND w.identifier=t.identifier RETURNING w.chat_content,w.chat_step_active,t.previous_active,t.previous_bytes,t.user_id), audited AS (INSERT INTO public.novu_rust_slack_workflow_audit(id,environment_id,workflow_identifier,api_key_hash,user_id,previous_active,new_active,previous_content_bytes,new_content_bytes) SELECT $6,$2,$3,$1,u.user_id,u.previous_active,u.chat_step_active,u.previous_bytes,octet_length(u.chat_content) FROM updated u RETURNING 1) SELECT u.chat_content,u.chat_step_active FROM updated u CROSS JOIN audited";
    match db
        .query_opt(
            sql,
            &[
                &hash,
                &env,
                &workflow,
                &content,
                &active,
                &id,
                &conditional,
                &expected_content,
                &expected_active,
            ],
        )
        .await
    {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(
                json!({"data":{"workflowIdentifier":workflow,"content":row.get::<_,Option<String>>(0),"active":row.get::<_,bool>(1)}}),
            ),
        ),
        Ok(None) if conditional => fail(
            StatusCode::CONFLICT,
            "Workflow changed, missing or permission revoked",
            &path,
        ),
        Ok(None) => fail(
            StatusCode::NOT_FOUND,
            "Workflow not found or permission revoked",
            &path,
        ),
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}
