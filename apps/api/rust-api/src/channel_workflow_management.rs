//! Separately authorized experimental static channel step management.
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
fn content_value(value: &Value) -> Result<Option<&str>, ()> {
    match value {
        Value::Null => Ok(None),
        Value::String(s) if s.len() <= 40_000 && !s.contains('\0') => Ok(Some(s)),
        _ => Err(()),
    }
}

// All SQL fragments come from this closed list, never from request input.
fn columns(channel: &str) -> Option<(&'static str, &'static str, &'static str, &'static str)> {
    match channel {
        "email" => Some((
            "w.email_subject",
            "w.email_html",
            "w.email_step_active",
            "email_subject=$4,email_html=$5,email_step_active=$6",
        )),
        "sms" => Some((
            "NULL::text",
            "w.sms_content",
            "w.sms_step_active",
            "sms_content=$5,sms_step_active=$6",
        )),
        "push" => Some((
            "w.push_title",
            "w.push_content",
            "w.push_step_active",
            "push_title=$4,push_content=$5,push_step_active=$6",
        )),
        _ => None,
    }
}

async fn access(
    db: &Client,
    headers: &HeaderMap,
    path: &str,
    channel: &str,
    permission: &str,
) -> Result<(String, String), ApiError> {
    let env = authenticate(db, headers, path).await?;
    if columns(channel).is_none() {
        return Err(fail(StatusCode::NOT_FOUND, "Channel not found", path));
    }
    let key = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("ApiKey "))
        .ok_or_else(|| fail(StatusCode::FORBIDDEN, "Forbidden", path))?;
    let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
    let allowed: bool = db
        .query_one(
            "SELECT public.novu_rust_lock_channel_workflow_grant($1,$2,$3,$4)",
            &[&hash, &env, &channel, &permission],
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
    Path((workflow, channel)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, &channel, "read").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (title, content, active, _) = columns(&channel).expect("validated channel");
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_channel_workflow_grant($1,$2,$4,'read') AS ok) SELECT {title},{content},{active} FROM public.novu_rust_workflows w CROSS JOIN auth a WHERE a.ok AND w.environment_id=$2 AND w.identifier=$3"
    );
    match db
        .query_opt(&sql, &[&hash, &env, &workflow, &channel])
        .await
    {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(
                json!({"data":{"workflowIdentifier":workflow,"channel":channel,"title":row.get::<_,Option<String>>(0),"content":row.get::<_,Option<String>>(1),"active":row.get::<_,bool>(2)}}),
            ),
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

pub(super) async fn put(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((workflow, channel)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiError {
    let path = uri.to_string();
    let (env, hash) = match access(&db, &headers, &path, &channel, "write").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let input: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return fail(StatusCode::BAD_REQUEST, "Invalid JSON", &path),
    };
    let Some(obj) = input.as_object().filter(|o| {
        (o.len() == 3 || (o.len() == 4 && o.contains_key("expected")))
            && o.contains_key("title")
            && o.contains_key("content")
            && o.contains_key("active")
    }) else {
        return fail(StatusCode::BAD_REQUEST, "Invalid channel step", &path);
    };
    let (title, content) = match (content_value(&obj["title"]), content_value(&obj["content"])) {
        (Ok(t), Ok(c)) => (t, c),
        _ => return fail(StatusCode::BAD_REQUEST, "Invalid channel content", &path),
    };
    if (channel == "sms" && title.is_some())
        || (channel != "sms" && title.is_some() != content.is_some())
    {
        return fail(StatusCode::BAD_REQUEST, "Invalid channel content", &path);
    }
    let Some(active) = obj["active"].as_bool() else {
        return fail(StatusCode::BAD_REQUEST, "Invalid channel step", &path);
    };
    let (conditional, expected_title, expected_content, expected_active) = match obj.get("expected")
    {
        None => (false, None, None, None),
        Some(Value::Object(e))
            if e.len() == 3
                && e.contains_key("title")
                && e.contains_key("content")
                && e.contains_key("active") =>
        {
            let (t, c) = match (content_value(&e["title"]), content_value(&e["content"])) {
                (Ok(t), Ok(c)) => (t, c),
                _ => return fail(StatusCode::BAD_REQUEST, "Invalid expected step", &path),
            };
            if (channel == "sms" && t.is_some()) || (channel != "sms" && t.is_some() != c.is_some())
            {
                return fail(StatusCode::BAD_REQUEST, "Invalid expected step", &path);
            }
            match e["active"].as_bool() {
                Some(a) => (true, t, c, Some(a)),
                None => return fail(StatusCode::BAD_REQUEST, "Invalid expected step", &path),
            }
        }
        _ => return fail(StatusCode::BAD_REQUEST, "Invalid expected step", &path),
    };
    let (title_col, content_col, active_col, setter) =
        columns(&channel).expect("validated channel");
    let sql = format!(
        "WITH auth AS MATERIALIZED (SELECT public.novu_rust_lock_channel_workflow_grant($1,$2,$8,'write') AS ok), target AS MATERIALIZED (SELECT w.environment_id,w.identifier,{active_col} AS previous_active,octet_length({title_col}) AS previous_title_bytes,octet_length({content_col}) AS previous_content_bytes,k.user_id FROM public.novu_rust_workflows w JOIN public.novu_rust_api_keys k ON k.key_hash=$1 CROSS JOIN auth a WHERE a.ok AND w.environment_id=$2 AND w.identifier=$3 AND ($8<>'sms' OR $4::text IS NULL) AND (NOT $9::boolean OR ({title_col} IS NOT DISTINCT FROM $10::text AND {content_col} IS NOT DISTINCT FROM $11::text AND {active_col} IS NOT DISTINCT FROM $12::boolean)) FOR UPDATE OF w), updated AS (UPDATE public.novu_rust_workflows w SET {setter} FROM target t WHERE w.environment_id=t.environment_id AND w.identifier=t.identifier RETURNING {title_col} AS title,{content_col} AS content,{active_col} AS active,t.previous_active,t.previous_title_bytes,t.previous_content_bytes,t.user_id), audited AS (INSERT INTO public.novu_rust_channel_workflow_audit(id,environment_id,workflow_identifier,channel,api_key_hash,user_id,previous_active,new_active,previous_title_bytes,new_title_bytes,previous_content_bytes,new_content_bytes) SELECT $7,$2,$3,$8,$1,u.user_id,u.previous_active,u.active,u.previous_title_bytes,octet_length(u.title),u.previous_content_bytes,octet_length(u.content) FROM updated u RETURNING 1) SELECT u.title,u.content,u.active FROM updated u CROSS JOIN audited"
    );
    let id = Uuid::new_v4();
    match db
        .query_opt(
            &sql,
            &[
                &hash,
                &env,
                &workflow,
                &title,
                &content,
                &active,
                &id,
                &channel,
                &conditional,
                &expected_title,
                &expected_content,
                &expected_active,
            ],
        )
        .await
    {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(
                json!({"data":{"workflowIdentifier":workflow,"channel":channel,"title":row.get::<_,Option<String>>(0),"content":row.get::<_,Option<String>>(1),"active":row.get::<_,bool>(2)}}),
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
