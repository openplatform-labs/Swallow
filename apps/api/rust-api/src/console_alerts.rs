//! Read-only alert slots behind a separate console credential grant.
use crate::error_at;
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::Client;
use uuid::Uuid;

pub(super) async fn request_status(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let path = uri.to_string();
    let failure = |status, message, label| error_at(status, message, label, &path).into_response();
    let Some(hash) = crate::console_auth::credential_hash(&headers) else {
        return failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized");
    };
    let request = params
        .get("requestId")
        .and_then(|v| Uuid::parse_str(v).ok());
    let Some(request) = request.filter(|_| params.len() == 1) else {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    };
    match db
        .query_one(
            "SELECT public.novu_rust_console_decision_request($1,$2)",
            &[&hash, &request],
        )
        .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_, Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized")
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request")
        }
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
        ),
    }
}

pub(super) async fn history(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let path = uri.to_string();
    let failure = |status, message, label| error_at(status, message, label, &path).into_response();
    let Some(hash) = crate::console_auth::credential_hash(&headers) else {
        return failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized");
    };
    let fields = (
        params.get("channel"),
        params.get("targetId"),
        params
            .get("attemptId")
            .and_then(|v| Uuid::parse_str(v).ok()),
    );
    let (Some(channel), Some(target), Some(attempt)) = fields else {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    };
    if params.len() != 3 {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    }
    match db
        .query_one(
            "SELECT public.novu_rust_console_decision_history($1,$2,$3,$4)",
            &[&hash, &channel, &target, &attempt],
        )
        .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_, Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized")
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request")
        }
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
        ),
    }
}

pub(super) async fn resolve(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    resolve_inner(db, headers, uri, body, false).await
}

pub(super) async fn resolve_v2(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    resolve_inner(db, headers, uri, body, true).await
}

async fn resolve_inner(
    db: Arc<Client>,
    headers: HeaderMap,
    uri: axum::http::Uri,
    body: Bytes,
    idempotent: bool,
) -> Response {
    let path = uri.to_string();
    let failure = |status, message, label| error_at(status, message, label, &path).into_response();
    let Some(hash) = crate::console_auth::credential_hash(&headers) else {
        return failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized");
    };
    let parsed = if body.len() <= 4096 {
        serde_json::from_slice::<Value>(&body).ok()
    } else {
        None
    };
    let Some(object) = parsed
        .as_ref()
        .and_then(Value::as_object)
        .filter(|o| o.len() == if idempotent { 6 } else { 5 })
    else {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    };
    let fields = (
        object.get("channel").and_then(Value::as_str),
        object.get("targetId").and_then(Value::as_str),
        object
            .get("attemptId")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok()),
        object.get("decision").and_then(Value::as_str),
        object.get("reason").and_then(Value::as_str),
    );
    let (Some(channel), Some(target), Some(attempt), Some(decision), Some(reason)) = fields else {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    };
    let request = if idempotent {
        let Some(value) = object
            .get("requestId")
            .and_then(Value::as_str)
            .and_then(|v| Uuid::parse_str(v).ok())
        else {
            return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
        };
        Some(value)
    } else {
        None
    };
    let result = if let Some(request) = request {
        db.query_one(
            "SELECT public.novu_rust_console_resolve_alert_v2($1,$2,$3,$4,$5,$6,$7)",
            &[
                &hash, &request, &channel, &target, &attempt, &decision, &reason,
            ],
        )
        .await
    } else {
        db.query_one(
            "SELECT public.novu_rust_console_resolve_alert($1,$2,$3,$4,$5,$6)",
            &[&hash, &channel, &target, &attempt, &decision, &reason],
        )
        .await
    };
    match result {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(if idempotent {
                serde_json::json!({"recorded":true,"replayed":row.get::<_,bool>(0)})
            } else {
                serde_json::json!({"recorded":true})
            }),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized")
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => failure(
            StatusCode::BAD_REQUEST,
            "Invalid or stale request",
            "Bad Request",
        ),
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
        ),
    }
}

pub(super) async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let path = uri.to_string();
    let failure = |status, message, label| error_at(status, message, label, &path).into_response();
    let Some(hash) = crate::console_auth::credential_hash(&headers) else {
        return failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized");
    };
    if params
        .keys()
        .any(|k| !["afterChannel", "afterTarget"].contains(&k.as_str()))
    {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    }
    let after_channel = params.get("afterChannel");
    let after_target = params.get("afterTarget");
    match db
        .query_one(
            "SELECT public.novu_rust_console_alerts_page($1,$2,$3)",
            &[&hash, &after_channel, &after_target],
        )
        .await
    {
        Ok(row) => {
            let value: Value = row.get(0);
            ([(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized")
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request")
        }
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
        ),
    }
}
