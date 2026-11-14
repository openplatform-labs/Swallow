//! Read-only alert slots behind a separate console credential grant.
use crate::error_at;
use axum::{
    Json,
    extract::{OriginalUri, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::Client;

pub(super) async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let path = uri.to_string();
    let failure = |status, message, label| error_at(status, message, label, &path).into_response();
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("SwallowConsole "));
    let Some(token) = token.filter(|v| {
        v.len() == 64
            && v.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    }) else {
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
    let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
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
