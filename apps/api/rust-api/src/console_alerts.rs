//! Read-only alert slots behind a separate console credential grant.
use crate::error_at;
use axum::{
    Json,
    extract::{OriginalUri, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::Client;

pub(super) async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
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
    if uri.query().is_some() {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    }
    let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
    match db
        .query_one("SELECT public.novu_rust_console_alerts($1)", &[&hash])
        .await
    {
        Ok(row) => {
            let value: Value = row.get(0);
            ([(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized")
        }
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
        ),
    }
}
