//! Narrow, independently provisioned console subscriber listing.
use crate::error_at;
use axum::{
    Json,
    extract::{OriginalUri, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::Value;
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
    let Some(hash) = crate::console_auth::credential_hash(&headers) else {
        return failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized");
    };
    if params
        .keys()
        .any(|k| !["name", "after", "before", "limit"].contains(&k.as_str()))
        || params.get("limit").is_some_and(|v| v != "20")
    {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    }
    let name = params.get("name").filter(|v| !v.is_empty());
    let after = params.get("after");
    let before = params.get("before");
    match db
        .query_one(
            "SELECT public.novu_rust_console_subscribers($1,$2,$3,$4)",
            &[&hash, &name, &after, &before],
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
