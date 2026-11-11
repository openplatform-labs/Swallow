//! Experimental, deliberately incomplete Novu trigger endpoint.
use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use novu_rust_contracts::trigger_gate::{GateResult, TenantState, evaluate};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;

pub const MIGRATION: &str = include_str!("../migrations/0001_experimental.sql");

pub fn router(db: Arc<Client>) -> Router {
    Router::new()
        .route("/v1/events/trigger", post(trigger))
        .with_state(db)
}

fn error(status: StatusCode, message: &str, label: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "statusCode": status.as_u16(), "message": message, "error": label })),
    )
}

fn valid_recipient(value: &Value) -> bool {
    match value {
        Value::String(s) => !s.is_empty(),
        Value::Object(o) => {
            let key = if o.get("type").and_then(Value::as_str) == Some("Topic") {
                "topicKey"
            } else if o.contains_key("subscriberId") {
                "subscriberId"
            } else {
                "topicKey"
            };
            o.get(key)
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        }
        _ => false,
    }
}

pub fn valid_recipients(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Array(items)) => !items.is_empty() && items.iter().all(valid_recipient),
        Some(value) => valid_recipient(value),
        None => false,
    }
}

async fn trigger(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> (StatusCode, Json<Value>) {
    let Some(header) = headers
        .get("authorization")
        .and_then(|x| x.to_str().ok())
        .filter(|x| !x.is_empty())
    else {
        return error(
            StatusCode::UNAUTHORIZED,
            "Missing authorization header",
            "Unauthorized",
        );
    };
    if header == "ApiKey" || header == "Bearer" || header.starts_with("Bearer ") {
        // User JWT authentication is not implemented; fail closed without treating Bearer as an unknown scheme.
        return error(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized");
    }
    let Some(key) = header.strip_prefix("ApiKey ") else {
        let scheme = header.split_whitespace().next().unwrap_or("");
        return error(
            StatusCode::UNAUTHORIZED,
            &format!("Invalid authentication scheme: \"{scheme}\""),
            "Unauthorized",
        );
    };
    if key.is_empty() {
        return error(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized");
    }
    let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
    let environment_id: String = match db
        .query_opt(
            "SELECT k.environment_id, u.id FROM novu_rust_api_keys k LEFT JOIN novu_rust_users u ON u.id = k.user_id WHERE k.key_hash = $1 AND k.enabled",
            &[&hash],
        )
        .await
    {
        Ok(Some(row)) => {
            if row.get::<_, Option<String>>(1).is_none() {
                return error(StatusCode::UNAUTHORIZED, "User not found", "Unauthorized");
            }
            row.get(0)
        },
        Ok(None) => {
            return error(
                StatusCode::UNAUTHORIZED,
                "API Key not found",
                "Unauthorized",
            );
        }
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "Internal Server Error",
            );
        }
    };
    let body: Value = match serde_json::from_slice(&bytes) {
        Ok(body) => body,
        Err(_) => return error(StatusCode::BAD_REQUEST, "Invalid JSON", "Bad Request"),
    };
    let Some(object) = body.as_object() else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Validation Error",
            "Unprocessable Entity",
        );
    };
    let Some(name) = object.get("name").and_then(Value::as_str) else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Validation Error",
            "Unprocessable Entity",
        );
    };
    if !valid_recipients(object.get("to"))
        || object
            .get("payload")
            .is_some_and(|v| !v.is_null() && !v.is_object())
        || object
            .get("transactionId")
            .is_some_and(|v| !v.is_null() && !v.is_string())
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Validation Error",
            "Unprocessable Entity",
        );
    }
    // Avoid implying compatibility for paths whose validation or behaviour is not implemented.
    if object
        .keys()
        .any(|k| !["name", "to", "payload", "transactionId"].contains(&k.as_str()))
    {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "Trigger option not implemented",
            "Not Implemented",
        );
    }
    let row = match db.query_opt("SELECT active, has_steps, in_app_content FROM novu_rust_workflows WHERE environment_id = $1 AND identifier = $2", &[&environment_id, &name]).await {
        Ok(Some(row)) => row,
        Ok(None) => return error(StatusCode::UNPROCESSABLE_ENTITY, "workflow_not_found", "Unprocessable Entity"),
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error"),
    };
    let txn = object
        .get("transactionId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("txn_{}", &Uuid::new_v4().simple().to_string()[..24]));
    let decision = evaluate(row.get(0), row.get(1), TenantState::NotSpecified);
    if decision == GateResult::Dispatch {
        let content: Option<String> = row.get(2);
        let recipients = match object.get("to") {
            Some(Value::String(s)) => Some(vec![s.clone()]),
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| v.as_str().map(str::to_owned))
                .collect(),
            _ => None,
        };
        let payload = object
            .get("payload")
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| json!({}));
        let (Some(content), Some(recipients)) = (content, recipients) else {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "Workflow execution not implemented",
                "Not Implemented",
            );
        };
        if payload.get("attachments").is_some()
            || recipients.iter().any(|id| {
                novu_rust_contracts::template::render_in_app(&content, &payload, id).is_err()
            })
        {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "Workflow execution not implemented",
                "Not Implemented",
            );
        }
        let data = json!({"workflow": name, "transactionId": txn, "recipients": recipients, "content": content, "payload": payload});
        // The ledger and queue entry are inserted atomically. This provisional deduplication
        // policy is not yet equivalent to Novu's tier-dependent retention behaviour.
        let result = db.execute("WITH accepted AS (INSERT INTO novu_rust_trigger_transactions (environment_id, transaction_id) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING 1) INSERT INTO novu_queue_jobs (id, environment_id, kind, payload) SELECT $3, $1, $4, $5 FROM accepted", &[&environment_id, &txn, &Uuid::new_v4(), &novu_rust_contracts::EXPERIMENTAL_IN_APP_JOB_KIND, &data]).await;
        if result.is_err() {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "Internal Server Error",
            );
        }
    }
    (
        StatusCode::CREATED,
        Json(
            json!({"data": {"acknowledged": true, "status": decision.status(), "transactionId": txn}}),
        ),
    )
}
