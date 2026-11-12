//! Global preferences for the experimental single-in-app workflow subset.
use crate::{authenticate, error_at};
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::{Client, Row};

fn internal(path: &str) -> Response {
    error_at(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Internal server error",
        "Internal Server Error",
        path,
    )
    .into_response()
}
fn unsupported(path: &str) -> Response {
    error_at(
        StatusCode::NOT_IMPLEMENTED,
        "Preference option not implemented",
        "Not Implemented",
        path,
    )
    .into_response()
}
fn workflow_slug(metadata: &Value) -> Option<String> {
    if let Some(slug) = metadata.get("slug").and_then(Value::as_str) {
        return Some(slug.to_owned());
    }
    let name = metadata.get("name")?.as_str()?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b' ' || b == b'-')
    {
        return None;
    }
    let slug = name
        .split([' ', '-'])
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if slug.is_empty() {
        return None;
    }
    let id = metadata.get("id")?.as_str()?;
    if id.len() != 24 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut number = u128::from_str_radix(id, 16).ok()?;
    let alphabet = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    while number > 0 {
        out.push(alphabet[(number % 62) as usize]);
        number /= 62;
    }
    for i in (0..id.len()).step_by(2) {
        if &id[i..i + 2] == "00" {
            out.push(b'0');
        } else {
            break;
        }
    }
    out.reverse();
    Some(format!("{slug}_wf_{}", String::from_utf8(out).ok()?))
}
async fn dto(
    db: &Client,
    env: &str,
    id: &str,
    row: Row,
    global_only: bool,
    path: &str,
) -> Response {
    let channels: Value = row.get(1);
    let active: bool = row.get(2);
    let mut visible = serde_json::Map::new();
    if active {
        visible.insert(
            "in_app".into(),
            channels
                .get("in_app")
                .and_then(Value::as_bool)
                .unwrap_or(true)
                .into(),
        );
    }
    let global = json!({"enabled":true,"channels":visible});
    if global_only {
        return Json(json!({"data":global})).into_response();
    }
    let rows=match db.query("SELECT w.identifier,w.inbox_metadata,r.read_only,r.all_enabled,r.in_app_enabled,s.channels,to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM novu_rust_workflows w LEFT JOIN novu_rust_workflow_preferences r ON r.environment_id=w.environment_id AND r.workflow_identifier=w.identifier LEFT JOIN novu_rust_subscriber_workflow_preferences s ON s.environment_id=w.environment_id AND s.workflow_identifier=w.identifier AND s.subscriber_id=$2 WHERE w.environment_id=$1 AND w.active AND w.has_steps AND w.in_app_content IS NOT NULL AND (r.workflow_identifier IS NOT NULL OR s.subscriber_id IS NOT NULL) AND NOT COALESCE(r.read_only,false) ORDER BY w.identifier",&[&env,&id]).await {Ok(rows)=>rows,Err(_)=>return internal(path)};
    let mut workflows = Vec::new();
    for item in rows {
        let identifier: String = item.get(0);
        let metadata: Option<Value> = item.get(1);
        let Some(metadata) = metadata else {
            return unsupported(path);
        };
        let Some(name) = metadata.get("name").and_then(Value::as_str) else {
            return unsupported(path);
        };
        let Some(slug) = workflow_slug(&metadata) else {
            return unsupported(path);
        };
        let explicit: Option<Value> = item.get(5);
        let enabled = novu_rust_contracts::preference::in_app_enabled(
            item.get(2),
            item.get(3),
            item.get(4),
            channels.get("in_app").and_then(Value::as_bool),
            explicit
                .as_ref()
                .and_then(|v| v.get("in_app"))
                .and_then(Value::as_bool),
        );
        let mut workflow = json!({"enabled":true,"channels":{"in_app":enabled},"overrides":[{"channel":"in_app","source":"subscriber"}],"workflow":{"slug":slug,"identifier":identifier,"name":name}});
        if let Some(updated) = metadata.get("updatedAt") {
            workflow["workflow"]["updatedAt"] = updated.clone();
        }
        if let Some(updated) = item.get::<_, Option<String>>(6) {
            workflow["updatedAt"] = updated.into();
        }
        workflows.push(workflow);
    }
    Json(json!({"data":{"global":global,"workflows":workflows}})).into_response()
}
async fn preflight(db: &Client, env: &str, id: &str, path: &str) -> Option<Response> {
    let row = match db.query_opt("SELECT s.subscriber_id,COALESCE(p.channels,'{}'::jsonb),EXISTS(SELECT 1 FROM novu_rust_workflows w WHERE w.environment_id=s.environment_id AND w.active AND w.has_steps AND w.in_app_content IS NOT NULL) FROM novu_rust_subscribers s LEFT JOIN novu_rust_subscriber_preferences p ON p.environment_id=s.environment_id AND p.subscriber_id=s.subscriber_id WHERE s.environment_id=$1 AND s.subscriber_id=$2", &[&env,&id]).await {
        Ok(row) => row,
        Err(_) => return Some(internal(path)),
    };
    if let Some(row) = row {
        let response = dto(db, env, id, row, false, path).await;
        if !response.status().is_success() {
            return Some(response);
        }
    }
    None
}
async fn fetch(
    db: &Client,
    headers: &HeaderMap,
    uri: &axum::http::Uri,
    id: &str,
    global_only: bool,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(db, headers, &path).await {
        Ok(env) => env,
        Err(e) => return e.into_response(),
    };
    if uri.query().is_some() {
        return unsupported(&path);
    }
    let row=db.query_opt("SELECT s.subscriber_id,COALESCE(p.channels,'{}'::jsonb),EXISTS(SELECT 1 FROM novu_rust_workflows w WHERE w.environment_id=s.environment_id AND w.active AND w.has_steps AND w.in_app_content IS NOT NULL) FROM novu_rust_subscribers s LEFT JOIN novu_rust_subscriber_preferences p ON p.environment_id=s.environment_id AND p.subscriber_id=s.subscriber_id WHERE s.environment_id=$1 AND s.subscriber_id=$2",&[&env,&id]).await;
    match row {
        Ok(Some(row)) => dto(db, &env, id, row, global_only, &path).await,
        Ok(None) => {
            let message = if global_only {
                format!("Subscriber {id} not found")
            } else {
                format!("Subscriber with id: {id} not found")
            };
            error_at(StatusCode::NOT_FOUND, &message, "Not Found", &path).into_response()
        }
        Err(_) => internal(&path),
    }
}
pub async fn get(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    fetch(&db, &headers, &uri, &id, false).await
}
pub async fn get_global(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    fetch(&db, &headers, &uri, &id, true).await
}
pub async fn update(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(env) => env,
        Err(e) => return e.into_response(),
    };
    if uri.query().is_some() {
        return unsupported(&path);
    }
    let body: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => {
            return error_at(
                StatusCode::BAD_REQUEST,
                "Invalid JSON",
                "Bad Request",
                &path,
            )
            .into_response();
        }
    };
    let Some(obj) = body.as_object() else {
        return unsupported(&path);
    };
    if obj.keys().any(|k| k != "channels" && k != "workflowId") {
        return unsupported(&path);
    }
    let channels = match obj.get("channels") {
        None | Some(Value::Null) => json!({}),
        Some(Value::Object(values))
            if values.iter().all(|(k, v)| {
                ["in_app", "email", "sms", "chat", "push", "tool"].contains(&k.as_str())
                    && v.is_boolean()
            }) =>
        {
            json!(values)
        }
        _ => return unsupported(&path),
    };
    let workflow_id = match obj.get("workflowId") {
        None | Some(Value::Null) => None,
        Some(Value::String(v)) if v.is_empty() => None,
        Some(Value::String(v)) => Some(v.as_str()),
        _ => return unsupported(&path),
    };
    if let Some(workflow_id) = workflow_id {
        let rows = match db
            .query(
                "SELECT identifier,inbox_metadata FROM novu_rust_workflows WHERE environment_id=$1",
                &[&env],
            )
            .await
        {
            Ok(rows) => rows,
            Err(_) => return internal(&path),
        };
        let found = rows.into_iter().find_map(|row| {
            let identifier: String = row.get(0);
            let metadata: Option<Value> = row.get(1);
            let by_id = metadata
                .as_ref()
                .and_then(|m| m.get("id"))
                .and_then(Value::as_str)
                == Some(workflow_id);
            let by_slug = metadata.as_ref().and_then(workflow_slug).as_deref() == Some(workflow_id);
            if identifier == workflow_id || by_id || by_slug {
                Some((identifier, metadata))
            } else {
                None
            }
        });
        let (identifier, metadata) = match found {
            Some((identifier, Some(metadata))) => (identifier, metadata),
            Some((_, None)) => return unsupported(&path),
            None => {
                let mut response = error_at(
                    StatusCode::NOT_FOUND,
                    "Workflow cannot be found",
                    "Not Found",
                    &path,
                );
                response
                    .1
                    .0
                    .as_object_mut()
                    .expect("error object")
                    .remove("error");
                response.1.0["workflowId"] = json!(workflow_id);
                response.1.0["ctx"] = json!({"workflowId":workflow_id});
                return response.into_response();
            }
        };
        let Some(critical) = metadata.get("critical").and_then(Value::as_bool) else {
            return unsupported(&path);
        };
        if critical {
            let Some(internal_id) = metadata.get("id").and_then(Value::as_str) else {
                return unsupported(&path);
            };
            return error_at(
                StatusCode::BAD_REQUEST,
                &format!("Critical workflow with id: {internal_id} can not be updated"),
                "Bad Request",
                &path,
            )
            .into_response();
        }
        if metadata.get("name").and_then(Value::as_str).is_none()
            || workflow_slug(&metadata).is_none()
        {
            return unsupported(&path);
        }
        if let Some(response) = preflight(&db, &env, &id, &path).await {
            return response;
        }
        let result=db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id,subscriber_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), updated AS (INSERT INTO novu_rust_subscriber_workflow_preferences(environment_id,subscriber_id,workflow_identifier,channels) SELECT environment_id,subscriber_id,$4,$3 FROM target ON CONFLICT (environment_id,subscriber_id,workflow_identifier) DO UPDATE SET channels=novu_rust_subscriber_workflow_preferences.channels || EXCLUDED.channels,updated_at=now() RETURNING 1) SELECT target.subscriber_id,COALESCE(p.channels,'{}'::jsonb),EXISTS(SELECT 1 FROM novu_rust_workflows w WHERE w.environment_id=$1 AND w.active AND w.has_steps AND w.in_app_content IS NOT NULL) FROM target JOIN updated ON true LEFT JOIN novu_rust_subscriber_preferences p ON p.environment_id=target.environment_id AND p.subscriber_id=target.subscriber_id",&[&env,&id,&channels,&identifier]).await;
        return match result {
            Ok(Some(row)) => dto(&db, &env, &id, row, false, &path).await,
            Ok(None) => error_at(
                StatusCode::NOT_FOUND,
                &format!("Subscriber with id: {id} is not found"),
                "Not Found",
                &path,
            )
            .into_response(),
            Err(_) => internal(&path),
        };
    }
    // Lock subscriber first, matching worker lock order, and update atomically.
    if let Some(response) = preflight(&db, &env, &id, &path).await {
        return response;
    }
    let result=db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id,subscriber_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), cleared AS (UPDATE novu_rust_subscriber_workflow_preferences sp SET channels=sp.channels - ARRAY(SELECT jsonb_object_keys($3::jsonb)),updated_at=now() FROM target WHERE sp.environment_id=target.environment_id AND sp.subscriber_id=target.subscriber_id AND sp.channels ?| ARRAY(SELECT jsonb_object_keys($3::jsonb)) RETURNING 1), updated AS (INSERT INTO novu_rust_subscriber_preferences(environment_id,subscriber_id,channels) SELECT environment_id,subscriber_id,$3 FROM target WHERE (SELECT count(*) FROM cleared)>=0 ON CONFLICT (environment_id,subscriber_id) DO UPDATE SET channels=novu_rust_subscriber_preferences.channels || EXCLUDED.channels RETURNING channels) SELECT target.subscriber_id,updated.channels,EXISTS(SELECT 1 FROM novu_rust_workflows w WHERE w.environment_id=$1 AND w.active AND w.has_steps AND w.in_app_content IS NOT NULL) FROM target JOIN updated ON true",&[&env,&id,&channels]).await;
    match result {
        Ok(Some(row)) => dto(&db, &env, &id, row, false, &path).await,
        Ok(None) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Subscriber with id: {id} is not found"),
            "Not Found",
            &path,
        )
        .into_response(),
        Err(_) => internal(&path),
    }
}
