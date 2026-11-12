//! Experimental v2 topic subscription deletion.
use crate::{authenticate, error_at, validation_error};
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc};
use tokio_postgres::{Client, Row};

fn failure(path: &str) -> Response {
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
        "Subscription option not implemented",
        "Not Implemented",
        path,
    )
    .into_response()
}
fn validation(path: &str, msg: &str) -> Response {
    let mut response = validation_error(vec![msg]);
    response.1.0["path"] = json!(path);
    response.into_response()
}
fn output(
    path: &str,
    data: Vec<Value>,
    total: usize,
    errors: Vec<Value>,
    include_empty_errors: bool,
) -> Response {
    let mut result = json!({"data":data,"meta":{"totalCount":total,"successful":data.len(),"failed":errors.len()}});
    if !errors.is_empty() || include_empty_errors {
        result["errors"] = json!(errors);
    }
    if !errors.is_empty() && data.is_empty() {
        let mut body = result.clone();
        body["statusCode"] = json!(400);
        body["timestamp"] =
            json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        body["path"] = json!(path);
        body["message"] = json!("Api Exception Raised with status 400");
        body["ctx"] = result;
        (StatusCode::BAD_REQUEST, Json(body)).into_response()
    } else {
        (StatusCode::OK, Json(result)).into_response()
    }
}
#[derive(Clone)]
struct Item {
    identifier: Option<String>,
    subscriber: Option<String>,
}

fn dto(row: &Row, topic: &Value) -> Value {
    let mut subscriber = json!({"_id":row.get::<_,String>(5),"subscriberId":row.get::<_,String>(6),"createdAt":row.get::<_,String>(8),"updatedAt":row.get::<_,String>(9)});
    let profile: Value = row.get(7);
    for key in ["avatar", "firstName", "lastName", "email"] {
        if let Some(value) = profile.get(key) {
            subscriber[key] = value.clone();
        }
    }
    let mut result = json!({"_id":row.get::<_,String>(0),"identifier":row.get::<_,String>(1),"topic":topic,"subscriber":subscriber,"createdAt":row.get::<_,String>(3),"updatedAt":row.get::<_,String>(4)});
    if let Some(keys) = row.get::<_, Option<Vec<String>>>(2) {
        result["contextKeys"] = json!(keys);
    }
    result
}

pub async fn delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(topic): Path<String>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let value: Value = match serde_json::from_slice(&bytes) {
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
    let Some(obj) = value.as_object() else {
        return unsupported(&path);
    };
    for (field, min, max) in [
        (
            "subscriberIds",
            "At least one subscriber identifier is required",
            "Cannot unsubscribe more than 100 subscribers at once",
        ),
        (
            "subscriptions",
            "At least one subscription is required",
            "Cannot unsubscribe more than 100 subscriptions at once",
        ),
    ] {
        if let Some(raw) = obj.get(field).filter(|v| !v.is_null()) {
            let Some(items) = raw.as_array() else {
                return validation(&path, &format!("{field} must be an array"));
            };
            if items.is_empty() {
                return validation(&path, min);
            }
            if items.len() > 100 {
                return validation(&path, max);
            }
            if field == "subscriberIds" && items.iter().any(|v| !v.is_string()) {
                return validation(&path, "each value in subscriberIds must be a string");
            }
        }
    }
    let raw = obj
        .get("subscriptions")
        .filter(|v| !v.is_null())
        .or_else(|| obj.get("subscriberIds").filter(|v| !v.is_null()));
    let mut items = Vec::new();
    if let Some(values) = raw.and_then(Value::as_array) {
        for value in values {
            if let Some(id) = value.as_str() {
                items.push(Item {
                    identifier: None,
                    subscriber: Some(id.to_owned()),
                });
            } else if let Some(object) = value.as_object() {
                if ["subscriberId", "identifier"].iter().any(|key| {
                    object
                        .get(*key)
                        .is_some_and(|v| !v.is_null() && !v.is_string())
                }) {
                    return unsupported(&path);
                }
                items.push(Item {
                    identifier: object
                        .get("identifier")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    subscriber: object
                        .get("subscriberId")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            } else {
                return unsupported(&path);
            }
        }
    }
    let topic_row=match db.query_opt("SELECT internal_id,name,name_present,data FROM novu_rust_topics WHERE environment_id=$1 AND topic_key=$2",&[&env,&topic]).await {Ok(Some(v))=>v,Ok(None)=>return error_at(StatusCode::NOT_FOUND,&format!("Topic with key {topic} not found"),"Not Found",&path).into_response(),Err(_)=>return failure(&path)};
    let mut topic_dto = json!({"_id":topic_row.get::<_,String>(0),"key":topic});
    if topic_row.get::<_, bool>(2) {
        topic_dto["name"] = json!(topic_row.get::<_, Option<String>>(1));
    }
    if let Some(data) = topic_row.get::<_, Option<Value>>(3) {
        topic_dto["data"] = data;
    }
    if items.is_empty() {
        return output(&path, vec![], 0, vec![], false);
    }
    let identifiers: Vec<String> = items
        .iter()
        .filter_map(|v| v.identifier.as_ref().filter(|v| !v.is_empty()).cloned())
        .collect();
    let subscribers: Vec<String> = items
        .iter()
        .filter_map(|v| v.subscriber.as_ref().filter(|v| !v.is_empty()).cloned())
        .collect();
    if identifiers.is_empty() && subscribers.is_empty() {
        let errors=items.iter().map(|item|json!({"subscriberId":item.subscriber.as_deref().filter(|v|!v.is_empty()).unwrap_or("unknown"),"identifier":item.identifier.as_deref().filter(|v|!v.is_empty()).unwrap_or("unknown"),"code":"INVALID_REQUEST","message":"Subscription identifier is required."})).collect();
        return output(&path, vec![], items.len(), errors, false);
    }
    let select = "SELECT m.internal_id,m.identifier,m.context_keys,to_char(m.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(m.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),s.internal_id,s.subscriber_id,s.profile,to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE m.environment_id=$1 AND m.topic_key=$2";
    let mut errors = Vec::new();
    let mut rows: Vec<Row> = Vec::new();
    if !identifiers.is_empty() {
        let sql = format!("{select} AND m.identifier=ANY($3) ORDER BY m.internal_id");
        let found = match db.query(&sql, &[&env, &topic, &identifiers]).await {
            Ok(v) => v,
            Err(_) => return failure(&path),
        };
        let existing: HashSet<String> = found.iter().map(|r| r.get(1)).collect();
        for id in &identifiers {
            if !existing.contains(id) {
                let sub = items
                    .iter()
                    .find(|v| v.identifier.as_deref() == Some(id))
                    .and_then(|v| v.subscriber.as_deref())
                    .filter(|v| !v.is_empty())
                    .unwrap_or("unknown");
                errors.push(json!({"subscriberId":sub,"code":"SUBSCRIPTION_NOT_FOUND","message":format!("Subscription with identifier '{id}' not found.")}));
            }
        }
        rows.extend(found);
    }
    if !subscribers.is_empty() {
        let existing_users=match db.query("SELECT subscriber_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=ANY($2)",&[&env,&subscribers]).await {Ok(v)=>v,Err(_)=>return failure(&path)};
        let found_users: HashSet<String> = existing_users.iter().map(|r| r.get(0)).collect();
        for sub in &subscribers {
            if !found_users.contains(sub) {
                errors.push(json!({"subscriberId":sub,"code":"SUBSCRIBER_NOT_FOUND","message":format!("Subscriber with ID '{sub}' could not be found.")}));
            }
        }
        if !found_users.is_empty() {
            let sql = format!("{select} AND m.subscriber_id=ANY($3) ORDER BY m.internal_id");
            let found = match db.query(&sql, &[&env, &topic, &subscribers]).await {
                Ok(v) => v,
                Err(_) => return failure(&path),
            };
            let existing_ids: HashSet<String> = found.iter().map(|r| r.get(1)).collect();
            let existing_users: HashSet<String> = found.iter().map(|r| r.get(6)).collect();
            for item in &items {
                if let Some(sub) = item
                    .subscriber
                    .as_ref()
                    .filter(|v| found_users.contains(*v))
                {
                    if let Some(id) = item.identifier.as_deref().filter(|v| !v.is_empty()) {
                        if !existing_ids.contains(id) {
                            errors.push(json!({"subscriberId":sub,"code":"SUBSCRIPTION_NOT_FOUND","message":format!("Subscription with identifier '{id}' for subscriber '{sub}' not found.")}));
                        }
                    } else if !existing_users.contains(sub) {
                        errors.push(json!({"subscriberId":sub,"code":"SUBSCRIPTION_NOT_FOUND","message":format!("Subscription for subscriber '{sub}' not found.")}));
                    }
                }
            }
            rows.extend(found);
        }
    }
    if rows.is_empty() {
        return output(&path, vec![], items.len(), errors, true);
    }
    let data: Vec<Value> = rows.iter().map(|r| dto(r, &topic_dto)).collect();
    let ids: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    if db.execute("WITH preferences AS (DELETE FROM novu_rust_subscription_preferences WHERE environment_id=$1 AND subscription_internal_id=ANY($2) RETURNING 1) DELETE FROM novu_rust_topic_subscribers WHERE environment_id=$1 AND internal_id=ANY($2) AND (SELECT count(*) FROM preferences)>=0",&[&env,&ids]).await.is_err(){return failure(&path)}
    output(&path, data, items.len(), errors, false)
}
