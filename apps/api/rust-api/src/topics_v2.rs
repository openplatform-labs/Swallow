//! Experimental API-key v2 topic upsert, retrieval and update.
use crate::{ApiError, authenticate, error_at, validation_error};
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{Client, Row};

fn fail(status: StatusCode, message: &str, path: &str) -> Response {
    let label = match status {
        StatusCode::BAD_REQUEST => "Bad Request",
        StatusCode::NOT_FOUND => "Not Found",
        StatusCode::CONFLICT => "Conflict",
        StatusCode::NOT_IMPLEMENTED => "Not Implemented",
        _ => "Internal Server Error",
    };
    error_at(status, message, label, path).into_response()
}
fn unsupported(path: &str) -> Response {
    fail(
        StatusCode::NOT_IMPLEMENTED,
        "Topic option not implemented",
        path,
    )
}

// The reference feature flag defaults to off, in which case it removes the
// last context suffix before looking up an admin API subscription.
pub async fn get_subscription(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((topic, identifier)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let identifier = identifier
        .rsplit_once(":ctx_")
        .map_or(identifier.as_str(), |(base, _)| base);
    match db.query_opt("SELECT m.internal_id,m.identifier,m.name,m.context_keys,m.name_present,s.internal_id FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE m.environment_id=$1 AND m.topic_key=$2 AND m.identifier=$3", &[&env, &topic, &identifier]).await {
        Ok(Some(row)) => {
            let mut data = json!({"id":row.get::<_,String>(0),"identifier":row.get::<_,String>(1)});
            if row.get::<_, bool>(4) { data["name"] = json!(row.get::<_, Option<String>>(2)); }
            if let Some(context) = row.get::<_, Option<Vec<String>>>(3) { data["contextKeys"] = json!(context); }
            let subscription_id:String=row.get(0);
            let subscriber_id:String=row.get(5);
            let preferences=match db.query("SELECT p.preferences,p.workflow_internal_id,w.identifier,w.inbox_metadata FROM novu_rust_subscription_preferences p LEFT JOIN novu_rust_workflows w ON w.environment_id=p.environment_id AND w.inbox_metadata->>'id'=p.workflow_internal_id WHERE p.environment_id=$1 AND p.subscription_internal_id=$2 AND p.subscriber_internal_id=$3 ORDER BY p.created_at,p.workflow_internal_id,p.context_keys_hash",&[&env,&subscription_id,&subscriber_id]).await {Ok(v)=>v,Err(_)=>return fail(StatusCode::INTERNAL_SERVER_ERROR,"Internal server error",&path)};
            if !preferences.is_empty() {
                let preferences:Vec<Value>=preferences.iter().map(|preference| {
                    let stored:Value=preference.get(0);
                    let enabled=stored.pointer("/all/enabled").filter(|v|!v.is_null()).cloned().unwrap_or(json!(true));
                    let mut item=json!({"subscriptionId":identifier,"enabled":enabled});
                    if let Some(condition)=stored.pointer("/all/condition") {item["condition"]=condition.clone();}
                    if let Some(workflow_identifier)=preference.get::<_,Option<String>>(2) {
                        let metadata:Value=preference.get::<_,Option<Value>>(3).unwrap_or_else(||json!({}));
                        let mut workflow=json!({"id":preference.get::<_,String>(1),"identifier":workflow_identifier,"name":metadata.get("name").and_then(Value::as_str).unwrap_or(""),"critical":metadata.get("critical").and_then(Value::as_bool).unwrap_or(false),"tags":metadata.get("tags").cloned().unwrap_or(json!([])),"severity":metadata.get("severity").and_then(Value::as_str).unwrap_or("none")});
                        if let Some(value)=metadata.get("data") {workflow["data"]=value.clone();}
                        item["workflow"]=workflow;
                    }
                    item
                }).collect();
                data["preferences"]=json!(preferences);
            }
            Json(json!({"data": data})).into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => fail(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", &path),
    }
}
fn body(bytes: &Bytes, path: &str, require_key: bool) -> Result<Value, ApiError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| error_at(StatusCode::BAD_REQUEST, "Invalid JSON", "Bad Request", path))?;
    let obj = value.as_object().ok_or_else(|| {
        error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Topic option not implemented",
            "Not Implemented",
            path,
        )
    })?;
    if obj.keys().any(|key| {
        !["key", "name", "data"].contains(&key.as_str()) || (!require_key && key == "key")
    }) {
        return Err(error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Topic option not implemented",
            "Not Implemented",
            path,
        ));
    }
    let mut errors: Vec<String> = Vec::new();
    if require_key {
        let key = obj.get("key").and_then(Value::as_str);
        if key.is_none_or(str::is_empty) {
            errors.push("key must be longer than or equal to 1 characters".into());
        } else if key.is_some_and(|value| value.encode_utf16().count() > 100) {
            errors.push("key must be shorter than or equal to 100 characters".into());
        }
        if obj
            .get("key")
            .is_none_or(|value| value.is_null() || value.as_str() == Some(""))
        {
            errors.push("key should not be empty".into());
        }
        if key.is_none() {
            errors.push("key must be a string".into());
        }
    }
    if let Some(name) = obj.get("name").filter(|v| !v.is_null()) {
        if let Some(name) = name.as_str() {
            if name.encode_utf16().count() > 100 {
                errors.push("name must be shorter than or equal to 100 characters".into());
            }
        } else {
            errors.push("name must be longer than or equal to 0 and shorter than or equal to 100 characters".into());
            errors.push("name must be a string".into());
        }
    }
    if let Some(data) = obj.get("data").filter(|v| !v.is_null()) {
        if let Some(map) = data.as_object() {
            if let Ok(encoded) = serde_json::to_vec(data)
                && encoded.len() > 65536
            {
                errors.push(format!(
                    "Data is too large: {}KB exceeds 64KB limit",
                    (encoded.len() + 512) / 1024
                ));
            }
            if let Some((key, _)) = map.iter().find(|(_, v)| {
                !matches!(v, Value::String(_) | Value::Number(_) | Value::Bool(_))
                    && !v
                        .as_array()
                        .is_some_and(|array| array.iter().all(Value::is_string))
            }) {
                errors.push(format!(
                    "Custom data field \"{key}\" must be a string, number, boolean, or string[]"
                ));
            }
        } else {
            errors.push("Context data must be an object".into());
            errors.push("Custom data must be an object".into());
            errors.push("data must be an object".into());
        }
    }
    if !errors.is_empty() {
        let mut response = validation_error(errors.iter().map(String::as_str).collect());
        response.1.0["path"] = json!(path);
        return Err(response);
    }
    Ok(value)
}
fn dto(row: &Row) -> Value {
    let mut result = json!({"_id":row.get::<_,String>(0),"key":row.get::<_,String>(1),"createdAt":row.get::<_,String>(5),"updatedAt":row.get::<_,String>(6)});
    if row.get::<_, bool>(3) {
        result["name"] = json!(row.get::<_, Option<String>>(2));
    }
    if let Some(data) = row.get::<_, Option<Value>>(4) {
        result["data"] = data;
    }
    result
}
pub(crate) fn valid_key(key: &str) -> bool {
    key.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"_:.-".contains(&b))
        || key.split_once('@').is_some_and(|(a, b)| {
            !a.is_empty()
                && !b.is_empty()
                && !b.starts_with('.')
                && b.contains('.')
                && !key.chars().any(char::is_whitespace)
        })
}
const COLUMNS: &str = "internal_id,topic_key,name,name_present,data,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')";

pub async fn upsert(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let fail_if_exists = match uri.query() {
        None | Some("") | Some("failIfExists=false") => false,
        Some("failIfExists=true") => true,
        _ => return unsupported(&path),
    };
    let input = match body(&bytes, &path, true) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let key = input["key"].as_str().expect("validated key");
    if !valid_key(key) {
        match db
            .query_opt(
                "SELECT 1 FROM novu_rust_topics WHERE environment_id=$1 AND topic_key=$2",
                &[&env, &key],
            )
            .await
        {
            Ok(None) => {
                return fail(
                    StatusCode::BAD_REQUEST,
                    &format!(
                        "Invalid topic key: \"{key}\". Topic keys must contain only alphanumeric characters (a-z, A-Z, 0-9), hyphens (-), underscores (_), colons (:), or be a valid email address."
                    ),
                    &path,
                );
            }
            Err(_) => {
                return fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    &path,
                );
            }
            _ => {}
        }
    }
    let has_name = input.get("name").is_some();
    let name = input.get("name").and_then(Value::as_str);
    let has_data = input.get("data").is_some();
    let data = input.get("data").filter(|v| !v.is_null());
    let query = format!(
        "INSERT INTO novu_rust_topics AS t(environment_id,topic_key,internal_id,name,name_present,data) VALUES ($1,$2,novu_rust_new_objectid(),$3,$4,$5) ON CONFLICT(environment_id,topic_key) DO UPDATE SET name=CASE WHEN $4 THEN EXCLUDED.name ELSE t.name END,name_present=CASE WHEN $4 THEN true ELSE t.name_present END,data=CASE WHEN $6 THEN EXCLUDED.data ELSE t.data END,updated_at=CASE WHEN $4 OR $6 THEN now() ELSE t.updated_at END WHERE NOT $7 RETURNING {COLUMNS}"
    );
    match db
        .query_opt(
            &query,
            &[
                &env,
                &key,
                &name,
                &has_name,
                &data,
                &has_data,
                &fail_if_exists,
            ],
        )
        .await
    {
        Ok(Some(row)) => (StatusCode::CREATED, Json(json!({"data":dto(&row)}))).into_response(),
        Ok(None) => fail(
            StatusCode::CONFLICT,
            &format!("Topic with key \"{key}\" already exists"),
            &path,
        ),
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

pub async fn get(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(topic): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let query =
        format!("SELECT {COLUMNS} FROM novu_rust_topics WHERE environment_id=$1 AND topic_key=$2");
    match db.query_opt(&query, &[&env, &topic]).await {
        Ok(Some(row)) => Json(json!({"data":dto(&row)})).into_response(),
        Ok(None) => fail(
            StatusCode::NOT_FOUND,
            &format!("Topic with key {topic} not found"),
            &path,
        ),
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

pub async fn update(
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
    let input = match body(&bytes, &path, false) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let has_name = input.get("name").is_some();
    let name = input.get("name").and_then(Value::as_str);
    let has_data = input.get("data").is_some();
    let data = input.get("data").filter(|v| !v.is_null());
    let query = format!(
        "UPDATE novu_rust_topics SET name=CASE WHEN $3 THEN $4 ELSE name END,name_present=CASE WHEN $3 THEN true ELSE name_present END,data=CASE WHEN $5 THEN $6 ELSE data END,updated_at=now() WHERE environment_id=$1 AND topic_key=$2 AND ($3 OR $5) RETURNING {COLUMNS}"
    );
    match db
        .query_opt(&query, &[&env, &topic, &has_name, &name, &has_data, &data])
        .await
    {
        Ok(Some(row)) => Json(json!({"data":dto(&row)})).into_response(),
        Ok(None) if !has_name && !has_data => match db
            .query_opt(
                "SELECT 1 FROM novu_rust_topics WHERE environment_id=$1 AND topic_key=$2",
                &[&env, &topic],
            )
            .await
        {
            Ok(Some(_)) => fail(
                StatusCode::BAD_REQUEST,
                "At least one of name or data must be provided",
                &path,
            ),
            Ok(None) => fail(
                StatusCode::NOT_FOUND,
                &format!("Topic with key {topic} not found"),
                &path,
            ),
            Err(_) => fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                &path,
            ),
        },
        Ok(None) => fail(
            StatusCode::NOT_FOUND,
            &format!("Topic with key {topic} not found"),
            &path,
        ),
        Err(_) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            &path,
        ),
    }
}

pub async fn delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(topic): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    // The FK protects against an insertion committed after this statement's
    // snapshot. Retry with a fresh snapshot rather than losing that member or
    // reporting an internal error for ordinary concurrent assignment.
    for _ in 0..3 {
        let result=db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id,topic_key FROM novu_rust_topics WHERE environment_id=$1 AND topic_key=$2 FOR UPDATE), members AS (DELETE FROM novu_rust_topic_subscribers s USING target WHERE s.environment_id=target.environment_id AND s.topic_key=target.topic_key RETURNING 1), removed AS (DELETE FROM novu_rust_topics t USING target WHERE t.environment_id=target.environment_id AND t.topic_key=target.topic_key AND (SELECT count(*) FROM members)>=0 RETURNING 1) SELECT 1 FROM removed",&[&env,&topic]).await;
        match result {
            Ok(Some(_)) => return Json(json!({"data":{"acknowledged":true}})).into_response(),
            Ok(None) => {
                return fail(
                    StatusCode::NOT_FOUND,
                    &format!("Topic with key {topic} not found"),
                    &path,
                );
            }
            Err(error)
                if error.as_db_error().is_some_and(|e| {
                    e.code() == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION
                        && e.constraint()
                            == Some("novu_rust_topic_subscribers_environment_id_topic_key_fkey")
                }) =>
            {
                continue;
            }
            Err(_) => {
                return fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    &path,
                );
            }
        }
    }
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Internal server error",
        &path,
    )
}

/// Cursor pagination for supported API-key topic search filters.
pub async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let after = params.get("after").filter(|v| !v.is_empty());
    let before = params.get("before").filter(|v| !v.is_empty());
    if after.is_some() && before.is_some() {
        return fail(
            StatusCode::BAD_REQUEST,
            "Cannot specify both \"before\" and \"after\" cursors at the same time.",
            &path,
        );
    }
    let cursor = after.or(before).map(|value| value.to_ascii_lowercase());
    let empty = || {
        Json(json!({"data":[],"next":null,"previous":null,"totalCount":0,"totalCountCapped":false}))
            .into_response()
    };
    if cursor.as_ref().is_some_and(|id| {
        id.len() != 24
            || !id
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    }) {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        return (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"statusCode":500,"timestamp":timestamp,"path":path,"message":"Internal server error, contact support and provide them with the errorId","errorId":uuid::Uuid::new_v4().to_string()}))).into_response();
    }
    let limit = match params.get("limit").map(|v| v.parse::<i64>()) {
        None | Some(Ok(0)) => 10,
        Some(Ok(v)) if (1..=100).contains(&v) => v,
        Some(Ok(v)) if v > 100 => {
            let mut response = validation_error(vec!["limit must not be greater than 100"]);
            response.1.0["path"] = json!(path);
            return response.into_response();
        }
        _ => return unsupported(&path),
    };
    let order = match params.get("orderBy").map(String::as_str) {
        None | Some("") | Some("_id") => "internal_id",
        Some("createdAt") => "created_at",
        Some("updatedAt") => "updated_at",
        Some("key") => "topic_key COLLATE \"C\"",
        Some("name") => {
            "ROW(CASE WHEN name IS NULL THEN 0 ELSE 1 END,COALESCE(name,'') COLLATE \"C\")"
        }
        _ => return unsupported(&path),
    };
    let descending = !matches!(
        params.get("orderDirection").map(String::as_str),
        Some("ASC")
    );
    let include = params.get("includeCursor").is_some_and(|v| v == "true");
    let key = params.get("key").filter(|v| !v.is_empty());
    let name = params.get("name").filter(|v| !v.is_empty());
    if let Some(cursor) = cursor.as_ref() {
        match db
            .query_opt(
                "SELECT 1 FROM novu_rust_topics WHERE environment_id=$1 AND internal_id=$2",
                &[&env, cursor],
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => return empty(),
            Err(_) => {
                return fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    &path,
                );
            }
        }
    }
    let filter = "environment_id=$1 AND ($2::text IS NULL OR position(lower($2) in lower(topic_key))>0) AND ($3::text IS NULL OR position(lower($3) in lower(COALESCE(name,'')))>0)";
    let count_sql = format!(
        "SELECT count(*) FROM (SELECT 1 FROM novu_rust_topics WHERE {filter} LIMIT 50001) counted"
    );
    let count: i64 = match db.query_one(&count_sql, &[&env, &key, &name]).await {
        Ok(row) => row.get(0),
        Err(_) => {
            return fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                &path,
            );
        }
    };
    let reverse = before.is_some();
    let ascending = descending == reverse;
    let sort = if ascending { "ASC" } else { "DESC" };
    let cmp = match (ascending, include) {
        (true, false) => ">",
        (true, true) => ">=",
        (false, false) => "<",
        (false, true) => "<=",
    };
    let cursor_clause = if cursor.is_some() {
        let type_guard = if order.starts_with("ROW(") {
            " AND (name IS NULL) = (SELECT name IS NULL FROM novu_rust_topics WHERE environment_id=$1 AND internal_id=$4)"
        } else {
            ""
        };
        if include && order != "internal_id" {
            // Mongo's inclusive primary-sort branch includes every item with
            // the same sort value, even items before the cursor ID.
            let inclusive_order = if order.starts_with("ROW(") {
                "COALESCE(name,'') COLLATE \"C\""
            } else {
                order
            };
            format!(
                " AND ({inclusive_order}) {cmp} (SELECT {inclusive_order} FROM novu_rust_topics WHERE environment_id=$1 AND internal_id=$4){type_guard}"
            )
        } else {
            format!(
                " AND ({order},internal_id) {cmp} (SELECT {order},internal_id FROM novu_rust_topics WHERE environment_id=$1 AND internal_id=$4){type_guard}"
            )
        }
    } else {
        " AND ($4::text IS NULL)".into()
    };
    let sql = format!(
        "SELECT {COLUMNS} FROM novu_rust_topics WHERE {filter}{cursor_clause} ORDER BY {order} {sort},internal_id {sort} LIMIT $5"
    );
    let mut rows = match db
        .query(&sql, &[&env, &key, &name, &cursor, &(limit + 1)])
        .await
    {
        Ok(rows) => rows,
        Err(_) => {
            return fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                &path,
            );
        }
    };
    let extra = rows.len() > limit as usize;
    if extra {
        rows.truncate(limit as usize);
    }
    if reverse {
        rows.reverse();
    }
    let mut next: Option<String> = None;
    let mut previous: Option<String> = None;
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        if extra {
            if reverse {
                previous = Some(first.get(0));
            } else {
                next = Some(last.get(0));
            }
        }
        let boundary: String = if reverse { last.get(0) } else { first.get(0) };
        let other_cmp = match (descending, reverse) {
            (true, true) => "<",
            (true, false) => ">",
            (false, true) => ">",
            (false, false) => "<",
        };
        let type_guard = if order.starts_with("ROW(") {
            " AND (name IS NULL) = (SELECT name IS NULL FROM novu_rust_topics WHERE environment_id=$1 AND internal_id=$4)"
        } else {
            ""
        };
        let probe = format!(
            "SELECT EXISTS(SELECT 1 FROM novu_rust_topics WHERE {filter} AND ({order},internal_id) {other_cmp} (SELECT {order},internal_id FROM novu_rust_topics WHERE environment_id=$1 AND internal_id=$4){type_guard})"
        );
        let exists: bool = match db.query_one(&probe, &[&env, &key, &name, &boundary]).await {
            Ok(row) => row.get(0),
            Err(_) => {
                return fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    &path,
                );
            }
        };
        if exists {
            if reverse {
                next = Some(boundary);
            } else {
                previous = Some(boundary);
            }
        }
    }
    let data: Vec<Value> = rows.iter().map(dto).collect();
    Json(json!({"data":data,"next":next,"previous":previous,"totalCount":count.min(50000),"totalCountCapped":count>50000})).into_response()
}
