//! Experimental API-key context CRUD and cursor listing. Bridge URLs are not implemented.
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

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn unsupported(path: &str) -> Response {
    error_at(
        StatusCode::NOT_IMPLEMENTED,
        "Context request not implemented",
        "Not Implemented",
        path,
    )
    .into_response()
}
fn internal(path: &str) -> Response {
    error_at(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Internal server error",
        "Internal Server Error",
        path,
    )
    .into_response()
}
fn dto(row: &Row, kind: &str, id: &str) -> Value {
    json!({"type":kind,"id":id,"data":row.get::<_,Value>(0),"createdAt":row.get::<_,String>(1),"updatedAt":row.get::<_,String>(2)})
}
const RETURNING: &str = "data,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')";
fn parse_data(bytes: &Bytes, create: bool) -> Option<(Option<String>, Option<String>, Value)> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    let obj = value.as_object()?;
    if obj
        .keys()
        .any(|key| !["id", "type", "data"].contains(&key.as_str()))
    {
        return None;
    }
    if !create && (obj.contains_key("type") || obj.contains_key("id")) {
        return None;
    }
    let kind = obj.get("type").and_then(Value::as_str).map(str::to_owned);
    let id = obj.get("id").and_then(Value::as_str).map(str::to_owned);
    if create && (!kind.as_deref().is_some_and(valid_id) || !id.as_deref().is_some_and(valid_id)) {
        return None;
    }
    let data = match obj.get("data") {
        None | Some(Value::Null) if create => json!({}),
        Some(value) if value.is_object() && value.to_string().len() <= 65536 => value.clone(),
        _ => return None,
    };
    Some((kind, id, data))
}

pub async fn create(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
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
    let Some((Some(kind), Some(id), data)) = parse_data(&bytes, true) else {
        return unsupported(&path);
    };
    let key = format!("{kind}:{id}");
    let query = format!(
        "INSERT INTO novu_rust_contexts(environment_id,context_key,data) VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING {RETURNING}"
    );
    match db.query_opt(&query, &[&env, &key, &data]).await {
        Ok(Some(row)) => (
            StatusCode::CREATED,
            Json(json!({"data":dto(&row,&kind,&id)})),
        )
            .into_response(),
        Ok(None) => error_at(
            StatusCode::CONFLICT,
            &format!("Context with type '{kind}' and id '{id}' already exists"),
            "Conflict",
            &path,
        )
        .into_response(),
        Err(_) => internal(&path),
    }
}

pub async fn get(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(env) => env,
        Err(e) => return e.into_response(),
    };
    if uri.query().is_some() || !valid_id(&kind) || !valid_id(&id) {
        return unsupported(&path);
    }
    let query = format!(
        "SELECT {RETURNING} FROM novu_rust_contexts WHERE environment_id=$1 AND context_key=$2"
    );
    match db.query_opt(&query, &[&env, &format!("{kind}:{id}")]).await {
        Ok(Some(row)) => Json(json!({"data":dto(&row,&kind,&id)})).into_response(),
        Ok(None) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Context with id '{id}' and type '{kind}' not found in environment {env}"),
            "Not Found",
            &path,
        )
        .into_response(),
        Err(_) => internal(&path),
    }
}

pub async fn update(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(env) => env,
        Err(e) => return e.into_response(),
    };
    if uri.query().is_some() || !valid_id(&kind) || !valid_id(&id) {
        return unsupported(&path);
    }
    let Some((_, _, data)) = parse_data(&bytes, false) else {
        return unsupported(&path);
    };
    let query = format!(
        "UPDATE novu_rust_contexts SET data=$3,updated_at=now() WHERE environment_id=$1 AND context_key=$2 RETURNING {RETURNING}"
    );
    match db
        .query_opt(&query, &[&env, &format!("{kind}:{id}"), &data])
        .await
    {
        Ok(Some(row)) => Json(json!({"data":dto(&row,&kind,&id)})).into_response(),
        Ok(None) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Context with type '{kind}' and id '{id}' not found"),
            "Not Found",
            &path,
        )
        .into_response(),
        Err(_) => internal(&path),
    }
}

pub async fn delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(env) => env,
        Err(e) => return e.into_response(),
    };
    if uri.query().is_some() || !valid_id(&kind) || !valid_id(&id) {
        return unsupported(&path);
    }
    match db
        .execute(
            "DELETE FROM novu_rust_contexts WHERE environment_id=$1 AND context_key=$2",
            &[&env, &format!("{kind}:{id}")],
        )
        .await
    {
        Ok(1) => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Context with id '{id}' and type '{kind}' not found in environment {env}"),
            "Not Found",
            &path,
        )
        .into_response(),
        Err(_) => internal(&path),
    }
}

pub async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(env) => env,
        Err(e) => return e.into_response(),
    };
    if params.keys().any(|k| {
        ![
            "type",
            "id",
            "search",
            "limit",
            "after",
            "before",
            "orderBy",
            "orderDirection",
            "includeCursor",
        ]
        .contains(&k.as_str())
    }) {
        return unsupported(&path);
    }
    let after = params.get("after").filter(|s| !s.is_empty());
    let before = params.get("before").filter(|s| !s.is_empty());
    if after.is_some() && before.is_some() {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Cannot specify both \"before\" and \"after\" cursors at the same time.",
            "Bad Request",
            &path,
        )
        .into_response();
    }
    let cursor = after.or(before);
    if cursor.is_some_and(|s| s.len() != 24 || !s.bytes().all(|b| b.is_ascii_hexdigit())) {
        return unsupported(&path);
    }
    let limit = match params.get("limit").map(|s| s.parse::<i64>()) {
        None | Some(Ok(0)) => 10,
        Some(Ok(n)) if (1..=1000).contains(&n) => n,
        _ => return unsupported(&path),
    };
    let order = match params.get("orderBy").map(String::as_str) {
        None | Some("") | Some("createdAt") => "created_at",
        Some("updatedAt") => "updated_at",
        _ => return unsupported(&path),
    };
    let descending = match params.get("orderDirection").map(String::as_str) {
        None | Some("DESC") => true,
        Some("ASC") => false,
        _ => return unsupported(&path),
    };
    let include = params.get("includeCursor").is_some_and(|s| s == "true");
    let kind = params.get("type").filter(|s| !s.is_empty());
    let id = params.get("id").filter(|s| !s.is_empty());
    let search = params.get("search").filter(|s| !s.is_empty());
    if let Some(cursor) = cursor {
        match db.query_opt("SELECT 1 FROM novu_rust_contexts WHERE environment_id=$1 AND internal_id=$2", &[&env, cursor]).await {
            Ok(Some(_)) => (),
            Ok(None) => return Json(json!({"data":[],"next":null,"previous":null,"totalCount":0,"totalCountCapped":false})).into_response(),
            Err(_) => return internal(&path),
        }
    }
    let filter = "environment_id=$1 AND ($2::text IS NULL OR split_part(context_key,':',1)=$2) AND ($3::text IS NULL OR split_part(context_key,':',2)=$3) AND ($4::text IS NULL OR position(lower($4) in lower(context_key))>0)";
    let count_sql = format!(
        "SELECT count(*) FROM (SELECT 1 FROM novu_rust_contexts WHERE {filter} LIMIT 50001) counted"
    );
    let count: i64 = match db.query_one(&count_sql, &[&env, &kind, &id, &search]).await {
        Ok(row) => row.get(0),
        Err(_) => return internal(&path),
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
        format!(
            " AND ({order},internal_id) {cmp} (SELECT {order},internal_id FROM novu_rust_contexts WHERE environment_id=$1 AND internal_id=$5)"
        )
    } else {
        String::new()
    };
    let sql = format!(
        "SELECT {RETURNING},context_key,internal_id FROM novu_rust_contexts WHERE {filter}{cursor_clause} ORDER BY {order} {sort},internal_id {sort} LIMIT $6"
    );
    // Keep the $5 parameter typed even without a cursor.
    let sql = if cursor.is_none() {
        sql.replace(" ORDER BY", " AND ($5::text IS NULL) ORDER BY")
    } else {
        sql
    };
    let mut rows = match db
        .query(&sql, &[&env, &kind, &id, &search, &cursor, &(limit + 1)])
        .await
    {
        Ok(rows) => rows,
        Err(_) => return internal(&path),
    };
    let extra = rows.len() > limit as usize;
    if extra {
        rows.truncate(limit as usize);
    }
    if reverse {
        rows.reverse();
    }
    let mut data = Vec::with_capacity(rows.len());
    for row in &rows {
        let key: String = row.get(3);
        let Some((kind, id)) = key.split_once(':') else {
            return internal(&path);
        };
        data.push(dto(row, kind, id));
    }
    let mut next: Option<String> = None;
    let mut previous: Option<String> = None;
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        if extra {
            if reverse {
                previous = Some(first.get(4));
            } else {
                next = Some(last.get(4));
            }
        }
        let boundary: String = if reverse { last.get(4) } else { first.get(4) };
        let other_cmp = match (descending, reverse) {
            (true, true) => "<",
            (true, false) => ">",
            (false, true) => ">",
            (false, false) => "<",
        };
        let probe = format!(
            "SELECT EXISTS(SELECT 1 FROM novu_rust_contexts WHERE {filter} AND ({order},internal_id) {other_cmp} (SELECT {order},internal_id FROM novu_rust_contexts WHERE environment_id=$1 AND internal_id=$5))"
        );
        let exists: bool = match db
            .query_one(&probe, &[&env, &kind, &id, &search, &boundary])
            .await
        {
            Ok(row) => row.get(0),
            Err(_) => return internal(&path),
        };
        if exists {
            if reverse {
                next = Some(boundary)
            } else {
                previous = Some(boundary)
            }
        }
    }
    Json(json!({"data":data,"next":next,"previous":previous,"totalCount":count.min(50000),"totalCountCapped":count>50000})).into_response()
}
