//! Bounded API-key subscriber search using the reference cursor contract.
use crate::{authenticate, error_at, subscriber_response};
use axum::{
    Json,
    extract::{OriginalUri, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::Client;

fn unsupported(path: &str) -> Response {
    error_at(
        StatusCode::NOT_IMPLEMENTED,
        "Subscriber search option not implemented",
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
fn empty() -> Response {
    Json(json!({"data":[],"next":null,"previous":null,"totalCount":0,"totalCountCapped":false}))
        .into_response()
}

pub async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(env) => env,
        Err(e) => return e.into_response(),
    };
    if params.keys().any(|k| {
        ![
            "email",
            "phone",
            "subscriberId",
            "name",
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
    if cursor.is_some_and(|s| {
        s.len() != 24
            || !s
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    }) {
        return empty();
    }
    let limit = match params.get("limit").map(|s| s.parse::<i64>()) {
        None | Some(Ok(0)) => 10,
        Some(Ok(n)) if (1..=1000).contains(&n) => n,
        _ => return unsupported(&path),
    };
    let order = match params.get("orderBy").map(String::as_str) {
        None | Some("") | Some("_id") => "internal_id",
        Some("updatedAt") => "updated_at",
        _ => return unsupported(&path),
    };
    let descending = match params.get("orderDirection").map(String::as_str) {
        None | Some("DESC") => true,
        Some("ASC") => false,
        _ => return unsupported(&path),
    };
    let include = params.get("includeCursor").is_some_and(|s| s == "true");
    let email = params.get("email").filter(|s| !s.is_empty());
    let phone = params.get("phone").filter(|s| !s.is_empty());
    let subscriber = params.get("subscriberId").filter(|s| !s.is_empty());
    let name = params.get("name").filter(|s| !s.is_empty());
    if let Some(cursor) = cursor {
        match db
            .query_opt(
                "SELECT 1 FROM novu_rust_subscribers WHERE environment_id=$1 AND internal_id=$2",
                &[&env, cursor],
            )
            .await
        {
            Ok(Some(_)) => (),
            Ok(None) => return empty(),
            Err(_) => return internal(&path),
        }
    }
    let filter = "environment_id=$1 AND ($2::text IS NULL OR profile->>'email'=$2) AND ($3::text IS NULL OR profile->>'phone'=$3) AND ($4::text IS NULL OR subscriber_id=$4) AND ($5::text IS NULL OR position(lower($5) in lower(trim(COALESCE(profile->>'firstName','') || ' ' || COALESCE(profile->>'lastName',''))))>0)";
    let count_sql = format!(
        "SELECT count(*) FROM (SELECT 1 FROM novu_rust_subscribers WHERE {filter} LIMIT 50001) c"
    );
    let count: i64 = match db
        .query_one(&count_sql, &[&env, &email, &phone, &subscriber, &name])
        .await
    {
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
            " AND ({order},internal_id) {cmp} (SELECT {order},internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND internal_id=$6)"
        )
    } else {
        " AND ($6::text IS NULL)".to_owned()
    };
    let sql = format!(
        "SELECT internal_id,profile,(SELECT organization_id FROM novu_rust_environments WHERE id=$1),to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),subscriber_id,is_online,to_char(last_online_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at FROM novu_rust_subscribers WHERE {filter}{cursor_clause} ORDER BY {order} {sort},internal_id {sort} LIMIT $7"
    );
    let mut rows = match db
        .query(
            &sql,
            &[
                &env,
                &email,
                &phone,
                &subscriber,
                &name,
                &cursor,
                &(limit + 1),
            ],
        )
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
        let probe = format!(
            "SELECT EXISTS(SELECT 1 FROM novu_rust_subscribers WHERE {filter} AND ({order},internal_id) {other_cmp} (SELECT {order},internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND internal_id=$6))"
        );
        let exists: bool = match db
            .query_one(
                &probe,
                &[&env, &email, &phone, &subscriber, &name, &boundary],
            )
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
    // Fetch page credentials in one environment-scoped query, rather than one
    // lookup for every subscriber. Legacy rows without an integration identity
    // are omitted consistently with the individual subscriber response.
    let subscriber_ids: Vec<String> = rows.iter().map(|row| row.get(5)).collect();
    let mut channels = HashMap::new();
    if !subscriber_ids.is_empty() {
        let credential_rows = match db.query("SELECT subscriber_id,integration_internal_id,device_tokens FROM novu_rust_fcm_devices WHERE environment_id=$1 AND subscriber_id=ANY($2) AND integration_internal_id IS NOT NULL ORDER BY channel_order", &[&env, &subscriber_ids]).await {
            Ok(rows) => rows,
            Err(_) => return internal(&path),
        };
        for row in credential_rows {
            channels.entry(row.get::<_, String>(0)).or_insert_with(Vec::new).push(
                json!({"providerId":"fcm","_integrationId":row.get::<_,String>(1),"credentials":{"deviceTokens":row.get::<_,Vec<String>>(2)}}),
            );
        }
    }
    let data: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let id: String = row.get(5);
            let mut data = subscriber_response(row, &id, &env, false).1.0["data"].clone();
            if let Some(value) = channels.remove(&id) {
                data["channels"] = json!(value);
            }
            data
        })
        .collect();
    Json(json!({"data":data,"next":next,"previous":previous,"totalCount":count.min(50000),"totalCountCapped":count>50000})).into_response()
}
