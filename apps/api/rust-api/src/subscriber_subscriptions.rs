//! Experimental subscriber-scoped topic subscription listing.
use crate::{authenticate, error_at, topic_subscription_preferences, validation_error};
use axum::{
    Json,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::Client;

fn failure(path: &str) -> Response {
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"statusCode":500,"timestamp":timestamp,"path":path,"message":"Internal server error, contact support and provide them with the errorId","errorId":uuid::Uuid::new_v4().to_string()}))).into_response()
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

pub async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber): Path<String>,
    OriginalUri(uri): OriginalUri,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Response {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let get = |key: &str| {
        pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    if [
        "limit",
        "after",
        "before",
        "key",
        "orderDirection",
        "orderBy",
        "includeCursor",
    ]
    .iter()
    .any(|key| pairs.iter().filter(|(k, _)| k == key).count() > 1)
    {
        return unsupported(&path);
    }
    let limit = match get("limit").map(|v| v.parse::<i64>()) {
        None | Some(Ok(0)) => 10,
        Some(Ok(v)) if (1..=100).contains(&v) => v,
        Some(Ok(v)) if v > 100 => {
            let mut result = validation_error(vec!["limit must not be greater than 100"]);
            result.1.0["path"] = json!(path);
            return result.into_response();
        }
        _ => return unsupported(&path),
    };
    let row=match db.query_opt("SELECT internal_id,profile FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2",&[&env,&subscriber]).await {
        Ok(Some(v))=>v,
        Ok(None)=>return error_at(StatusCode::NOT_FOUND,"Subscriber not found","Not Found",&path).into_response(),
        Err(_)=>return failure(&path),
    };
    let internal_id: String = row.get(0);
    let profile: Value = row.get(1);
    let mut subscriber_dto = json!({"_id":internal_id,"subscriberId":subscriber});
    for field in ["firstName", "lastName", "email", "avatar"] {
        if let Some(value) = profile.get(field) {
            subscriber_dto[field] = value.clone();
        }
    }
    let after = get("after").filter(|v| !v.is_empty());
    let before = get("before").filter(|v| !v.is_empty());
    if before.is_some() && after.is_some() {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Cannot specify both \"before\" and \"after\" cursors at the same time.",
            "Bad Request",
            &path,
        )
        .into_response();
    }
    let cursor = after.or(before).map(str::to_ascii_lowercase);
    let empty = || {
        Json(json!({"data":[],"next":null,"previous":null,"totalCount":0,"totalCountCapped":false}))
            .into_response()
    };
    if let Some(id) = cursor.as_ref() {
        if id.len() != 24
            || !id
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return failure(&path);
        }
        match db.query_opt("SELECT 1 FROM novu_rust_topic_subscribers WHERE environment_id=$1 AND internal_id=$2",&[&env,id]).await {Ok(Some(_))=>{},Ok(None)=>return empty(),Err(_)=>return failure(&path)}
    }
    let key = get("key").filter(|v| !v.is_empty());
    let context: Option<Vec<String>> = if pairs.iter().any(|(k, _)| k == "contextKeys") {
        Some(
            pairs
                .iter()
                .filter(|(k, v)| k == "contextKeys" && !v.is_empty())
                .map(|(_, v)| v.clone())
                .collect(),
        )
    } else {
        None
    };
    let filter = "m.environment_id=$1 AND m.subscriber_id=$2 AND ($3::text IS NULL OR m.topic_key=$3) AND ($4::text[] IS NULL OR (cardinality($4)=0 AND COALESCE(cardinality(m.context_keys),0)=0) OR (cardinality($4)>0 AND cardinality(m.context_keys)=cardinality($4) AND m.context_keys @> $4))";
    let count_sql = format!(
        "SELECT count(*) FROM (SELECT 1 FROM novu_rust_topic_subscribers m WHERE {filter} LIMIT 50001) counted"
    );
    let count: i64 = match db
        .query_one(&count_sql, &[&env, &subscriber, &key, &context])
        .await
    {
        Ok(v) => v.get(0),
        Err(_) => return failure(&path),
    };
    let reverse = before.is_some();
    let descending = get("orderDirection") != Some("ASC");
    let ascending = descending == reverse;
    let direction = if ascending { "ASC" } else { "DESC" };
    let inclusive = get("includeCursor") == Some("true");
    let cmp = match (ascending, inclusive) {
        (true, false) => ">",
        (true, true) => ">=",
        (false, false) => "<",
        (false, true) => "<=",
    };
    let query = format!(
        "SELECT m.internal_id,m.identifier,m.context_keys,to_char(m.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),t.internal_id,t.topic_key,t.name,t.name_present,t.data,to_char(t.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(t.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM novu_rust_topic_subscribers m JOIN novu_rust_topics t ON t.environment_id=m.environment_id AND t.topic_key=m.topic_key WHERE {filter} AND ($5::text IS NULL OR m.internal_id {cmp} $5) ORDER BY m.internal_id {direction} LIMIT $6"
    );
    let mut rows = match db
        .query(
            &query,
            &[&env, &subscriber, &key, &context, &cursor, &(limit + 1)],
        )
        .await
    {
        Ok(v) => v,
        Err(_) => return failure(&path),
    };
    let extra = rows.len() > limit as usize;
    if extra {
        rows.truncate(limit as usize)
    }
    if reverse {
        rows.reverse()
    }
    let mut next: Option<String> = None;
    let mut previous: Option<String> = None;
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        if extra {
            if reverse {
                previous = Some(first.get(0))
            } else {
                next = Some(last.get(0))
            }
        }
        let boundary: String = if reverse { last.get(0) } else { first.get(0) };
        let cmp = match (descending, reverse) {
            (true, true) => "<",
            (true, false) => ">",
            (false, true) => ">",
            (false, false) => "<",
        };
        let probe = format!(
            "SELECT EXISTS(SELECT 1 FROM novu_rust_topic_subscribers m WHERE {filter} AND m.internal_id {cmp} $5)"
        );
        let exists: bool = match db
            .query_one(&probe, &[&env, &subscriber, &key, &context, &boundary])
            .await
        {
            Ok(v) => v.get(0),
            Err(_) => return failure(&path),
        };
        if exists {
            if reverse {
                next = Some(boundary)
            } else {
                previous = Some(boundary)
            }
        }
    }
    let subscription_ids: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    let identifiers: HashMap<String, String> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    let mut by_subscription: HashMap<String, Vec<Value>> = HashMap::new();
    if !subscription_ids.is_empty() {
        let preferences=match db.query("SELECT p.subscription_internal_id,p.workflow_internal_id,p.preferences,w.identifier,w.inbox_metadata FROM novu_rust_subscription_preferences p LEFT JOIN novu_rust_workflows w ON w.environment_id=p.environment_id AND w.inbox_metadata->>'id'=p.workflow_internal_id WHERE p.environment_id=$1 AND p.subscriber_internal_id=$2 AND p.subscription_internal_id=ANY($3) ORDER BY p.created_at,p.workflow_internal_id,p.context_keys_hash",&[&env,&internal_id,&subscription_ids]).await {Ok(v)=>v,Err(_)=>return failure(&path)};
        for preference in preferences {
            let subscription_id: String = preference.get(0);
            let workflow_id: String = preference.get(1);
            let settings: Value = preference.get(2);
            let workflow: Option<String> = preference.get(3);
            let metadata: Option<Value> = preference.get(4);
            if let Some(identifier) = identifiers.get(&subscription_id) {
                let item = topic_subscription_preferences::stored_dto(
                    identifier,
                    &workflow_id,
                    workflow.as_deref(),
                    metadata.as_ref(),
                    &settings,
                );
                by_subscription
                    .entry(subscription_id)
                    .or_default()
                    .push(item);
            }
        }
    }
    let data:Vec<Value>=rows.iter().map(|row|{
        let id:String=row.get(0);let mut topic=json!({"_id":row.get::<_,String>(4),"key":row.get::<_,String>(5),"createdAt":row.get::<_,String>(9),"updatedAt":row.get::<_,String>(10)});
        if row.get::<_,bool>(7){topic["name"]=json!(row.get::<_,Option<String>>(6));}
        if let Some(data)=row.get::<_,Option<Value>>(8){topic["data"]=data;}
        let mut item=json!({"_id":id,"identifier":row.get::<_,String>(1),"createdAt":row.get::<_,String>(3),"topic":topic,"subscriber":subscriber_dto});
        if let Some(keys)=row.get::<_,Option<Vec<String>>>(2){item["contextKeys"]=json!(keys);}
        if let Some(preferences)=by_subscription.get(&id){item["preferences"]=json!(preferences);}
        item
    }).collect();
    Json(json!({"data":data,"next":next,"previous":previous,"totalCount":count.min(50000),"totalCountCapped":count>50000})).into_response()
}
