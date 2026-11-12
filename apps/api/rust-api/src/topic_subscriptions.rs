//! Experimental v2 topic subscription creation for the default context flag.
use crate::{
    authenticate, error_at, topic_subscription_preferences as subscription_preferences, topics_v2,
    validation_error,
};
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc};
use tokio_postgres::Client;

fn failure(path: &str) -> Response {
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"statusCode":500,"timestamp":timestamp,"path":path,"message":"Internal server error, contact support and provide them with the errorId","errorId":uuid::Uuid::new_v4().to_string()}))).into_response()
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
fn validation(path: &str, messages: Vec<&str>) -> Response {
    let mut result = validation_error(messages);
    result.1.0["path"] = json!(path);
    result.into_response()
}
fn output(path: &str, data: Vec<Value>, total: usize, errors: Vec<Value>) -> Response {
    let mut result = json!({"data":data,"meta":{"totalCount":total,"successful":data.len(),"failed":errors.len()}});
    if !errors.is_empty() {
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
        (StatusCode::CREATED, Json(result)).into_response()
    }
}
#[derive(Clone)]
struct Input {
    subscriber: String,
    identifier: Option<String>,
    name: Option<String>,
    name_present: bool,
}

pub async fn create(
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
    if obj.get("context").is_some_and(|v| !v.is_null()) {
        return unsupported(&path);
    }
    let workflows =
        match subscription_preferences::resolve(&db, &env, obj.get("preferences"), &path).await {
            Ok(v) => v,
            Err(e) => return e.into_response(),
        };
    let Some(raw) = obj
        .get("subscriptions")
        .filter(|v| !v.is_null())
        .or_else(|| obj.get("subscriberIds").filter(|v| !v.is_null()))
    else {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
            "statusCode":422, "timestamp":timestamp, "path":path,
            "message":"Validation failed for CreateSubscriptionsCommand: subscriptions: At least one subscription is required",
            "ctx":{}, "errors":{"subscriptions":{"messages":["At least one subscription is required"],"value":[]}}
        }))).into_response();
    };
    let Some(array) = raw.as_array() else {
        return validation(&path, vec!["subscriptions must be an array"]);
    };
    if array.is_empty() {
        return validation(
            &path,
            vec![if obj.get("subscriptions").is_some_and(|v| !v.is_null()) {
                "At least one subscription is required"
            } else {
                "At least one subscriber identifier is required"
            }],
        );
    }
    if array.len() > 100 {
        return validation(
            &path,
            vec![if obj.get("subscriptions").is_some_and(|v| !v.is_null()) {
                "Cannot subscribe more than 100 subscriptions at once"
            } else {
                "Cannot subscribe more than 100 subscribers at once"
            }],
        );
    }
    let explicit = obj.get("subscriptions").is_some_and(|v| !v.is_null());
    let mut inputs = Vec::new();
    for item in array {
        if let Some(id) = item.as_str() {
            if explicit {
                return validation(
                    &path,
                    vec![
                        "subscriptions.each value in nested property subscriptions must be either object or array",
                    ],
                );
            }
            inputs.push(Input {
                subscriber: id.to_owned(),
                identifier: None,
                name: None,
                name_present: false,
            });
        } else if explicit {
            let Some(id) = item.get("subscriberId").and_then(Value::as_str) else {
                return unsupported(&path);
            };
            if item
                .get("identifier")
                .is_some_and(|v| !v.is_null() && !v.is_string())
                || item
                    .get("name")
                    .is_some_and(|v| !v.is_null() && !v.is_string())
            {
                return unsupported(&path);
            }
            inputs.push(Input {
                subscriber: id.to_owned(),
                identifier: item
                    .get("identifier")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                name: item.get("name").and_then(Value::as_str).map(str::to_owned),
                name_present: item.get("name").is_some(),
            });
        } else {
            return validation(&path, vec!["each value in subscriberIds must be a string"]);
        }
    }
    let name = match obj.get("name") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.as_str()),
        _ => return validation(&path, vec!["name must be a string"]),
    };
    let mut errors = Vec::new();
    let valid:Vec<Input>=inputs.iter().filter_map(|item| {
        if item.identifier.as_ref().is_some_and(|s|s.encode_utf16().count()>512) {
            errors.push(json!({"subscriberId":item.subscriber,"code":"INVALID_SUBSCRIPTION_IDENTIFIER","message":"Subscription identifier must not exceed 512 characters."})); None
        } else { Some(item.clone()) }
    }).collect();
    if valid.is_empty() {
        return output(&path, vec![], inputs.len(), errors);
    }
    if !topics_v2::valid_key(&topic) {
        match db.query_opt("SELECT 1 FROM novu_rust_topics WHERE environment_id=$1 AND topic_key=$2",&[&env,&topic]).await {
            Ok(None)=>return error_at(StatusCode::BAD_REQUEST,&format!("Invalid topic key: \"{topic}\". Topic keys must contain only alphanumeric characters (a-z, A-Z, 0-9), hyphens (-), underscores (_), colons (:), or be a valid email address."),"Bad Request",&path).into_response(),
            Err(_)=>return failure(&path), _=>{}
        }
    }
    let name_update = name.filter(|s| !s.is_empty());
    let topic_row=match db.query_one("INSERT INTO novu_rust_topics AS t(environment_id,topic_key,internal_id,name,name_present) VALUES ($1,$2,novu_rust_new_objectid(),$3,$3::text IS NOT NULL) ON CONFLICT(environment_id,topic_key) DO UPDATE SET name=CASE WHEN $4::text IS NOT NULL THEN $4 ELSE t.name END,name_present=CASE WHEN $4::text IS NOT NULL THEN true ELSE t.name_present END,updated_at=CASE WHEN $4::text IS NOT NULL THEN now() ELSE t.updated_at END RETURNING internal_id,name,name_present,data",&[&env,&topic,&name,&name_update]).await { Ok(v)=>v,Err(_)=>return failure(&path) };
    let mut topic_dto = json!({"_id":topic_row.get::<_,String>(0),"key":topic});
    if topic_row.get::<_, bool>(2) {
        topic_dto["name"] = json!(topic_row.get::<_, Option<String>>(1));
    }
    if let Some(data) = topic_row.get::<_, Option<Value>>(3) {
        topic_dto["data"] = data;
    }
    let ids: Vec<String> = valid.iter().map(|v| v.subscriber.clone()).collect();
    let found=match db.query("SELECT subscriber_id,internal_id,profile,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=ANY($2) ORDER BY internal_id",&[&env,&ids]).await { Ok(v)=>v,Err(_)=>return failure(&path) };
    let found_ids: HashSet<String> = found.iter().map(|r| r.get(0)).collect();
    for item in &valid {
        if !found_ids.contains(&item.subscriber) {
            errors.push(json!({"subscriberId":item.subscriber,"code":"SUBSCRIBER_NOT_FOUND","message":format!("Subscriber with ID '{}' could not be found.",item.subscriber)}));
        }
    }
    let mut existing = Vec::new();
    let mut created = Vec::new();
    for subscriber in found {
        let sid: String = subscriber.get(0);
        let input = valid
            .iter()
            .find(|v| v.subscriber == sid)
            .expect("found subscriber in request");
        let identifier = input
            .identifier
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("tk_{topic}:si_{sid}"));
        let lookup=match db.query_opt("SELECT internal_id,name,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),context_keys,subscriber_id,name_present FROM novu_rust_topic_subscribers WHERE environment_id=$1 AND topic_key=$2 AND identifier=$3",&[&env,&topic,&identifier]).await { Ok(v)=>v,Err(_)=>return failure(&path) };
        let (id, sub_name, name_present, created_at, updated_at, keys, is_new) = if let Some(row) =
            lookup
        {
            if row.get::<_, String>(5) != sid {
                return unsupported(&path);
            }
            (
                row.get::<_, String>(0),
                row.get::<_, Option<String>>(1),
                row.get::<_, bool>(6),
                row.get::<_, String>(2),
                row.get::<_, String>(3),
                row.get::<_, Option<Vec<String>>>(4),
                false,
            )
        } else {
            let count:i64=match db.query_one("SELECT count(*) FROM novu_rust_topic_subscribers WHERE environment_id=$1 AND topic_key=$2 AND subscriber_id=$3",&[&env,&topic,&sid]).await { Ok(v)=>v.get(0),Err(_)=>return failure(&path) };
            if count >= 10 {
                errors.push(json!({"subscriberId":sid,"code":"SUBSCRIPTION_LIMIT_EXCEEDED","message":format!("Subscriber {sid} has reached the maximum allowed of 10 subscriptions for topic \"{topic}\"")}));
                continue;
            }
            let row=match db.query_opt("INSERT INTO novu_rust_topic_subscribers(environment_id,topic_key,subscriber_id,identifier,name,name_present) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(environment_id,identifier) DO NOTHING RETURNING internal_id",&[&env,&topic,&sid,&identifier,&input.name,&input.name_present]).await {Ok(Some(v))=>v,Ok(None)=>return unsupported(&path),Err(_)=>return failure(&path)};
            (
                row.get::<_, String>(0),
                input.name.clone(),
                input.name_present,
                String::new(),
                String::new(),
                None,
                true,
            )
        };
        let profile: Value = subscriber.get(2);
        let mut subscriber_dto = json!({"_id":subscriber.get::<_,String>(1),"subscriberId":sid,"createdAt":subscriber.get::<_,String>(3),"updatedAt":subscriber.get::<_,String>(4)});
        for field in ["avatar", "firstName", "lastName", "email"] {
            if let Some(v) = profile.get(field) {
                subscriber_dto[field] = v.clone();
            }
        }
        let mut result = json!({"_id":id,"identifier":identifier,"topic":topic_dto,"subscriber":subscriber_dto,"createdAt":created_at,"updatedAt":updated_at});
        if name_present {
            result["name"] = json!(sub_name);
        }
        if let Some(k) = keys {
            result["contextKeys"] = json!(k);
        }
        if let Some(workflows) = workflows.as_ref().filter(|v| !v.is_empty()) {
            let subscriber_internal_id: String = subscriber.get(1);
            let preferences = if is_new {
                subscription_preferences::insert_new(
                    &db,
                    &env,
                    &subscriber_internal_id,
                    &id,
                    &identifier,
                    workflows,
                )
                .await
            } else {
                subscription_preferences::fetch(
                    &db,
                    &env,
                    &subscriber_internal_id,
                    &id,
                    &identifier,
                    workflows,
                )
                .await
            };
            match preferences {
                Ok(preferences) if !preferences.is_empty() => {
                    result["preferences"] = json!(preferences)
                }
                Ok(_) => {}
                Err(_) => return failure(&path),
            }
        }
        if is_new {
            created.push(result);
        } else {
            existing.push(result);
        }
    }
    existing.extend(created);
    output(&path, existing, inputs.len(), errors)
}

/// List topic subscriptions using the reference's ObjectId cursor ordering.
pub async fn list(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(topic): Path<String>,
    OriginalUri(uri): OriginalUri,
    axum::extract::Query(pairs): axum::extract::Query<Vec<(String, String)>>,
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
        "subscriberId",
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
            return validation(&path, vec!["limit must not be greater than 100"]);
        }
        _ => return unsupported(&path),
    };
    let topic_row = match db.query_opt("SELECT internal_id,name,name_present,data,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM novu_rust_topics WHERE environment_id=$1 AND topic_key=$2", &[&env,&topic]).await {
        Ok(Some(v))=>v,
        Ok(None)=>return error_at(StatusCode::NOT_FOUND,&format!("Topic with key {topic} not found"),"Not Found",&path).into_response(),
        Err(_)=>return failure(&path),
    };
    let mut topic_dto = json!({"_id":topic_row.get::<_,String>(0),"key":topic,"createdAt":topic_row.get::<_,String>(4),"updatedAt":topic_row.get::<_,String>(5)});
    if topic_row.get::<_, bool>(2) {
        topic_dto["name"] = json!(topic_row.get::<_, Option<String>>(1));
    }
    if let Some(data) = topic_row.get::<_, Option<Value>>(3) {
        topic_dto["data"] = data;
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
            let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            return (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"statusCode":500,"timestamp":timestamp,"path":path,"message":"Internal server error, contact support and provide them with the errorId","errorId":uuid::Uuid::new_v4().to_string()}))).into_response();
        }
        match db.query_opt("SELECT 1 FROM novu_rust_topic_subscribers WHERE environment_id=$1 AND internal_id=$2",&[&env,id]).await {Ok(Some(_))=>{},Ok(None)=>return empty(),Err(_)=>return failure(&path)};
    }
    let subscriber = get("subscriberId").filter(|v| !v.is_empty());
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
    let filter = "m.environment_id=$1 AND m.topic_key=$2 AND ($3::text IS NULL OR m.subscriber_id=$3) AND ($4::text[] IS NULL OR (cardinality($4)=0 AND COALESCE(cardinality(m.context_keys),0)=0) OR (cardinality($4)>0 AND cardinality(m.context_keys)=cardinality($4) AND m.context_keys @> $4))";
    let count_sql = format!(
        "SELECT count(*) FROM (SELECT 1 FROM novu_rust_topic_subscribers m WHERE {filter} LIMIT 50001) counted"
    );
    let count: i64 = match db
        .query_one(&count_sql, &[&env, &topic, &subscriber, &context])
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
        "SELECT m.internal_id,m.identifier,m.context_keys,to_char(m.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),s.internal_id,s.subscriber_id,s.profile FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE {filter} AND ($5::text IS NULL OR m.internal_id {cmp} $5) ORDER BY m.internal_id {direction} LIMIT $6"
    );
    let mut rows = match db
        .query(
            &query,
            &[&env, &topic, &subscriber, &context, &cursor, &(limit + 1)],
        )
        .await
    {
        Ok(v) => v,
        Err(_) => return failure(&path),
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
            .query_one(&probe, &[&env, &topic, &subscriber, &context, &boundary])
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
    let data:Vec<Value>=rows.iter().map(|row| {
        let profile:Value=row.get(6);
        let mut subscriber=json!({"_id":row.get::<_,String>(4),"subscriberId":row.get::<_,String>(5)});
        for field in ["firstName","lastName","email","avatar"] {if let Some(v)=profile.get(field){subscriber[field]=v.clone();}}
        let mut item=json!({"_id":row.get::<_,String>(0),"identifier":row.get::<_,String>(1),"createdAt":row.get::<_,String>(3),"topic":topic_dto,"subscriber":subscriber});
        if let Some(keys)=row.get::<_,Option<Vec<String>>>(2) {item["contextKeys"]=json!(keys);}
        item
    }).collect();
    Json(json!({"data":data,"next":next,"previous":previous,"totalCount":count.min(50000),"totalCountCapped":count>50000})).into_response()
}

/// Update a subscription and optionally replace its preferences.
pub async fn update(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((topic, identifier)): Path<(String, String)>,
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
    if obj
        .get("name")
        .is_some_and(|v| !v.is_null() && !v.is_string())
    {
        return validation(&path, vec!["name must be a string"]);
    }
    let workflows =
        match subscription_preferences::resolve(&db, &env, obj.get("preferences"), &path).await {
            Ok(v) => v,
            Err(e) => return e.into_response(),
        };
    let name = obj.get("name").and_then(Value::as_str);
    let has_name = obj.contains_key("name");
    let identifier = identifier
        .rsplit_once(":ctx_")
        .map_or(identifier.as_str(), |(base, _)| base);
    match db
        .query_opt(
            "SELECT 1 FROM novu_rust_topics WHERE environment_id=$1 AND topic_key=$2",
            &[&env, &topic],
        )
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return error_at(
                StatusCode::NOT_FOUND,
                &format!("Topic with key {topic} not found"),
                "Not Found",
                &path,
            )
            .into_response();
        }
        Err(_) => return failure(&path),
    }
    let mut updated_preferences = Vec::new();
    if let Some(workflows) = workflows.as_ref() {
        let target=match db.query_opt("SELECT m.internal_id,s.internal_id FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE m.environment_id=$1 AND m.topic_key=$2 AND m.identifier=$3",&[&env,&topic,&identifier]).await {Ok(v)=>v,Err(_)=>return failure(&path)};
        if let Some(target) = target {
            let subscription_id: String = target.get(0);
            let subscriber_id: String = target.get(1);
            updated_preferences = match subscription_preferences::replace(
                &db,
                &env,
                &subscriber_id,
                &subscription_id,
                identifier,
                workflows,
            )
            .await
            {
                Ok(v) => v,
                Err(_) => return failure(&path),
            };
        }
    }
    let select = "SELECT m.internal_id,m.identifier,m.name,m.name_present,m.context_keys,to_char(m.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(m.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),t.internal_id,t.name,t.name_present,t.data,s.internal_id,s.subscriber_id,s.profile,to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')";
    let joins = "JOIN novu_rust_topics t ON t.environment_id=m.environment_id AND t.topic_key=m.topic_key LEFT JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id";
    let query = if has_name {
        format!(
            "WITH m AS (UPDATE novu_rust_topic_subscribers SET name=$4,name_present=true,updated_at=now() WHERE environment_id=$1 AND topic_key=$2 AND identifier=$3 RETURNING *) {select} FROM m {joins}"
        )
    } else {
        format!(
            "{select} FROM novu_rust_topic_subscribers m {joins} WHERE m.environment_id=$1 AND m.topic_key=$2 AND m.identifier=$3"
        )
    };
    let row = if has_name {
        db.query_opt(&query, &[&env, &topic, &identifier, &name])
            .await
    } else {
        db.query_opt(&query, &[&env, &topic, &identifier]).await
    };
    match row {
        Ok(Some(row)) => {
            let mut topic_dto = json!({"_id":row.get::<_,String>(7),"key":topic});
            if row.get::<_, bool>(9) {
                topic_dto["name"] = json!(row.get::<_, Option<String>>(8));
            }
            if let Some(data) = row.get::<_, Option<Value>>(10) {
                topic_dto["data"] = data;
            }
            let subscriber = if let Some(id) = row.get::<_, Option<String>>(11) {
                let mut subscriber = json!({"_id":id,"subscriberId":row.get::<_,String>(12),"createdAt":row.get::<_,String>(14),"updatedAt":row.get::<_,String>(15)});
                let profile: Value = row.get(13);
                for field in ["avatar", "firstName", "lastName", "email"] {
                    if let Some(v) = profile.get(field) {
                        subscriber[field] = v.clone();
                    }
                }
                subscriber
            } else {
                Value::Null
            };
            let mut result = json!({"_id":row.get::<_,String>(0),"identifier":row.get::<_,String>(1),"topic":topic_dto,"subscriber":subscriber,"createdAt":row.get::<_,String>(5),"updatedAt":row.get::<_,String>(6)});
            if row.get::<_, bool>(3) {
                result["name"] = json!(row.get::<_, Option<String>>(2));
            }
            if let Some(keys) = row.get::<_, Option<Vec<String>>>(4) {
                result["contextKeys"] = json!(keys);
            }
            if !updated_preferences.is_empty() {
                result["preferences"] = json!(updated_preferences);
            }
            Json(json!({"data":result})).into_response()
        }
        Ok(None) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Subscription with identifier {identifier} not found for topic {topic}"),
            "Not Found",
            &path,
        )
        .into_response(),
        Err(_) => failure(&path),
    }
}
