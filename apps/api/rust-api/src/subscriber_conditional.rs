//! API-key scoped conditional subscriber profile management.
use super::{ApiError, authenticate, error_at, profile_errors};
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;

fn err(code: StatusCode, path: &str) -> ApiError {
    let (message, label) = match code {
        StatusCode::BAD_REQUEST => ("Invalid request", "Bad Request"),
        StatusCode::NOT_FOUND => ("Subscriber was not found", "Not Found"),
        StatusCode::CONFLICT => ("Subscriber changed", "Conflict"),
        _ => ("Internal server error", "Internal Server Error"),
    };
    error_at(code, message, label, path)
}

pub(super) async fn get(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> ApiError {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    match db.query_opt("SELECT management_generation,profile_revision::text,profile FROM public.novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2",&[&env,&id]).await {
        Ok(Some(row)) => (StatusCode::OK,Json(json!({"data":{"subscriberId":id,"generation":row.get::<_,Uuid>(0).to_string(),"revision":row.get::<_,String>(1),"profile":row.get::<_,Value>(2)}}))),
        Ok(None)=>err(StatusCode::NOT_FOUND,&path),Err(_)=>err(StatusCode::INTERNAL_SERVER_ERROR,&path),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConditionalChange {
    generation: String,
    revision: String,
    profile: Option<Value>,
}

pub(super) async fn patch(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiError {
    change(db, headers, id, uri, body, false).await
}
pub(super) async fn delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiError {
    change(db, headers, id, uri, body, true).await
}
async fn change(
    db: Arc<Client>,
    headers: HeaderMap,
    id: String,
    uri: axum::http::Uri,
    body: Bytes,
    deleting: bool,
) -> ApiError {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let Ok(change) = serde_json::from_slice::<ConditionalChange>(&body) else {
        return err(StatusCode::BAD_REQUEST, &path);
    };
    let Ok(generation) = Uuid::parse_str(&change.generation) else {
        return err(StatusCode::BAD_REQUEST, &path);
    };
    let Ok(revision) = change.revision.parse::<i64>() else {
        return err(StatusCode::BAD_REQUEST, &path);
    };
    if revision < 0 || revision.to_string() != change.revision {
        return err(StatusCode::BAD_REQUEST, &path);
    }
    if deleting && change.profile.is_some() {
        return err(StatusCode::BAD_REQUEST, &path);
    }
    let profile = if deleting {
        json!({})
    } else {
        let Some(Value::Object(fields)) = change.profile else {
            return err(StatusCode::BAD_REQUEST, &path);
        };
        if fields.is_empty()
            || fields.keys().any(|k| {
                ![
                    "firstName",
                    "lastName",
                    "email",
                    "phone",
                    "avatar",
                    "locale",
                    "timezone",
                    "data",
                ]
                .contains(&k.as_str())
            })
            || !profile_errors(&fields).is_empty()
            || fields
                .get("email")
                .and_then(Value::as_str)
                .is_some_and(|v| novu_rust_contracts::email::validate(v).is_none())
        {
            return err(StatusCode::BAD_REQUEST, &path);
        }
        Value::Object(fields)
    };
    let sql = if deleting {
        "WITH target AS MATERIALIZED (SELECT environment_id,subscriber_id,internal_id,management_generation,profile_revision FROM public.novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), matched AS MATERIALIZED (SELECT * FROM target WHERE management_generation=$3 AND profile_revision=$4), messages AS (DELETE FROM public.novu_rust_in_app_messages m USING matched t WHERE m.environment_id=t.environment_id AND m.subscriber_id=t.subscriber_id AND m.subscriber_internal_id=t.internal_id RETURNING 1), preferences AS (DELETE FROM public.novu_rust_subscription_preferences p USING matched t WHERE p.environment_id=t.environment_id AND p.subscriber_internal_id=t.internal_id RETURNING 1), email_messages AS (DELETE FROM public.novu_rust_email_messages m USING matched t WHERE m.environment_id=t.environment_id AND m.subscriber_internal_id=t.internal_id RETURNING 1), sms_messages AS (DELETE FROM public.novu_rust_sms_messages m USING matched t WHERE m.environment_id=t.environment_id AND m.subscriber_internal_id=t.internal_id RETURNING 1), push_messages AS (DELETE FROM public.novu_rust_push_messages m USING matched t WHERE m.environment_id=t.environment_id AND m.subscriber_internal_id=t.internal_id RETURNING 1), removed AS (DELETE FROM public.novu_rust_subscribers s USING matched t WHERE s.environment_id=t.environment_id AND s.subscriber_id=t.subscriber_id AND (SELECT count(*) FROM messages)>=0 AND (SELECT count(*) FROM preferences)>=0 AND (SELECT count(*) FROM email_messages)>=0 AND (SELECT count(*) FROM sms_messages)>=0 AND (SELECT count(*) FROM push_messages)>=0 RETURNING 1) SELECT EXISTS(SELECT 1 FROM target),EXISTS(SELECT 1 FROM removed),NULL::text,NULL::jsonb WHERE $5::jsonb IS NOT NULL"
    } else {
        "WITH target AS MATERIALIZED (SELECT environment_id,subscriber_id,management_generation,profile_revision,profile FROM public.novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), updated AS (UPDATE public.novu_rust_subscribers s SET profile=t.profile || $5::jsonb,updated_at=CASE WHEN t.profile IS DISTINCT FROM t.profile || $5::jsonb THEN now() ELSE s.updated_at END FROM target t WHERE s.environment_id=t.environment_id AND s.subscriber_id=t.subscriber_id AND t.management_generation=$3 AND t.profile_revision=$4 RETURNING s.profile_revision::text,s.profile) SELECT EXISTS(SELECT 1 FROM target),EXISTS(SELECT 1 FROM updated),(SELECT profile_revision FROM updated),(SELECT profile FROM updated)"
    };
    match db
        .query_one(sql, &[&env, &id, &generation, &revision, &profile])
        .await
    {
        Ok(row) if !row.get::<_, bool>(0) => err(StatusCode::NOT_FOUND, &path),
        Ok(row) if !row.get::<_, bool>(1) => err(StatusCode::CONFLICT, &path),
        Ok(_row) if deleting => (
            StatusCode::OK,
            Json(json!({"data":{"subscriberId":id,"deleted":true}})),
        ),
        Ok(row) => (
            StatusCode::OK,
            Json(
                json!({"data":{"subscriberId":id,"generation":change.generation,"revision":row.get::<_,String>(2),"profile":row.get::<_,Value>(3)}}),
            ),
        ),
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, &path),
    }
}
