//! Experimental FCM-only subscriber credential endpoints.
use super::{ApiError, authenticate, error_at, subscriber_entity_response};
use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Client;

pub(super) async fn attach(
    db: &Client,
    environment: &str,
    subscriber: &str,
    mut response: ApiError,
    path: &str,
) -> ApiError {
    let row=db.query_opt("SELECT integration_internal_id,device_tokens FROM novu_rust_fcm_devices WHERE environment_id=$1 AND subscriber_id=$2 AND integration_internal_id IS NOT NULL",&[&environment,&subscriber]).await;
    match row {
        Ok(row) => {
            let channels=row.map(|r|json!([{"providerId":"fcm","_integrationId":r.get::<_,String>(0),"credentials":{"deviceTokens":r.get::<_,Vec<String>>(1)}}])).unwrap_or_else(||json!([]));
            response.1.0["data"]["channels"] = channels;
            response
        }
        Err(_) => error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            path,
        ),
    }
}

pub(super) async fn update(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber): Path<String>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    bytes: Bytes,
) -> ApiError {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    let body: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return error_at(
                StatusCode::BAD_REQUEST,
                "Invalid JSON",
                "Bad Request",
                &path,
            );
        }
    };
    let Some(fields) = body.as_object() else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Credentials not implemented",
            "Not Implemented",
            &path,
        );
    };
    if fields
        .keys()
        .any(|key| !["providerId", "credentials", "integrationIdentifier"].contains(&key.as_str()))
        || fields.get("providerId").and_then(Value::as_str) != Some("fcm")
    {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Credentials not implemented",
            "Not Implemented",
            &path,
        );
    }
    let Some(credentials) = fields.get("credentials").and_then(Value::as_object) else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Credentials not implemented",
            "Not Implemented",
            &path,
        );
    };
    if credentials.keys().any(|key| key != "deviceTokens") {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Credentials not implemented",
            "Not Implemented",
            &path,
        );
    }
    let Some(tokens) = credentials.get("deviceTokens").and_then(Value::as_array) else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Credentials not implemented",
            "Not Implemented",
            &path,
        );
    };
    let Some(tokens) = tokens
        .iter()
        .map(|v| v.as_str().map(str::to_owned))
        .collect::<Option<Vec<_>>>()
    else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Credentials not implemented",
            "Not Implemented",
            &path,
        );
    };
    let identifier = match fields.get("integrationIdentifier") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        _ => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Credentials not implemented",
                "Not Implemented",
                &path,
            );
        }
    };
    match db
        .query_opt(
            "SELECT 1 FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2",
            &[&env, &subscriber],
        )
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return error_at(
                StatusCode::BAD_REQUEST,
                &format!("SubscriberId: {subscriber} not found"),
                "Bad Request",
                &path,
            );
        }
        Err(_) => {
            return error_at(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "Internal Server Error",
                &path,
            );
        }
    }
    let found=match db.query_opt("SELECT internal_id FROM novu_rust_fcm_integrations WHERE environment_id=$1 AND active AND ($2::text IS NULL OR $2='' OR identifier=$2)",&[&env,&identifier]).await {Ok(value)=>value,Err(_)=>return error_at(StatusCode::INTERNAL_SERVER_ERROR,"Internal server error","Internal Server Error",&path)};
    let Some(found) = found else {
        return error_at(
            StatusCode::BAD_REQUEST,
            &format!("Subscribers environment ({env}) do not have active fcm integration."),
            "Bad Request",
            &path,
        );
    };
    let integration: String = found.get(0);
    let replace = method == Method::PUT;
    let row=db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id,subscriber_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), incoming AS MATERIALIZED (SELECT COALESCE(array_agg(v ORDER BY first_pos),'{}'::text[]) AS tokens FROM (SELECT v,min(pos) AS first_pos FROM unnest($3::text[]) WITH ORDINALITY AS x(v,pos) GROUP BY v) dedup), written AS (INSERT INTO novu_rust_fcm_devices AS d(environment_id,subscriber_id,device_tokens,integration_internal_id) SELECT target.environment_id,target.subscriber_id,incoming.tokens,$4 FROM target CROSS JOIN incoming WHERE cardinality(incoming.tokens)<=100 ON CONFLICT(environment_id,subscriber_id) DO UPDATE SET device_tokens=(SELECT COALESCE(array_agg(v ORDER BY first_pos),'{}'::text[]) FROM (SELECT v,min(pos) AS first_pos FROM unnest(CASE WHEN $5::boolean OR cardinality(EXCLUDED.device_tokens)=0 THEN EXCLUDED.device_tokens ELSE d.device_tokens || EXCLUDED.device_tokens END) WITH ORDINALITY AS x(v,pos) GROUP BY v) dedup),integration_internal_id=EXCLUDED.integration_internal_id WHERE (SELECT count(DISTINCT v) FROM unnest(CASE WHEN $5::boolean OR cardinality(EXCLUDED.device_tokens)=0 THEN EXCLUDED.device_tokens ELSE d.device_tokens || EXCLUDED.device_tokens END) AS x(v))<=100 RETURNING 1), changed AS (UPDATE novu_rust_subscribers s SET updated_at=now() FROM target WHERE s.environment_id=target.environment_id AND s.subscriber_id=target.subscriber_id AND EXISTS(SELECT 1 FROM written) RETURNING s.*) SELECT changed.internal_id,changed.profile,e.organization_id,to_char(changed.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(changed.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),changed.is_online,to_char(changed.last_online_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at FROM changed JOIN novu_rust_environments e ON e.id=changed.environment_id",&[&env,&subscriber,&tokens,&integration,&replace]).await;
    match row {
        Ok(Some(row)) => {
            attach(
                &db,
                &env,
                &subscriber,
                subscriber_entity_response(row, &subscriber, &env),
                &path,
            )
            .await
        }
        Ok(None) => {
            let exists=db.query_opt("SELECT 1 FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2",&[&env,&subscriber]).await;
            match exists {
                Ok(Some(_)) => error_at(
                    StatusCode::BAD_REQUEST,
                    "Device tokens limit exceeded",
                    "Bad Request",
                    &path,
                ),
                Ok(None) => error_at(
                    StatusCode::BAD_REQUEST,
                    &format!("SubscriberId: {subscriber} not found"),
                    "Bad Request",
                    &path,
                ),
                Err(_) => error_at(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    "Internal Server Error",
                    &path,
                ),
            }
        }
        Err(_) => error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            &path,
        ),
    }
}

pub(super) async fn delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((subscriber, provider)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> impl axum::response::IntoResponse {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if provider != "fcm" {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Credentials not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    }
    let result=db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id,subscriber_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), removed AS (DELETE FROM novu_rust_fcm_devices d USING target WHERE d.environment_id=target.environment_id AND d.subscriber_id=target.subscriber_id RETURNING 1), touched AS (UPDATE novu_rust_subscribers s SET updated_at=now() FROM target WHERE s.environment_id=target.environment_id AND s.subscriber_id=target.subscriber_id AND EXISTS(SELECT 1 FROM removed) RETURNING 1) SELECT subscriber_id FROM target",&[&env,&subscriber]).await;
    match result {
        Ok(Some(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(None) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Subscriber '{subscriber}' was not found"),
            "Not Found",
            &path,
        )
        .into_response(),
        Err(_) => error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            &path,
        )
        .into_response(),
    }
}
