//! Experimental FCM-only subscriber credential endpoints.
use super::{ApiError, FcmRegistrationPolicy, authenticate, error_at, subscriber_entity_response};
use axum::{
    Extension,
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
    let rows=db.query("SELECT integration_internal_id,device_tokens FROM novu_rust_fcm_devices WHERE environment_id=$1 AND subscriber_id=$2 AND integration_internal_id IS NOT NULL ORDER BY channel_order",&[&environment,&subscriber]).await;
    match rows {
        Ok(rows) => {
            let channels:Vec<_>=rows.into_iter().map(|r|json!({"providerId":"fcm","_integrationId":r.get::<_,String>(0),"credentials":{"deviceTokens":r.get::<_,Vec<String>>(1)}})).collect();
            response.1.0["data"]["channels"] = json!(channels);
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
    Extension(registration): Extension<FcmRegistrationPolicy>,
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
    let found=match db.query_opt("SELECT internal_id FROM novu_rust_fcm_integrations WHERE environment_id=$1 AND active AND ($2::text IS NULL OR $2='' OR identifier=$2) ORDER BY created_at DESC,internal_id DESC LIMIT 1",&[&env,&identifier]).await {Ok(value)=>value,Err(_)=>return error_at(StatusCode::INTERNAL_SERVER_ERROR,"Internal server error","Internal Server Error",&path)};
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
    for _ in 0..4 {
        let guard = if registration.0 {
            "public.novu_rust_assert_fcm_registration_guard() AND "
        } else {
            ""
        };
        let query = format!(
            "WITH fcm_guard AS MATERIALIZED (SELECT 1 WHERE {guard}public.novu_rust_lock_active_fcm_integration($1,$4,$6)), target AS MATERIALIZED (SELECT s.environment_id,s.subscriber_id FROM novu_rust_subscribers s CROSS JOIN fcm_guard WHERE s.environment_id=$1 AND s.subscriber_id=$2 FOR UPDATE OF s), incoming AS MATERIALIZED (SELECT COALESCE(array_agg(v ORDER BY first_pos),'{{}}'::text[]) AS tokens FROM (SELECT v,min(pos) AS first_pos FROM unnest($3::text[]) WITH ORDINALITY AS x(v,pos) GROUP BY v) dedup), written AS (INSERT INTO novu_rust_fcm_devices AS d(environment_id,subscriber_id,device_tokens,integration_internal_id) SELECT target.environment_id,target.subscriber_id,incoming.tokens,$4 FROM target CROSS JOIN incoming WHERE cardinality(incoming.tokens)<=100 ON CONFLICT(environment_id,subscriber_id,integration_internal_id) WHERE integration_internal_id IS NOT NULL DO UPDATE SET device_tokens=(SELECT COALESCE(array_agg(v ORDER BY first_pos),'{{}}'::text[]) FROM (SELECT v,min(pos) AS first_pos FROM unnest(CASE WHEN $5::boolean OR cardinality(EXCLUDED.device_tokens)=0 THEN EXCLUDED.device_tokens ELSE d.device_tokens || EXCLUDED.device_tokens END) WITH ORDINALITY AS x(v,pos) GROUP BY v) dedup),integration_internal_id=EXCLUDED.integration_internal_id WHERE (SELECT count(DISTINCT v) FROM unnest(CASE WHEN $5::boolean OR cardinality(EXCLUDED.device_tokens)=0 THEN EXCLUDED.device_tokens ELSE d.device_tokens || EXCLUDED.device_tokens END) AS x(v))<=100 AND ($7::boolean OR d.integration_internal_id IS DISTINCT FROM EXCLUDED.integration_internal_id OR d.device_tokens IS DISTINCT FROM EXCLUDED.device_tokens) RETURNING 1), changed AS (UPDATE novu_rust_subscribers s SET updated_at=now() FROM target WHERE s.environment_id=target.environment_id AND s.subscriber_id=target.subscriber_id AND EXISTS(SELECT 1 FROM written) RETURNING s.*) SELECT changed.internal_id,changed.profile,e.organization_id,to_char(changed.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(changed.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),changed.is_online,to_char(changed.last_online_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at FROM changed JOIN novu_rust_environments e ON e.id=changed.environment_id"
        );
        let row = db
            .query_opt(
                &query,
                &[
                    &env,
                    &subscriber,
                    &tokens,
                    &integration,
                    &replace,
                    &identifier,
                    &registration.0,
                ],
            )
            .await;
        match row {
            Ok(Some(row)) => {
                return attach(
                    &db,
                    &env,
                    &subscriber,
                    subscriber_entity_response(row, &subscriber, &env),
                    &path,
                )
                .await;
            }
            Ok(None) => {
                // A conflict can be an exact no-op, a token limit violation, or a
                // concurrent change after the statement snapshot. Re-read and retry
                // the last case rather than misreporting it as a limit violation.
                let current = db.query_opt("WITH fcm_guard AS MATERIALIZED (SELECT 1 WHERE public.novu_rust_lock_active_fcm_integration($1,$3,$6)) SELECT s.internal_id,s.profile,e.organization_id,to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),s.is_online,to_char(s.last_online_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at,COALESCE(d.integration_internal_id=$3 AND d.device_tokens=ARRAY(SELECT v FROM (SELECT v,min(pos) AS first_pos FROM unnest($4::text[]) WITH ORDINALITY AS x(v,pos) GROUP BY v) dedup ORDER BY first_pos),false) AS unchanged,(SELECT count(DISTINCT v) FROM unnest(CASE WHEN $5::boolean OR cardinality($4::text[])=0 THEN $4::text[] ELSE COALESCE(d.device_tokens,'{}'::text[]) || $4::text[] END) AS x(v)) AS token_count,EXISTS(SELECT 1 FROM fcm_guard) AS integration_active FROM novu_rust_subscribers s JOIN novu_rust_environments e ON e.id=s.environment_id LEFT JOIN novu_rust_fcm_devices d ON d.environment_id=s.environment_id AND d.subscriber_id=s.subscriber_id AND d.integration_internal_id=$3 WHERE s.environment_id=$1 AND s.subscriber_id=$2", &[&env,&subscriber,&integration,&tokens,&replace,&identifier]).await;
                match current {
                    Ok(Some(row)) if !row.get::<_, bool>(9) => {
                        return error_at(
                            StatusCode::BAD_REQUEST,
                            &format!(
                                "Subscribers environment ({env}) do not have active fcm integration."
                            ),
                            "Bad Request",
                            &path,
                        );
                    }
                    Ok(Some(row)) if row.get::<_, bool>(7) && !registration.0 => {
                        return attach(
                            &db,
                            &env,
                            &subscriber,
                            subscriber_entity_response(row, &subscriber, &env),
                            &path,
                        )
                        .await;
                    }
                    Ok(Some(row)) if row.get::<_, i64>(8) > 100 => {
                        let count: i64 = row.get(8);
                        let mut error = error_at(
                            StatusCode::BAD_REQUEST,
                            &format!(
                                "Device tokens limit exceeded. Maximum allowed tokens per subscriber channel is 100, but got {count} tokens."
                            ),
                            "Bad Request",
                            &path,
                        );
                        if let Some(body) = error.1.0.as_object_mut() {
                            body.remove("error");
                            body.insert("currentCount".into(), json!(count));
                            body.insert("limit".into(), json!(100));
                            body.insert("ctx".into(), json!({"currentCount":count,"limit":100}));
                        }
                        return error;
                    }
                    Ok(Some(_)) => continue,
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
            }
            Err(error) => {
                if registration.0
                    && error
                        .code()
                        .is_some_and(|code| matches!(code.code(), "55000" | "42883" | "42501"))
                {
                    return error_at(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "FCM registration protection unavailable",
                        "Service Unavailable",
                        &path,
                    );
                }
                return error_at(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    "Internal Server Error",
                    &path,
                );
            }
        }
    }
    error_at(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Concurrent credentials update could not be completed",
        "Internal Server Error",
        &path,
    )
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
    let result=db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id,subscriber_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), removed AS (DELETE FROM novu_rust_fcm_devices d USING target WHERE d.environment_id=target.environment_id AND d.subscriber_id=target.subscriber_id RETURNING 1), touched AS (UPDATE novu_rust_subscribers s SET updated_at=now() FROM target WHERE s.environment_id=target.environment_id AND s.subscriber_id=target.subscriber_id RETURNING 1) SELECT subscriber_id FROM target",&[&env,&subscriber]).await;
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
