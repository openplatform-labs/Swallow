//! Experimental, deliberately incomplete Novu trigger endpoint.
mod contexts;
mod inbox_context;
mod inbox_data;
mod preferences;
mod sendgrid_webhook;
mod subscriber_credentials;
mod subscriber_jwt;
mod subscriber_subscriptions;
mod subscribers_list;
mod topic_subscription_delete;
mod topic_subscription_preferences;
mod topic_subscriptions;
mod topics;
mod topics_v2;
pub mod twilio_webhook;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Extension, OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use novu_rust_contracts::trigger_gate::{GateResult, TenantState, evaluate};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::Client;
use uuid::Uuid;

pub const MIGRATION: &str = include_str!("../migrations/0001_experimental.sql");
pub const INBOX_SESSION_MIGRATION: &str =
    include_str!("../migrations/0002_experimental_inbox_session.sql");
pub const SNOOZE_POLICY_MIGRATION: &str =
    include_str!("../migrations/0003_experimental_snooze_policy.sql");

#[derive(Clone, Default)]
struct InboxHmacKeys(HashMap<String, Vec<String>>);

/// Matches the reference's txn_ + 4-byte Unix-seconds hex + 12 base36 characters.
fn generate_transaction_id() -> Option<String> {
    let seconds =
        u32::try_from(SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs()).ok()?;
    let mut suffix = String::with_capacity(12);
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    while suffix.len() < 12 {
        let id = Uuid::new_v4();
        for (index, byte) in id.as_bytes().iter().enumerate() {
            // UUID version and variant bytes are not uniformly random.
            if index == 6 || index == 8 || *byte >= 252 {
                continue;
            }
            suffix.push(ALPHABET[usize::from(*byte % 36)] as char);
            if suffix.len() == 12 {
                break;
            }
        }
    }
    Some(format!("txn_{seconds:08x}{suffix}"))
}

pub fn router(db: Arc<Client>) -> Router {
    let secret = std::env::var("JWT_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
        .map(String::into_bytes);
    let keys = std::env::var("INBOX_HMAC_KEYS_JSON")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    router_with_twilio_config(db, secret, keys, twilio_webhook::Config::from_env())
}

pub fn router_with_subscriber_secret(db: Arc<Client>, secret: Option<Vec<u8>>) -> Router {
    router_with_session_keys(db, secret, HashMap::new())
}

pub fn router_with_session_keys(
    db: Arc<Client>,
    secret: Option<Vec<u8>>,
    keys: HashMap<String, Vec<String>>,
) -> Router {
    router_with_twilio_config(db, secret, keys, None)
}

pub fn router_with_twilio_config(
    db: Arc<Client>,
    secret: Option<Vec<u8>>,
    keys: HashMap<String, Vec<String>>,
    twilio: Option<twilio_webhook::Config>,
) -> Router {
    Router::new()
        .route(
            "/webhooks/organizations/{organization}/environments/{environment}/email/{provider}",
            post(sendgrid_webhook::receive),
        )
        .route(
            "/webhooks/organizations/{organization}/environments/{environment}/sms/twilio",
            post(twilio_webhook::receive),
        )
        .route(
            "/webhooks/organizations/{organization}/environments/{environment}/sms/twilio/jobs/{job}",
            post(twilio_webhook::receive_job),
        )
        .route(
            "/webhooks/organizations/{organization}/environments/{environment}/sms/twilio/jobs/{job}/attempts/{dispatch}",
            post(twilio_webhook::receive_dispatch),
        )
        .route("/v1/events/trigger", post(trigger))
        .route(
            "/v1/subscribers/{subscriber_id}/credentials",
            axum::routing::put(subscriber_credentials::update)
                .patch(subscriber_credentials::update),
        )
        .route(
            "/v1/subscribers/{subscriber_id}/credentials/{provider_id}",
            axum::routing::delete(subscriber_credentials::delete),
        )
        .route(
            "/v1/subscribers/{subscriber_id}/online-status",
            axum::routing::patch(update_online_status),
        )
        .route("/v1/subscribers/{subscriber_id}", get(get_subscriber_v1))
        .route(
            "/v2/subscribers/{subscriber_id}/subscriptions",
            get(subscriber_subscriptions::list),
        )
        .route("/v1/topics", post(topics::create).get(topics::list))
        .route("/v2/topics", post(topics_v2::upsert).get(topics_v2::list))
        .route(
            "/v2/topics/{topic}/subscriptions",
            post(topic_subscriptions::create)
                .get(topic_subscriptions::list)
                .delete(topic_subscription_delete::delete),
        )
        .route(
            "/v2/topics/{topic}/subscriptions/{identifier}",
            get(topics_v2::get_subscription).patch(topic_subscriptions::update),
        )
        .route(
            "/v2/topics/{topic}",
            get(topics_v2::get)
                .patch(topics_v2::update)
                .delete(topics_v2::delete),
        )
        .route(
            "/v1/topics/{topic}",
            get(topics::get)
                .patch(topics::rename)
                .delete(topics::delete),
        )
        .route("/v1/topics/{topic}/subscribers", post(topics::assign))
        .route(
            "/v1/topics/{topic}/subscribers/{subscriber_id}",
            get(topics::get_subscriber),
        )
        .route(
            "/v1/topics/{topic}/subscribers/removal",
            post(topics::remove),
        )
        .route("/v2/contexts", post(contexts::create).get(contexts::list))
        .route(
            "/v2/contexts/{kind}/{id}",
            get(contexts::get)
                .patch(contexts::update)
                .delete(contexts::delete),
        )
        .route("/v1/inbox/session", post(inbox_session))
        .route("/v1/inbox/notifications", get(inbox_notifications))
        .route("/v1/inbox/notifications/count", get(inbox_count))
        .route("/v1/inbox/notifications/seen", post(inbox_bulk))
        .route("/v1/inbox/notifications/read", post(inbox_bulk))
        .route("/v1/inbox/notifications/archive", post(inbox_bulk))
        .route("/v1/inbox/notifications/read-archive", post(inbox_bulk))
        .route("/v1/inbox/notifications/delete", post(inbox_bulk))
        .route(
            "/v1/inbox/notifications/{notification_id}/{action}",
            axum::routing::patch(inbox_mark).delete(inbox_delete),
        )
        .route(
            "/v2/subscribers",
            post(create_subscriber).get(subscribers_list::list),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/preferences",
            get(preferences::get).patch(preferences::update),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/preferences/global",
            get(preferences::get_global),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications",
            get(list_notifications),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/count",
            get(count_notifications),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/read",
            post(update_all_notifications),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/archive",
            post(update_all_notifications),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/read-archive",
            post(update_all_notifications),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/seen",
            post(mark_notifications_seen),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/{notification_id}",
            axum::routing::delete(delete_notification),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/{notification_id}/actions/{action_type}/complete",
            axum::routing::patch(management_action),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/{notification_id}/actions/{action_type}/revert",
            axum::routing::patch(management_action),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/{notification_id}/{action}",
            axum::routing::patch(mark_notification),
        )
        .route(
            "/v2/subscribers/{subscriber_id}/notifications/delete",
            post(delete_all_notifications),
        )
        .route(
            "/v2/subscribers/{subscriber_id}",
            get(get_subscriber)
                .patch(patch_subscriber)
                .delete(delete_subscriber),
        )
        .with_state(db)
        .layer(Extension(Arc::new(secret)))
        .layer(Extension(Arc::new(InboxHmacKeys(keys))))
        .layer(Extension(Arc::new(twilio)))
}

async fn authenticate_inbox(
    db: &Client,
    headers: &HeaderMap,
    secret: &Option<Vec<u8>>,
    path: &str,
) -> Result<subscriber_jwt::Claims, (StatusCode, Json<Value>)> {
    let unauthorized = || {
        error_at(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "Unauthorized",
            path,
        )
    };
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(unauthorized)?;
    let mut parts = raw.split_whitespace();
    let scheme = parts.next().ok_or_else(unauthorized)?;
    let token = parts.next().ok_or_else(unauthorized)?;
    if !scheme.eq_ignore_ascii_case("bearer") || parts.next().is_some() {
        return Err(unauthorized());
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| unauthorized())?
        .as_secs();
    let claims = subscriber_jwt::verify(token, secret.as_deref().ok_or_else(unauthorized)?, now)
        .ok_or_else(unauthorized)?;
    let exists = db
        .query_opt(
            "SELECT internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2",
            &[&claims.environment_id, &claims.subscriber_id],
        )
        .await
        .map_err(|_| {
            error_at(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "Internal Server Error",
                path,
            )
        })?;
    if exists.as_ref().is_none_or(|row| {
        std::env::var("NOVU_ALLOW_RECREATED_SUBSCRIBER").as_deref() != Ok("true")
            && claims.internal_id.as_deref() != Some(row.get::<_, String>(0).as_str())
    }) {
        return Err(unauthorized());
    }
    Ok(claims)
}

async fn inbox_notifications(
    State(db): State<Arc<Client>>,
    Extension(secret): Extension<Arc<Option<Vec<u8>>>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(pairs): Query<Vec<(String, String)>>,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let claims = match authenticate_inbox(&db, &headers, &secret, &path).await {
        Ok(claims) => claims,
        Err(response) => return response,
    };
    list_notifications_for(
        &db,
        &claims.environment_id,
        claims.subscriber_id,
        path,
        pairs,
        Some(claims.context_keys),
    )
    .await
}

async fn inbox_count(
    State(db): State<Arc<Client>>,
    Extension(secret): Extension<Arc<Option<Vec<u8>>>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(query): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let claims = match authenticate_inbox(&db, &headers, &secret, &path).await {
        Ok(claims) => claims,
        Err(response) => return response,
    };
    count_notifications_for(
        &db,
        &claims.environment_id,
        claims.subscriber_id,
        path,
        query,
        Some(claims.context_keys),
    )
    .await
}

async fn inbox_session(
    State(db): State<Arc<Client>>,
    Extension(secret): Extension<Arc<Option<Vec<u8>>>>,
    Extension(keys): Extension<Arc<InboxHmacKeys>>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let unsupported = || {
        error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Inbox session option not implemented",
            "Not Implemented",
            &path,
        )
    };
    if uri.query().is_some() {
        return unsupported();
    }
    let Ok(body) = serde_json::from_slice::<Value>(&bytes) else {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Invalid JSON",
            "Bad Request",
            &path,
        );
    };
    let Some(fields) = body.as_object() else {
        return unsupported();
    };
    if fields.keys().any(|key| {
        ![
            "applicationIdentifier",
            "subscriberId",
            "subscriber",
            "subscriberHash",
            "context",
            "contextHash",
        ]
        .contains(&key.as_str())
    }) {
        return unsupported();
    }
    let identifier = fields
        .get("applicationIdentifier")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty());
    let source = match fields.get("subscriber") {
        Some(Value::String(id)) if !id.is_empty() => json!({"subscriberId":id}),
        Some(Value::Object(object))
            if object
                .get("subscriberId")
                .and_then(Value::as_str)
                .is_some_and(|id| !id.is_empty()) =>
        {
            Value::Object(object.clone())
        }
        None | Some(Value::Null) | Some(Value::String(_)) | Some(Value::Object(_)) => {
            json!({"subscriberId": fields.get("subscriberId").and_then(Value::as_str).unwrap_or("")})
        }
        _ => return unsupported(),
    };
    let source = source.as_object().expect("subscriber object");
    let id = source
        .get("subscriberId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some(identifier) = identifier else {
        if !id.is_empty() {
            return error_at(
                StatusCode::UNPROCESSABLE_ENTITY,
                "A valid application identifier is required when providing subscriber information",
                "Unprocessable Entity",
                &path,
            );
        }
        return error_at(
            StatusCode::BAD_REQUEST,
            "Keyless is not supported in community edition",
            "Bad Request",
            &path,
        );
    };
    if id.is_empty() {
        return error_at(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Subscriber ID is required",
            "Unprocessable Entity",
            &path,
        );
    }
    if id.trim() != id
        || source.keys().any(|key| {
            ![
                "subscriberId",
                "id",
                "firstName",
                "lastName",
                "email",
                "phone",
                "avatar",
                "locale",
                "timezone",
                "data",
            ]
            .contains(&key.as_str())
        })
    {
        return unsupported();
    }
    let hash = match fields.get("subscriberHash") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        _ => return unsupported(),
    };
    let context_hash = match fields.get("contextHash") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        _ => return unsupported(),
    };
    let parsed_context = match fields.get("context") {
        None | Some(Value::Null) => None,
        Some(value) => match inbox_context::parse(value) {
            Some(context) => Some(context),
            None => return unsupported(),
        },
    };
    let context_keys = parsed_context
        .as_ref()
        .map(|context| context.keys.clone())
        .unwrap_or_default();
    let context_entries = parsed_context
        .as_ref()
        .map(|context| context.entries.clone())
        .unwrap_or_else(|| json!([]));
    let canonical_context = parsed_context
        .as_ref()
        .and_then(|context| context.canonical.as_ref());
    let mut profile = source.clone();
    profile.remove("subscriberId");
    profile.remove("id");
    if !profile_errors(&profile).is_empty()
        || profile
            .get("email")
            .and_then(Value::as_str)
            .is_some_and(|email| novu_rust_contracts::email::validate(email).is_none())
    {
        return unsupported();
    }
    let env = match db.query_opt("SELECT e.id,e.organization_id,i.in_app_active,i.hmac_required,i.is_development,i.remove_novu_branding,i.max_snooze_duration_hours FROM novu_rust_inbox_environments i JOIN novu_rust_environments e ON e.id=i.environment_id WHERE i.application_identifier=$1", &[&identifier]).await {
        Ok(Some(row)) => row,
        Ok(None) => return error_at(StatusCode::BAD_REQUEST, "Please provide a valid application identifier", "Bad Request", &path),
        Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path),
    };
    let environment: String = env.get(0);
    let organization: String = env.get(1);
    if !env.get::<_, bool>(2) {
        return error_at(
            StatusCode::NOT_FOUND,
            "The active in-app integration could not be found",
            "Not Found",
            &path,
        );
    }
    let configured_keys = keys.0.get(&environment).map(Vec::as_slice).unwrap_or(&[]);
    let hash_supplied = hash.is_some_and(|hash| !hash.is_empty());
    if env.get::<_, bool>(3) && !hash_supplied {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Please provide a valid HMAC hash",
            "Bad Request",
            &path,
        );
    }
    if hash_supplied && configured_keys.is_empty() {
        return unsupported();
    }
    let active_keys = if hash_supplied {
        let key_hashes: Vec<String> = configured_keys
            .iter()
            .filter(|key| !key.is_empty())
            .map(|key| format!("{:x}", Sha256::digest(key.as_bytes())))
            .collect();
        let active = match db.query("SELECT key_hash FROM novu_rust_api_keys WHERE environment_id=$1 AND enabled AND key_hash=ANY($2)", &[&environment, &key_hashes]).await {
            Ok(rows) => rows.into_iter().map(|row| row.get::<_, String>(0)).collect::<std::collections::HashSet<_>>(),
            Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path),
        };
        configured_keys
            .iter()
            .filter(|key| active.contains(&format!("{:x}", Sha256::digest(key.as_bytes()))))
            .cloned()
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let valid_hash = hash.is_some_and(|hash| subscriber_jwt::verify_hmac(&active_keys, id, hash));
    if env.get::<_, bool>(3) && !valid_hash {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Please provide a valid HMAC hash",
            "Bad Request",
            &path,
        );
    }
    if env.get::<_, bool>(3) && parsed_context.is_some() && canonical_context.is_none() {
        return unsupported();
    }
    if env.get::<_, bool>(3)
        && let Some(canonical) = canonical_context
        && !context_hash
            .is_some_and(|hash| subscriber_jwt::verify_hmac(&active_keys, canonical, hash))
    {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Please provide a valid context HMAC hash",
            "Bad Request",
            &path,
        );
    }
    let Some(secret) = secret.as_ref().as_deref().filter(|s| !s.is_empty()) else {
        return error_at(
            StatusCode::SERVICE_UNAVAILABLE,
            "Inbox signing key is not configured",
            "Service Unavailable",
            &path,
        );
    };
    let Some(now) = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u32::try_from(d.as_secs()).ok())
    else {
        return error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            &path,
        );
    };
    let profile = Value::Object(profile);
    let subscriber = match db.query_one("WITH contexts AS (INSERT INTO novu_rust_contexts (environment_id,context_key,data) SELECT $1, entry->>'key', COALESCE(NULLIF(entry->'data','null'::jsonb),'{}'::jsonb) FROM jsonb_array_elements($5::jsonb) entry ON CONFLICT DO NOTHING) INSERT INTO novu_rust_subscribers (environment_id,subscriber_id,internal_id,profile) VALUES ($1,$2,novu_rust_new_objectid(),$3) ON CONFLICT (environment_id,subscriber_id) DO UPDATE SET profile=CASE WHEN $4 THEN novu_rust_subscribers.profile || EXCLUDED.profile ELSE novu_rust_subscribers.profile END, updated_at=CASE WHEN $4 AND novu_rust_subscribers.profile IS DISTINCT FROM novu_rust_subscribers.profile || EXCLUDED.profile THEN now() ELSE novu_rust_subscribers.updated_at END RETURNING internal_id,profile", &[&environment, &id, &profile, &valid_hash, &context_entries]).await {
        Ok(row) => row,
        Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path),
    };
    let subscriber_id: String = subscriber.get(0);
    let stored_profile: Value = subscriber.get(1);
    let counts = match db.query_one("WITH scoped AS NOT MATERIALIZED (SELECT severity,severity_present FROM novu_rust_in_app_messages WHERE environment_id=$1 AND subscriber_id=$2 AND subscriber_internal_id=(SELECT internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2) AND NOT is_read AND snoozed_until IS NULL AND cardinality(context_keys)=cardinality($3::text[]) AND context_keys @> $3::text[]) SELECT (SELECT count(*) FROM (SELECT 1 FROM scoped LIMIT 100) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE severity='high' LIMIT 100) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE severity='medium' LIMIT 100) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE severity='low' LIMIT 100) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE severity='none' OR NOT severity_present LIMIT 100) q)", &[&environment, &id, &context_keys]).await {
        Ok(row) => [row.get::<_, i64>(0),row.get(1),row.get(2),row.get(3),row.get(4)],
        Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path),
    };
    let mut claims = json!({"_id":subscriber_id,"organizationId":organization,"environmentId":environment,"subscriberId":id,"contextKeys":context_keys,"iat":now,"exp":u64::from(now)+15*24*60*60,"aud":"widget_user","iss":"novu_api"});
    for key in ["firstName", "lastName", "email"] {
        if let Some(value) = stored_profile.get(key) {
            claims[key] = value.clone();
        }
    }
    let Some(token) = subscriber_jwt::sign(&claims, secret) else {
        return error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            &path,
        );
    };
    (
        StatusCode::CREATED,
        Json(
            json!({"data":{"applicationIdentifier":identifier,"token":token,"totalUnreadCount":counts[0],"unreadCount":{"total":counts[0],"severity":{"high":counts[1],"medium":counts[2],"low":counts[3],"none":counts[4]}},"removeNovuBranding":env.get::<_,bool>(5),"maxSnoozeDurationHours":env.get::<_,i32>(6),"isDevelopmentMode":env.get::<_,bool>(4),"contextKeys":context_keys}}),
        ),
    )
}

async fn inbox_mark(
    State(db): State<Arc<Client>>,
    Extension(secret): Extension<Arc<Option<Vec<u8>>>>,
    headers: HeaderMap,
    Path((notification_id, action)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    let claims = match authenticate_inbox(&db, &headers, &secret, &uri.to_string()).await {
        Ok(claims) => claims,
        Err(response) => return response.into_response(),
    };
    if action == "complete" || action == "revert" {
        let input: Value = match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(_) => {
                return error_at(
                    StatusCode::BAD_REQUEST,
                    "Invalid JSON",
                    "Bad Request",
                    &uri.to_string(),
                )
                .into_response();
            }
        };
        let action_type = input
            .get("actionType")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if !["primary", "secondary"].contains(&action_type.as_str()) {
            let mut messages = Vec::new();
            if input.get("actionType").is_none_or(Value::is_null) {
                messages.push("actionType should not be null or undefined");
            }
            messages.push("actionType must be one of the following values: primary, secondary");
            let mut response = validation_error(messages);
            response.1.0["path"] = json!(uri.to_string());
            return response.into_response();
        }
        return notification_action_for(
            &db,
            claims.environment_id,
            claims.subscriber_id,
            notification_id,
            action_type,
            action,
            uri,
            Some(claims.context_keys),
        )
        .await;
    }
    if action == "snooze" {
        return snooze_notification_for(
            &db,
            claims.environment_id,
            claims.subscriber_id,
            notification_id,
            uri,
            Some(claims.context_keys),
            body,
        )
        .await;
    }
    mark_notification_for(
        &db,
        claims.environment_id,
        claims.subscriber_id,
        notification_id,
        action,
        uri,
        Some(claims.context_keys),
    )
    .await
}

async fn inbox_delete(
    State(db): State<Arc<Client>>,
    Extension(secret): Extension<Arc<Option<Vec<u8>>>>,
    headers: HeaderMap,
    Path((notification_id, action)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let claims = match authenticate_inbox(&db, &headers, &secret, &uri.to_string()).await {
        Ok(claims) => claims,
        Err(response) => return response.into_response(),
    };
    if action != "delete" {
        return StatusCode::NOT_FOUND.into_response();
    }
    delete_notification_for(
        &db,
        claims.environment_id,
        claims.subscriber_id,
        notification_id,
        uri,
        Some(claims.context_keys),
    )
    .await
}

async fn inbox_bulk(
    State(db): State<Arc<Client>>,
    Extension(secret): Extension<Arc<Option<Vec<u8>>>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> Response {
    let claims = match authenticate_inbox(&db, &headers, &secret, &uri.to_string()).await {
        Ok(claims) => claims,
        Err(response) => return response.into_response(),
    };
    match uri.path().rsplit('/').next() {
        Some("seen") => {
            mark_notifications_seen_for(
                &db,
                claims.environment_id,
                claims.subscriber_id,
                uri,
                bytes,
                Some(claims.context_keys),
            )
            .await
        }
        Some("delete") => {
            delete_all_notifications_for(
                &db,
                claims.environment_id,
                claims.subscriber_id,
                uri,
                bytes,
                Some(claims.context_keys),
            )
            .await
        }
        _ => {
            update_all_notifications_for(
                &db,
                claims.environment_id,
                claims.subscriber_id,
                uri,
                bytes,
                Some(claims.context_keys),
            )
            .await
        }
    }
}

async fn delete_notification(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((subscriber_id, notification_id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response.into_response(),
    };
    delete_notification_for(
        &db,
        environment_id,
        subscriber_id,
        notification_id,
        uri,
        None,
    )
    .await
}

// Management endpoints take contextKeys from the query. Subscriber sessions
// retain their own scope and cannot override it through query parameters.
fn individual_context_keys(
    uri: &axum::http::Uri,
    session: Option<Vec<String>>,
) -> Result<Option<Vec<String>>, ()> {
    if uri.query().is_none() {
        return Ok(session);
    }
    if session.is_some() {
        return Err(());
    }
    let Query(pairs) = Query::<Vec<(String, String)>>::try_from_uri(uri).map_err(|_| ())?;
    let mut keys = Vec::new();
    for (key, value) in pairs {
        if key != "contextKeys" {
            return Err(());
        }
        keys.push(value);
    }
    Ok((!keys.is_empty()).then_some(keys))
}

async fn delete_notification_for(
    db: &Client,
    environment_id: String,
    subscriber_id: String,
    notification_id: String,
    uri: axum::http::Uri,
    context_keys: Option<Vec<String>>,
) -> Response {
    let path = uri.to_string();
    let context_keys = match individual_context_keys(&uri, context_keys) {
        Ok(keys) => keys,
        Err(()) => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification filters not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
    };
    if notification_id.len() != 24 || !notification_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"statusCode":422,"timestamp":timestamp,"path":path,"message":"Validation failed for DeleteNotificationCommand: notificationId: notificationId must be a mongodb id","ctx":{},"errors":{"notificationId":{"messages":["notificationId must be a mongodb id"],"value":notification_id}}}))).into_response();
    }
    delete_notification_rows(
        db,
        &environment_id,
        &subscriber_id,
        Some(&notification_id),
        &json!([]),
        &json!([]),
        context_keys,
        &path,
    )
    .await
}

async fn mark_notification(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((subscriber_id, notification_id, action)): Path<(String, String, String)>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response.into_response(),
    };
    if action == "snooze" {
        return snooze_notification_for(
            &db,
            environment_id,
            subscriber_id,
            notification_id,
            uri,
            None,
            body,
        )
        .await;
    }
    mark_notification_for(
        &db,
        environment_id,
        subscriber_id,
        notification_id,
        action,
        uri,
        None,
    )
    .await
}

async fn management_action(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path((subscriber_id, notification_id, action_type)): Path<(String, String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response.into_response(),
    };
    let operation = if uri.path().ends_with("/complete") {
        "complete"
    } else {
        "revert"
    }
    .to_owned();
    notification_action_for(
        &db,
        environment_id,
        subscriber_id,
        notification_id,
        action_type,
        operation,
        uri,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn notification_action_for(
    db: &Client,
    environment_id: String,
    subscriber_id: String,
    notification_id: String,
    action_type: String,
    operation: String,
    uri: axum::http::Uri,
    context_keys: Option<Vec<String>>,
) -> Response {
    let path = uri.to_string();
    if !["complete", "revert"].contains(&operation.as_str()) {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification operation not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    }
    let context_keys = match individual_context_keys(&uri, context_keys) {
        Ok(keys) => keys,
        Err(()) => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification operation not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
    };
    if notification_id.len() != 24 || !notification_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"statusCode":422,"timestamp":timestamp,"path":path,"message":"Validation failed for UpdateNotificationActionCommand: notificationId: notificationId must be a mongodb id","ctx":{},"errors":{"notificationId":{"messages":["notificationId must be a mongodb id"],"value":notification_id}}}))).into_response();
    }
    if !["primary", "secondary"].contains(&action_type.as_str()) {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"statusCode":422,"timestamp":timestamp,"path":path,"message":"Validation failed for UpdateNotificationActionCommand: actionType: actionType must be one of the following values: primary, secondary","ctx":{},"errors":{"actionType":{"messages":["actionType must be one of the following values: primary, secondary"],"value":action_type}}}))).into_response();
    }
    let status = if operation == "complete" {
        "done"
    } else {
        "pending"
    };
    let result = db.query_opt(r#"WITH target AS MATERIALIZED (
        SELECT environment_id,subscriber_id,internal_id,profile FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE
      ), candidate AS MATERIALIZED (
        SELECT m.id,w.inbox_metadata AS metadata,EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(m.cta#>'{action,buttons}')='array' THEN m.cta#>'{action,buttons}' ELSE '[]'::jsonb END) b WHERE b->>'type'=$4) AS has_button
        FROM novu_rust_in_app_messages m JOIN target ON m.environment_id=target.environment_id AND m.subscriber_id=target.subscriber_id AND m.subscriber_internal_id=target.internal_id
        LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier
        WHERE m.internal_id=lower($3) AND ($6::text[] IS NULL OR (cardinality(m.context_keys)=cardinality($6) AND m.context_keys @> $6)) FOR UPDATE OF m
      ), updated AS (
        UPDATE novu_rust_in_app_messages m SET
        cta=jsonb_set(jsonb_set(m.cta,'{action,result}',COALESCE(m.cta#>'{action,result}','{}'::jsonb)||jsonb_build_object('type',$4::text),true),'{action,status}',to_jsonb($5::text),true),
        is_seen=CASE WHEN m.is_read THEN m.is_seen ELSE true END,
        last_seen_at=CASE WHEN m.is_read THEN m.last_seen_at ELSE now() END,
        last_read_at=CASE WHEN m.is_read THEN m.last_read_at ELSE now() END,
        read_at_present=CASE WHEN m.is_read THEN m.read_at_present ELSE true END,is_read=true
        FROM candidate c WHERE m.id=c.id AND c.has_button AND jsonb_typeof(c.metadata->'id')='string' AND jsonb_typeof(c.metadata->'identifier')='string' AND jsonb_typeof(c.metadata->'name')='string' AND jsonb_typeof(c.metadata->'critical')='boolean' AND jsonb_typeof(c.metadata->'tags')='array' AND jsonb_typeof(c.metadata->'payloadAsData')='boolean' AND c.metadata->>'severity' IN ('none','low','medium','high') RETURNING m.*
      ) SELECT m.internal_id,m.transaction_id,m.content,m.payload,m.is_read,m.is_seen,m.is_archived,
        to_char(m.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),to_char(m.delivered_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        to_char(m.last_read_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),to_char(m.first_seen_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),to_char(m.archived_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        m.tags,w.inbox_metadata,m.read_at_present,m.archived_at_present,m.data,target.internal_id,target.profile,candidate.id::text,candidate.has_button,
        to_char(m.snoozed_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS snoozed_until,
        CASE WHEN m.delivered_history IS NULL THEN NULL ELSE to_jsonb(ARRAY(SELECT to_char(t AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') FROM unnest(m.delivered_history) WITH ORDINALITY AS h(t,ord) ORDER BY ord)) END AS delivered_history,m.severity AS message_severity,m.cta AS message_cta
      FROM target LEFT JOIN candidate ON true LEFT JOIN updated m ON true LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier"#,
      &[&environment_id,&subscriber_id,&notification_id,&action_type,&status,&context_keys]).await;
    let row = match result {
        Ok(Some(row)) => row,
        Ok(None) => {
            return error_at(
                StatusCode::NOT_FOUND,
                &format!("Subscriber '{subscriber_id}' was not found"),
                "Not Found",
                &path,
            )
            .into_response();
        }
        Err(_) => {
            return error_at(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "Internal Server Error",
                &path,
            )
            .into_response();
        }
    };
    if row.get::<_, Option<String>>(19).is_none() {
        return error_at(
            StatusCode::NOT_FOUND,
            &format!("Notification with id: {notification_id} is not found."),
            "Not Found",
            &path,
        )
        .into_response();
    }
    if row.get::<_, Option<bool>>(20) == Some(false) {
        return error_at(
            StatusCode::BAD_REQUEST,
            &format!(
                "Could not perform action on the {action_type} button because it does not exist."
            ),
            "Bad Request",
            &path,
        )
        .into_response();
    }
    if row.get::<_, Option<String>>(0).is_none() {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification metadata not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    }
    let profile: Value = row.get(18);
    let mut to = json!({"id":row.get::<_,String>(17),"subscriberId":subscriber_id});
    for key in ["firstName", "lastName", "avatar"] {
        if let Some(value) = profile.get(key) {
            to[key] = value.clone();
        }
    }
    match notification_dto(&row, &to) {
        Some(item) => (StatusCode::OK, Json(json!({"data":item}))).into_response(),
        None => error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification metadata not implemented",
            "Not Implemented",
            &path,
        )
        .into_response(),
    }
}

async fn snooze_notification_for(
    db: &Client,
    environment_id: String,
    subscriber_id: String,
    notification_id: String,
    uri: axum::http::Uri,
    context_keys: Option<Vec<String>>,
    body: Bytes,
) -> Response {
    let path = uri.to_string();
    let context_keys = match individual_context_keys(&uri, context_keys) {
        Ok(keys) => keys,
        Err(()) => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification operation not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
    };
    let input: Value = if body.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(_) => {
                return error_at(
                    StatusCode::BAD_REQUEST,
                    "Invalid JSON",
                    "Bad Request",
                    &path,
                )
                .into_response();
            }
        }
    };
    let date = input.get("snoozeUntil").and_then(|value| {
        if let Some(value) = value.as_str() {
            chrono::DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|v| v.with_timezone(&chrono::Utc))
        } else {
            value
                .as_i64()
                .and_then(chrono::DateTime::from_timestamp_millis)
        }
    });
    let duration = date.map(|value| {
        value
            .signed_duration_since(chrono::Utc::now())
            .num_milliseconds()
    });
    if duration.is_none_or(|value| value < 60_000) {
        let mut messages = vec!["Snooze time must be at least 60 seconds in the future"];
        if date.is_none() {
            messages.push("snoozeUntil must be a Date instance");
        }
        let mut response = validation_error(messages);
        response.1.0["path"] = json!(path);
        return response.into_response();
    }
    if notification_id.len() != 24 || !notification_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"statusCode":422,"timestamp":timestamp,"path":path,"message":"Validation failed for SnoozeNotificationCommand: notificationId: notificationId must be a mongodb id","ctx":{},"errors":{"notificationId":{"messages":["notificationId must be a mongodb id"],"value":notification_id}}}))).into_response();
    }
    let limit = match db.query_one("SELECT CASE WHEN p.organization_id IS NULL THEN 86400000 ELSE p.max_duration_ms END FROM novu_rust_environments e LEFT JOIN novu_rust_organization_snooze_limits p ON p.organization_id=e.organization_id WHERE e.id=$1", &[&environment_id]).await {
        Ok(row) => row.get::<_, Option<i64>>(0),
        Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path).into_response(),
    };
    if limit.is_some_and(|limit| duration.unwrap_or_default() > limit) {
        let reason = "The snooze duration you selected exceeds your current plan limit. Please upgrade your plan for extended snooze durations.";
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        return (StatusCode::PAYMENT_REQUIRED, Json(json!({"reason":reason,"statusCode":402,"timestamp":timestamp,"path":path,"message":"Snooze Duration Limit Exceeded","ctx":{"reason":reason}}))).into_response();
    }
    let stamp = date
        .expect("validated date")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let job_id = Uuid::new_v4();
    let result = db.query_opt(r#"WITH target AS MATERIALIZED (
          SELECT environment_id,subscriber_id,internal_id,profile FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE
        ), candidate AS MATERIALIZED (
          SELECT m.id,w.inbox_metadata AS metadata FROM novu_rust_in_app_messages m JOIN target ON m.environment_id=target.environment_id AND m.subscriber_id=target.subscriber_id AND m.subscriber_internal_id=target.internal_id
          LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier
          WHERE m.internal_id=lower($3) AND ($5::text[] IS NULL OR (cardinality(m.context_keys)=cardinality($5) AND m.context_keys @> $5)) FOR UPDATE OF m
        ), updated AS (
          UPDATE novu_rust_in_app_messages m SET snoozed_until=$4::text::timestamptz,is_seen=true,first_seen_at=COALESCE(m.first_seen_at,now()),last_seen_at=now(),is_archived=false,archived_at=NULL,archived_at_present=true
          FROM candidate c WHERE m.id=c.id AND jsonb_typeof(c.metadata->'id')='string' AND jsonb_typeof(c.metadata->'identifier')='string' AND jsonb_typeof(c.metadata->'name')='string' AND jsonb_typeof(c.metadata->'critical')='boolean' AND jsonb_typeof(c.metadata->'tags')='array' AND jsonb_typeof(c.metadata->'payloadAsData')='boolean' AND c.metadata->>'severity' IN ('none','low','medium','high') RETURNING m.*
        ), queued AS (
          INSERT INTO novu_queue_jobs(id,environment_id,kind,payload,available_at)
          SELECT $6,$1,'experimental-in-app-unsnooze',jsonb_build_object('subscriberId',$2::text,'notificationId',lower($3::text)),$4::text::timestamptz FROM updated RETURNING id
        ), emitted AS MATERIALIZED (SELECT novu_rust_emit_ws_event(t.environment_id,t.internal_id,COALESCE($5::text[],ARRAY[]::text[]),'unread_count_changed','{"recalculate":true}'::jsonb) AS sent FROM target t WHERE EXISTS(SELECT 1 FROM queued)
        ) SELECT m.internal_id,m.transaction_id,m.content,m.payload,m.is_read,m.is_seen,m.is_archived,
          to_char(m.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),to_char(m.delivered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          to_char(m.last_read_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),to_char(m.first_seen_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),to_char(m.archived_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          m.tags,w.inbox_metadata,m.read_at_present,m.archived_at_present,m.data,target.internal_id,target.profile,candidate.id::text,
          to_char(m.snoozed_until AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS snoozed_until,
          CASE WHEN m.delivered_history IS NULL THEN NULL ELSE to_jsonb(ARRAY(SELECT to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') FROM unnest(m.delivered_history) WITH ORDINALITY AS h(t,ord) ORDER BY ord)) END AS delivered_history,
          (SELECT count(*) FROM queued),(SELECT count(*) FROM emitted),m.severity AS message_severity,m.cta AS message_cta FROM target LEFT JOIN candidate ON true LEFT JOIN updated m ON true LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier"#,
        &[&environment_id,&subscriber_id,&notification_id,&stamp,&context_keys,&job_id]).await;
    let row = match result {
        Ok(Some(row)) => row,
        Ok(None) => {
            return error_at(
                StatusCode::NOT_FOUND,
                &format!("Subscriber '{subscriber_id}' was not found"),
                "Not Found",
                &path,
            )
            .into_response();
        }
        Err(_) => {
            return error_at(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "Internal Server Error",
                &path,
            )
            .into_response();
        }
    };
    if row.get::<_, Option<String>>(0).is_none() {
        if row.get::<_, Option<String>>(19).is_some() {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification metadata not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
        return error_at(
            StatusCode::NOT_FOUND,
            &format!("Notification id: '{notification_id}' not found"),
            "Not Found",
            &path,
        )
        .into_response();
    }
    let profile: Value = row.get(18);
    let mut to = json!({"id":row.get::<_,String>(17),"subscriberId":subscriber_id});
    for key in ["firstName", "lastName", "avatar"] {
        if let Some(value) = profile.get(key) {
            to[key] = value.clone();
        }
    }
    match notification_dto(&row, &to) {
        Some(item) => (StatusCode::OK, Json(json!({"data":item}))).into_response(),
        None => error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification metadata not implemented",
            "Not Implemented",
            &path,
        )
        .into_response(),
    }
}

async fn mark_notification_for(
    db: &Client,
    environment_id: String,
    subscriber_id: String,
    notification_id: String,
    action: String,
    uri: axum::http::Uri,
    context_keys: Option<Vec<String>>,
) -> Response {
    let path = uri.to_string();
    if !["read", "unread", "archive", "unarchive", "unsnooze"].contains(&action.as_str()) {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification operation not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    }
    let context_keys = match individual_context_keys(&uri, context_keys) {
        Ok(keys) => keys,
        Err(()) => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification operation not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
    };
    if notification_id.len() != 24 || !notification_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let command = if action == "unsnooze" {
            "UnsnoozeNotificationCommand"
        } else {
            "MarkNotificationAsCommand"
        };
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"statusCode":422,"timestamp":timestamp,"path":path,"message":format!("Validation failed for {command}: notificationId: notificationId must be a mongodb id"),"ctx":{},"errors":{"notificationId":{"messages":["notificationId must be a mongodb id"],"value":notification_id}}}))).into_response();
    }
    // Lock the subscriber and update the selected message in the same statement.
    let result = db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id, subscriber_id, internal_id, profile FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), candidate AS MATERIALIZED (SELECT m.id, m.snoozed_until IS NOT NULL AS is_snoozed, w.inbox_metadata AS metadata FROM novu_rust_in_app_messages m JOIN target ON m.environment_id=target.environment_id AND m.subscriber_id=target.subscriber_id AND m.subscriber_internal_id=target.internal_id LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier WHERE m.internal_id=lower($3) AND ($5::text[] IS NULL OR (cardinality(m.context_keys)=cardinality($5) AND m.context_keys @> $5)) FOR UPDATE OF m), updated AS (UPDATE novu_rust_in_app_messages m SET is_seen=true, first_seen_at=COALESCE(m.first_seen_at,now()), last_seen_at=now(), is_read=CASE WHEN $4='unsnooze' THEN m.is_read ELSE ($4 <> 'unread') END, last_read_at=CASE WHEN $4='unsnooze' THEN m.last_read_at WHEN $4='unread' THEN NULL ELSE now() END, read_at_present=CASE WHEN $4='unsnooze' THEN m.read_at_present ELSE true END, snoozed_until=CASE WHEN $4='unsnooze' THEN NULL ELSE m.snoozed_until END, archived_at_present=CASE WHEN $4='read' THEN m.archived_at_present ELSE true END, is_archived=CASE WHEN $4='read' THEN m.is_archived WHEN $4='archive' THEN true ELSE false END, archived_at=CASE WHEN $4='read' THEN m.archived_at WHEN $4='archive' THEN now() ELSE NULL END FROM candidate WHERE m.id=candidate.id AND ($4 <> 'unsnooze' OR m.snoozed_until IS NOT NULL) AND jsonb_typeof(candidate.metadata->'id')='string' AND jsonb_typeof(candidate.metadata->'identifier')='string' AND jsonb_typeof(candidate.metadata->'name')='string' AND jsonb_typeof(candidate.metadata->'critical')='boolean' AND jsonb_typeof(candidate.metadata->'tags')='array' AND jsonb_typeof(candidate.metadata->'payloadAsData')='boolean' AND candidate.metadata->>'severity' IN ('none','low','medium','high') RETURNING m.*), cancelled AS (UPDATE novu_queue_jobs q SET status='cancelled' WHERE q.id=(SELECT pending.id FROM novu_queue_jobs pending WHERE $4='unsnooze' AND EXISTS(SELECT 1 FROM updated) AND pending.environment_id=$1 AND pending.kind='experimental-in-app-unsnooze' AND pending.status='pending' AND pending.payload->>'subscriberId'=$2 AND pending.payload->>'notificationId'=lower($3) ORDER BY pending.created_at,pending.id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING q.id), emitted AS MATERIALIZED (SELECT novu_rust_emit_ws_event(t.environment_id,t.internal_id,COALESCE($5::text[],ARRAY[]::text[]),'unread_count_changed','{\"recalculate\":true}'::jsonb) AS sent FROM target t WHERE EXISTS(SELECT 1 FROM updated)) SELECT m.internal_id,m.transaction_id,m.content,m.payload,m.is_read,m.is_seen,m.is_archived,to_char(m.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(m.delivered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(m.last_read_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(m.first_seen_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(m.archived_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),m.tags,w.inbox_metadata,m.read_at_present,m.archived_at_present,m.data,target.internal_id,target.profile,candidate.id::text,to_char(m.snoozed_until AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS snoozed_until,candidate.is_snoozed,CASE WHEN m.delivered_history IS NULL THEN NULL ELSE to_jsonb(ARRAY(SELECT to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM unnest(m.delivered_history) WITH ORDINALITY AS h(t,ord) ORDER BY ord)) END AS delivered_history,(SELECT count(*) FROM cancelled) AS cancelled_jobs,(SELECT count(*) FROM emitted),m.severity AS message_severity,m.cta AS message_cta FROM target LEFT JOIN candidate ON true LEFT JOIN updated m ON true LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier", &[&environment_id, &subscriber_id, &notification_id, &action, &context_keys]).await;
    let row = match result {
        Ok(Some(row)) => row,
        Ok(None) => {
            return error_at(
                StatusCode::NOT_FOUND,
                &format!("Subscriber '{subscriber_id}' was not found"),
                "Not Found",
                &path,
            )
            .into_response();
        }
        Err(_) => {
            return error_at(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "Internal Server Error",
                &path,
            )
            .into_response();
        }
    };
    if row.get::<_, Option<String>>(0).is_none() {
        if action == "unsnooze" && row.get::<_, Option<bool>>(21) != Some(true) {
            return error_at(StatusCode::NOT_FOUND, &format!("Could not find a snoozed notification with id '{notification_id}'. The notification may not exist or may not be in a snoozed state."), "Not Found", &path).into_response();
        }
        if row.get::<_, Option<String>>(19).is_some() {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification metadata not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
        return error_at(
            StatusCode::NOT_FOUND,
            &format!("Notification with id: {notification_id} is not found."),
            "Not Found",
            &path,
        )
        .into_response();
    }
    let profile: Value = row.get(18);
    let mut to = json!({"id":row.get::<_, String>(17), "subscriberId":subscriber_id});
    for key in ["firstName", "lastName", "avatar"] {
        if let Some(value) = profile.get(key) {
            to[key] = value.clone();
        }
    }
    match notification_dto(&row, &to) {
        Some(item) => (StatusCode::OK, Json(json!({"data":item}))).into_response(),
        None => error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification metadata not implemented",
            "Not Implemented",
            &path,
        )
        .into_response(),
    }
}

async fn delete_all_notifications(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> Response {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response.into_response(),
    };
    delete_all_notifications_for(&db, environment_id, subscriber_id, uri, bytes, None).await
}

async fn delete_all_notifications_for(
    db: &Client,
    environment_id: String,
    subscriber_id: String,
    uri: axum::http::Uri,
    bytes: Bytes,
    context_keys: Option<Vec<String>>,
) -> Response {
    let path = uri.to_string();
    if uri.query().is_some() {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    }
    let body: Value = if bytes.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return error_at(
                    StatusCode::BAD_REQUEST,
                    "Invalid JSON",
                    "Bad Request",
                    &path,
                )
                .into_response();
            }
        }
    };
    let Some(groups) = notification_tags(&body, false, context_keys.is_none()) else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    };
    let context_keys = match body_context_keys(&body, context_keys) {
        Ok(keys) => keys,
        Err(()) => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification filters not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
    };
    let (_, data_filter) = match parse_inbox_data(body.get("data").and_then(Value::as_str), &path) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    delete_notification_rows(
        db,
        &environment_id,
        &subscriber_id,
        None,
        &groups,
        &data_filter,
        context_keys,
        &path,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn delete_notification_rows(
    db: &Client,
    environment_id: &str,
    subscriber_id: &str,
    notification_id: Option<&str>,
    groups: &Value,
    data_filter: &Value,
    context_keys: Option<Vec<String>>,
    path: &str,
) -> Response {
    // Both lookup and deletion happen in a single statement while the subscriber is locked.
    let result = db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id, subscriber_id, internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE), removed AS (DELETE FROM novu_rust_in_app_messages m USING target WHERE m.environment_id=target.environment_id AND m.subscriber_id=target.subscriber_id AND m.subscriber_internal_id=target.internal_id AND ($3::text IS NULL OR m.internal_id=lower($3)) AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements($4::jsonb) g(group_value) WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(g.group_value) t(tag) WHERE m.tags ? t.tag)) AND novu_rust_inbox_data_matches(m.data,$5) AND ($6::text[] IS NULL OR ($3::text IS NULL AND (cardinality($6)=0 OR m.context_keys && $6)) OR ($3::text IS NOT NULL AND cardinality(m.context_keys)=cardinality($6) AND m.context_keys @> $6)) RETURNING m.id), emitted AS MATERIALIZED (SELECT novu_rust_emit_ws_event(t.environment_id,t.internal_id,COALESCE($6::text[],ARRAY[]::text[]),'unread_count_changed','{\"recalculate\":true}'::jsonb) AS sent FROM target t WHERE $3::text IS NULL OR EXISTS(SELECT 1 FROM removed)) SELECT EXISTS(SELECT 1 FROM target), EXISTS(SELECT 1 FROM removed),(SELECT count(*) FROM emitted)", &[&environment_id, &subscriber_id, &notification_id, &groups, &data_filter, &context_keys]).await;
    match result {
        Ok(Some(row)) if !row.get::<_, bool>(0) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Subscriber '{subscriber_id}' was not found"),
            "Not Found",
            path,
        )
        .into_response(),
        Ok(Some(row)) if notification_id.is_some() && !row.get::<_, bool>(1) => error_at(
            StatusCode::NOT_FOUND,
            &format!(
                "Notification with id: {} is not found.",
                notification_id.unwrap_or_default()
            ),
            "Not Found",
            path,
        )
        .into_response(),
        Ok(Some(_)) => StatusCode::NO_CONTENT.into_response(),
        _ => error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            path,
        )
        .into_response(),
    }
}

async fn list_notifications(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    Query(pairs): Query<Vec<(String, String)>>,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    list_notifications_for(&db, &environment_id, subscriber_id, path, pairs, None).await
}

async fn list_notifications_for(
    db: &Client,
    environment_id: &str,
    subscriber_id: String,
    path: String,
    pairs: Vec<(String, String)>,
    context_keys: Option<Vec<String>>,
) -> (StatusCode, Json<Value>) {
    let unsupported = || {
        error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification list option not implemented",
            "Not Implemented",
            &path,
        )
    };
    let mut query = HashMap::new();
    let mut tags = Vec::new();
    let mut severities = Vec::new();
    let mut requested_context_keys = Vec::new();
    for (key, value) in pairs {
        if key == "contextKeys" && context_keys.is_none() {
            requested_context_keys.push(value);
        } else if key == "tags" {
            tags.push(value);
        } else if key == "severity" {
            severities.push(value);
        } else if query.insert(key, value).is_some() {
            return unsupported();
        }
    }
    let context_keys = if requested_context_keys.is_empty() {
        context_keys
    } else {
        Some(requested_context_keys)
    };
    if tags.len() > 100 {
        return unsupported();
    }
    let tag_filter = if tags.is_empty() { None } else { Some(tags) };
    if severities
        .iter()
        .any(|value| !matches!(value.as_str(), "none" | "low" | "medium" | "high"))
    {
        return unsupported();
    }
    let severity_filter = if severities.is_empty() {
        None
    } else {
        Some(severities)
    };
    if query.keys().any(|key| {
        ![
            "limit",
            "offset",
            "after",
            "read",
            "seen",
            "archived",
            "snoozed",
            "data",
            "createdGte",
            "createdLte",
        ]
        .contains(&key.as_str())
    }) {
        return unsupported();
    }
    let limit = match query.get("limit") {
        None => 10_i64,
        Some(value) => match value.parse::<i64>() {
            Ok(value) if (1..=100).contains(&value) => value,
            _ => return unsupported(),
        },
    };
    let offset = match query.get("offset") {
        None => 0_i64,
        Some(value) => match value.parse::<i64>() {
            Ok(value) if value >= 0 => value,
            _ => return unsupported(),
        },
    };
    let after = query.get("after").map(|value| value.to_ascii_lowercase());
    if let Some(cursor) = &after
        && (cursor.len() != 24 || !cursor.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return unsupported();
    }
    let mut filter = serde_json::Map::new();
    if let Some(tags) = &tag_filter {
        filter.insert("tags".into(), json!(tags));
    }
    if let Some(severities) = &severity_filter {
        filter.insert(
            "severity".into(),
            if severities.len() == 1 {
                json!(severities[0])
            } else {
                json!(severities)
            },
        );
    }
    for key in ["read", "seen", "archived", "snoozed"] {
        if let Some(value) = query.get(key) {
            match value.as_str() {
                "true" => {
                    filter.insert(key.into(), json!(true));
                }
                "false" => {
                    filter.insert(key.into(), json!(false));
                }
                "" => (),
                _ => return unsupported(),
            }
        }
    }
    let mut created_bounds = [None, None];
    let mut date_errors = Vec::new();
    for (index, key) in ["createdGte", "createdLte"].iter().enumerate() {
        if let Some(value) = query.get(*key).filter(|value| !value.is_empty()) {
            let timestamp = match parse_date_integer(value) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    date_errors.push(if index == 0 {
                        "createdGte must be an integer number"
                    } else {
                        "createdLte must be an integer number"
                    });
                    continue;
                }
                Err(()) => return unsupported(),
            };
            // JavaScript Date accepts milliseconds in this range. Novu keeps
            // zero in the response but treats it as no database bound.
            if timestamp.unsigned_abs() > 8_640_000_000_000_000 {
                return unsupported();
            }
            filter.insert((*key).into(), json!(timestamp));
            if timestamp != 0 {
                created_bounds[index] = Some(timestamp);
            }
        }
    }
    if !date_errors.is_empty() {
        let mut response = validation_error(date_errors);
        response.1.0["path"] = json!(path);
        return response;
    }
    let subscriber = match db.query_opt("SELECT internal_id, profile FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2", &[&environment_id, &subscriber_id]).await {
        Ok(Some(row)) => row,
        Ok(None) => return error_at(StatusCode::NOT_FOUND, &format!("Subscriber '{subscriber_id}' was not found"), "Not Found", &path),
        Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path),
    };
    if let Some(cursor) = &after {
        match db.query_opt("SELECT 1 FROM novu_rust_in_app_messages WHERE environment_id=$1 AND internal_id=$2", &[&environment_id, &cursor]).await {
            Ok(Some(_)) => (),
            Ok(None) => return unsupported(),
            Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path),
        }
    }
    if filter.get("read") == Some(&json!(false)) && filter.get("archived") == Some(&json!(true)) {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Filtering for unread and archived notifications is not supported.",
            "Bad Request",
            &path,
        );
    }
    let (data, data_filter) = match parse_inbox_data(query.get("data").map(String::as_str), &path) {
        Ok(value) => value,
        Err(error) => return error,
    };
    if let Some(data) = data {
        filter.insert("data".into(), data);
    }
    let read = filter.get("read").and_then(Value::as_bool);
    let seen = filter.get("seen").and_then(Value::as_bool);
    let archived = filter.get("archived").and_then(Value::as_bool);
    let snoozed = filter.get("snoozed").and_then(Value::as_bool);
    let actual_offset = if after.is_some() { 0 } else { offset };
    let rows = match db.query("SELECT m.internal_id, m.transaction_id, m.content, m.payload, m.is_read, m.is_seen, m.is_archived, to_char(m.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), to_char(m.delivered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), to_char(m.last_read_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), to_char(m.first_seen_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), to_char(m.archived_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), m.tags, w.inbox_metadata, m.read_at_present, m.archived_at_present, m.data, to_char(m.snoozed_until AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS snoozed_until,CASE WHEN m.delivered_history IS NULL THEN NULL ELSE to_jsonb(ARRAY(SELECT to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM unnest(m.delivered_history) WITH ORDINALITY AS h(t,ord) ORDER BY ord)) END AS delivered_history, m.severity AS message_severity,m.cta AS message_cta FROM novu_rust_in_app_messages m LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier WHERE m.environment_id=$1 AND m.subscriber_id=$2 AND m.subscriber_internal_id=(SELECT internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2) AND ($3::boolean IS NULL OR m.is_read=$3) AND ($4::boolean IS NULL OR m.is_seen=$4) AND ($5::boolean IS NULL OR m.is_archived=$5) AND ($9::text[] IS NULL OR m.tags ?| $9) AND novu_rust_inbox_data_matches(m.data,$10) AND ($11::boolean IS NULL OR (m.snoozed_until IS NOT NULL)=$11) AND ($13::text[] IS NULL OR (NOT m.severity_present AND 'none'=ANY($13)) OR m.severity=ANY($13)) AND ($14::bigint IS NULL OR m.created_at >= to_timestamp($14::double precision/1000.0)) AND ($15::bigint IS NULL OR m.created_at <= to_timestamp($15::double precision/1000.0)) AND ($12::text[] IS NULL OR (cardinality(m.context_keys)=cardinality($12) AND m.context_keys @> $12)) AND ($8::text IS NULL OR (m.created_at,m.internal_id) < (SELECT boundary.created_at,boundary.internal_id FROM novu_rust_in_app_messages boundary WHERE boundary.environment_id=$1 AND boundary.internal_id=$8)) ORDER BY m.created_at DESC, m.internal_id DESC LIMIT $6 OFFSET $7", &[&environment_id, &subscriber_id, &read, &seen, &archived, &(limit + 1), &actual_offset, &after, &tag_filter, &data_filter, &snoozed, &context_keys, &severity_filter, &created_bounds[0], &created_bounds[1]]).await {
        Ok(rows) => rows,
        Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path),
    };
    let has_more = rows.len() > limit as usize;
    let subscriber_profile: Value = subscriber.get(1);
    let mut to = json!({"id":subscriber.get::<_, String>(0), "subscriberId": subscriber_id});
    for key in ["firstName", "lastName", "avatar"] {
        if let Some(value) = subscriber_profile.get(key) {
            to[key] = value.clone();
        }
    }
    let mut data = Vec::new();
    for row in rows.into_iter().take(limit as usize) {
        let Some(item) = notification_dto(&row, &to) else {
            return unsupported();
        };
        data.push(item);
    }
    (
        StatusCode::OK,
        Json(json!({"data":data, "hasMore":has_more, "filter":filter})),
    )
}

// Match the decimal-prefix behavior of JavaScript parseInt(value, 10).
// An overflow remains unsupported instead of wrapping or rounding an i64.
fn parse_date_integer(value: &str) -> Result<Option<i64>, ()> {
    let value = value.trim_start();
    let (negative, digits) = if let Some(rest) = value.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = value.strip_prefix('+') {
        (false, rest)
    } else {
        (false, value)
    };
    let prefix = digits.bytes().take_while(u8::is_ascii_digit).count();
    if prefix == 0 {
        return Ok(None);
    }
    let number = digits[..prefix].parse::<i128>().map_err(|_| ())?;
    let signed = if negative { -number } else { number };
    i64::try_from(signed).map(Some).map_err(|_| ())
}

// Bounded implementation of the source in-app redirect allowlist.
fn in_app_redirect(value: &Value) -> Option<Value> {
    let url = value.get("url")?.as_str()?;
    if url.is_empty() {
        return None;
    }
    let rest = if let Some(stripped) = url.strip_prefix("{{") {
        let end = stripped.find('}')? + 2;
        if !url[end..].starts_with("}}") || url[end + 2..].contains(['\n', '\r']) {
            return None;
        }
        None
    } else if url.starts_with('/') && !url.starts_with("//") {
        Some(&url[1..])
    } else if let Some(rest) = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
    {
        let first = rest.chars().next()?;
        if first.is_whitespace() || first == '\u{feff}' || "/$.?#{}".contains(first) {
            return None;
        }
        Some(rest)
    } else {
        return None;
    };
    if let Some(mut rest) = rest {
        while !rest.is_empty() {
            if rest.starts_with("{{") {
                let end = rest[2..].find('}')? + 2;
                if !rest[end..].starts_with("}}") {
                    return None;
                }
                rest = &rest[end + 2..];
            } else {
                let ch = rest.chars().next()?;
                if ch.is_whitespace() || ch == '\u{feff}' || ch == '{' || ch == '}' {
                    return None;
                }
                rest = &rest[ch.len_utf8()..];
            }
        }
    }
    let mut result = json!({"url":url});
    if let Some(target) = value
        .get("target")
        .and_then(Value::as_str)
        .filter(|v| ["_self", "_blank", "_parent", "_top", "_unfencedTop"].contains(v))
    {
        result["target"] = json!(target);
    }
    Some(result)
}

fn notification_dto(row: &tokio_postgres::Row, to: &Value) -> Option<Value> {
    let metadata: Option<Value> = row.get(13);
    let metadata = metadata.filter(Value::is_object)?;
    if !metadata.get("id").is_some_and(Value::is_string)
        || !metadata.get("identifier").is_some_and(Value::is_string)
        || !metadata.get("name").is_some_and(Value::is_string)
        || !metadata.get("critical").is_some_and(Value::is_boolean)
        || !metadata.get("tags").is_some_and(Value::is_array)
        || !metadata.get("payloadAsData").is_some_and(Value::is_boolean)
        || !metadata
            .get("severity")
            .and_then(Value::as_str)
            .is_some_and(|value| matches!(value, "none" | "low" | "medium" | "high"))
    {
        return None;
    }
    let workflow = json!({"id":metadata["id"],"identifier":metadata["identifier"],"name":metadata["name"],"critical":metadata["critical"],"tags":metadata["tags"],"severity":metadata["severity"]});
    let tags: Value = row.get(12);
    let snoozed_until: Option<String> = row.get("snoozed_until");
    let message_severity = row
        .get::<_, Option<String>>("message_severity")
        .unwrap_or_else(|| "none".to_string());
    let mut item = json!({"id":row.get::<_, String>(0), "transactionId":row.get::<_, String>(1), "body":row.get::<_, String>(2), "to":to, "isRead":row.get::<_, bool>(4), "isSeen":row.get::<_, bool>(5), "isArchived":row.get::<_, bool>(6), "isSnoozed":snoozed_until.is_some(), "createdAt":row.get::<_, String>(7), "channelType":"in_app", "tags":tags, "severity":message_severity, "workflow":workflow});
    if let Some(value) = snoozed_until {
        item["snoozedUntil"] = json!(value);
    }
    if let Some(history) = row.get::<_, Option<Value>>("delivered_history") {
        item["deliveredAt"] = history;
    }
    for (column, key) in [
        (8, "deliveredAt"),
        (9, "readAt"),
        (10, "firstSeenAt"),
        (11, "archivedAt"),
    ] {
        if key == "deliveredAt" && item.get(key).is_some() {
            continue;
        }
        if let Some(value) = row.get::<_, Option<String>>(column) {
            item[key] = if key == "deliveredAt" {
                json!([value])
            } else {
                json!(value)
            };
        }
    }
    if row.get::<_, bool>(14) && item.get("readAt").is_none() {
        item["readAt"] = Value::Null;
    }
    if row.get::<_, bool>(15) && item.get("archivedAt").is_none() {
        item["archivedAt"] = Value::Null;
    }
    let payload = novu_rust_contracts::message::in_app_payload(&row.get::<_, Value>(3));
    let message_data: Option<Value> = row.get(16);
    if let Some(data) = message_data.filter(|value| !value.is_null()) {
        item["data"] = data;
    } else if metadata.get("payloadAsData") == Some(&Value::Bool(true))
        && payload.as_object().is_some_and(|value| !value.is_empty())
    {
        item["data"] = payload;
    }
    let cta: Option<Value> = row.get("message_cta");
    if let Some(cta) = cta {
        if let Some(redirect) = cta.get("data").and_then(in_app_redirect) {
            item["redirect"] = redirect;
        }
        if let Some(buttons) = cta.pointer("/action/buttons").and_then(Value::as_array) {
            for (kind, key) in [
                ("primary", "primaryAction"),
                ("secondary", "secondaryAction"),
            ] {
                if let Some(button) = buttons
                    .iter()
                    .find(|button| button.get("type").and_then(Value::as_str) == Some(kind))
                {
                    let completed = cta.pointer("/action/result/type").and_then(Value::as_str)
                        == Some(kind)
                        && cta.pointer("/action/status").and_then(Value::as_str) == Some("done");
                    let mut action = json!({"isCompleted":completed});
                    if let Some(label) = button.get("content") {
                        action["label"] = label.clone();
                    }
                    if let Some(redirect) = in_app_redirect(button) {
                        action["redirect"] = redirect;
                    }
                    item[key] = action;
                }
            }
        }
    }
    Some(item)
}

async fn mark_notifications_seen(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> Response {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response.into_response(),
    };
    mark_notifications_seen_for(&db, environment_id, subscriber_id, uri, bytes, None).await
}

async fn mark_notifications_seen_for(
    db: &Client,
    environment_id: String,
    subscriber_id: String,
    uri: axum::http::Uri,
    bytes: Bytes,
    context_keys: Option<Vec<String>>,
) -> Response {
    let path = uri.to_string();
    if uri.query().is_some() {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    }
    let body: Value = if bytes.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return error_at(
                    StatusCode::BAD_REQUEST,
                    "Invalid JSON",
                    "Bad Request",
                    &path,
                )
                .into_response();
            }
        }
    };
    let Some(fields) = body.as_object() else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    };
    let Some(groups) = notification_tags(&body, true, context_keys.is_none()) else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    };
    let context_keys = match body_context_keys(&body, context_keys) {
        Ok(keys) => keys,
        Err(()) => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification filters not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
    };
    let ids: Option<Vec<String>> = match fields.get("notificationIds") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let valid = |value: &Value| {
                value
                    .as_str()
                    .is_some_and(|id| id.len() == 24 && id.bytes().all(|b| b.is_ascii_hexdigit()))
            };
            let mut errors = Vec::new();
            if match value {
                Value::Array(items) => !items.iter().all(valid),
                _ => !valid(value),
            } {
                errors.push("each value in notificationIds must be a mongodb id");
            }
            if !value.is_array() {
                errors.push("notificationIds must be an array");
            }
            if !errors.is_empty() {
                let mut response = validation_error(errors);
                response.1.0["path"] = json!(path);
                return response.into_response();
            }
            Some(
                value
                    .as_array()
                    .expect("array validated")
                    .iter()
                    .map(|id| id.as_str().expect("id validated").to_ascii_lowercase())
                    .collect(),
            )
        }
    };
    if ids.as_ref().is_some_and(Vec::is_empty) {
        return StatusCode::NO_CONTENT.into_response();
    }
    let (_, data_filter) = if ids.is_some() {
        (None, json!([]))
    } else {
        match parse_inbox_data(fields.get("data").and_then(Value::as_str), &path) {
            Ok(value) => value,
            Err(error) => return error.into_response(),
        }
    };
    let result = db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id, subscriber_id, internal_id FROM novu_rust_subscribers WHERE environment_id = $1 AND subscriber_id = $2 FOR UPDATE), updated AS (UPDATE novu_rust_in_app_messages m SET is_seen = true, first_seen_at = COALESCE(m.first_seen_at, now()), last_seen_at = now() FROM target WHERE m.environment_id = target.environment_id AND m.subscriber_id = target.subscriber_id AND m.subscriber_internal_id = target.internal_id AND ($3::text[] IS NULL OR m.internal_id = ANY($3::text[])) AND ($3::text[] IS NOT NULL OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements($4::jsonb) g(group_value) WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(g.group_value) t(tag) WHERE m.tags ? t.tag))) AND ($3::text[] IS NOT NULL OR novu_rust_inbox_data_matches(m.data,$5)) AND ($6::text[] IS NULL OR cardinality($6)=0 OR m.context_keys && $6) RETURNING m.id), emitted AS MATERIALIZED (SELECT novu_rust_emit_ws_event(t.environment_id,t.internal_id,COALESCE($6::text[],ARRAY[]::text[]),'unseen_count_changed','{\"recalculate\":true}'::jsonb) AS sent FROM target t WHERE (SELECT count(*) FROM updated)>=0) SELECT EXISTS (SELECT 1 FROM target), (SELECT count(*) FROM updated),(SELECT count(*) FROM emitted)", &[&environment_id, &subscriber_id, &ids, &groups, &data_filter, &context_keys]).await;
    match result {
        Ok(Some(row)) if row.get::<_, bool>(0) => StatusCode::NO_CONTENT.into_response(),
        Ok(Some(_)) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Subscriber '{subscriber_id}' was not found"),
            "Not Found",
            &path,
        )
        .into_response(),
        _ => error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            &path,
        )
        .into_response(),
    }
}

async fn update_all_notifications(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> Response {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response.into_response(),
    };
    update_all_notifications_for(&db, environment_id, subscriber_id, uri, bytes, None).await
}

async fn update_all_notifications_for(
    db: &Client,
    environment_id: String,
    subscriber_id: String,
    uri: axum::http::Uri,
    bytes: Bytes,
    context_keys: Option<Vec<String>>,
) -> Response {
    let path = uri.to_string();
    if uri.query().is_some() {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    }
    let body: Value = if bytes.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return error_at(
                    StatusCode::BAD_REQUEST,
                    "Invalid JSON",
                    "Bad Request",
                    &path,
                )
                .into_response();
            }
        }
    };
    let Some(groups) = notification_tags(&body, false, context_keys.is_none()) else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        )
        .into_response();
    };
    let context_keys = match body_context_keys(&body, context_keys) {
        Ok(keys) => keys,
        Err(()) => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Notification filters not implemented",
                "Not Implemented",
                &path,
            )
            .into_response();
        }
    };
    let (_, data_filter) = match parse_inbox_data(body.get("data").and_then(Value::as_str), &path) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let action = uri.path().rsplit('/').next().unwrap_or("");
    let archive = action != "read";
    let only_read = action == "read-archive";
    // Lock the subscriber through the update, avoiding a delete/update race.
    let result = db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id, subscriber_id, internal_id FROM novu_rust_subscribers WHERE environment_id = $1 AND subscriber_id = $2 FOR UPDATE), updated AS (UPDATE novu_rust_in_app_messages m SET is_seen = true, is_read = true, is_archived = CASE WHEN $3 THEN true ELSE m.is_archived END, first_seen_at = COALESCE(m.first_seen_at, now()), last_seen_at = now(), last_read_at = now(), read_at_present = true, archived_at_present = CASE WHEN $3 THEN true ELSE m.archived_at_present END, archived_at = CASE WHEN $3 THEN now() ELSE m.archived_at END FROM target WHERE m.environment_id = target.environment_id AND m.subscriber_id = target.subscriber_id AND m.subscriber_internal_id = target.internal_id AND (NOT $4 OR m.is_read) AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements($5::jsonb) g(group_value) WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(g.group_value) t(tag) WHERE m.tags ? t.tag)) AND novu_rust_inbox_data_matches(m.data,$6) AND ($7::text[] IS NULL OR cardinality($7)=0 OR m.context_keys && $7) RETURNING m.id), emitted AS MATERIALIZED (SELECT novu_rust_emit_ws_event(t.environment_id,t.internal_id,COALESCE($7::text[],ARRAY[]::text[]),'unread_count_changed','{\"recalculate\":true}'::jsonb) AS sent FROM target t WHERE (SELECT count(*) FROM updated)>=0) SELECT EXISTS (SELECT 1 FROM target), (SELECT count(*) FROM updated),(SELECT count(*) FROM emitted)", &[&environment_id, &subscriber_id, &archive, &only_read, &groups, &data_filter, &context_keys]).await;
    match result {
        Ok(Some(row)) if row.get::<_, bool>(0) => StatusCode::NO_CONTENT.into_response(),
        Ok(Some(_)) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Subscriber '{subscriber_id}' was not found"),
            "Not Found",
            &path,
        )
        .into_response(),
        _ => error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            &path,
        )
        .into_response(),
    }
}

async fn count_notifications(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    Query(query): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    count_notifications_for(&db, &environment_id, subscriber_id, path, query, None).await
}

async fn count_notifications_for(
    db: &Client,
    environment_id: &str,
    subscriber_id: String,
    path: String,
    query: HashMap<String, String>,
    context_keys: Option<Vec<String>>,
) -> (StatusCode, Json<Value>) {
    let Some(raw_filters) = query.get("filters") else {
        let mut response = validation_error(vec![
            "filters should not be null or undefined",
            "filters must contain no more than 30 elements",
            "filters must be an array",
        ]);
        response.1.0["path"] = json!(path);
        return response;
    };
    let Ok(filters) = serde_json::from_str::<Value>(raw_filters) else {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Invalid filters, the JSON object should be provided.",
            "Bad Request",
            &path,
        );
    };
    let Some(filters) = filters.as_array() else {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        );
    };
    if query.len() != 1
        || filters.len() > 30
        || filters.iter().any(|filter| {
            filter.as_object().is_none_or(|fields| {
                fields.iter().any(|(key, value)| match key.as_str() {
                    "read" | "seen" | "archived" | "snoozed" => {
                        !value.is_boolean() && !value.is_null()
                    }
                    "tags" => normalize_tag_groups(value).is_none(),
                    // Novu validates filter.contextKeys, but its DAL does not
                    // apply this property to the count query.
                    "contextKeys" => {
                        !value.is_null()
                            && value
                                .as_array()
                                .is_none_or(|keys| !keys.iter().all(Value::is_string))
                    }
                    "severity" => {
                        let valid = |v: &Value| {
                            matches!(v.as_str(), Some("none" | "low" | "medium" | "high"))
                        };
                        !value.is_null()
                            && !valid(value)
                            && value
                                .as_array()
                                .is_none_or(|values| !values.iter().all(valid))
                    }
                    _ => true,
                })
            })
        })
    {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Notification filters not implemented",
            "Not Implemented",
            &path,
        );
    }
    let subscriber = db
        .query_opt(
            "SELECT 1 FROM novu_rust_subscribers WHERE environment_id = $1 AND subscriber_id = $2",
            &[&environment_id, &subscriber_id],
        )
        .await;
    match subscriber {
        Ok(Some(_)) => (),
        Ok(None) => {
            return error_at(
                StatusCode::BAD_REQUEST,
                &format!(
                    "Subscriber {subscriber_id} doesn't exist in environment {environment_id}"
                ),
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
    if filters.iter().any(|filter| {
        filter.get("read") == Some(&Value::Bool(false))
            && filter.get("archived") == Some(&Value::Bool(true))
    }) {
        return error_at(
            StatusCode::BAD_REQUEST,
            "Filtering for unread and archived notifications is not supported.",
            "Bad Request",
            &path,
        );
    }
    let requests: Vec<Value> = filters
        .iter()
        .map(|filter| {
            json!({
                "filter": filter,
                "groups": filter.get("tags").and_then(normalize_tag_groups).unwrap_or_default(),
                "severities": match filter.get("severity") {
                    Some(Value::String(value)) => json!([value]),
                    Some(Value::Array(values)) => json!(values),
                    _ => json!([]),
                },
            })
        })
        .collect();
    let requests = json!(requests);
    let row = match db.query_one("SELECT COALESCE(jsonb_agg(jsonb_build_object('count', c.n, 'filter', f.item->'filter') ORDER BY f.ord), '[]'::jsonb) FROM jsonb_array_elements($3::jsonb) WITH ORDINALITY f(item, ord) CROSS JOIN LATERAL (SELECT count(*) AS n FROM (SELECT 1 FROM novu_rust_in_app_messages m WHERE m.environment_id=$1 AND m.subscriber_id=$2 AND m.subscriber_internal_id=(SELECT internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2) AND ($4::text[] IS NULL OR (cardinality(m.context_keys)=cardinality($4) AND m.context_keys @> $4)) AND (f.item->'filter'->>'read' IS NULL OR m.is_read=(f.item->'filter'->>'read')::boolean) AND (f.item->'filter'->>'seen' IS NULL OR m.is_seen=(f.item->'filter'->>'seen')::boolean) AND (f.item->'filter'->>'archived' IS NULL OR m.is_archived=(f.item->'filter'->>'archived')::boolean) AND (f.item->'filter'->>'snoozed' IS NULL OR (m.snoozed_until IS NOT NULL)=(f.item->'filter'->>'snoozed')::boolean) AND (jsonb_array_length(f.item->'severities')=0 OR (NOT m.severity_present AND (f.item->'severities') ? 'none') OR m.severity IN (SELECT jsonb_array_elements_text(f.item->'severities'))) AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(f.item->'groups') AS g(group_value) WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(g.group_value) AS t(tag) WHERE m.tags ? t.tag)) LIMIT 100) matches) c", &[&environment_id, &subscriber_id, &requests, &context_keys]).await {
        Ok(row) => row,
        Err(_) => return error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", &path),
    };
    let result: Value = row.get(0);
    (StatusCode::OK, Json(json!({"data": result})))
}

// Management bulk operations accept contextKeys in the body. Subscriber
// session context remains authoritative and cannot be overridden by the body.
fn body_context_keys(
    body: &Value,
    session: Option<Vec<String>>,
) -> Result<Option<Vec<String>>, ()> {
    if session.is_some() {
        return Ok(session);
    }
    match body.get("contextKeys") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(keys)) => keys
            .iter()
            .map(|key| key.as_str().map(str::to_owned).ok_or(()))
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(_) => Err(()),
    }
}

fn notification_tags(body: &Value, allow_ids: bool, allow_context: bool) -> Option<Value> {
    let fields = body.as_object()?;
    if fields.keys().any(|key| {
        key != "tags"
            && key != "data"
            && !(allow_ids && key == "notificationIds")
            && !(allow_context && key == "contextKeys")
    }) {
        return None;
    }
    if fields
        .get("data")
        .is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return None;
    }
    let groups = match fields.get("tags") {
        Some(tags) => normalize_tag_groups(tags)?,
        None => Vec::new(),
    };
    Some(json!(groups))
}

fn parse_inbox_data(
    raw: Option<&str>,
    path: &str,
) -> Result<(Option<Value>, Value), (StatusCode, Json<Value>)> {
    let Some(raw) = raw.filter(|s| !s.is_empty()) else {
        return Ok((None, json!([])));
    };
    let value: Value = serde_json::from_str(raw).map_err(|_| {
        error_at(
            StatusCode::BAD_REQUEST,
            "Invalid JSON format for data parameter",
            "Bad Request",
            path,
        )
    })?;
    let filter = inbox_data::normalize(&value).ok_or_else(|| {
        error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Data filter not implemented",
            "Not Implemented",
            path,
        )
    })?;
    Ok((Some(value), filter))
}

// Equivalent supported forms and limits from @novu/shared's normalizeTagGroups.
fn normalize_tag_groups(value: &Value) -> Option<Vec<Vec<String>>> {
    fn group(value: &Value, empty_allowed: bool) -> Option<Vec<String>> {
        let values = value.as_array()?;
        if values.len() > 100 || (!empty_allowed && values.is_empty()) {
            return None;
        }
        values
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect()
    }
    if value.is_array() {
        let tags = group(value, true)?;
        return Some(if tags.is_empty() { vec![] } else { vec![tags] });
    }
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    if let Some(or) = object.get("or") {
        let tags = group(or, true)?;
        return Some(if tags.is_empty() { vec![] } else { vec![tags] });
    }
    let and = object.get("and")?.as_array()?;
    if and.len() > 30 {
        return None;
    }
    let mut groups = Vec::with_capacity(and.len());
    for entry in and {
        let inner = entry.as_object()?;
        if inner.len() != 1 {
            return None;
        }
        groups.push(group(inner.get("or")?, false)?);
    }
    if groups.iter().map(Vec::len).sum::<usize>() > 200 {
        return None;
    }
    Some(groups)
}

fn error(status: StatusCode, message: &str, label: &str) -> (StatusCode, Json<Value>) {
    let code = status.as_u16();
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut body = json!({"statusCode": code, "timestamp": timestamp, "path": "/v1/events/trigger", "message": message});
    if status == StatusCode::UNAUTHORIZED && message == "Unauthorized" {
        body["ctx"] = json!({"statusCode": code});
    } else {
        body["error"] = json!(label);
        body["ctx"] = json!({"error": label, "statusCode": code});
    }
    (status, Json(body))
}

fn validation_error(messages: Vec<&str>) -> (StatusCode, Json<Value>) {
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({
            "statusCode": 422, "timestamp": timestamp, "path": "/v1/events/trigger",
            "message": "Validation Error", "ctx": {},
            "errors": {"general": {"messages": messages, "value": "No Value Recorded"}}
        })),
    )
}

fn valid_recipient(value: &Value) -> bool {
    match value {
        Value::String(s) => !s.is_empty(),
        Value::Object(o) => {
            let key = if o.get("type").and_then(Value::as_str) == Some("Topic") {
                "topicKey"
            } else if o.contains_key("subscriberId") {
                "subscriberId"
            } else {
                "topicKey"
            };
            o.get(key)
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        }
        _ => false,
    }
}

pub fn valid_recipients(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Array(items)) => !items.is_empty() && items.iter().all(valid_recipient),
        Some(value) => valid_recipient(value),
        None => false,
    }
}

type ApiError = (StatusCode, Json<Value>);

fn error_at(status: StatusCode, message: &str, label: &str, path: &str) -> ApiError {
    let mut response = error(status, message, label);
    response.1.0["path"] = json!(path);
    response
}

async fn authenticate(db: &Client, headers: &HeaderMap, path: &str) -> Result<String, ApiError> {
    let header = headers
        .get("authorization")
        .and_then(|x| x.to_str().ok())
        .filter(|x| !x.is_empty())
        .ok_or_else(|| {
            error_at(
                StatusCode::UNAUTHORIZED,
                "Missing authorization header",
                "Unauthorized",
                path,
            )
        })?;
    if header == "ApiKey" || header == "Bearer" || header.starts_with("Bearer ") {
        return Err(error_at(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "Unauthorized",
            path,
        ));
    }
    let key = header.strip_prefix("ApiKey ").ok_or_else(|| {
        let scheme = header.split_whitespace().next().unwrap_or("");
        error_at(
            StatusCode::UNAUTHORIZED,
            &format!("Invalid authentication scheme: \"{scheme}\""),
            "Unauthorized",
            path,
        )
    })?;
    if key.is_empty() {
        return Err(error_at(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "Unauthorized",
            path,
        ));
    }
    let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
    match db.query_opt("SELECT k.environment_id, u.id FROM novu_rust_api_keys k LEFT JOIN novu_rust_users u ON u.id = k.user_id WHERE k.key_hash = $1 AND k.enabled", &[&hash]).await {
        Ok(Some(row)) if row.get::<_, Option<String>>(1).is_none() => Err(error_at(StatusCode::UNAUTHORIZED, "User not found", "Unauthorized", path)),
        Ok(Some(row)) => Ok(row.get(0)),
        Ok(None) => Err(error_at(StatusCode::UNAUTHORIZED, "API Key not found", "Unauthorized", path)),
        Err(_) => Err(error_at(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error", path)),
    }
}

async fn update_online_status(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response,
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
    let online = match body.get("isOnline").and_then(Value::as_bool) {
        Some(value) => value,
        None => {
            let mut errors = Vec::new();
            if body.get("isOnline").is_none_or(Value::is_null) {
                errors.push("isOnline should not be null or undefined");
            }
            errors.push("isOnline must be a boolean value");
            let mut response = validation_error(errors);
            response.1.0["path"] = json!(path);
            return response;
        }
    };
    let row = db.query_opt("WITH changed AS (UPDATE novu_rust_subscribers SET is_online=$3,last_online_at=CASE WHEN $3 THEN last_online_at ELSE now() END,updated_at=now() WHERE environment_id=$1 AND subscriber_id=$2 RETURNING *) SELECT changed.internal_id,changed.profile,e.organization_id,to_char(changed.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(changed.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),changed.is_online,to_char(changed.last_online_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at FROM changed JOIN novu_rust_environments e ON e.id=changed.environment_id", &[&environment_id,&subscriber_id,&online]).await;
    match row {
        Ok(Some(row)) => {
            subscriber_credentials::attach(
                &db,
                &environment_id,
                &subscriber_id,
                subscriber_entity_response(row, &subscriber_id, &environment_id),
                &path,
            )
            .await
        }
        Ok(None) => error_at(
            StatusCode::NOT_FOUND,
            "Subscriber not found",
            "Not Found",
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

fn subscriber_entity_response(
    row: tokio_postgres::Row,
    subscriber_id: &str,
    environment_id: &str,
) -> ApiError {
    let mut response = subscriber_response(row, subscriber_id, environment_id, false);
    let subscriber = response.1.0["data"]
        .as_object_mut()
        .expect("subscriber object");
    subscriber.insert("id".into(), subscriber["_id"].clone());
    subscriber.insert("__v".into(), json!(0));
    if subscriber.get("lastOnlineAt").is_some_and(Value::is_null) {
        subscriber.remove("lastOnlineAt");
    }
    response
}

async fn get_subscriber_v1(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> ApiError {
    let path = uri.to_string();
    let env = match authenticate(&db, &headers, &path).await {
        Ok(env) => env,
        Err(err) => return err,
    };
    let result = db.query_opt("SELECT s.internal_id,s.profile,e.organization_id,to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(s.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),s.is_online,to_char(s.last_online_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at,ARRAY(SELECT DISTINCT topic_key FROM novu_rust_topic_subscribers t WHERE t.environment_id=s.environment_id AND t.subscriber_id=s.subscriber_id ORDER BY topic_key) AS topics FROM novu_rust_subscribers s JOIN novu_rust_environments e ON e.id=s.environment_id WHERE s.environment_id=$1 AND s.subscriber_id=$2", &[&env,&subscriber_id]).await;
    match result {
        Ok(Some(row)) => {
            let topics: Vec<String> = row.get("topics");
            let mut response = subscriber_entity_response(row, &subscriber_id, &env);
            if params
                .get("includeTopics")
                .is_some_and(|value| value == "true")
            {
                response.1.0["data"]["topics"] = json!(topics);
            }
            subscriber_credentials::attach(&db, &env, &subscriber_id, response, &path).await
        }
        Ok(None) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Subscriber '{subscriber_id}' was not found"),
            "Not Found",
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

async fn get_subscriber(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let result = db.query_opt("SELECT s.internal_id, s.profile, e.organization_id, to_char(s.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), to_char(s.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), s.is_online, to_char(s.last_online_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at FROM novu_rust_subscribers s JOIN novu_rust_environments e ON e.id = s.environment_id WHERE s.environment_id = $1 AND s.subscriber_id = $2", &[&environment_id, &subscriber_id]).await;
    let row = match result {
        Ok(Some(row)) => row,
        Ok(None) => {
            return error_at(
                StatusCode::NOT_FOUND,
                &format!("Subscriber: {subscriber_id} was not found"),
                "Not Found",
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
    };
    subscriber_credentials::attach(
        &db,
        &environment_id,
        &subscriber_id,
        subscriber_response(row, &subscriber_id, &environment_id, false),
        &path,
    )
    .await
}

async fn delete_subscriber(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    // Lock the original internal identity, so deletion and its related data
    // commit together without affecting a subsequent external-ID recreation.
    let result = db.query_opt("WITH target AS MATERIALIZED (SELECT environment_id, subscriber_id, internal_id FROM novu_rust_subscribers WHERE environment_id = $1 AND subscriber_id = $2 FOR UPDATE), messages AS (DELETE FROM novu_rust_in_app_messages m USING target WHERE m.environment_id = target.environment_id AND m.subscriber_id = target.subscriber_id AND m.subscriber_internal_id = target.internal_id RETURNING m.id), preferences AS (DELETE FROM novu_rust_subscription_preferences p USING target WHERE p.environment_id=target.environment_id AND p.subscriber_internal_id=target.internal_id RETURNING 1), email_messages AS (DELETE FROM novu_rust_email_messages m USING target WHERE m.environment_id=target.environment_id AND m.subscriber_internal_id=target.internal_id RETURNING 1), sms_messages AS (DELETE FROM novu_rust_sms_messages m USING target WHERE m.environment_id=target.environment_id AND m.subscriber_internal_id=target.internal_id RETURNING 1), push_messages AS (DELETE FROM novu_rust_push_messages m USING target WHERE m.environment_id=target.environment_id AND m.subscriber_internal_id=target.internal_id RETURNING 1), removed AS (DELETE FROM novu_rust_subscribers s USING target WHERE s.environment_id = target.environment_id AND s.subscriber_id = target.subscriber_id AND (SELECT count(*) FROM messages) >= 0 AND (SELECT count(*) FROM preferences) >= 0 AND (SELECT count(*) FROM email_messages) >= 0 AND (SELECT count(*) FROM sms_messages) >= 0 AND (SELECT count(*) FROM push_messages) >= 0 RETURNING s.subscriber_id) SELECT subscriber_id FROM removed", &[&environment_id, &subscriber_id]).await;
    match result {
        Ok(Some(_)) => (
            StatusCode::OK,
            Json(json!({"data":{"acknowledged":true,"status":"deleted"}})),
        ),
        Ok(None) => {
            let mut response = error_at(
                StatusCode::NOT_FOUND,
                "Subscriber was not found",
                "Not Found",
                &path,
            );
            response
                .1
                .0
                .as_object_mut()
                .expect("error body object")
                .remove("error");
            response.1.0["externalSubscriberId"] = json!(subscriber_id);
            response.1.0["ctx"] = json!({"externalSubscriberId": subscriber_id});
            response
        }
        Err(_) => error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            &path,
        ),
    }
}

fn subscriber_response(
    row: tokio_postgres::Row,
    subscriber_id: &str,
    environment_id: &str,
    include_empty_data: bool,
) -> (StatusCode, Json<Value>) {
    let profile: Value = row.get(1);
    let mut subscriber = json!({"_id": row.get::<_, String>(0), "subscriberId": subscriber_id, "_environmentId": environment_id, "_organizationId": row.get::<_, String>(2), "createdAt": row.get::<_, String>(3), "updatedAt": row.get::<_, String>(4), "deleted": false, "channels": [], "isOnline": row.get::<_,bool>("is_online"), "lastOnlineAt": row.get::<_,Option<String>>("last_online_at")});
    if let Some(profile) = profile.as_object() {
        for field in [
            "firstName",
            "lastName",
            "email",
            "phone",
            "avatar",
            "locale",
            "timezone",
            "data",
        ] {
            if let Some(value) = profile.get(field).filter(|value| {
                field != "data"
                    || include_empty_data
                    || !value.as_object().is_some_and(|object| object.is_empty())
            }) {
                subscriber[field] = value.clone();
            }
        }
    }
    (StatusCode::OK, Json(json!({"data": subscriber})))
}

fn profile_errors(fields: &serde_json::Map<String, Value>) -> Vec<&'static str> {
    let mut errors = Vec::new();
    for (field, message) in [
        ("firstName", "firstName must be a string"),
        ("lastName", "lastName must be a string"),
        ("email", "email must be an email"),
        ("phone", "phone must be a string"),
        ("avatar", "avatar must be a string"),
        ("locale", "locale must be locale"),
        ("timezone", "timezone must be a valid IANA time-zone"),
        ("data", "data must be an object"),
    ] {
        if fields.get(field).is_some_and(|value| {
            !value.is_null()
                && if field == "email" {
                    value
                        .as_str()
                        .and_then(novu_rust_contracts::email::validate)
                        == Some(false)
                        || !value.is_string()
                } else if field == "locale" {
                    !value
                        .as_str()
                        .is_some_and(novu_rust_contracts::locale::valid)
                } else if field == "timezone" {
                    !value
                        .as_str()
                        .is_some_and(novu_rust_contracts::timezone::valid)
                } else if field == "data" {
                    !value.is_object()
                } else {
                    !value.is_string()
                }
        }) {
            errors.push(message);
        }
    }
    errors
}

async fn create_subscriber(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let fail_if_exists = match uri.query() {
        None | Some("") | Some("failIfExists=false") => false,
        Some("failIfExists=true") => true,
        _ => {
            return error_at(
                StatusCode::NOT_IMPLEMENTED,
                "Subscriber create option not implemented",
                "Not Implemented",
                &path,
            );
        }
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
            "Subscriber create not implemented",
            "Not Implemented",
            &path,
        );
    };
    if fields.keys().any(|key| {
        ![
            "subscriberId",
            "firstName",
            "lastName",
            "email",
            "phone",
            "avatar",
            "locale",
            "timezone",
            "data",
        ]
        .contains(&key.as_str())
    }) {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Subscriber create not implemented",
            "Not Implemented",
            &path,
        );
    }
    if fields
        .get("email")
        .and_then(Value::as_str)
        .is_some_and(|email| novu_rust_contracts::email::validate(email).is_none())
    {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Email format not implemented",
            "Not Implemented",
            &path,
        );
    }
    let mut errors = Vec::new();
    match fields.get("subscriberId") {
        None | Some(Value::Null) => errors.extend([
            "subscriberId should not be null or undefined",
            "SubscriberId is required",
            "subscriberId must be a string",
        ]),
        Some(Value::String(value)) if value.trim().is_empty() => {
            errors.push("SubscriberId is required")
        }
        Some(Value::String(_)) => (),
        Some(_) => errors.push("subscriberId must be a string"),
    }
    errors.extend(profile_errors(fields));
    if !errors.is_empty() {
        let mut response = validation_error(errors);
        response.1.0["path"] = json!(path);
        return response;
    }
    let Some(subscriber_id) = fields
        .get("subscriberId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return error_at(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
            &path,
        );
    };
    let mut profile = fields.clone();
    profile.remove("subscriberId");
    if profile.get("data").is_none_or(Value::is_null) {
        profile.insert("data".into(), json!({}));
    }
    let profile = Value::Object(profile);
    let result = db.query_opt("WITH changed AS (INSERT INTO novu_rust_subscribers (environment_id, subscriber_id, internal_id, profile) VALUES ($1, $2, novu_rust_new_objectid(), $3) ON CONFLICT (environment_id, subscriber_id) DO UPDATE SET profile = novu_rust_subscribers.profile || EXCLUDED.profile, updated_at = CASE WHEN novu_rust_subscribers.profile IS DISTINCT FROM novu_rust_subscribers.profile || EXCLUDED.profile THEN now() ELSE novu_rust_subscribers.updated_at END WHERE NOT $4 RETURNING *) SELECT changed.internal_id, changed.profile, e.organization_id, to_char(changed.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), to_char(changed.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), changed.is_online, to_char(changed.last_online_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at FROM changed JOIN novu_rust_environments e ON e.id = changed.environment_id", &[&environment_id, &subscriber_id, &profile, &fail_if_exists]).await;
    match result {
        Ok(Some(row)) => {
            let response = subscriber_credentials::attach(
                &db,
                &environment_id,
                subscriber_id,
                subscriber_response(row, subscriber_id, &environment_id, false),
                &path,
            )
            .await;
            if response.0 == StatusCode::OK {
                (StatusCode::CREATED, response.1)
            } else {
                response
            }
        }
        Ok(None) => error_at(
            StatusCode::CONFLICT,
            &format!("Subscriber with id \"{subscriber_id}\" already exists"),
            "Conflict",
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

async fn patch_subscriber(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    Path(subscriber_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    bytes: Bytes,
) -> (StatusCode, Json<Value>) {
    let path = uri.to_string();
    let environment_id = match authenticate(&db, &headers, &path).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let body: Value = match serde_json::from_slice(&bytes) {
        Ok(body) => body,
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
            "Subscriber update not implemented",
            "Not Implemented",
            &path,
        );
    };
    if fields.keys().any(|key| {
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
        .contains(&key.as_str())
    }) {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Subscriber update not implemented",
            "Not Implemented",
            &path,
        );
    }
    if fields
        .get("email")
        .and_then(Value::as_str)
        .is_some_and(|email| novu_rust_contracts::email::validate(email).is_none())
    {
        return error_at(
            StatusCode::NOT_IMPLEMENTED,
            "Email format not implemented",
            "Not Implemented",
            &path,
        );
    }
    let errors = profile_errors(fields);
    if !errors.is_empty() {
        let mut response = validation_error(errors);
        response.1.0["path"] = json!(path);
        return response;
    }
    // Lock and update one environment-scoped row atomically. Like the reference
    // use case, return the old updatedAt in this PATCH response; GET sees the new one.
    let result = db.query_opt("WITH old AS MATERIALIZED (SELECT * FROM novu_rust_subscribers WHERE environment_id = $1 AND subscriber_id = $2 FOR UPDATE), updated AS (UPDATE novu_rust_subscribers s SET profile = old.profile || $3::jsonb, updated_at = CASE WHEN old.profile IS DISTINCT FROM old.profile || $3::jsonb THEN now() ELSE old.updated_at END FROM old WHERE s.environment_id = old.environment_id AND s.subscriber_id = old.subscriber_id RETURNING s.internal_id, s.profile, s.environment_id, s.is_online, s.last_online_at, old.created_at, old.updated_at) SELECT updated.internal_id, updated.profile, e.organization_id, to_char(updated.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), to_char(updated.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), updated.is_online, to_char(updated.last_online_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_online_at FROM updated JOIN novu_rust_environments e ON e.id = updated.environment_id", &[&environment_id, &subscriber_id, &body]).await;
    match result {
        Ok(Some(row)) => {
            subscriber_credentials::attach(
                &db,
                &environment_id,
                &subscriber_id,
                subscriber_response(
                    row,
                    &subscriber_id,
                    &environment_id,
                    fields.contains_key("data"),
                ),
                &path,
            )
            .await
        }
        Ok(None) => error_at(
            StatusCode::NOT_FOUND,
            &format!("Subscriber {subscriber_id} was not found"),
            "Not Found",
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

async fn trigger(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> (StatusCode, Json<Value>) {
    let environment_id = match authenticate(&db, &headers, "/v1/events/trigger").await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let body: Value = match serde_json::from_slice(&bytes) {
        Ok(body) => body,
        Err(_) => return error(StatusCode::BAD_REQUEST, "Invalid JSON", "Bad Request"),
    };
    let Some(object) = body.as_object() else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Validation Error",
            "Unprocessable Entity",
        );
    };
    let mut errors = Vec::new();
    if object.get("name").is_none_or(Value::is_null) {
        errors.push("name should not be null or undefined");
    }
    if !object.get("name").is_some_and(Value::is_string) {
        errors.push("name must be a string");
    }
    if object
        .get("payload")
        .is_some_and(|v| !v.is_null() && !v.is_object())
    {
        errors.push("payload must be an object");
    }
    if object.get("to").is_none_or(Value::is_null) {
        errors.push("to should not be null or undefined");
    }
    if !valid_recipients(object.get("to")) {
        errors.push("to must be a non-empty subscriberId string, an object with a non-empty subscriberId, a topic object with a non-empty topicKey, or a non-empty array of those");
    }
    if object
        .get("transactionId")
        .is_some_and(|v| !v.is_null() && !v.is_string())
    {
        errors.push("transactionId must be a string");
    }
    if !errors.is_empty() {
        return validation_error(errors);
    }
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .expect("name validated");
    // Avoid implying compatibility for paths whose validation or behaviour is not implemented.
    if object.keys().any(|k| {
        ![
            "name",
            "to",
            "payload",
            "transactionId",
            "context",
            "actor",
            "overrides",
        ]
        .contains(&k.as_str())
    }) {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "Trigger option not implemented",
            "Not Implemented",
        );
    }
    let (override_severity, email_override, sms_override) = match object.get("overrides") {
        None | Some(Value::Null) => (None, None, None),
        Some(Value::Object(overrides))
            if overrides.keys().all(|key| {
                [
                    "severity",
                    "email",
                    "sms",
                    "sendgrid",
                    "twilio",
                    "fcm",
                    "providers",
                    "steps",
                ]
                .contains(&key.as_str())
            }) =>
        {
            let severity = match overrides.get("severity") {
                None | Some(Value::Null) => None,
                Some(Value::String(value))
                    if matches!(value.as_str(), "none" | "low" | "medium" | "high") =>
                {
                    Some(value.clone())
                }
                _ => {
                    return error(
                        StatusCode::NOT_IMPLEMENTED,
                        "Trigger severity not implemented",
                        "Not Implemented",
                    );
                }
            };
            let email = match overrides.get("email") {
                None | Some(Value::Null) => None,
                Some(Value::Object(email))
                    if email.iter().all(|(key, value)| match key.as_str() {
                        "toRecipient" | "from" | "senderName" | "replyTo" | "subject" | "html"
                        | "text" => value.is_string(),
                        "to" | "cc" | "bcc" => value.as_array().is_some_and(|items| {
                            items
                                .iter()
                                .all(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                        }),
                        "replaceToRecipient" => value.is_boolean(),
                        "headers" => value
                            .as_object()
                            .is_some_and(|items| items.values().all(Value::is_string)),
                        "customData" => value.is_object(),
                        _ => false,
                    }) && (!email
                        .get("replaceToRecipient")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                        || email
                            .get("to")
                            .and_then(Value::as_array)
                            .is_some_and(|to| !to.is_empty())) =>
                {
                    Some(Value::Object(email.clone()))
                }
                _ => {
                    return error(
                        StatusCode::NOT_IMPLEMENTED,
                        "Email override not implemented",
                        "Not Implemented",
                    );
                }
            };
            let sms = match overrides.get("sms") {
                None | Some(Value::Null) => None,
                Some(Value::Object(sms))
                    if sms.iter().all(|(key, value)| {
                        ["to", "from", "content"].contains(&key.as_str()) && value.is_string()
                    }) =>
                {
                    Some(Value::Object(sms.clone()))
                }
                _ => {
                    return error(
                        StatusCode::NOT_IMPLEMENTED,
                        "SMS override not implemented",
                        "Not Implemented",
                    );
                }
            };
            (severity, email, sms)
        }
        _ => {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "Trigger option not implemented",
                "Not Implemented",
            );
        }
    };
    let overrides = object.get("overrides").and_then(Value::as_object);
    // Provider/step layers are bounded to understood fields. Unknown
    // passthrough shapes must not be silently accepted.
    let valid_layer = |layer: &Value| {
        layer.as_object().is_some_and(|fields| {
            fields.iter().all(|(key, value)| match key.as_str() {
                "subject" | "html" | "senderName" => value.is_string(),
                "from" => value.as_str().is_some_and(|s| !s.is_empty()),
                "to" | "cc" | "bcc" => value.as_array().is_some_and(|items| {
                    items
                        .iter()
                        .all(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                }),
                "headers" => value.as_object().is_some_and(|items| {
                    items.iter().all(|(key, value)| {
                        key.bytes().any(|b| b.is_ascii_alphabetic())
                            && key.bytes().all(|b| b.is_ascii_alphabetic() || b == b'-')
                            && value.is_string()
                    })
                }),
                _ => false,
            })
        })
    };
    let flat = overrides.and_then(|v| v.get("sendgrid"));
    let sms_flat = overrides.and_then(|v| v.get("twilio"));
    let fcm_flat = overrides.and_then(|v| v.get("fcm"));
    let providers = overrides.and_then(|v| v.get("providers"));
    let steps = overrides.and_then(|v| v.get("steps"));
    let valid_sms_fields = |fields: &serde_json::Map<String, Value>| {
        fields.iter().all(|(key, value)| match key.as_str() {
            "to"
            | "from"
            | "content"
            | "body"
            | "statusCallback"
            | "applicationSid"
            | "messagingServiceSid"
            | "contentRetention"
            | "addressRetention"
            | "scheduleType"
            | "sendAt"
            | "contentVariables"
            | "riskCheck"
            | "contentSid" => value.is_string(),
            "maxPrice" | "attempt" | "validityPeriod" => value.is_string() || value.is_number(),
            "provideFeedback" | "forceDelivery" | "smartEncoded" | "shortenUrls" | "sendAsMms" => {
                value.is_boolean() || value.is_string()
            }
            "persistentAction" | "mediaUrl" => {
                value.is_string()
                    || value
                        .as_array()
                        .is_some_and(|items| items.iter().all(Value::is_string))
            }
            _ => false,
        })
    };
    let valid_sms_layer = |layer: &Value| {
        layer.as_object().is_some_and(|fields| {
            fields.iter().all(|(key, value)| {
                if key == "_passthrough" {
                    value.as_object().is_some_and(|v| {
                        v.keys().all(|k| k == "body")
                            && v.get("body")
                                .and_then(Value::as_object)
                                .is_some_and(valid_sms_fields)
                    })
                } else {
                    valid_sms_fields(&serde_json::Map::from_iter([(key.clone(), value.clone())]))
                }
            })
        })
    };
    let valid_fcm_fields = |fields: &serde_json::Map<String, Value>| {
        fields.iter().all(|(key, value)| match key.as_str() {
            "token" | "topic" | "condition" => value.as_str().is_some_and(|v| !v.is_empty()),
            "tokens" => value.as_array().is_some_and(|items| {
                !items.is_empty()
                    && items.len() <= 500
                    && items
                        .iter()
                        .all(|v| v.as_str().is_some_and(|s| !s.is_empty()))
            }),
            "data" => value
                .as_object()
                .is_some_and(|v| v.values().all(Value::is_string)),
            "notification" => value.as_object().is_some_and(|v| {
                v.iter().all(|(key, value)| {
                    ["title", "body", "imageUrl"].contains(&key.as_str()) && value.is_string()
                })
            }),
            _ => false,
        })
    };
    let valid_fcm_layer = |layer: &Value| {
        layer.as_object().is_some_and(|fields| {
            fields.iter().all(|(key, value)| {
                if key == "_passthrough" {
                    value.as_object().is_some_and(|v| {
                        v.len() == 1
                            && v.get("body")
                                .and_then(Value::as_object)
                                .is_some_and(valid_fcm_fields)
                    })
                } else {
                    valid_fcm_fields(&serde_json::Map::from_iter([(key.clone(), value.clone())]))
                }
            })
        })
    };
    let valid_providers = |value: &Value| {
        value.as_object().is_some_and(|entries| {
            entries.iter().all(|(key, layer)| match key.as_str() {
                "sendgrid" => valid_layer(layer),
                "twilio" => valid_sms_layer(layer),
                "fcm" => valid_fcm_layer(layer),
                _ => false,
            })
        })
    };
    if flat.is_some_and(|v| !valid_layer(v))
        || sms_flat.is_some_and(|v| !valid_sms_layer(v))
        || fcm_flat.is_some_and(|v| {
            !v.as_object().is_some_and(|fields| {
                fields.iter().all(|(key, value)| match key.as_str() {
                    "type" => value
                        .as_str()
                        .is_some_and(|v| v == "data" || v == "notification"),
                    "data" => value
                        .as_object()
                        .is_some_and(|v| v.values().all(Value::is_string)),
                    "deviceTokens" => value.as_array().is_some_and(|items| {
                        !items.is_empty()
                            && items
                                .iter()
                                .all(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                    }),
                    _ => false,
                })
            })
        })
        || providers.is_some_and(|v| !valid_providers(v))
        || steps.is_some_and(|v| {
            !v.as_object().is_some_and(|entries| {
                entries.iter().all(|(step, value)| {
                    !step.is_empty()
                        && value.as_object().is_some_and(|step_value| {
                            step_value.keys().all(|k| k == "providers")
                                && step_value.get("providers").is_some_and(valid_providers)
                        })
                })
            })
        })
    {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "Provider override not implemented",
            "Not Implemented",
        );
    }
    let parsed_context = match object.get("context") {
        None | Some(Value::Null) => None,
        Some(value) => match inbox_context::parse(value) {
            Some(context) => Some(context),
            None => {
                return error(
                    StatusCode::NOT_IMPLEMENTED,
                    "Trigger context not implemented",
                    "Not Implemented",
                );
            }
        },
    };
    let context_keys = parsed_context
        .as_ref()
        .map(|context| context.keys.clone())
        .unwrap_or_default();
    let context_entries = parsed_context
        .map(|context| context.entries)
        .unwrap_or_else(|| json!([]));
    let row = match db.query_opt("SELECT active, has_steps, in_app_content, inbox_metadata, (SELECT organization_id FROM novu_rust_environments WHERE id=$1), email_subject, email_html, sms_content, push_title, push_content FROM novu_rust_workflows WHERE environment_id = $1 AND identifier = $2", &[&environment_id, &name]).await {
        Ok(Some(row)) => row,
        Ok(None) => return error(StatusCode::UNPROCESSABLE_ENTITY, "workflow_not_found", "Unprocessable Entity"),
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error", "Internal Server Error"),
    };
    let provided_txn = object
        .get("transactionId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let Some(txn) = provided_txn.or_else(generate_transaction_id) else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
        );
    };
    let decision = evaluate(row.get(0), row.get(1), TenantState::NotSpecified);
    if decision == GateResult::Dispatch {
        let actor = match object.get("actor") {
            None | Some(Value::Null) => None,
            Some(value) => match novu_rust_contracts::recipients::normalize(value) {
                Ok(mut actors) if actors.len() == 1 && !value.is_array() => actors.pop(),
                _ => {
                    return error(
                        StatusCode::NOT_IMPLEMENTED,
                        "Actor not implemented",
                        "Not Implemented",
                    );
                }
            },
        };
        let content: Option<String> = row.get(2);
        let email_subject: Option<String> = row.get(5);
        let email_html: Option<String> = row.get(6);
        let sms_content: Option<String> = row.get(7);
        let push_title: Option<String> = row.get(8);
        let push_content: Option<String> = row.get(9);
        let content = if content.is_none()
            && ((email_subject.is_some() && email_html.is_some())
                || sms_content.is_some()
                || (push_title.is_some() && push_content.is_some()))
        {
            Some(String::new())
        } else {
            content
        };
        let recipients = object
            .get("to")
            .and_then(|v| novu_rust_contracts::recipients::split(v).ok());
        let payload = object
            .get("payload")
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| json!({}));
        let (Some(content), Some((recipients, topic_keys, topic_exclusions))) =
            (content, recipients)
        else {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "Workflow execution not implemented",
                "Not Implemented",
            );
        };
        if payload.get("attachments").is_some()
            || recipients.iter().any(|profile| {
                novu_rust_contracts::template::render_in_app_with_actor(
                    &content,
                    &payload,
                    profile,
                    &json!({}),
                    actor.as_ref(),
                )
                .is_err()
            })
        {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "Workflow execution not implemented",
                "Not Implemented",
            );
        }
        let metadata: Option<Value> = row.get(3);
        let tags = metadata
            .as_ref()
            .and_then(|value| value.get("tags"))
            .filter(|value| value.is_array())
            .cloned()
            .unwrap_or_else(|| json!([]));
        let severity = override_severity
            .as_deref()
            .or_else(|| {
                metadata
                    .as_ref()
                    .and_then(|value| value.get("severity"))
                    .and_then(Value::as_str)
            })
            .unwrap_or("none");
        if !matches!(severity, "none" | "low" | "medium" | "high") {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "Workflow severity not implemented",
                "Not Implemented",
            );
        }
        let organization_id: Option<String> = row.get(4);
        let data = json!({"organizationId": organization_id, "workflow": name, "transactionId": txn, "recipients": recipients, "topicKeys": topic_keys, "topicExclusions": topic_exclusions, "actor": actor, "content": content, "payload": payload, "tags": tags, "contextKeys": context_keys, "severity": severity, "severityOverride": override_severity, "emailOverride": email_override, "smsOverride": sms_override, "emailFlatProviderOverride":flat,"smsFlatProviderOverride":sms_flat,"pushFlatProviderOverride":fcm_flat,"emailProviderOverrides":providers,"emailStepOverrides":steps});
        // The ledger and queue entry are inserted atomically. This provisional deduplication
        // policy is not yet equivalent to Novu's tier-dependent retention behaviour.
        let result = db.execute("WITH accepted AS (INSERT INTO novu_rust_trigger_transactions (environment_id, transaction_id) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING 1), contexts AS (INSERT INTO novu_rust_contexts (environment_id,context_key,data) SELECT $1, entry->>'key', COALESCE(NULLIF(entry->'data','null'::jsonb),'{}'::jsonb) FROM accepted CROSS JOIN jsonb_array_elements($6::jsonb) entry ON CONFLICT (environment_id,context_key) DO UPDATE SET data=CASE WHEN EXISTS (SELECT 1 FROM jsonb_array_elements($6::jsonb) element WHERE element->>'key'=EXCLUDED.context_key AND element->'data' <> 'null'::jsonb) THEN EXCLUDED.data ELSE novu_rust_contexts.data END, updated_at=CASE WHEN EXISTS (SELECT 1 FROM jsonb_array_elements($6::jsonb) element WHERE element->>'key'=EXCLUDED.context_key AND element->'data' <> 'null'::jsonb) AND novu_rust_contexts.data IS DISTINCT FROM EXCLUDED.data THEN now() ELSE novu_rust_contexts.updated_at END RETURNING 1) INSERT INTO novu_queue_jobs (id, environment_id, kind, payload) SELECT $3, $1, $4, $5::jsonb || jsonb_build_object('recipientIdentitySnapshot',(SELECT COALESCE(jsonb_object_agg(r->>'subscriberId',s.internal_id),'{}'::jsonb) FROM jsonb_array_elements($5::jsonb->'recipients') r LEFT JOIN novu_rust_subscribers s ON s.environment_id=$1 AND s.subscriber_id=r->>'subscriberId'),'actorIdentitySnapshot',(SELECT s.internal_id FROM novu_rust_subscribers s WHERE s.environment_id=$1 AND s.subscriber_id=$5::jsonb->'actor'->>'subscriberId'),'topicIdentitySnapshot',(SELECT COALESCE(jsonb_object_agg(t.subscriber_id,s.internal_id),'{}'::jsonb) FROM novu_rust_topic_subscribers t JOIN novu_rust_subscribers s ON s.environment_id=t.environment_id AND s.subscriber_id=t.subscriber_id WHERE t.environment_id=$1 AND t.topic_key IN (SELECT jsonb_array_elements_text($5::jsonb->'topicKeys')))) FROM accepted", &[&environment_id, &txn, &Uuid::new_v4(), &novu_rust_contracts::EXPERIMENTAL_IN_APP_JOB_KIND, &data, &context_entries]).await;
        if result.is_err() {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "Internal Server Error",
            );
        }
    }
    (
        StatusCode::CREATED,
        Json(
            json!({"data": {"acknowledged": true, "status": decision.status(), "transactionId": txn}}),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::generate_transaction_id;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn generated_transaction_id_matches_reference_format() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let value = generate_transaction_id().unwrap();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(value.len(), 24);
        assert_eq!(&value[..4], "txn_");
        let timestamp = u64::from_str_radix(&value[4..12], 16).unwrap();
        assert!((before..=after).contains(&timestamp));
        assert!(
            value[12..]
                .bytes()
                .all(|x| x.is_ascii_digit() || x.is_ascii_lowercase())
        );
    }
}
