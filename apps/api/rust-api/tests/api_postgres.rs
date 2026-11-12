use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tower::ServiceExt;

fn subscriber_token(payload: Value, secret: &[u8]) -> String {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(payload.to_string());
    let input = format!("{header}.{payload}");
    let mut mac = Hmac::<sha2_11::Sha256>::new_from_slice(secret).unwrap();
    mac.update(input.as_bytes());
    format!(
        "{input}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

fn subscriber_hash(secret: &str, id: &str) -> String {
    let mut mac = Hmac::<sha2_11::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(id.as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn inbox_get(app: axum::Router, path: &str, auth: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder().method("GET").uri(path);
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    let response = app
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn inbox_mutate(
    app: axum::Router,
    method: &str,
    path: &str,
    auth: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    let response = app
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}

async fn request(app: axum::Router, auth: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri("/v1/events/trigger")
        .header("content-type", "application/json");
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn get_subscriber(
    app: axum::Router,
    auth: Option<&str>,
    subscriber: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("GET")
        .uri(format!("/v2/subscribers/{subscriber}"));
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn patch_subscriber(
    app: axum::Router,
    auth: Option<&str>,
    subscriber: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("PATCH")
        .uri(format!("/v2/subscribers/{subscriber}"))
        .header("content-type", "application/json");
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn create_subscriber(
    app: axum::Router,
    auth: Option<&str>,
    query: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/v2/subscribers{query}"))
        .header("content-type", "application/json");
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn delete_subscriber(
    app: axum::Router,
    auth: Option<&str>,
    subscriber: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("DELETE")
        .uri(format!("/v2/subscribers/{subscriber}"));
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn count_notifications(
    app: axum::Router,
    auth: Option<&str>,
    subscriber: &str,
    filters: &str,
) -> (StatusCode, Value) {
    let encoded: String = filters.bytes().map(|byte| format!("%{byte:02X}")).collect();
    let mut req = Request::builder().method("GET").uri(format!(
        "/v2/subscribers/{subscriber}/notifications/count?filters={encoded}"
    ));
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn bulk_notifications(
    app: axum::Router,
    auth: Option<&str>,
    subscriber: &str,
    action: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!(
            "/v2/subscribers/{subscriber}/notifications/{action}"
        ))
        .header("content-type", "application/json");
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        },
    )
}

async fn list_notifications(
    app: axum::Router,
    auth: Option<&str>,
    subscriber: &str,
    query: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("GET")
        .uri(format!("/v2/subscribers/{subscriber}/notifications{query}"));
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn delete_notification(
    app: axum::Router,
    auth: Option<&str>,
    subscriber: &str,
    notification: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method("DELETE").uri(format!(
        "/v2/subscribers/{subscriber}/notifications/{notification}"
    ));
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        },
    )
}

async fn mark_one(
    app: axum::Router,
    auth: Option<&str>,
    subscriber: &str,
    id: &str,
    action: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method("PATCH").uri(format!(
        "/v2/subscribers/{subscriber}/notifications/{id}/{action}"
    ));
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

// This drops tables: only run against a dedicated disposable database.
#[test]
#[ignore = "requires a disposable PostgreSQL database in TEST_DATABASE_URL"]
fn trigger_api_contract() {
    // This long integration scenario holds many async states; use an explicit
    // test thread stack instead of depending on platform-specific defaults.
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(trigger_api_contract_inner());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn trigger_api_contract_inner() {
    let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let (db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db.batch_execute("DROP TABLE IF EXISTS novu_rust_ws_events, novu_rust_organization_snooze_limits, novu_rust_subscription_preferences, novu_rust_topic_subscribers, novu_rust_topics, novu_rust_contexts, novu_rust_in_app_messages, novu_rust_subscribers, novu_queue_jobs, novu_rust_trigger_transactions, novu_rust_workflows, novu_rust_api_keys, novu_rust_users, novu_rust_environments CASCADE")
        .await
        .unwrap();
    db.batch_execute(novu_rust_api::MIGRATION).await.unwrap();
    db.batch_execute(novu_rust_api::INBOX_SESSION_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_rust_api::SNOOZE_POLICY_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::MIGRATION)
        .await
        .unwrap();
    db.batch_execute("INSERT INTO novu_rust_subscribers (environment_id, subscriber_id, internal_id) VALUES ('legacy-env', 'legacy', 'legacy-id'); INSERT INTO novu_rust_in_app_messages (id, environment_id, subscriber_id, workflow_identifier, transaction_id, content, payload) VALUES ('00000000-0000-4000-8000-000000000099', 'legacy-env', 'legacy', 'legacy', 'legacy', 'x', '{}'::jsonb)").await.unwrap();
    db.batch_execute(novu_postgres_queue::in_app::STATE_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::STATE_DATES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::IDENTIFIER_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::INBOX_METADATA_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::INBOX_DATE_PRESENCE_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::INBOX_DATA_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::INBOX_DATA_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::INBOX_DATA_ARRAY_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::CONTEXT_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::CONTEXT_DATA_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::CONTEXT_TIMESTAMPS_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::CONTEXT_CURSOR_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::SUBSCRIBER_OBJECTID_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::GLOBAL_PREFERENCES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::WORKFLOW_PREFERENCES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::SUBSCRIBER_WORKFLOW_PREFERENCES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::STEP_OUTCOMES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::SUBSCRIBER_ONLINE_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::TOPICS_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::TOPIC_DELETE_RESTRICT_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::TOPIC_DATA_MIGRATION)
        .await
        .unwrap();
    db.batch_execute("INSERT INTO novu_rust_topics(environment_id,topic_key,internal_id,name) VALUES ('legacy-env','old-topic','old-topic-id','Old'); INSERT INTO novu_rust_topic_subscribers(environment_id,topic_key,subscriber_id,created_at) VALUES ('legacy-env','old-topic','legacy','2020-01-01T00:00:00Z')").await.unwrap();
    // Two legal legacy pairs can produce the same reference default identifier.
    // Migration must fail atomically rather than discard either membership.
    db.batch_execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('legacy-env','b:si_c','collision-a'),('legacy-env','c','collision-b'); INSERT INTO novu_rust_topics(environment_id,topic_key,internal_id,name) VALUES ('legacy-env','a','collision-topic-a','A'),('legacy-env','a:si_b','collision-topic-b','B'); INSERT INTO novu_rust_topic_subscribers(environment_id,topic_key,subscriber_id) VALUES ('legacy-env','a','b:si_c'),('legacy-env','a:si_b','c')").await.unwrap();
    assert!(
        db.batch_execute(novu_postgres_queue::in_app::TOPIC_SUBSCRIPTIONS_MIGRATION)
            .await
            .is_err()
    );
    db.batch_execute("ROLLBACK").await.unwrap();
    let columns: i64 = db.query_one("SELECT count(*) FROM information_schema.columns WHERE table_name='novu_rust_topic_subscribers' AND column_name='identifier'", &[]).await.unwrap().get(0);
    assert_eq!(columns, 0);
    db.batch_execute("DELETE FROM novu_rust_topic_subscribers WHERE environment_id='legacy-env' AND topic_key IN ('a','a:si_b'); DELETE FROM novu_rust_topics WHERE environment_id='legacy-env' AND topic_key IN ('a','a:si_b'); DELETE FROM novu_rust_subscribers WHERE environment_id='legacy-env' AND subscriber_id IN ('b:si_c','c')").await.unwrap();
    db.batch_execute(novu_postgres_queue::in_app::TOPIC_SUBSCRIPTIONS_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::TOPIC_SUBSCRIPTIONS_CURSOR_MIGRATION)
        .await
        .unwrap();
    db.execute(
        "UPDATE novu_rust_topic_subscribers SET name='Old name' WHERE environment_id='legacy-env'",
        &[],
    )
    .await
    .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::TOPIC_SUBSCRIPTION_NAME_MIGRATION)
        .await
        .unwrap();
    let name_present: bool = db.query_one("SELECT name_present FROM novu_rust_topic_subscribers WHERE environment_id='legacy-env'", &[]).await.unwrap().get(0);
    assert!(name_present);
    db.execute(
        "UPDATE novu_rust_topic_subscribers SET name=NULL WHERE environment_id='legacy-env'",
        &[],
    )
    .await
    .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::TOPIC_SUBSCRIPTION_NAME_MIGRATION)
        .await
        .unwrap();
    let name_present: bool = db.query_one("SELECT name_present FROM novu_rust_topic_subscribers WHERE environment_id='legacy-env'", &[]).await.unwrap().get(0);
    assert!(name_present);
    db.batch_execute(novu_postgres_queue::in_app::TOPIC_SUBSCRIPTION_PREFERENCES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::SUBSCRIBER_SUBSCRIPTION_CURSOR_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::SNOOZED_UNTIL_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::DELIVERY_HISTORY_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::WS_EVENTS_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::WS_BEST_EFFORT_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::MESSAGE_SEVERITY_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::IN_APP_STEP_ACTIVE_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::NOTIFICATIONS_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::EMAIL_STEP_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::EMAIL_MESSAGES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::EMAIL_PROVIDER_RESULT_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::EMAIL_STEP_ID_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::EMAIL_REVALIDATION_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::SMS_STEP_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::SMS_MESSAGES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::SMS_STEP_ID_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::PUSH_STEP_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::FCM_CREDENTIALS_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::PUSH_MESSAGES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::MESSAGE_SUBSCRIBER_CLEANUP_INDEXES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute("INSERT INTO novu_rust_notifications(internal_id,job_id,environment_id,subscriber_id,subscriber_internal_id,workflow_identifier,transaction_id,recipient,payload,channels,tags,severity,critical) VALUES ('old-notification','00000000-0000-4000-8000-000000000098','legacy-env','legacy','previous-id','legacy','old','{}','{}','{}','[]','none',false); INSERT INTO novu_rust_in_app_messages(id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,notification_internal_id) VALUES ('00000000-0000-4000-8000-000000000098','legacy-env','legacy','legacy','old','x','{}','old-notification')").await.unwrap();
    db.batch_execute(novu_postgres_queue::in_app::IN_APP_SUBSCRIBER_IDENTITY_MIGRATION)
        .await
        .unwrap();
    db.batch_execute("INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier) VALUES ('legacy-env','legacy-integration','legacy-fcm'); INSERT INTO novu_rust_fcm_devices(environment_id,subscriber_id,integration_internal_id,device_tokens) VALUES ('legacy-env','legacy','legacy-integration',ARRAY['legacy-token'])").await.unwrap();
    db.batch_execute(novu_postgres_queue::in_app::MULTIPLE_FCM_CHANNELS_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::PUSH_STEP_ID_MIGRATION)
        .await
        .unwrap();
    for migration in [
        novu_postgres_queue::sendgrid_queue::MIGRATION,
        novu_postgres_queue::twilio_queue::MIGRATION,
        novu_postgres_queue::fcm_queue::MIGRATION,
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    db.batch_execute("INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ('00000000-0000-4000-8000-000000000091','legacy-env','experimental_sendgrid_email','{}'),('00000000-0000-4000-8000-000000000092','legacy-env','experimental_sendgrid_email','{}'); INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ('00000000-0000-4000-8000-000000000091','legacy-env','accepted'); INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id) VALUES ('legacy-accepted','legacy-env','legacy','legacy-id','notification','workflow','one','subject','sent','00000000-0000-4000-8000-000000000091'),('legacy-ambiguous','legacy-env','legacy','legacy-id','notification','workflow','two','subject','sent','00000000-0000-4000-8000-000000000092')").await.unwrap();
    for _ in 0..2 {
        db.batch_execute(novu_postgres_queue::in_app::DELIVERY_STATE_MIGRATION)
            .await
            .unwrap();
        let states: Vec<String> = db.query_one("SELECT array_agg(delivery_state ORDER BY internal_id) FROM novu_rust_email_messages WHERE internal_id IN ('legacy-accepted','legacy-ambiguous')", &[]).await.unwrap().get(0);
        assert_eq!(states, vec!["accepted", "unknown"]);
    }
    db.batch_execute("DELETE FROM novu_rust_email_messages WHERE internal_id IN ('legacy-accepted','legacy-ambiguous'); DELETE FROM novu_queue_jobs WHERE id IN ('00000000-0000-4000-8000-000000000091','00000000-0000-4000-8000-000000000092')").await.unwrap();
    db.batch_execute(novu_postgres_queue::in_app::MULTIPLE_FCM_CHANNELS_MIGRATION)
        .await
        .unwrap();
    let legacy_fcm: Vec<String> = db.query_one("SELECT device_tokens FROM novu_rust_fcm_devices WHERE environment_id='legacy-env' AND subscriber_id='legacy' AND integration_internal_id='legacy-integration'",&[]).await.unwrap().get(0);
    assert_eq!(legacy_fcm, vec!["legacy-token"]);
    db.batch_execute(novu_postgres_queue::in_app::IN_APP_SUBSCRIBER_IDENTITY_MIGRATION)
        .await
        .unwrap();
    let legacy_identities: Vec<String> = db.query_one("SELECT array_agg(subscriber_internal_id ORDER BY transaction_id) FROM novu_rust_in_app_messages WHERE environment_id='legacy-env' AND subscriber_id='legacy'",&[]).await.unwrap().get(0);
    assert_eq!(legacy_identities, vec!["legacy-id", "previous-id"]);
    db.execute(
        "DELETE FROM novu_rust_in_app_messages WHERE id='00000000-0000-4000-8000-000000000098'",
        &[],
    )
    .await
    .unwrap();
    let old = db.query_one("SELECT internal_id,identifier,updated_at=created_at FROM novu_rust_topic_subscribers WHERE environment_id='legacy-env'", &[]).await.unwrap();
    let old_id: String = old.get(0);
    assert_eq!(old_id.len(), 24);
    assert_eq!(old.get::<_, String>(1), "tk_old-topic:si_legacy");
    assert!(old.get::<_, bool>(2));
    db.batch_execute(novu_postgres_queue::in_app::TOPIC_SUBSCRIPTIONS_MIGRATION)
        .await
        .unwrap();
    let migrated_id: String = db
        .query_one(
            "SELECT internal_id FROM novu_rust_topic_subscribers WHERE environment_id='legacy-env'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(old_id, migrated_id);
    // One database-backed generator is shared by API and worker processes.
    let generated: Vec<String> = db.query_one("SELECT array_agg(id ORDER BY n) FROM (SELECT n,novu_rust_new_objectid() AS id FROM generate_series(1,2000) n) x",&[]).await.unwrap().get(0);
    assert!(generated.iter().all(|id| {
        id.len() == 24
            && id
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    }));
    assert!(generated.windows(2).all(|pair| pair[0] < pair[1]));
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
            tokio::spawn(async move { connection.await.unwrap() });
            client
                .query_one(
                    "SELECT array_agg(novu_rust_new_objectid()) FROM generate_series(1,250)",
                    &[],
                )
                .await
                .unwrap()
                .get::<_, Vec<String>>(0)
        }));
    }
    let mut unique = std::collections::HashSet::new();
    for task in tasks {
        for id in task.await.unwrap() {
            assert!(unique.insert(id));
        }
    }
    assert_eq!(unique.len(), 1000);
    let first_prefix: String = db
        .query_one("SELECT prefix FROM novu_rust_objectid_identity", &[])
        .await
        .unwrap()
        .get(0);
    db.batch_execute(novu_postgres_queue::in_app::SUBSCRIBER_OBJECTID_MIGRATION)
        .await
        .unwrap();
    let second_prefix: String = db
        .query_one("SELECT prefix FROM novu_rust_objectid_identity", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(first_prefix, second_prefix);
    let complex_data = json!({"orders":[{"status":"open"}]});
    let null_filter = json!([{"path":["orders","status"],"groups":[[null]]}]);
    let safely_rejected: bool = db
        .query_one(
            "SELECT novu_rust_inbox_data_matches($1,$2)",
            &[&complex_data, &null_filter],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!safely_rejected);
    let array_data = json!({"orders":[{"status":"open"},{}]});
    let missing_or_open = json!([{"path":["orders","status"],"groups":[[null],["open"]]}]);
    let matches: bool = db
        .query_one(
            "SELECT novu_rust_inbox_data_matches($1,$2)",
            &[&array_data, &missing_or_open],
        )
        .await
        .unwrap()
        .get(0);
    assert!(matches);
    let empty_data = json!({"orders":[]});
    let matches: bool = db
        .query_one(
            "SELECT novu_rust_inbox_data_matches($1,$2)",
            &[&empty_data, &null_filter],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!matches);
    let legacy = db.query_one("SELECT is_read, is_seen, is_archived FROM novu_rust_in_app_messages WHERE subscriber_id = 'legacy'", &[]).await.unwrap();
    assert!(!legacy.get::<_, bool>(0) && !legacy.get::<_, bool>(1) && !legacy.get::<_, bool>(2));
    let legacy_id: String = db
        .query_one(
            "SELECT internal_id FROM novu_rust_in_app_messages WHERE subscriber_id = 'legacy'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(legacy_id.len(), 24);
    db.batch_execute(novu_postgres_queue::in_app::IDENTIFIER_MIGRATION)
        .await
        .unwrap();
    let repeated_id: String = db
        .query_one(
            "SELECT internal_id FROM novu_rust_in_app_messages WHERE subscriber_id = 'legacy'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(legacy_id, repeated_id);
    db.batch_execute("DELETE FROM novu_rust_in_app_messages WHERE subscriber_id = 'legacy'; DELETE FROM novu_rust_subscribers WHERE subscriber_id = 'legacy'").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"test-key"));
    db.batch_execute("INSERT INTO novu_rust_users VALUES ('user-a'); INSERT INTO novu_rust_environments VALUES ('env-a', 'org-a'), ('env-b', 'org-b')").await.unwrap();
    db.execute(
        "INSERT INTO novu_rust_api_keys (key_hash, environment_id, user_id) VALUES ($1, 'env-a', 'user-a')",
        &[&hash],
    )
    .await
    .unwrap();
    let orphan_hash = format!("{:x}", Sha256::digest(b"orphan-key"));
    db.execute("INSERT INTO novu_rust_api_keys (key_hash, environment_id, user_id) VALUES ($1, 'env-a', 'missing-user')", &[&orphan_hash]).await.unwrap();
    let second_hash = format!("{:x}", Sha256::digest(b"test-key-b"));
    db.execute("INSERT INTO novu_rust_api_keys (key_hash, environment_id, user_id) VALUES ($1, 'env-b', 'user-a')", &[&second_hash]).await.unwrap();
    let disabled_hash = format!("{:x}", Sha256::digest(b"disabled-key"));
    db.execute("INSERT INTO novu_rust_api_keys (key_hash, environment_id, user_id, enabled) VALUES ($1, 'env-a', 'user-a', false)", &[&disabled_hash]).await.unwrap();
    db.batch_execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-a', 'inactive', false, true, NULL), ('env-a', 'empty', true, false, NULL), ('env-a', 'active', true, true, NULL), ('env-a', 'inbox', true, true, 'Hello'), ('env-b', 'private', false, true, NULL)").await.unwrap();
    let db = Arc::new(db);
    let app = novu_rust_api::router(db.clone());
    for i in 0..5 {
        db.execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile,created_at,updated_at) VALUES('env-a',$1,$2,$3,'2024-01-01T00:00:00Z','2024-01-01T00:00:00Z')", &[&format!("list-{i}"),&format!("64a0000000000000000000{i:02x}"),&json!({"firstName":format!("List {i}"),"lastName":"Smith","email":format!("{i}@example.test"),"phone":"+123"})]).await.unwrap();
    }
    db.execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES('env-b','list-foreign','64a0000000000000000000ff',$1)",&[&json!({"firstName":"List Foreign"})]).await.unwrap();
    let base = "/v2/subscribers?name=list&limit=2&orderDirection=ASC";
    let (status, first) = inbox_get(app.clone(), base, Some("ApiKey test-key")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["totalCount"], 5);
    assert_eq!(first["data"][0]["subscriberId"], "list-0");
    let cursor = first["next"].as_str().unwrap();
    let (_, second) = inbox_get(
        app.clone(),
        &format!("{base}&after={cursor}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(second["data"][0]["subscriberId"], "list-2");
    let prev = second["previous"].as_str().unwrap();
    let (_, back) = inbox_get(
        app.clone(),
        &format!("{base}&before={prev}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(back["data"], first["data"]);
    let (_, inclusive) = inbox_get(
        app.clone(),
        &format!("{base}&after={cursor}&includeCursor=true"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(inclusive["data"][0]["subscriberId"], "list-1");
    let (_, filtered) = inbox_get(
        app.clone(),
        "/v2/subscribers?email=2%40example.test&phone=%2B123",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(filtered["totalCount"], 1);
    assert_eq!(filtered["data"][0]["subscriberId"], "list-2");
    let (_, literal) = inbox_get(
        app.clone(),
        "/v2/subscribers?name=%5B",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(literal["totalCount"], 0);
    let (_, foreign) = inbox_get(
        app.clone(),
        "/v2/subscribers?after=64a0000000000000000000ff",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(foreign["totalCount"], 0);
    let (_, invalid) = inbox_get(
        app.clone(),
        "/v2/subscribers?after=invalid",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(invalid["totalCount"], 0);
    let (status, _) = inbox_get(
        app.clone(),
        "/v2/subscribers?after=x&before=y",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (first_db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let (second_db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let first_app = novu_rust_api::router(Arc::new(first_db));
    let second_app = novu_rust_api::router(Arc::new(second_db));
    let (first_result, second_result) = tokio::join!(
        inbox_mutate(
            first_app,
            "POST",
            "/v2/topics?failIfExists=true",
            Some("ApiKey test-key"),
            json!({"key":"race-v2-topic","name":"Race"})
        ),
        inbox_mutate(
            second_app,
            "POST",
            "/v2/topics?failIfExists=true",
            Some("ApiKey test-key"),
            json!({"key":"race-v2-topic","name":"Race"})
        )
    );
    let mut statuses = [first_result.0.as_u16(), second_result.0.as_u16()];
    statuses.sort();
    assert_eq!(statuses, [201, 409]);
    let count:i64=db.query_one("SELECT count(*) FROM novu_rust_topics WHERE environment_id='env-a' AND topic_key='race-v2-topic'",&[]).await.unwrap().get(0);
    assert_eq!(count, 1);
    let (status, _) = inbox_get(app.clone(), base, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES('env-b','list-0',novu_rust_new_objectid())",&[]).await.unwrap();
    let online_path = "/v1/subscribers/list-0/online-status";
    let (status, online) = inbox_mutate(
        app.clone(),
        "PATCH",
        online_path,
        Some("ApiKey test-key"),
        json!({"isOnline":true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(online["data"]["isOnline"], true);
    assert_eq!(online["data"]["lastOnlineAt"], Value::Null);
    let (_, listed) = inbox_get(
        app.clone(),
        "/v2/subscribers?subscriberId=list-0",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(listed["data"][0]["isOnline"], true);
    let (_, foreign) = inbox_get(
        app.clone(),
        "/v2/subscribers/list-0",
        Some("ApiKey test-key-b"),
    )
    .await;
    assert_eq!(foreign["data"]["isOnline"], false);
    let (status, offline) = inbox_mutate(
        app.clone(),
        "PATCH",
        online_path,
        Some("ApiKey test-key"),
        json!({"isOnline":false}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let last = offline["data"]["lastOnlineAt"].as_str().unwrap().to_owned();
    let (_, back_online) = inbox_mutate(
        app.clone(),
        "PATCH",
        online_path,
        Some("ApiKey test-key"),
        json!({"isOnline":true}),
    )
    .await;
    assert_eq!(back_online["data"]["lastOnlineAt"], last);
    let (_, read) = inbox_get(
        app.clone(),
        "/v2/subscribers/list-0",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(read["data"]["isOnline"], true);
    assert_eq!(read["data"]["lastOnlineAt"], last);
    let topic_path = "/v1/topics/test-topic/subscribers";
    let (status, assigned) = inbox_mutate(
        app.clone(),
        "POST",
        topic_path,
        Some("ApiKey test-key"),
        json!({"subscribers":["list-0","absent","list-0"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        assigned["data"],
        json!({"succeeded":["list-0"],"failed":{"notFound":["absent"]}})
    );
    let (status, topic) = inbox_get(
        app.clone(),
        "/v1/topics/test-topic",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(topic["data"]["subscribers"], json!(["list-0"]));
    let (status, member) = inbox_get(
        app.clone(),
        "/v1/topics/test-topic/subscribers/list-0",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(member["data"]["_topicId"], topic["data"]["_id"]);
    let (status, _) = inbox_get(
        app.clone(),
        "/v1/topics/test-topic/subscribers/list-0",
        Some("ApiKey test-key-b"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v1/topics/test-topic",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, changed) = inbox_mutate(
        app.clone(),
        "PATCH",
        "/v1/topics/test-topic",
        Some("ApiKey test-key"),
        json!({"name":"Renamed"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(changed["data"]["name"], "Renamed");
    for index in 0..3 {
        let (status, _) = inbox_mutate(
            app.clone(),
            "POST",
            "/v1/topics",
            Some("ApiKey test-key"),
            json!({"key":format!("list-topic-{index}"),"name":"list-group"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v1/topics",
        Some("ApiKey test-key-b"),
        json!({"key":"list-topic-foreign","name":"list-group"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, first) = inbox_get(
        app.clone(),
        "/v1/topics?name=list-group&pageSize=2&page=0",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["totalCount"], 3);
    assert_eq!(first["data"][0]["key"], "list-topic-0");
    assert_eq!(first["data"][1]["key"], "list-topic-1");
    let (_, second) = inbox_get(
        app.clone(),
        "/v1/topics?name=list-group&pageSize=2&page=1",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(second["data"][0]["key"], "list-topic-2");
    let (_, exact) = inbox_get(
        app.clone(),
        "/v1/topics?key=list-topic-foreign",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(exact["totalCount"], 0);
    db.execute("UPDATE novu_rust_topics SET created_at='2024-01-01T00:00:00Z',updated_at='2024-01-01T00:00:00Z' WHERE environment_id='env-a' AND topic_key LIKE 'list-topic-%'",&[]).await.unwrap();
    for order in ["key", "name", "createdAt", "updatedAt", "_id"] {
        let query =
            format!("/v2/topics?key=LIST-TOPIC-&limit=2&orderDirection=ASC&orderBy={order}");
        let (status, first) = inbox_get(app.clone(), &query, Some("ApiKey test-key")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["totalCount"], 3);
        assert_eq!(first["data"][0]["key"], "list-topic-0");
        let cursor = first["next"].as_str().unwrap();
        let (_, second) = inbox_get(
            app.clone(),
            &format!("{query}&after={cursor}"),
            Some("ApiKey test-key"),
        )
        .await;
        assert_eq!(second["data"][0]["key"], "list-topic-2");
        let back = second["previous"].as_str().unwrap();
        let (_, previous) = inbox_get(
            app.clone(),
            &format!("{query}&before={back}"),
            Some("ApiKey test-key"),
        )
        .await;
        assert_eq!(previous["data"], first["data"]);
        let (_, inclusive) = inbox_get(
            app.clone(),
            &format!("{query}&after={cursor}&includeCursor=true"),
            Some("ApiKey test-key"),
        )
        .await;
        let expected = if ["name", "createdAt", "updatedAt"].contains(&order) {
            "list-topic-0"
        } else {
            "list-topic-1"
        };
        assert_eq!(inclusive["data"][0]["key"], expected);
    }
    let (_, foreign) = inbox_get(
        app.clone(),
        "/v2/topics?key=list-topic-foreign",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(foreign["totalCount"], 0);
    let (status, _) = inbox_get(app.clone(), "/v2/topics?limit=101", Some("ApiKey test-key")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    for body in [
        json!({"key":"null-cursor-0"}),
        json!({"key":"null-cursor-1","name":null}),
        json!({"key":"null-cursor-2","name":""}),
        json!({"key":"null-cursor-3","name":"Named"}),
    ] {
        let (status, _) = inbox_mutate(
            app.clone(),
            "POST",
            "/v2/topics",
            Some("ApiKey test-key"),
            body,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }
    let query = "/v2/topics?key=null-cursor-&orderBy=name&orderDirection=ASC&limit=2";
    let (_, first) = inbox_get(app.clone(), query, Some("ApiKey test-key")).await;
    assert_eq!(first["data"].as_array().unwrap().len(), 2);
    let cursor = first["next"].as_str().unwrap();
    let (_, second) = inbox_get(
        app.clone(),
        &format!("{query}&after={cursor}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(second["data"], json!([]));
    assert_eq!(second["totalCount"], 4);
    let (status, subscriptions) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/topics/create-api/subscriptions",
        Some("ApiKey test-key"),
        json!({"subscriptions":[{"subscriberId":"list-0","identifier":"custom-api","name":"Custom"},{"subscriberId":"absent","identifier":"missing-api"}]}),
    ).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        subscriptions["meta"],
        json!({"totalCount":2,"successful":1,"failed":1})
    );
    assert_eq!(subscriptions["data"][0]["createdAt"], "");
    let created_subscription_id = subscriptions["data"][0]["_id"].clone();
    let (status, existing) = inbox_mutate(
        app.clone(), "POST", "/v2/topics/create-api/subscriptions", Some("ApiKey test-key"),
        json!({"subscriptions":[{"subscriberId":"list-0","identifier":"custom-api","name":"Changed"}]}),
    ).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(existing["data"][0]["_id"], created_subscription_id);
    assert_eq!(existing["data"][0]["name"], "Custom");
    assert_ne!(existing["data"][0]["createdAt"], "");
    let subscription_path = "/v2/topics/create-api/subscriptions/custom-api";
    db.batch_execute("INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT m.environment_id,s.internal_id,m.internal_id,'workflow-id','{\"all\":{\"enabled\":false}}'::jsonb FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE m.environment_id='env-a' AND m.identifier='custom-api'; INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT m.environment_id,'foreign-subscriber',m.internal_id,'workflow-id','{\"all\":{\"enabled\":false}}'::jsonb FROM novu_rust_topic_subscribers m WHERE m.environment_id='env-a' AND m.identifier='custom-api'").await.unwrap();
    let (_, with_preference) =
        inbox_get(app.clone(), subscription_path, Some("ApiKey test-key")).await;
    assert_eq!(
        with_preference["data"]["preferences"],
        json!([{"subscriptionId":"custom-api","enabled":false}])
    );
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        subscription_path,
        Some("ApiKey test-key"),
        json!({"preferences":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let remaining: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-a'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 1);
    db.execute(
        "DELETE FROM novu_rust_subscription_preferences WHERE environment_id='env-a'",
        &[],
    )
    .await
    .unwrap();
    let (status, cleared) = inbox_mutate(
        app.clone(),
        "PATCH",
        subscription_path,
        Some("ApiKey test-key"),
        json!({"name":null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cleared["data"]["name"], Value::Null);
    assert!(cleared["data"].get("name").is_some());
    let (_, read_cleared) =
        inbox_get(app.clone(), subscription_path, Some("ApiKey test-key")).await;
    assert_eq!(read_cleared["data"]["name"], Value::Null);
    assert!(read_cleared["data"].get("name").is_some());
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        subscription_path,
        Some("ApiKey test-key-b"),
        json!({"name":"Foreign"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, second_subscription) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/topics/create-api/subscriptions",
        Some("ApiKey test-key"),
        json!({"subscriberIds":["list-1"]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, first_page) = inbox_get(
        app.clone(),
        "/v2/topics/create-api/subscriptions?limit=1&orderDirection=ASC",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_page["totalCount"], 2);
    assert_eq!(first_page["data"][0]["_id"], created_subscription_id);
    assert!(first_page["data"][0].get("name").is_none());
    let cursor = first_page["next"].as_str().unwrap();
    let (_, next_page) = inbox_get(
        app.clone(),
        &format!("/v2/topics/create-api/subscriptions?limit=1&orderDirection=ASC&after={cursor}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(
        next_page["data"][0]["_id"],
        second_subscription["data"][0]["_id"]
    );
    db.execute("UPDATE novu_rust_topic_subscribers SET context_keys=ARRAY['tenant:one','project:two'] WHERE environment_id='env-a' AND identifier='custom-api'", &[]).await.unwrap();
    let (_, filtered) = inbox_get(
        app.clone(),
        "/v2/topics/create-api/subscriptions?contextKeys=project%3Atwo&contextKeys=tenant%3Aone",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(filtered["totalCount"], 1);
    assert_eq!(filtered["data"][0]["identifier"], "custom-api");
    let (status, missing) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/topics/create-api/subscriptions",
        Some("ApiKey test-key-b"),
        json!({"subscriberIds":["list-1"]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(missing["errors"][0]["code"], "SUBSCRIBER_NOT_FOUND");
    let (status, other_environment) = inbox_get(
        app.clone(),
        "/v2/topics/create-api/subscriptions",
        Some("ApiKey test-key-b"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other_environment["totalCount"], 0);
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/topics/create-api/subscriptions",
        Some("ApiKey test-key-b"),
        json!({"subscriptions":[{"subscriberId":"list-0","identifier":"custom-api"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        subscription_path,
        Some("ApiKey test-key-b"),
        json!({"name":"Foreign"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, isolated) = inbox_get(app.clone(), subscription_path, Some("ApiKey test-key")).await;
    assert!(isolated["data"]["name"].is_null());
    for auth in ["ApiKey test-key", "ApiKey test-key-b"] {
        let (status, _) = inbox_mutate(
            app.clone(),
            "DELETE",
            "/v2/topics/create-api",
            Some(auth),
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    db.execute("INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,inbox_metadata) VALUES ('env-a','preference-api-workflow',true,true,$1)",&[&json!({"id":"64a00000000000000000000e","name":"Preference test","tags":[],"critical":false})]).await.unwrap();
    let pref_path = "/v2/topics/api-preference-test/subscriptions";
    let (status, created_pref) = inbox_mutate(app.clone(), "POST", pref_path, Some("ApiKey test-key"), json!({"subscriptions":[{"subscriberId":"list-0","identifier":"preference-api-test"}],"preferences":[{"workflowId":"preference-api-workflow","enabled":false}]})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created_pref["data"][0]["preferences"][0]["enabled"], false);
    db.execute("INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT m.environment_id,'foreign-subscriber',m.internal_id,'64a00000000000000000000e','{\"all\":{\"enabled\":true}}'::jsonb FROM novu_rust_topic_subscribers m WHERE m.environment_id='env-a' AND m.identifier='preference-api-test'",&[]).await.unwrap();
    let (status, subscriber_topics) = inbox_get(
        app.clone(),
        "/v2/subscribers/list-0/subscriptions?key=api-preference-test",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(subscriber_topics["totalCount"], 1);
    assert_eq!(
        subscriber_topics["data"][0]["topic"]["key"],
        "api-preference-test"
    );
    assert_eq!(
        subscriber_topics["data"][0]["preferences"][0]["enabled"],
        false
    );
    assert_eq!(
        subscriber_topics["data"][0]["preferences"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    db.execute("DELETE FROM novu_rust_subscription_preferences WHERE environment_id='env-a' AND subscriber_internal_id='foreign-subscriber'",&[]).await.unwrap();
    let (status, foreign_topics) = inbox_get(
        app.clone(),
        "/v2/subscribers/list-0/subscriptions?key=api-preference-test",
        Some("ApiKey test-key-b"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(foreign_topics["totalCount"], 0);
    let pref_item = "/v2/topics/api-preference-test/subscriptions/preference-api-test";
    let (_, retrieved) = inbox_get(app.clone(), pref_item, Some("ApiKey test-key")).await;
    assert_eq!(retrieved["data"]["preferences"][0]["enabled"], false);
    // Other workflows in this fixture have no internal metadata. Avoid
    // guessing an ObjectId match and changing the existing setting.
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        pref_item,
        Some("ApiKey test-key"),
        json!({"preferences":[{"workflowId":"64a00000000000000000000e","enabled":true}]}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (_, retrieved) = inbox_get(app.clone(), pref_item, Some("ApiKey test-key")).await;
    assert_eq!(retrieved["data"]["preferences"][0]["enabled"], false);
    let (status, replacement) = inbox_mutate(app.clone(),"PATCH",pref_item,Some("ApiKey test-key"),json!({"name":"Changed","preferences":[{"workflowId":"preference-api-workflow","enabled":true,"condition":{"===":[{"var":"payload.tier"},"gold"]}}]})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replacement["data"]["name"], "Changed");
    assert_eq!(replacement["data"]["preferences"][0]["enabled"], true);
    let (_, retrieved) = inbox_get(app.clone(), pref_item, Some("ApiKey test-key")).await;
    assert_eq!(
        retrieved["data"]["preferences"][0]["condition"],
        json!({"===":[{"var":"payload.tier"},"gold"]})
    );
    db.batch_execute("ALTER TABLE novu_rust_subscription_preferences ADD CONSTRAINT test_preference_insert_failure CHECK ((preferences->'all'->>'enabled') IS DISTINCT FROM 'false') NOT VALID").await.unwrap();
    let (status, _) = inbox_mutate(app.clone(), "PATCH", pref_item, Some("ApiKey test-key"), json!({"name":"After failure","preferences":[{"workflowId":"preference-api-workflow","enabled":false}]})).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    db.batch_execute("ALTER TABLE novu_rust_subscription_preferences DROP CONSTRAINT test_preference_insert_failure").await.unwrap();
    let (_, state) = inbox_get(app.clone(), pref_item, Some("ApiKey test-key")).await;
    assert!(state["data"].get("preferences").is_none());
    assert_eq!(state["data"]["name"], "Changed");
    db.batch_execute("ALTER TABLE novu_rust_topic_subscribers ADD CONSTRAINT test_subscription_name_failure CHECK (name IS DISTINCT FROM 'Blocked') NOT VALID").await.unwrap();
    let (status, _) = inbox_mutate(app.clone(), "PATCH", pref_item, Some("ApiKey test-key"), json!({"name":"Blocked","preferences":[{"workflowId":"preference-api-workflow","enabled":true}]})).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    db.batch_execute(
        "ALTER TABLE novu_rust_topic_subscribers DROP CONSTRAINT test_subscription_name_failure",
    )
    .await
    .unwrap();
    let (_, state) = inbox_get(app.clone(), pref_item, Some("ApiKey test-key")).await;
    assert_eq!(state["data"]["preferences"][0]["enabled"], true);
    assert_eq!(state["data"]["name"], "Changed");
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        pref_item,
        Some("ApiKey test-key"),
        json!({"preferences":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // An invisible concurrent insert wins the unique key while PATCH is
    // inserting. The response must return the winning stored preference.
    let (mut concurrent, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let concurrent_tx = concurrent.transaction().await.unwrap();
    concurrent_tx.execute("INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT m.environment_id,s.internal_id,m.internal_id,'64a00000000000000000000e','{\"all\":{\"enabled\":true}}'::jsonb FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE m.environment_id='env-a' AND m.identifier='preference-api-test'",&[]).await.unwrap();
    let pid: i32 = db
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let pending_app = app.clone();
    let pending = tokio::spawn(async move {
        inbox_mutate(
            pending_app,
            "PATCH",
            "/v2/topics/api-preference-test/subscriptions/preference-api-test",
            Some("ApiKey test-key"),
            json!({"preferences":[{"workflowId":"preference-api-workflow","enabled":false}]}),
        )
        .await
    });
    let (observer, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let mut blocked = false;
    for _ in 0..200 {
        let waiting:bool=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",&[&pid]).await.unwrap().get(0);
        if waiting {
            blocked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        blocked,
        "preference insertion did not wait for concurrent insert"
    );
    concurrent_tx.commit().await.unwrap();
    let (status, result) = pending.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["data"]["preferences"][0]["enabled"], true);
    let (_, state) = inbox_get(app.clone(), pref_item, Some("ApiKey test-key")).await;
    assert_eq!(state["data"]["preferences"][0]["enabled"], true);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        pref_path,
        Some("ApiKey test-key"),
        json!({"subscriberIds":["list-0"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-a'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v2/topics/api-preference-test",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    db.execute("DELETE FROM novu_rust_workflows WHERE environment_id='env-a' AND identifier='preference-api-workflow'",&[]).await.unwrap();
    for auth in ["ApiKey test-key", "ApiKey test-key-b"] {
        for identifier in ["delete-custom-1", "delete-custom-2"] {
            let (status, _) = inbox_mutate(
                app.clone(),
                "POST",
                "/v2/topics/delete-subscription-api/subscriptions",
                Some(auth),
                json!({"subscriptions":[{"subscriberId":"list-0","identifier":identifier}]}),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED);
        }
    }
    db.batch_execute("INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT m.environment_id,s.internal_id,m.internal_id,'workflow-id','{\"all\":{\"enabled\":false}}'::jsonb FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE m.topic_key='delete-subscription-api'").await.unwrap();
    db.batch_execute("CREATE TABLE subscription_delete_test_blocker(environment_id text NOT NULL,internal_id text NOT NULL,FOREIGN KEY(environment_id,internal_id) REFERENCES novu_rust_topic_subscribers(environment_id,internal_id)); INSERT INTO subscription_delete_test_blocker SELECT environment_id,internal_id FROM novu_rust_topic_subscribers WHERE environment_id='env-a' AND identifier='delete-custom-2'").await.unwrap();
    let delete_path = "/v2/topics/delete-subscription-api/subscriptions";
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        delete_path,
        Some("ApiKey test-key"),
        json!({"subscriberIds":["list-0"]}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let count:i64=db.query_one("SELECT count(*) FROM novu_rust_topic_subscribers WHERE environment_id='env-a' AND topic_key='delete-subscription-api'",&[]).await.unwrap().get(0);
    assert_eq!(count, 2);
    let preference_count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-a'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(preference_count, 2);
    db.batch_execute("DROP TABLE subscription_delete_test_blocker")
        .await
        .unwrap();
    let (status, result) = inbox_mutate(
        app.clone(),
        "DELETE",
        delete_path,
        Some("ApiKey test-key"),
        json!({"subscriptions":[{"identifier":"delete-custom-1"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["meta"]["successful"], 1);
    let preference_count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-a'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(preference_count, 1);
    let (status, result) = inbox_mutate(
        app.clone(),
        "DELETE",
        delete_path,
        Some("ApiKey test-key"),
        json!({"subscriberIds":["list-0"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["meta"]["successful"], 1);
    let preference_count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-a'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(preference_count, 0);
    let count:i64=db.query_one("SELECT count(*) FROM novu_rust_topic_subscribers WHERE environment_id='env-b' AND topic_key='delete-subscription-api'",&[]).await.unwrap().get(0);
    assert_eq!(count, 2);
    let preference_count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-b'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(preference_count, 2);
    for auth in ["ApiKey test-key", "ApiKey test-key-b"] {
        let (status, _) = inbox_mutate(
            app.clone(),
            "DELETE",
            "/v2/topics/delete-subscription-api",
            Some(auth),
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    // Original force topic deletion leaves subscription preference documents.
    let preference_count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-b'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(preference_count, 2);
    db.execute(
        "DELETE FROM novu_rust_subscription_preferences WHERE environment_id='env-b'",
        &[],
    )
    .await
    .unwrap();
    let (status, created) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/topics",
        Some("ApiKey test-key"),
        json!({"key":"v2-topic"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(created["data"].get("name").is_none());
    let id = created["data"]["_id"].clone();
    let (status, foreign) = inbox_get(
        app.clone(),
        "/v2/topics/v2-topic",
        Some("ApiKey test-key-b"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(foreign["message"], "Topic with key v2-topic not found");
    let (status, updated) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/topics",
        Some("ApiKey test-key"),
        json!({"key":"v2-topic","name":"Title","data":{"priority":2,"tags":["a"]}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(updated["data"]["_id"], id);
    assert_eq!(updated["data"]["data"], json!({"priority":2,"tags":["a"]}));
    let (_, legacy) = inbox_get(app.clone(), "/v1/topics/v2-topic", Some("ApiKey test-key")).await;
    assert_eq!(legacy["data"]["data"], updated["data"]["data"]);
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/topics?failIfExists=true",
        Some("ApiKey test-key"),
        json!({"key":"v2-topic","name":"Wrong"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, cleared) = inbox_mutate(
        app.clone(),
        "PATCH",
        "/v2/topics/v2-topic",
        Some("ApiKey test-key"),
        json!({"data":null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(cleared["data"].get("data").is_none());
    assert_eq!(cleared["data"]["name"], "Title");
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        "/v2/topics/v2-topic",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    for auth in ["ApiKey test-key", "ApiKey test-key-b"] {
        let (status, _) = inbox_mutate(
            app.clone(),
            "POST",
            "/v1/topics/v2-delete/subscribers",
            Some(auth),
            json!({"subscribers":["list-0"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    db.execute("INSERT INTO novu_rust_topic_subscribers(environment_id,topic_key,subscriber_id,identifier,name,name_present,context_keys) VALUES ('env-a','v2-delete','list-0','custom-subscription','Custom',true,ARRAY['tenant:one'])", &[]).await.unwrap();
    let (status, topic) =
        inbox_get(app.clone(), "/v1/topics/v2-delete", Some("ApiKey test-key")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(topic["data"]["subscribers"], json!(["list-0", "list-0"]));
    let (status, _) = inbox_get(
        app.clone(),
        "/v1/topics/v2-delete/subscribers/list-0",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, subscriber) = inbox_get(
        app.clone(),
        "/v1/subscribers/list-0?includeTopics=true",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(
        subscriber["data"]["topics"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| *v == "v2-delete")
            .count(),
        1
    );
    // An unrelated FK error must roll back both topic and membership deletion.
    db.batch_execute("CREATE TABLE topic_delete_test_blocker(environment_id text NOT NULL,topic_key text NOT NULL,FOREIGN KEY(environment_id,topic_key) REFERENCES novu_rust_topics(environment_id,topic_key)); INSERT INTO topic_delete_test_blocker VALUES ('env-a','v2-delete')").await.unwrap();
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v2/topics/v2-delete",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let count:i64=db.query_one("SELECT count(*) FROM novu_rust_topic_subscribers WHERE environment_id='env-a' AND topic_key='v2-delete'",&[]).await.unwrap().get(0);
    assert_eq!(count, 2);
    db.batch_execute("DROP TABLE topic_delete_test_blocker")
        .await
        .unwrap();
    let (status, result) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v2/topics/v2-delete?force=false",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result, json!({"data":{"acknowledged":true}}));
    let count:i64=db.query_one("SELECT count(*) FROM novu_rust_topic_subscribers WHERE environment_id='env-a' AND topic_key='v2-delete'",&[]).await.unwrap().get(0);
    assert_eq!(count, 0);
    let (status, other) = inbox_get(
        app.clone(),
        "/v1/subscribers/list-0?includeTopics=true",
        Some("ApiKey test-key-b"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other["data"]["topics"], json!(["v2-delete"]));
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v2/topics/v2-delete",
        Some("ApiKey test-key-b"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = inbox_get(
        app.clone(),
        "/v2/subscribers/list-0",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/topics",
        Some("ApiKey test-key"),
        json!({"key":"v2-delete-race"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (mut inserting, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let tx = inserting.transaction().await.unwrap();
    tx.execute("INSERT INTO novu_rust_topic_subscribers(environment_id,topic_key,subscriber_id,identifier) VALUES ('env-a','v2-delete-race','list-1','tk_v2-delete-race:si_list-1')",&[]).await.unwrap();
    let pid: i32 = db
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let pending_app = app.clone();
    let pending = tokio::spawn(async move {
        inbox_mutate(
            pending_app,
            "DELETE",
            "/v2/topics/v2-delete-race",
            Some("ApiKey test-key"),
            json!({}),
        )
        .await
    });
    let (observer, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let mut blocked = false;
    for _ in 0..200 {
        let waiting:bool=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",&[&pid]).await.unwrap().get(0);
        if waiting {
            blocked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(blocked, "v2 topic deletion did not wait for assignment");
    tx.commit().await.unwrap();
    let (status, _) = pending.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    let (status, _) = inbox_get(
        app.clone(),
        "/v2/topics/v2-delete-race",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let count:i64=db.query_one("SELECT count(*) FROM novu_rust_topic_subscribers WHERE environment_id='env-a' AND topic_key='v2-delete-race'",&[]).await.unwrap().get(0);
    assert_eq!(count, 0);
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v1/topics",
        Some("ApiKey test-key"),
        json!({"key":"race-topic","name":"Race"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (mut blocker, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let tx = blocker.transaction().await.unwrap();
    tx.execute("INSERT INTO novu_rust_topic_subscribers(environment_id,topic_key,subscriber_id,identifier) VALUES ('env-a','race-topic','list-1','tk_race-topic:si_list-1')",&[]).await.unwrap();
    let pid: i32 = db
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let pending_app = app.clone();
    let pending = tokio::spawn(async move {
        inbox_mutate(
            pending_app,
            "DELETE",
            "/v1/topics/race-topic",
            Some("ApiKey test-key"),
            json!({}),
        )
        .await
    });
    let (observer, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let mut blocked = false;
    for _ in 0..200 {
        let waiting:bool=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",&[&pid]).await.unwrap().get(0);
        if waiting {
            blocked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        blocked,
        "topic deletion did not wait for concurrent assignment"
    );
    tx.commit().await.unwrap();
    let (status, _) = pending.await.unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, topic) = inbox_get(
        app.clone(),
        "/v1/topics/race-topic",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(topic["data"]["subscribers"], json!(["list-1"]));
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v1/topics/race-topic/subscribers/removal",
        Some("ApiKey test-key"),
        json!({"subscribers":["list-1"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v1/topics/race-topic",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, member) = inbox_get(
        app.clone(),
        "/v1/subscribers/list-0?includeTopics=true",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(member["data"]["topics"], json!(["test-topic"]));
    let (_, foreign) = inbox_get(
        app.clone(),
        "/v1/subscribers/list-0?includeTopics=true",
        Some("ApiKey test-key-b"),
    )
    .await;
    assert_eq!(foreign["data"]["topics"], json!([]));
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v1/topics/test-topic/subscribers/removal",
        Some("ApiKey test-key-b"),
        json!({"subscribers":["list-0"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, member) = inbox_get(
        app.clone(),
        "/v1/subscribers/list-0?includeTopics=true",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(member["data"]["topics"], json!(["test-topic"]));
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v2/subscribers/list-0",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let count:i64 = db.query_one("SELECT count(*) FROM novu_rust_topic_subscribers WHERE environment_id='env-a' AND subscriber_id='list-0'",&[]).await.unwrap().get(0);
    assert_eq!(count, 0);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v1/topics/test-topic",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = inbox_get(
        app.clone(),
        "/v1/topics/test-topic",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    db.execute(
        "DELETE FROM novu_rust_subscribers WHERE subscriber_id LIKE 'list-%'",
        &[],
    )
    .await
    .unwrap();
    for id in ["topic-a", "topic-b", "topic-c"] {
        let (status, _) = inbox_mutate(
            app.clone(),
            "POST",
            "/v2/subscribers",
            Some("ApiKey test-key"),
            json!({"subscriberId":id}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }
    for (topic, ids) in [
        ("alpha", json!(["topic-a", "topic-b"])),
        ("beta", json!(["topic-b"])),
    ] {
        let (status, _) = inbox_mutate(
            app.clone(),
            "POST",
            &format!("/v1/topics/{topic}/subscribers"),
            Some("ApiKey test-key"),
            json!({"subscribers":ids}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"inbox","to":[{"type":"Topic","topicKey":"alpha","exclude":["topic-b"]},{"type":"Topic","topicKey":"beta"},"topic-b"],"transactionId":"topic-late-resolution"})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v1/topics/alpha/subscribers/removal",
        Some("ApiKey test-key"),
        json!({"subscribers":["topic-a"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v1/topics/beta/subscribers",
        Some("ApiKey test-key"),
        json!({"subscribers":["topic-c"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (mut topic_worker, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    assert!(
        novu_postgres_queue::in_app::process_one(&mut topic_worker)
            .await
            .unwrap()
    );
    let delivered:Vec<String> = db.query("SELECT subscriber_id FROM novu_rust_in_app_messages WHERE transaction_id='topic-late-resolution' ORDER BY subscriber_id",&[]).await.unwrap().into_iter().map(|row|row.get(0)).collect();
    assert_eq!(delivered, vec!["topic-b", "topic-c"]);
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"inbox","to":{"type":"Topic","topicKey":"missing-topic"},"transactionId":"empty-topic"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut topic_worker)
            .await
            .unwrap()
    );
    let row = db.query_one("SELECT status,(SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='empty-topic') FROM novu_queue_jobs WHERE payload->>'transactionId'='empty-topic'",&[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, i64>(1), 0);
    db.execute("UPDATE novu_rust_workflows SET in_app_content='{{actor.firstName}} -> {{subscriber.subscriberId}}' WHERE environment_id='env-a' AND identifier='inbox'",&[]).await.unwrap();
    let (status, _) = request(app.clone(), Some("ApiKey test-key"),json!({"name":"inbox","to":{"type":"Topic","topicKey":"beta"},"actor":{"subscriberId":"topic-b","firstName":"Actor"},"transactionId":"topic-actor"})).await;
    assert_eq!(status, StatusCode::CREATED);
    let row = db.query_one("SELECT payload->>'actorIdentitySnapshot', (SELECT internal_id FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='topic-b') FROM novu_queue_jobs WHERE payload->>'transactionId'='topic-actor'", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), row.get::<_, String>(1));
    assert!(
        novu_postgres_queue::in_app::process_one(&mut topic_worker)
            .await
            .unwrap()
    );
    let rows = db.query("SELECT subscriber_id,content FROM novu_rust_in_app_messages WHERE transaction_id='topic-actor'",&[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "topic-c");
    assert_eq!(rows[0].get::<_, String>(1), "Actor -> topic-c");
    let (status, _) = request(app.clone(), Some("ApiKey test-key"),json!({"name":"inbox","to":[{"type":"Topic","topicKey":"beta"},{"subscriberId":"topic-b","firstName":"Direct"}],"actor":{"subscriberId":"topic-b","firstName":"Actor"},"transactionId":"topic-actor-direct"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut topic_worker)
            .await
            .unwrap()
    );
    let rows = db.query("SELECT subscriber_id,content FROM novu_rust_in_app_messages WHERE transaction_id='topic-actor-direct' ORDER BY subscriber_id",&[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "topic-b");
    assert_eq!(rows[0].get::<_, String>(1), "Direct -> topic-b");
    assert_eq!(rows[1].get::<_, String>(0), "topic-c");
    assert_eq!(rows[1].get::<_, String>(1), "Direct -> topic-c");
    db.execute("UPDATE novu_rust_workflows SET in_app_content='Hello' WHERE environment_id='env-a' AND identifier='inbox'",&[]).await.unwrap();
    for id in ["topic-a", "topic-b", "topic-c"] {
        let (status, _) = inbox_mutate(
            app.clone(),
            "DELETE",
            &format!("/v2/subscribers/{id}"),
            Some("ApiKey test-key"),
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/subscribers",
        Some("ApiKey test-key"),
        json!({"subscriberId":"pref-test"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let path = "/v2/subscribers/pref-test/preferences";
    let (status, initial) = inbox_get(app.clone(), path, Some("ApiKey test-key")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        initial["data"],
        json!({"global":{"enabled":true,"channels":{"in_app":true}},"workflows":[]})
    );
    let (status, disabled) = inbox_mutate(
        app.clone(),
        "PATCH",
        path,
        Some("ApiKey test-key"),
        json!({"channels":{"in_app":false,"email":false}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        disabled["data"]["global"]["channels"],
        json!({"in_app":false})
    );
    db.execute("UPDATE novu_rust_workflows SET inbox_metadata=$1 WHERE environment_id='env-a' AND identifier='inbox'",&[&json!({"id":"64a000000000000000000004","name":"Greeting","slug":"greeting_wf_eV5Qd9HRCowdmEZY","critical":false,"updatedAt":"2024-01-01T00:00:00.000Z"})]).await.unwrap();
    db.execute("INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,in_app_enabled) VALUES ('env-a','inbox',true,false,true)",&[]).await.unwrap();
    db.execute("UPDATE novu_rust_workflows SET inbox_metadata=NULL WHERE environment_id='env-a' AND identifier='inbox'",&[]).await.unwrap();
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        path,
        Some("ApiKey test-key"),
        json!({"channels":{"in_app":true}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let unchanged:Value=db.query_one("SELECT channels FROM novu_rust_subscriber_preferences WHERE environment_id='env-a' AND subscriber_id='pref-test'",&[]).await.unwrap().get(0);
    assert_eq!(unchanged["in_app"], false);
    db.execute("UPDATE novu_rust_workflows SET inbox_metadata=$1 WHERE environment_id='env-a' AND identifier='inbox'",&[&json!({"id":"64a000000000000000000004","name":"Greeting","slug":"greeting_wf_eV5Qd9HRCowdmEZY","critical":false,"updatedAt":"2024-01-01T00:00:00.000Z"})]).await.unwrap();
    let (status, workflow_pref) = inbox_mutate(
        app.clone(),
        "PATCH",
        path,
        Some("ApiKey test-key"),
        json!({"workflowId":"greeting_wf_eV5Qd9HRCowdmEZY","channels":{"in_app":true}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        workflow_pref["data"]["workflows"][0]["channels"]["in_app"],
        true
    );
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inbox","to":"pref-test","transactionId":"pref-workflow-override"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (mut workflow_worker, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    assert!(
        novu_postgres_queue::in_app::process_one(&mut workflow_worker)
            .await
            .unwrap()
    );
    let delivered:i64=workflow_worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='pref-workflow-override'",&[]).await.unwrap().get(0);
    assert_eq!(delivered, 1);
    let (status, cleared) = inbox_mutate(
        app.clone(),
        "PATCH",
        path,
        Some("ApiKey test-key"),
        json!({"channels":{"in_app":false}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cleared["data"]["workflows"][0]["channels"]["in_app"], false);
    let cleared_channels:Value=db.query_one("SELECT channels FROM novu_rust_subscriber_workflow_preferences WHERE environment_id='env-a' AND subscriber_id='pref-test' AND workflow_identifier='inbox'",&[]).await.unwrap().get(0);
    assert_eq!(cleared_channels, json!({}));
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inbox","to":"pref-test","transactionId":"pref-off"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (mut worker, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let count: i64 = worker
        .query_one(
            "SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='pref-off'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inbox","to":["pref-test","pref-enabled"],"transactionId":"pref-mixed"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let outcomes = worker.query("SELECT subscriber_id,status,detail FROM novu_rust_in_app_step_outcomes WHERE transaction_id='pref-mixed' ORDER BY subscriber_id",&[]).await.unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(
        (
            outcomes[0].get::<_, String>(0),
            outcomes[0].get::<_, String>(1),
            outcomes[0].get::<_, Option<String>>(2)
        ),
        ("pref-enabled".into(), "completed".into(), None)
    );
    assert_eq!(
        (
            outcomes[1].get::<_, String>(0),
            outcomes[1].get::<_, String>(1),
            outcomes[1].get::<_, Option<String>>(2)
        ),
        (
            "pref-test".into(),
            "canceled".into(),
            Some("preference".into())
        )
    );
    let count: i64 = worker
        .query_one(
            "SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='pref-mixed'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v2/subscribers/pref-enabled",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    worker.execute("UPDATE novu_rust_workflow_preferences SET all_enabled=true,read_only=true,in_app_enabled=true WHERE environment_id='env-a' AND workflow_identifier='inbox'",&[]).await.unwrap();
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inbox","to":"pref-test","transactionId":"pref-readonly-on"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let count:i64=worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='pref-readonly-on'",&[]).await.unwrap().get(0);
    assert_eq!(count, 1);
    worker.execute("UPDATE novu_rust_workflow_preferences SET in_app_enabled=false WHERE environment_id='env-a' AND workflow_identifier='inbox'",&[]).await.unwrap();
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inbox","to":"pref-test","transactionId":"pref-readonly-off"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let count:i64=worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='pref-readonly-off'",&[]).await.unwrap().get(0);
    assert_eq!(count, 0);
    worker.execute("UPDATE novu_rust_workflow_preferences SET read_only=false WHERE environment_id='env-a' AND workflow_identifier='inbox'",&[]).await.unwrap();
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        path,
        Some("ApiKey test-key"),
        json!({"channels":{"in_app":true}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inbox","to":"pref-test","transactionId":"pref-on"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let count: i64 = worker
        .query_one(
            "SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='pref-on'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
    worker.execute("DELETE FROM novu_rust_workflow_preferences WHERE environment_id='env-a' AND workflow_identifier='inbox'",&[]).await.unwrap();
    let (status, _) = inbox_get(app.clone(), path, Some("ApiKey test-key-b")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        path,
        Some("ApiKey test-key"),
        json!({"workflowId":"not-supported","channels":{"in_app":false}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v2/subscribers/pref-test",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let count: i64 = worker
        .query_one(
            "SELECT count(*) FROM novu_rust_subscriber_preferences WHERE subscriber_id='pref-test'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let count:i64=worker.query_one("SELECT count(*) FROM novu_rust_subscriber_workflow_preferences WHERE subscriber_id='pref-test'",&[]).await.unwrap().get(0);
    assert_eq!(count, 0);
    let retained: i64 = worker
        .query_one(
            "SELECT count(*) FROM novu_rust_in_app_step_outcomes WHERE transaction_id='pref-mixed'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(retained, 2);
    let valid = json!({"name":"inactive", "to":"subscriber", "transactionId":"tx-1"});
    let (status, body) = request(app.clone(), None, valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "Missing authorization header");
    let (status, body) = request(app.clone(), Some("ApiKey invalid"), valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "API Key not found");
    let (status, body) = request(app.clone(), Some("ApiKey disabled-key"), valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "API Key not found");
    let (status, body) = request(app.clone(), Some("ApiKey orphan-key"), valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "User not found");
    for auth in ["ApiKey", "Bearer", "Bearer invalid"] {
        let (status, body) = request(app.clone(), Some(auth), valid.clone()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["message"], "Unauthorized");
    }
    let (status, body) = request(app.clone(), Some("Invalid invalid_value"), valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        body["message"],
        "Invalid authentication scheme: \"Invalid\""
    );
    let (status, body) = request(app.clone(), Some("ApiKey test-key"), valid).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        body,
        json!({"data":{"acknowledged":true,"status":"trigger_not_active","transactionId":"tx-1"}})
    );
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (status, body) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inactive", "to":"s"}),
    )
    .await;
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert_eq!(status, StatusCode::CREATED);
    let generated = body["data"]["transactionId"].as_str().unwrap();
    assert_eq!(generated.len(), 24);
    assert!(generated.starts_with("txn_"));
    let timestamp = u64::from_str_radix(&generated[4..12], 16).unwrap();
    assert!((before..=after).contains(&timestamp));
    assert!(
        generated[12..]
            .bytes()
            .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
    );
    let (status, body) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"empty","to":["s"],"transactionId":"tx-2"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["status"], "no_workflow_steps_defined");
    assert_eq!(body["data"]["transactionId"], "tx-2");
    for name in ["missing", "private"] {
        let (status, body) = request(
            app.clone(),
            Some("ApiKey test-key"),
            json!({"name":name,"to":"s"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["message"], "workflow_not_found");
    }
    let (status, body) = request(
        app.clone(),
        Some("ApiKey test-key-b"),
        json!({"name":"private","to":"s", "transactionId":"env-b-txn"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["status"], "trigger_not_active");
    let (status, body) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inactive","to":{"subscriberId":""}}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["message"], "Validation Error");
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"active","to":"s"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inactive","to":"s","tenant":"a"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);

    let trigger = json!({"name":"inbox","to":["alice", "bob", "alice"],"transactionId":"test-inbox","payload":{"case":1}});
    let (first, second) = tokio::join!(
        request(app.clone(), Some("ApiKey test-key"), trigger.clone()),
        request(app.clone(), Some("ApiKey test-key"), trigger)
    );
    let (status, response) = first;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(response["data"]["status"], "processed");
    assert_eq!(response["data"]["transactionId"], "test-inbox");
    let (status, _) = second;
    assert_eq!(status, StatusCode::CREATED);
    let (mut worker, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    assert!(
        !novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let rows = worker.query("SELECT subscriber_id, content, payload, environment_id FROM novu_rust_in_app_messages ORDER BY subscriber_id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip(["alice", "bob"]) {
        assert_eq!(row.get::<_, String>(0), expected);
        assert_eq!(row.get::<_, String>(1), "Hello");
        assert_eq!(row.get::<_, Value>(2), json!({"case":1}));
        assert_eq!(row.get::<_, String>(3), "env-a");
    }
    assert_eq!(
        worker
            .query_one("SELECT COUNT(*) FROM novu_rust_subscribers", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    worker.execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-a', 'templated', true, true, $1), ('env-a', 'unsupported', true, true, $2)", &[&"Hello {{subscriber.subscriberId}}: {{name}}", &"{{#if name}}yes{{/if}}"] ).await.unwrap();
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"unsupported", "to":"alice", "payload":{"name":"x"}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"templated", "to":"alice", "payload":{"name":"<A & B>"}, "transactionId":"template-test"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let content: String = worker.query_one("SELECT content FROM novu_rust_in_app_messages WHERE environment_id = 'env-a' AND transaction_id = 'template-test'", &[]).await.unwrap().get(0);
    assert_eq!(content, "Hello alice: &lt;A &amp; B&gt;");
    worker.execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-a', 'names', true, true, $1)", &[&"{{subscriber.firstName}} {{subscriber.lastName}}"] ).await.unwrap();
    for (transaction_id, to, expected) in [
        (
            "profile-create",
            json!({"subscriberId":"alice", "firstName":"Jane", "lastName":"Doe"}),
            "Jane Doe",
        ),
        (
            "profile-update",
            json!({"type":"Subscriber", "subscriberId":"alice", "lastName":null}),
            "Jane ",
        ),
        ("profile-existing", json!("alice"), "Jane "),
    ] {
        let (status, _) = request(
            app.clone(),
            Some("ApiKey test-key"),
            json!({"name":"names", "to":to, "transactionId":transaction_id}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(
            novu_postgres_queue::in_app::process_one(&mut worker)
                .await
                .unwrap()
        );
        let content: String = worker.query_one("SELECT content FROM novu_rust_in_app_messages WHERE environment_id = 'env-a' AND transaction_id = $1", &[&transaction_id]).await.unwrap().get(0);
        assert_eq!(content, expected);
    }
    let profile: Value = worker.query_one("SELECT profile FROM novu_rust_subscribers WHERE environment_id = 'env-a' AND subscriber_id = 'alice'", &[]).await.unwrap().get(0);
    assert_eq!(profile, json!({"firstName":"Jane", "lastName":null}));
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"names", "to":{"subscriberId":"alice", "email":"한@example.com"}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    worker.execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-a', 'email-template', true, true, '{{subscriber.email}}')", &[]).await.unwrap();
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"email-template", "to":{"subscriberId":"email-user", "email":"Jane+alerts@example.test"}, "transactionId":"email-test"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let content: String = worker
        .query_one(
            "SELECT content FROM novu_rust_in_app_messages WHERE transaction_id='email-test'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(content, "Jane+alerts@example.test");
    let (_, profile) = get_subscriber(app.clone(), Some("ApiKey test-key"), "email-user").await;
    assert_eq!(profile["data"]["email"], "Jane+alerts@example.test");
    let (status, _) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "email-user",
        json!({"email":"bad..name@example.test"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, profile) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "email-user",
        json!({"email":null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profile["data"]["email"], Value::Null);
    worker.execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-a', 'profile-fields', true, true, $1)", &[&"{{subscriber.phone}}|{{subscriber.avatar}}|{{subscriber.data.plan}}"] ).await.unwrap();
    for (transaction_id, recipient, expected) in [
        (
            "fields-create",
            json!({"subscriberId":"carol", "phone":"+12345", "avatar":"https://example.test/a", "data":{"plan":"gold", "old":true}}),
            "+12345|https://example.test/a|gold",
        ),
        (
            "fields-update",
            json!({"subscriberId":"carol", "phone":null, "data":{"plan":"silver"}}),
            "|https://example.test/a|silver",
        ),
        (
            "fields-clear",
            json!({"subscriberId":"carol", "data":null}),
            "|https://example.test/a|",
        ),
    ] {
        let (status, _) = request(
            app.clone(),
            Some("ApiKey test-key"),
            json!({"name":"profile-fields", "to":recipient, "transactionId":transaction_id}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(
            novu_postgres_queue::in_app::process_one(&mut worker)
                .await
                .unwrap()
        );
        let content: String = worker.query_one("SELECT content FROM novu_rust_in_app_messages WHERE environment_id = 'env-a' AND transaction_id = $1", &[&transaction_id]).await.unwrap().get(0);
        assert_eq!(content, expected);
        let (_, data) = get_subscriber(app.clone(), Some("ApiKey test-key"), "carol").await;
        if transaction_id == "fields-update" {
            assert_eq!(data["data"]["data"], json!({"plan":"silver"}));
            assert_eq!(data["data"]["phone"], Value::Null);
        }
        if transaction_id == "fields-clear" {
            assert_eq!(data["data"]["data"], Value::Null);
            assert_eq!(data["data"]["avatar"], "https://example.test/a");
        }
    }
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"profile-fields", "to":{"subscriberId":"carol", "data":[]}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    worker.execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-b', 'names', true, true, '{{subscriber.firstName}}')", &[]).await.unwrap();
    let (status, _) = request(app.clone(), Some("ApiKey test-key-b"), json!({"name":"names", "to":{"subscriberId":"alice", "firstName":"Other"}, "transactionId":"profile-env-b"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let rows = worker.query("SELECT environment_id, profile FROM novu_rust_subscribers WHERE subscriber_id = 'alice' ORDER BY environment_id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "env-a");
    assert_eq!(
        rows[0].get::<_, Value>(1),
        json!({"firstName":"Jane", "lastName":null})
    );
    assert_eq!(rows[1].get::<_, String>(0), "env-b");
    assert_eq!(rows[1].get::<_, Value>(1), json!({"firstName":"Other"}));
    let (status, data) = get_subscriber(app.clone(), Some("ApiKey test-key"), "alice").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(data["data"]["subscriberId"], "alice");
    assert_eq!(data["data"]["firstName"], "Jane");
    assert_eq!(data["data"]["lastName"], Value::Null);
    assert_eq!(data["data"]["_environmentId"], "env-a");
    assert_eq!(data["data"]["_organizationId"], "org-a");
    assert_eq!(data["data"]["channels"], json!([]));
    assert_eq!(data["data"]["_id"].as_str().unwrap().len(), 24);
    let (status, data) = get_subscriber(app.clone(), Some("ApiKey test-key-b"), "alice").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(data["data"]["firstName"], "Other");
    assert_eq!(data["data"]["_environmentId"], "env-b");
    let (status, data) = get_subscriber(app.clone(), Some("ApiKey test-key-b"), "bob").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(data["message"], "Subscriber: bob was not found");
    assert_eq!(data["path"], "/v2/subscribers/bob");
    let (status, data) = get_subscriber(app.clone(), None, "alice").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(data["path"], "/v2/subscribers/alice");
    worker.execute("UPDATE novu_rust_subscribers SET updated_at = '2024-01-02T03:04:05.123Z' WHERE environment_id = 'env-a' AND subscriber_id = 'carol'", &[]).await.unwrap();
    let (status, data) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "carol",
        json!({"firstName":"Kim", "phone":"+9", "data":{"new":true}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(data["data"]["updatedAt"], "2024-01-02T03:04:05.123Z");
    assert_eq!(data["data"]["data"], json!({"new":true}));
    assert_eq!(data["data"]["avatar"], "https://example.test/a");
    let (_, fetched) = get_subscriber(app.clone(), Some("ApiKey test-key"), "carol").await;
    assert_ne!(fetched["data"]["updatedAt"], "2024-01-02T03:04:05.123Z");
    assert_eq!(fetched["data"]["phone"], "+9");
    let (_, data) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "carol",
        json!({"firstName":null, "data":null}),
    )
    .await;
    assert_eq!(data["data"]["firstName"], Value::Null);
    assert_eq!(data["data"]["data"], Value::Null);
    let (status, data) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key-b"),
        "carol",
        json!({"firstName":"Intruder"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(data["message"], "Subscriber carol was not found");
    let (status, _) =
        patch_subscriber(app.clone(), None, "carol", json!({"firstName":"Intruder"})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "carol",
        json!({"email":"한@example.com"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (first, second) = tokio::join!(
        patch_subscriber(
            app.clone(),
            Some("ApiKey test-key"),
            "carol",
            json!({"firstName":"Parallel"})
        ),
        patch_subscriber(
            app.clone(),
            Some("ApiKey test-key"),
            "carol",
            json!({"lastName":"Update"})
        ),
    );
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    let (_, data) = get_subscriber(app.clone(), Some("ApiKey test-key"), "carol").await;
    assert_eq!(data["data"]["firstName"], "Parallel");
    assert_eq!(data["data"]["lastName"], "Update");
    let (status, data) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "carol",
        json!({"phone":123, "firstName":false, "avatar":[], "data":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        data["errors"]["general"]["messages"],
        json!([
            "firstName must be a string",
            "phone must be a string",
            "avatar must be a string",
            "data must be an object"
        ])
    );
    let (status, created) = create_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "",
        json!({"subscriberId":" new-user ", "firstName":"Jane", "data":{"plan":"gold"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["data"]["subscriberId"], "new-user");
    assert_eq!(created["data"]["data"], json!({"plan":"gold"}));
    assert_eq!(created["data"]["_id"].as_str().unwrap().len(), 24);
    let (status, updated) = create_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "",
        json!({"subscriberId":"new-user", "phone":"123"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(updated["data"]["firstName"], "Jane");
    assert_eq!(updated["data"]["phone"], "123");
    assert!(updated["data"].get("data").is_none());
    let (_, patch_result) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "new-user",
        json!({"data":{}}),
    )
    .await;
    assert_eq!(patch_result["data"]["data"], json!({}));
    let (_, after_patch) = get_subscriber(app.clone(), Some("ApiKey test-key"), "new-user").await;
    assert!(after_patch["data"].get("data").is_none());
    let (status, conflict) = create_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "?failIfExists=true",
        json!({"subscriberId":"new-user", "firstName":"Other"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        conflict["message"],
        "Subscriber with id \"new-user\" already exists"
    );
    let (_, other_env) = create_subscriber(
        app.clone(),
        Some("ApiKey test-key-b"),
        "?failIfExists=true",
        json!({"subscriberId":"new-user", "firstName":"Other"}),
    )
    .await;
    assert_eq!(other_env["data"]["_environmentId"], "env-b");
    let (_, unchanged) = get_subscriber(app.clone(), Some("ApiKey test-key"), "new-user").await;
    assert_eq!(unchanged["data"]["firstName"], "Jane");
    let (status, _) =
        create_subscriber(app.clone(), None, "", json!({"subscriberId":"anonymous"})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, data) = create_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "",
        json!({"phone":123}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        data["errors"]["general"]["messages"],
        json!([
            "subscriberId should not be null or undefined",
            "SubscriberId is required",
            "subscriberId must be a string",
            "phone must be a string"
        ])
    );
    let (first, second) = tokio::join!(
        create_subscriber(
            app.clone(),
            Some("ApiKey test-key"),
            "?failIfExists=true",
            json!({"subscriberId":"concurrent"})
        ),
        create_subscriber(
            app.clone(),
            Some("ApiKey test-key"),
            "?failIfExists=true",
            json!({"subscriberId":"concurrent"})
        ),
    );
    let statuses = [first.0, second.0];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::CREATED)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::CONFLICT)
            .count(),
        1
    );
    let before: i64 = worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id = 'env-a' AND subscriber_id = 'carol'", &[]).await.unwrap().get(0);
    assert!(before > 0);
    worker.execute("INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT environment_id,internal_id,'orphan','workflow','{}'::jsonb FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='carol'", &[]).await.unwrap();
    worker.batch_execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status) SELECT 'rollback-email',environment_id,subscriber_id,internal_id,'n','w','delete-rollback','subject','sent' FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='carol'; INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status) SELECT 'rollback-sms',environment_id,subscriber_id,internal_id,'n','w','delete-rollback','sent' FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='carol'; INSERT INTO novu_rust_push_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,device_tokens,title,status,push_job_id,target_index) SELECT 'rollback-push',s.environment_id,s.subscriber_id,s.internal_id,'n','w','delete-rollback','{}','title','sent',q.id,98765 FROM novu_rust_subscribers s CROSS JOIN LATERAL (SELECT id FROM novu_queue_jobs WHERE environment_id='env-a' LIMIT 1) q WHERE s.environment_id='env-a' AND s.subscriber_id='carol'").await.unwrap();
    // A dependent FK forces the final delete to fail; all related deletions must roll back.
    worker.batch_execute("CREATE TABLE delete_test_blocker (environment_id text NOT NULL, subscriber_id text NOT NULL, FOREIGN KEY (environment_id, subscriber_id) REFERENCES novu_rust_subscribers(environment_id, subscriber_id)); INSERT INTO delete_test_blocker VALUES ('env-a', 'carol')").await.unwrap();
    let (status, _) = delete_subscriber(app.clone(), Some("ApiKey test-key"), "carol").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let after: i64 = worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id = 'env-a' AND subscriber_id = 'carol'", &[]).await.unwrap().get(0);
    assert_eq!(before, after);
    let preferences: i64 = worker.query_one("SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-a' AND subscription_internal_id='orphan'", &[]).await.unwrap().get(0);
    assert_eq!(preferences, 1);
    let channel_history: i64 = worker.query_one("SELECT (SELECT count(*) FROM novu_rust_email_messages WHERE transaction_id='delete-rollback')+(SELECT count(*) FROM novu_rust_sms_messages WHERE transaction_id='delete-rollback')+(SELECT count(*) FROM novu_rust_push_messages WHERE transaction_id='delete-rollback')",&[]).await.unwrap().get(0);
    assert_eq!(channel_history, 3);
    worker
        .batch_execute("DROP TABLE delete_test_blocker")
        .await
        .unwrap();
    let (status, _) = delete_subscriber(app.clone(), Some("ApiKey test-key-b"), "carol").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = delete_subscriber(app.clone(), None, "carol").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, response) = delete_subscriber(app.clone(), Some("ApiKey test-key"), "carol").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        response,
        json!({"data":{"acknowledged":true,"status":"deleted"}})
    );
    let after: i64 = worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id = 'env-a' AND subscriber_id = 'carol'", &[]).await.unwrap().get(0);
    assert_eq!(after, 0);
    let preferences: i64 = worker.query_one("SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='env-a' AND subscription_internal_id='orphan'", &[]).await.unwrap().get(0);
    assert_eq!(preferences, 0);
    let channel_history: i64 = worker.query_one("SELECT (SELECT count(*) FROM novu_rust_email_messages WHERE transaction_id='delete-rollback')+(SELECT count(*) FROM novu_rust_sms_messages WHERE transaction_id='delete-rollback')+(SELECT count(*) FROM novu_rust_push_messages WHERE transaction_id='delete-rollback')",&[]).await.unwrap().get(0);
    assert_eq!(channel_history, 0);
    let (status, response) = delete_subscriber(app.clone(), Some("ApiKey test-key"), "carol").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(response["message"], "Subscriber was not found");
    assert_eq!(response["externalSubscriberId"], "carol");
    assert_eq!(response["ctx"], json!({"externalSubscriberId":"carol"}));
    let (status, _) = get_subscriber(app.clone(), Some("ApiKey test-key-b"), "alice").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"names", "to":{"subscriberId":"counter","firstName":"Jane"}, "transactionId":"count-workflow"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let (_, count) =
        count_notifications(app.clone(), Some("ApiKey test-key"), "counter", "[{}]").await;
    assert_eq!(count["data"], json!([{"count":1,"filter":{}}]));
    worker.batch_execute("INSERT INTO novu_rust_in_app_messages (id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,is_read,is_seen,is_archived) VALUES ('00000000-0000-4000-8000-000000000002','env-a','counter','fixture','read','x','{}',true,true,false),('00000000-0000-4000-8000-000000000003','env-a','counter','fixture','archive','x','{}',true,true,true)").await.unwrap();
    let (status, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        r#"[{},{"read":false},{"read":true},{"seen":false},{"archived":true}]"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        counts["data"],
        json!([{"count":3,"filter":{}},{"count":1,"filter":{"read":false}},{"count":2,"filter":{"read":true}},{"count":1,"filter":{"seen":false}},{"count":1,"filter":{"archived":true}}])
    );
    let (status, _) =
        count_notifications(app.clone(), Some("ApiKey test-key-b"), "counter", "[{}]").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = count_notifications(app.clone(), None, "counter", "[{}]").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        r#"[{"read":false,"archived":true}]"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        r#"[{"tags":{"and":[{"or":[]}]}}]"#,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        "read-archive",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        r#"[{"read":false},{"archived":true}]"#,
    )
    .await;
    assert_eq!(
        counts["data"],
        json!([{"count":1,"filter":{"read":false}},{"count":2,"filter":{"archived":true}}])
    );
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        "read",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        r#"[{"read":true},{"seen":true},{"archived":true}]"#,
    )
    .await;
    assert_eq!(
        counts["data"],
        json!([{"count":3,"filter":{"read":true}},{"count":3,"filter":{"seen":true}},{"count":2,"filter":{"archived":true}}])
    );
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        "archive",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        r#"[{"archived":true}]"#,
    )
    .await;
    assert_eq!(counts["data"][0]["count"], 3);
    let timestamps: i64 = worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id = 'env-a' AND subscriber_id = 'counter' AND first_seen_at IS NOT NULL AND last_seen_at IS NOT NULL AND last_read_at IS NOT NULL AND archived_at IS NOT NULL", &[]).await.unwrap().get(0);
    assert_eq!(timestamps, 3);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key-b"),
        "counter",
        "read",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = bulk_notifications(app.clone(), None, "counter", "read", json!({})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "counter",
        "read",
        json!({"contextKeys":["x"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let metadata = json!({"id":"64a000000000000000000004","identifier":"names","name":"Names","critical":false,"tags":[],"severity":"none","payloadAsData":true});
    worker.execute("UPDATE novu_rust_workflows SET inbox_metadata=$1 WHERE environment_id='env-a' AND identifier='names'", &[&metadata]).await.unwrap();
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"names", "to":{"subscriberId":"seen-user","firstName":"Jane"}, "transactionId":"seen-workflow"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let message_id: String = worker.query_one("SELECT internal_id FROM novu_rust_in_app_messages WHERE transaction_id = 'seen-workflow'", &[]).await.unwrap().get(0);
    assert_eq!(message_id.len(), 24);
    worker.execute("INSERT INTO novu_rust_in_app_messages (id, internal_id, environment_id, subscriber_id, workflow_identifier, transaction_id, content, payload) VALUES ('00000000-0000-4000-8000-000000000020', '64a000000000000000000102', 'env-a', 'seen-user', 'names', 'seen-other', 'x', '{}'::jsonb)", &[]).await.unwrap();
    let secret = b"isolated-subscriber-secret";
    let inbox_app = novu_rust_api::router_with_subscriber_secret(db.clone(), Some(secret.to_vec()));
    worker.execute("INSERT INTO novu_rust_inbox_environments (environment_id,application_identifier,in_app_active,hmac_required,is_development,remove_novu_branding,max_snooze_duration_hours) VALUES ('env-a','app-a',true,false,true,false,0),('env-b','app-b',true,true,false,false,0)", &[]).await.unwrap();
    for (key, environment) in [
        ("old-key", "env-a"),
        ("new-key", "env-a"),
        ("other-key", "env-b"),
    ] {
        let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
        worker.execute("INSERT INTO novu_rust_api_keys (key_hash,environment_id,user_id) VALUES ($1,$2,'test-user')", &[&hash, &environment]).await.unwrap();
    }
    let session_app = novu_rust_api::router_with_session_keys(
        db.clone(),
        Some(secret.to_vec()),
        [
            (
                "env-a".to_string(),
                vec!["old-key".to_string(), "new-key".to_string()],
            ),
            ("env-b".to_string(), vec!["other-key".to_string()]),
        ]
        .into_iter()
        .collect(),
    );
    let session_path = "/v1/inbox/session";
    let (status, session) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a", "subscriber":{"subscriberId":"session-user","firstName":"Jane"}})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(session["data"]["unreadCount"]["total"], 0);
    let issued = format!("Bearer {}", session["data"]["token"].as_str().unwrap());
    let (status, _) = inbox_get(
        session_app.clone(),
        "/v1/inbox/notifications",
        Some(&issued),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a", "subscriber":{"subscriberId":"session-user","firstName":"Attacker"}})).await;
    assert_eq!(status, StatusCode::CREATED);
    let stored: Value = worker.query_one("SELECT profile FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='session-user'", &[]).await.unwrap().get(0);
    assert_eq!(stored["firstName"], "Jane");
    for key in ["old-key", "new-key"] {
        let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a", "subscriber":{"subscriberId":"session-user","firstName":key},"subscriberHash":subscriber_hash(key,"session-user")})).await;
        assert_eq!(status, StatusCode::CREATED);
        let stored: Value = worker.query_one("SELECT profile FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='session-user'", &[]).await.unwrap().get(0);
        assert_eq!(stored["firstName"], key);
    }
    let old_hash = format!("{:x}", Sha256::digest(b"old-key"));
    worker
        .execute(
            "UPDATE novu_rust_api_keys SET enabled=false WHERE key_hash=$1",
            &[&old_hash],
        )
        .await
        .unwrap();
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a", "subscriber":{"subscriberId":"session-user","firstName":"revoked"},"subscriberHash":subscriber_hash("old-key","session-user")})).await;
    assert_eq!(status, StatusCode::CREATED);
    let stored: Value = worker.query_one("SELECT profile FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='session-user'", &[]).await.unwrap().get(0);
    assert_eq!(stored["firstName"], "new-key");
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "POST",
        session_path,
        None,
        json!({"applicationIdentifier":"app-b", "subscriberId":"secure-user"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b", "subscriberId":"secure-user", "subscriberHash":subscriber_hash("wrong-key","secure-user")})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b", "subscriberId":"secure-user", "subscriberHash":subscriber_hash("new-key","secure-user")})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = inbox_mutate(inbox_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b", "subscriberId":"secure-user", "subscriberHash":subscriber_hash("other-key","secure-user")})).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let exists: i64 = worker.query_one("SELECT count(*) FROM novu_rust_subscribers WHERE environment_id='env-b' AND subscriber_id='secure-user'", &[]).await.unwrap().get(0);
    assert_eq!(exists, 0);
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b", "subscriberId":"secure-user", "subscriberHash":subscriber_hash("other-key","secure-user")})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a", "subscriberId":"never-create", "context":{"tenant":{"id":""}}})).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let no_signing_key =
        novu_rust_api::router_with_session_keys(db.clone(), None, Default::default());
    let (status, _) = inbox_mutate(
        no_signing_key,
        "POST",
        session_path,
        None,
        json!({"applicationIdentifier":"app-a", "subscriberId":"never-create"}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let exists: i64 = worker.query_one("SELECT count(*) FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='never-create'", &[]).await.unwrap().get(0);
    assert_eq!(exists, 0);
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"names", "to":"session-user", "transactionId":"context-test", "context":{"tenant":"acme"}})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let (status, scoped_session) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a","subscriberId":"session-user","context":{"tenant":"acme"}})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        scoped_session["data"]["contextKeys"],
        json!(["tenant:acme"])
    );
    assert_eq!(scoped_session["data"]["totalUnreadCount"], 1);
    let scoped_bearer = format!(
        "Bearer {}",
        scoped_session["data"]["token"].as_str().unwrap()
    );
    let (status, list) = inbox_get(
        session_app.clone(),
        "/v1/inbox/notifications",
        Some(&scoped_bearer),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["data"].as_array().unwrap().len(), 1);
    let (_, default_list) = inbox_get(
        session_app.clone(),
        "/v1/inbox/notifications",
        Some(&issued),
    )
    .await;
    assert_eq!(default_list["data"].as_array().unwrap().len(), 0);
    // A single-item operation requires an exact context, whereas Novu bulk
    // operations use overlap and an empty context does not filter at all.
    for (transaction, context) in [
        ("context-multi", json!({"tenant":"acme", "app":"jira"})),
        ("context-other", json!({"tenant":"other"})),
    ] {
        let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"names", "to":"session-user", "transactionId":transaction, "context":context})).await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(
            novu_postgres_queue::in_app::process_one(&mut worker)
                .await
                .unwrap()
        );
    }
    let multi_id: String = worker.query_one("SELECT internal_id FROM novu_rust_in_app_messages WHERE transaction_id='context-multi'", &[]).await.unwrap().get(0);
    let single_path = format!("/v1/inbox/notifications/{multi_id}/read");
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "PATCH",
        &single_path,
        Some(&scoped_bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let exact_id: String = worker
        .query_one(
            "SELECT internal_id FROM novu_rust_in_app_messages WHERE transaction_id='context-test'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let exact_path = format!("/v1/inbox/notifications/{exact_id}/read");
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "PATCH",
        &exact_path,
        Some(&scoped_bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let other_id: String = worker.query_one("SELECT internal_id FROM novu_rust_in_app_messages WHERE transaction_id='context-other'", &[]).await.unwrap().get(0);
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "POST",
        "/v1/inbox/notifications/seen",
        Some(&scoped_bearer),
        json!({"notificationIds":[multi_id,other_id]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let seen: bool = worker
        .query_one(
            "SELECT is_seen FROM novu_rust_in_app_messages WHERE transaction_id='context-other'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!seen);
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "POST",
        "/v1/inbox/notifications/read",
        Some(&scoped_bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let states = worker.query("SELECT transaction_id,is_read FROM novu_rust_in_app_messages WHERE transaction_id IN ('context-test','context-multi','context-other') ORDER BY transaction_id", &[]).await.unwrap();
    assert_eq!(
        states
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, bool>(1)))
            .collect::<Vec<_>>(),
        vec![
            ("context-multi".into(), true),
            ("context-other".into(), false),
            ("context-test".into(), true)
        ]
    );
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "POST",
        "/v1/inbox/notifications/read",
        Some(&issued),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let is_read: bool = worker
        .query_one(
            "SELECT is_read FROM novu_rust_in_app_messages WHERE transaction_id='context-other'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(is_read);
    let delete_path = format!("/v1/inbox/notifications/{multi_id}/delete");
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "DELETE",
        &delete_path,
        Some(&scoped_bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let exact_delete_path = format!("/v1/inbox/notifications/{exact_id}/delete");
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "DELETE",
        &exact_delete_path,
        Some(&scoped_bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = inbox_mutate(
        session_app.clone(),
        "POST",
        "/v1/inbox/notifications/delete",
        Some(&scoped_bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let remaining: Vec<String> = worker.query("SELECT transaction_id FROM novu_rust_in_app_messages WHERE transaction_id IN ('context-test','context-multi','context-other')", &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
    assert_eq!(remaining, vec!["context-other"]);
    let context = json!({"tenant":"acme"});
    let correct_hash = subscriber_hash("other-key", &context.to_string());
    let signed_id = subscriber_hash("other-key", "secure-user");
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b","subscriberId":"secure-user","subscriberHash":signed_id,"context":context})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b","subscriberId":"secure-user","subscriberHash":signed_id,"context":context,"contextHash":correct_hash})).await;
    assert_eq!(status, StatusCode::CREATED);
    let rich_context = json!({"tenant":{"id":"rich","data":{"plan":"basic","nested":{"count":2}}}});
    let (status, rich_session) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a","subscriberId":"session-user","context":rich_context})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(rich_session["data"]["contextKeys"], json!(["tenant:rich"]));
    let stored: Value = worker.query_one("SELECT data FROM novu_rust_contexts WHERE environment_id='env-a' AND context_key='tenant:rich'", &[]).await.unwrap().get(0);
    assert_eq!(stored, json!({"plan":"basic","nested":{"count":2}}));
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a","subscriberId":"session-user","context":{"tenant":{"id":"rich","data":{"plan":"malicious"}}}})).await;
    assert_eq!(status, StatusCode::CREATED);
    let stored: Value = worker.query_one("SELECT data FROM novu_rust_contexts WHERE environment_id='env-a' AND context_key='tenant:rich'", &[]).await.unwrap().get(0);
    assert_eq!(stored, json!({"plan":"basic","nested":{"count":2}}));
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"names","to":"session-user","transactionId":"rich-context-update","context":{"tenant":{"id":"rich","data":{"plan":"enterprise"}}}})).await;
    assert_eq!(status, StatusCode::CREATED);
    let before_worker: Value = worker.query_one("SELECT data FROM novu_rust_contexts WHERE environment_id='env-a' AND context_key='tenant:rich'", &[]).await.unwrap().get(0);
    assert_eq!(before_worker, json!({"plan":"enterprise"}));
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let stored: Value = worker.query_one("SELECT data FROM novu_rust_contexts WHERE environment_id='env-a' AND context_key='tenant:rich'", &[]).await.unwrap().get(0);
    assert_eq!(stored, json!({"plan":"enterprise"}));
    let rich_hash = subscriber_hash(
        "other-key",
        r#"{"tenant":{"data":{"nested":{"count":2},"plan":"basic"},"id":"rich"}}"#,
    );
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b","subscriberId":"secure-user","subscriberHash":signed_id,"context":rich_context,"contextHash":rich_hash})).await;
    assert_eq!(status, StatusCode::CREATED);
    let stored: Value = worker.query_one("SELECT data FROM novu_rust_contexts WHERE environment_id='env-b' AND context_key='tenant:rich'", &[]).await.unwrap().get(0);
    assert_eq!(stored, json!({"plan":"basic","nested":{"count":2}}));
    worker.execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-a','context-template',true,true,$1),('env-b','context-template',true,true,$1)", &[&"{{context.tenant.id}}|{{context.tenant.data.plan}}|{{context.tenant.data.nested.name}}|{{subscriber.firstName}}"] ).await.unwrap();
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"context-template","to":{"subscriberId":"context-render","firstName":"Jane"},"transactionId":"context-render-first","payload":{"context":{"tenant":{"id":"fake","data":{"plan":"forged"}}}},"context":{"tenant":{"id":"render","data":{"plan":"basic"}}}})).await;
    assert_eq!(status, StatusCode::CREATED);
    // A later trigger updates context before the first queued job is processed.
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"context-template","to":{"subscriberId":"context-render","firstName":"Jane"},"transactionId":"context-render-second","context":{"tenant":{"id":"render","data":{"plan":"gold & <safe>","nested":{"name":"Team"}}}}})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = request(app.clone(), Some("ApiKey test-key-b"), json!({"name":"context-template","to":{"subscriberId":"context-render","firstName":"Other"},"transactionId":"context-render-foreign","context":{"tenant":{"id":"render","data":{"plan":"foreign"}}}})).await;
    assert_eq!(status, StatusCode::CREATED);
    for _ in 0..3 {
        assert!(
            novu_postgres_queue::in_app::process_one(&mut worker)
                .await
                .unwrap()
        );
    }
    let rows = worker.query("SELECT transaction_id,content FROM novu_rust_in_app_messages WHERE transaction_id LIKE 'context-render-%' ORDER BY transaction_id", &[]).await.unwrap();
    let rendered: Vec<(String, String)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(
        rendered,
        vec![
            (
                "context-render-first".into(),
                "render|gold &amp; &lt;safe&gt;|Team|Jane".into()
            ),
            (
                "context-render-foreign".into(),
                "render|foreign||Other".into()
            ),
            (
                "context-render-second".into(),
                "render|gold &amp; &lt;safe&gt;|Team|Jane".into()
            )
        ]
    );
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"context-template","to":{"subscriberId":"context-empty","firstName":"Empty"},"transactionId":"context-render-empty","payload":{"context":{"tenant":{"id":"fake"}}}})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let rendered: String = worker.query_one("SELECT content FROM novu_rust_in_app_messages WHERE transaction_id='context-render-empty'", &[]).await.unwrap().get(0);
    assert_eq!(rendered, "|||Empty");
    // Context management is scoped to the authenticated environment.
    for index in 0..5 {
        let (status, _) = inbox_mutate(
            app.clone(),
            "POST",
            "/v2/contexts",
            Some("ApiKey test-key"),
            json!({"type":"page","id":format!("item-{index}"),"data":{"index":index}}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/contexts",
        Some("ApiKey test-key-b"),
        json!({"type":"page","id":"foreign"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    worker.execute("UPDATE novu_rust_contexts SET created_at='2024-01-01T00:00:00Z' WHERE environment_id='env-a' AND context_key LIKE 'page:%'", &[]).await.unwrap();
    let path = "/v2/contexts?type=page&limit=2&orderDirection=ASC";
    let (status, page1) = inbox_get(app.clone(), path, Some("ApiKey test-key")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page1["totalCount"], 5);
    assert_eq!(page1["data"].as_array().unwrap().len(), 2);
    assert_eq!(page1["data"][0]["id"], "item-0");
    assert_eq!(page1["data"][1]["id"], "item-1");
    assert_eq!(page1["previous"], Value::Null);
    let next = page1["next"].as_str().unwrap();
    let (status, page2) = inbox_get(
        app.clone(),
        &format!("{path}&after={next}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page2["data"][0]["id"], "item-2");
    assert_eq!(page2["data"][1]["id"], "item-3");
    let previous = page2["previous"].as_str().unwrap();
    let (_, back) = inbox_get(
        app.clone(),
        &format!("{path}&before={previous}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(back["data"], page1["data"]);
    let (_, inclusive) = inbox_get(
        app.clone(),
        &format!("{path}&after={next}&includeCursor=true"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(inclusive["data"][0]["id"], "item-1");
    let (_, descending) = inbox_get(
        app.clone(),
        "/v2/contexts?type=page&limit=2",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(descending["data"][0]["id"], "item-4");
    let next_desc = descending["next"].as_str().unwrap();
    let (_, descending2) = inbox_get(
        app.clone(),
        &format!("/v2/contexts?type=page&limit=2&after={next_desc}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(descending2["data"][0]["id"], "item-2");
    let before_desc = descending2["previous"].as_str().unwrap();
    let (_, descending_back) = inbox_get(
        app.clone(),
        &format!("/v2/contexts?type=page&limit=2&before={before_desc}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(descending_back["data"], descending["data"]);
    let (_, one_id) = inbox_get(
        app.clone(),
        "/v2/contexts?type=page&id=item-3&orderBy=updatedAt",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(one_id["totalCount"], 1);
    assert_eq!(one_id["data"][0]["id"], "item-3");
    let (_, search) = inbox_get(
        app.clone(),
        "/v2/contexts?type=page&search=ITEM-4",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(search["totalCount"], 1);
    assert_eq!(search["data"][0]["id"], "item-4");
    let (_, literal) = inbox_get(
        app.clone(),
        "/v2/contexts?type=page&search=%5B",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(literal["totalCount"], 0);
    let (status, _) = inbox_get(
        app.clone(),
        &format!("{path}&after={next}&before={previous}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let foreign_cursor: String = worker.query_one("SELECT internal_id FROM novu_rust_contexts WHERE environment_id='env-b' AND context_key='page:foreign'", &[]).await.unwrap().get(0);
    let (_, foreign) = inbox_get(
        app.clone(),
        &format!("{path}&after={foreign_cursor}"),
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(foreign["totalCount"], 0);
    assert_eq!(foreign["data"], json!([]));
    let (status, _) = inbox_get(app.clone(), path, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let context_path = "/v2/contexts/tenant/managed";
    let (status, created) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/contexts",
        Some("ApiKey test-key"),
        json!({"type":"tenant","id":"managed","data":{"old":true}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["data"]["data"], json!({"old":true}));
    assert!(
        created["data"]["createdAt"]
            .as_str()
            .unwrap()
            .ends_with('Z')
    );
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/contexts",
        Some("ApiKey test-key"),
        json!({"type":"tenant","id":"managed"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = inbox_get(app.clone(), context_path, Some("ApiKey test-key-b")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        context_path,
        Some("ApiKey test-key-b"),
        json!({"data":{"changed":true}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, updated) = inbox_mutate(
        app.clone(),
        "PATCH",
        context_path,
        Some("ApiKey test-key"),
        json!({"data":{"new":{"name":"fresh"}}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["data"]["data"], json!({"new":{"name":"fresh"}}));
    assert_eq!(updated["data"]["createdAt"], created["data"]["createdAt"]);
    let (status, fetched) = inbox_get(app.clone(), context_path, Some("ApiKey test-key")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["data"], updated["data"]);
    let (status, _) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/contexts",
        Some("ApiKey test-key"),
        json!({"type":"tenant","id":"blocked-url","bridgeUrl":"http://127.0.0.1"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"context-template","to":{"subscriberId":"deleted-context","firstName":"Gone"},"transactionId":"deleted-context-test","context":{"tenant":"managed"}})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        context_path,
        Some("ApiKey test-key-b"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        context_path,
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = inbox_get(app.clone(), context_path, Some("ApiKey test-key")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let content: String = worker.query_one("SELECT content FROM novu_rust_in_app_messages WHERE transaction_id='deleted-context-test'", &[]).await.unwrap().get(0);
    assert_eq!(content, "|||Gone");
    let (status, _) = inbox_get(app.clone(), context_path, Some("ApiKey test-key")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = inbox_get(app.clone(), "/v2/contexts/tenant/rich", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, stored_context) = inbox_get(
        app.clone(),
        "/v2/contexts/tenant/rich",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored_context["data"]["data"], json!({"plan":"enterprise"}));
    let (status, _) = inbox_mutate(
        app.clone(),
        "DELETE",
        "/v2/contexts/tenant/rich",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let count: i64 = worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='rich-context-update'", &[]).await.unwrap().get(0);
    assert_eq!(count, 1);
    let large_context =
        json!({"tenant":{"id":"large","data":{"large":1e21,"rounded":9007199254740992u64}}});
    let large_hash = subscriber_hash(
        "other-key",
        r#"{"tenant":{"data":{"large":1e+21,"rounded":9007199254740992},"id":"large"}}"#,
    );
    let (status, large_session) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b","subscriberId":"secure-user","subscriberHash":signed_id,"context":large_context,"contextHash":large_hash})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        large_session["data"]["contextKeys"],
        json!(["tenant:large"])
    );
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-b","subscriberId":"secure-user","subscriberHash":signed_id,"context":{"tenant":{"id":"decimal","data":{"price":1.5}}},"contextHash":"irrelevant"})).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (status, created) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/subscribers",
        Some("ApiKey test-key"),
        json!({"subscriberId":"locale-user","locale":"ko-KR"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["data"]["locale"], "ko-KR");
    let (status, updated) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "locale-user",
        json!({"locale":"zh-Hant-TW"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["data"]["locale"], "zh-Hant-TW");
    let (status, _) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "locale-user",
        json!({"locale":"en--US"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a","subscriber":{"subscriberId":"session-locale","locale":"en-US"}})).await;
    assert_eq!(status, StatusCode::CREATED);
    let stored: Value = worker.query_one("SELECT profile FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='session-locale'", &[]).await.unwrap().get(0);
    assert_eq!(stored["locale"], "en-US");
    worker.execute("INSERT INTO novu_rust_workflows (environment_id,identifier,active,has_steps,in_app_content) VALUES ('env-a','locale-template',true,true,'{{subscriber.locale}}')", &[]).await.unwrap();
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"locale-template","to":{"subscriberId":"locale-recipient","locale":"ko-KR"},"transactionId":"locale-trigger"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let content: String = worker
        .query_one(
            "SELECT content FROM novu_rust_in_app_messages WHERE transaction_id='locale-trigger'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(content, "ko-KR");
    let (status, _) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "locale-user",
        json!({"locale":null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, fetched) = get_subscriber(app.clone(), Some("ApiKey test-key"), "locale-user").await;
    assert_eq!(fetched["data"]["locale"], Value::Null);
    let (status, created) = inbox_mutate(
        app.clone(),
        "POST",
        "/v2/subscribers",
        Some("ApiKey test-key"),
        json!({"subscriberId":"timezone-user","timezone":"Asia/Seoul"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["data"]["timezone"], "Asia/Seoul");
    let (status, updated) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "timezone-user",
        json!({"timezone":"US/Eastern"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["data"]["timezone"], "US/Eastern");
    let (status, _) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "timezone-user",
        json!({"timezone":"+24:00"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = inbox_mutate(session_app.clone(), "POST", session_path, None, json!({"applicationIdentifier":"app-a","subscriber":{"subscriberId":"session-tz","timezone":"+05:30"}})).await;
    assert_eq!(status, StatusCode::CREATED);
    let stored: Value = worker.query_one("SELECT profile FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='session-tz'", &[]).await.unwrap().get(0);
    assert_eq!(stored["timezone"], "+05:30");
    worker.execute("INSERT INTO novu_rust_workflows (environment_id,identifier,active,has_steps,in_app_content) VALUES ('env-a','timezone-template',true,true,'{{subscriber.timezone}}')", &[]).await.unwrap();
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), json!({"name":"timezone-template","to":{"subscriberId":"timezone-recipient","timezone":"Asia/Seoul"},"transactionId":"timezone-trigger"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let content: String = worker
        .query_one(
            "SELECT content FROM novu_rust_in_app_messages WHERE transaction_id='timezone-trigger'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(content, "Asia/Seoul");
    let (status, _) = patch_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "timezone-user",
        json!({"timezone":null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, fetched) = get_subscriber(app.clone(), Some("ApiKey test-key"), "timezone-user").await;
    assert_eq!(fetched["data"]["timezone"], Value::Null);
    let claims = json!({"environmentId":"env-a","subscriberId":"seen-user","aud":"widget_user","contextKeys":[],"exp":4102444800u64});
    let token = subscriber_token(claims.clone(), secret);
    let bearer = format!("Bearer {token}");
    let (status, list) =
        inbox_get(inbox_app.clone(), "/v1/inbox/notifications", Some(&bearer)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["data"].as_array().unwrap().len(), 2);
    let (status, count) = inbox_get(
        inbox_app.clone(),
        "/v1/inbox/notifications/count?filters=%5B%7B%7D%5D",
        Some(&bearer),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(count["data"][0]["count"], 2);
    for invalid in [
        subscriber_token(claims.clone(), b"wrong-secret"),
        subscriber_token(
            json!({"environmentId":"env-a","subscriberId":"seen-user","aud":"user"}),
            secret,
        ),
        subscriber_token(
            json!({"environmentId":"env-a","subscriberId":"seen-user","aud":"widget_user","exp":1}),
            secret,
        ),
        subscriber_token(
            json!({"environmentId":"env-a","subscriberId":"seen-user","aud":"widget_user","nbf":4102444800u64}),
            secret,
        ),
        subscriber_token(
            json!({"environmentId":"env-b","subscriberId":"seen-user","aud":"widget_user"}),
            secret,
        ),
        subscriber_token(
            json!({"environmentId":"env-a","subscriberId":"missing","aud":"widget_user"}),
            secret,
        ),
    ] {
        let (status, _) = inbox_get(
            inbox_app.clone(),
            "/v1/inbox/notifications",
            Some(&format!("Bearer {invalid}")),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    for auth in [None, Some("ApiKey test-key"), Some("Bearer invalid")] {
        let (status, _) = inbox_get(inbox_app.clone(), "/v1/inbox/notifications", auth).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    let no_secret = novu_rust_api::router_with_subscriber_secret(db.clone(), None);
    let (status, _) = inbox_get(no_secret, "/v1/inbox/notifications", Some(&bearer)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let unsigned = format!(
        "{}.{}.",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let (status, _) = inbox_get(
        inbox_app.clone(),
        "/v1/inbox/notifications",
        Some(&format!("Bearer {unsigned}")),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = inbox_get(
        inbox_app.clone(),
        "/v1/inbox/notifications?snoozed=true",
        Some(&bearer),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, count) = inbox_get(
        inbox_app.clone(),
        "/v1/inbox/notifications/count?filters=%5B%7B%22snoozed%22%3Atrue%7D%5D",
        Some(&bearer),
    )
    .await;
    assert_eq!(count["data"][0]["count"], 0);
    worker.execute("UPDATE novu_rust_in_app_messages SET snoozed_until='2020-01-02T03:04:05.123Z' WHERE environment_id='env-a' AND internal_id='64a000000000000000000102'", &[]).await.unwrap();
    let (status, snoozed) = inbox_get(
        inbox_app.clone(),
        "/v1/inbox/notifications?snoozed=true",
        Some(&bearer),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(snoozed["data"].as_array().unwrap().len(), 1);
    assert_eq!(snoozed["data"][0]["isSnoozed"], true);
    assert_eq!(
        snoozed["data"][0]["snoozedUntil"],
        "2020-01-02T03:04:05.123Z"
    );
    let (_, count) = inbox_get(
        inbox_app.clone(),
        "/v1/inbox/notifications/count?filters=%5B%7B%22snoozed%22%3Atrue%7D%5D",
        Some(&bearer),
    )
    .await;
    assert_eq!(count["data"][0]["count"], 1);
    worker.execute("UPDATE novu_rust_in_app_messages SET snoozed_until=NULL WHERE environment_id='env-a' AND internal_id='64a000000000000000000102'", &[]).await.unwrap();
    create_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "",
        json!({"subscriberId":"unsnooze-user"}),
    )
    .await;
    worker.execute("INSERT INTO novu_rust_in_app_messages (id,internal_id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,is_read,is_archived,snoozed_until) VALUES ('00000000-0000-4000-8000-000000000070','64a000000000000000000170','env-a','unsnooze-user','names','unsnooze-test','x','{}',false,true,'2020-01-02T03:04:05.123Z')", &[]).await.unwrap();
    let unsnooze_path =
        "/v2/subscribers/unsnooze-user/notifications/64a000000000000000000170/unsnooze";
    let (status, _) = inbox_mutate(app.clone(), "PATCH", unsnooze_path, None, json!({})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        unsnooze_path,
        Some("ApiKey test-key-b"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, unsnoozed) = inbox_mutate(
        app.clone(),
        "PATCH",
        unsnooze_path,
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(unsnoozed["data"]["isRead"], false);
    assert_eq!(unsnoozed["data"]["isSeen"], true);
    assert_eq!(unsnoozed["data"]["isArchived"], false);
    assert_eq!(unsnoozed["data"]["isSnoozed"], false);
    assert!(unsnoozed["data"].get("snoozedUntil").is_none());
    worker.execute("UPDATE novu_rust_in_app_messages SET delivered_at='2019-01-01T00:00:00Z',delivered_history=ARRAY['2020-01-01T00:00:00Z'::timestamptz,'2021-01-01T00:00:00Z'::timestamptz] WHERE internal_id='64a000000000000000000170' AND environment_id='env-a'", &[]).await.unwrap();
    let (status, list) = inbox_get(
        app.clone(),
        "/v2/subscribers/unsnooze-user/notifications",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        list["data"][0]["deliveredAt"],
        json!(["2020-01-01T00:00:00.000Z", "2021-01-01T00:00:00.000Z"])
    );
    let (status, read) = inbox_mutate(
        app.clone(),
        "PATCH",
        "/v2/subscribers/unsnooze-user/notifications/64a000000000000000000170/read",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(read["data"]["deliveredAt"], list["data"][0]["deliveredAt"]);
    worker.execute("UPDATE novu_rust_in_app_messages SET delivered_history=ARRAY[]::timestamptz[] WHERE internal_id='64a000000000000000000170' AND environment_id='env-a'", &[]).await.unwrap();
    let (_, list) = inbox_get(
        app.clone(),
        "/v2/subscribers/unsnooze-user/notifications",
        Some("ApiKey test-key"),
    )
    .await;
    assert_eq!(list["data"][0]["deliveredAt"], json!([]));
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        unsnooze_path,
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // A manually unsnoozed message cancels its pending delayed job atomically.
    worker.batch_execute("UPDATE novu_rust_in_app_messages SET snoozed_until=now()+interval '1 hour' WHERE environment_id='env-a' AND internal_id='64a000000000000000000170'; INSERT INTO novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES ('00000000-0000-4000-8000-000000000071','env-a','experimental-in-app-unsnooze','{\"subscriberId\":\"unsnooze-user\",\"notificationId\":\"64a000000000000000000170\"}',now()+interval '1 hour')").await.unwrap();
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        unsnooze_path,
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let job_status: String = worker
        .query_one(
            "SELECT status FROM novu_queue_jobs WHERE id='00000000-0000-4000-8000-000000000071'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(job_status, "cancelled");
    // A due job resets read state, appends to the existing delivery history,
    // and commits the queue result in the same SQL statement.
    worker.batch_execute("UPDATE novu_rust_in_app_messages SET snoozed_until=now()-interval '1 second',is_read=true,last_read_at=now(),delivered_history=ARRAY['2020-01-01T00:00:00Z'::timestamptz] WHERE environment_id='env-a' AND internal_id='64a000000000000000000170'; INSERT INTO novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES ('00000000-0000-4000-8000-000000000072','env-a','experimental-in-app-unsnooze','{\"subscriberId\":\"unsnooze-user\",\"notificationId\":\"64a000000000000000000170\"}',now()-interval '1 second')").await.unwrap();
    let (first, second) = tokio::join!(
        novu_postgres_queue::in_app::process_one_unsnooze(&worker),
        novu_postgres_queue::in_app::process_one_unsnooze(&db)
    );
    assert_ne!(first.unwrap(), second.unwrap());
    let state = worker.query_one("SELECT is_read,snoozed_until IS NULL,cardinality(delivered_history),status,attempts FROM novu_rust_in_app_messages m JOIN novu_queue_jobs j ON j.id='00000000-0000-4000-8000-000000000072' WHERE m.environment_id='env-a' AND m.internal_id='64a000000000000000000170'", &[]).await.unwrap();
    assert!(!state.get::<_, bool>(0));
    assert!(state.get::<_, bool>(1));
    assert_eq!(state.get::<_, i32>(2), 2);
    assert_eq!(state.get::<_, String>(3), "completed");
    assert_eq!(state.get::<_, i32>(4), 1);
    worker.batch_execute("UPDATE novu_rust_in_app_messages SET snoozed_until=now()-interval '1 second',is_read=true WHERE environment_id='env-a' AND internal_id='64a000000000000000000170'; INSERT INTO novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES ('00000000-0000-4000-8000-000000000073','env-a','experimental-in-app-unsnooze','{\"subscriberId\":\"unsnooze-user\",\"notificationId\":\"64a000000000000000000170\"}',now()-interval '1 second'); ALTER TABLE novu_queue_jobs ADD CONSTRAINT test_unsnooze_completion CHECK (id <> '00000000-0000-4000-8000-000000000073'::uuid OR status <> 'completed')").await.unwrap();
    assert!(
        novu_postgres_queue::in_app::process_one_unsnooze(&worker)
            .await
            .is_err()
    );
    let state = worker.query_one("SELECT is_read,snoozed_until IS NOT NULL,cardinality(delivered_history),status FROM novu_rust_in_app_messages m JOIN novu_queue_jobs j ON j.id='00000000-0000-4000-8000-000000000073' WHERE m.environment_id='env-a' AND m.internal_id='64a000000000000000000170'", &[]).await.unwrap();
    assert!(state.get::<_, bool>(0));
    assert!(state.get::<_, bool>(1));
    assert_eq!(state.get::<_, i32>(2), 2);
    assert_eq!(state.get::<_, String>(3), "pending");
    worker
        .batch_execute("ALTER TABLE novu_queue_jobs DROP CONSTRAINT test_unsnooze_completion")
        .await
        .unwrap();
    assert!(
        novu_postgres_queue::in_app::process_one_unsnooze(&worker)
            .await
            .unwrap()
    );
    worker.execute("INSERT INTO novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES ('00000000-0000-4000-8000-000000000074','env-a','experimental-in-app-unsnooze','{\"subscriberId\":\"deleted-user\",\"notificationId\":\"64a000000000000000000199\"}',now()-interval '1 second')", &[]).await.unwrap();
    assert!(
        novu_postgres_queue::in_app::process_one_unsnooze(&worker)
            .await
            .unwrap()
    );
    let orphan_status: String = worker
        .query_one(
            "SELECT status FROM novu_queue_jobs WHERE id='00000000-0000-4000-8000-000000000074'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(orphan_status, "failed");
    worker.batch_execute("UPDATE novu_rust_in_app_messages SET snoozed_until=now()-interval '1 second' WHERE environment_id='env-a' AND internal_id='64a000000000000000000170'; INSERT INTO novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES ('00000000-0000-4000-8000-000000000075','env-a','experimental-in-app-unsnooze','{\"subscriberId\":\"unsnooze-user\",\"notificationId\":\"64a000000000000000000170\"}',now()-interval '1 second')").await.unwrap();
    let (processed, manual) = tokio::join!(
        novu_postgres_queue::in_app::process_one_unsnooze(&worker),
        inbox_mutate(
            app.clone(),
            "PATCH",
            unsnooze_path,
            Some("ApiKey test-key"),
            json!({})
        )
    );
    let processed = processed.unwrap();
    let job_status: String = worker
        .query_one(
            "SELECT status FROM novu_queue_jobs WHERE id='00000000-0000-4000-8000-000000000075'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    if processed {
        assert_eq!(manual.0, StatusCode::NOT_FOUND);
        assert_eq!(job_status, "completed");
    } else {
        assert_eq!(manual.0, StatusCode::OK);
        assert_eq!(job_status, "cancelled");
    }
    create_subscriber(
        app.clone(),
        Some("ApiKey test-key"),
        "",
        json!({"subscriberId":"snooze-user"}),
    )
    .await;
    worker.execute("INSERT INTO novu_rust_in_app_messages (id,internal_id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,is_read,is_archived) VALUES ('00000000-0000-4000-8000-000000000076','64a000000000000000000176','env-a','snooze-user','names','snooze-test','x','{}',true,true)", &[]).await.unwrap();
    // Original WS queue failures are best effort; the mutation still succeeds.
    worker.batch_execute("ALTER TABLE novu_rust_ws_events ADD CONSTRAINT test_api_event_write CHECK (event <> 'unread_count_changed') NOT VALID").await.unwrap();
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        "/v2/subscribers/snooze-user/notifications/64a000000000000000000176/unread",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let row = worker.query_one("SELECT is_read,(SELECT count(*) FROM novu_rust_ws_events e JOIN novu_rust_subscribers s ON s.environment_id=e.environment_id AND s.internal_id=e.subscriber_internal_id WHERE s.environment_id='env-a' AND s.subscriber_id='snooze-user') FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND internal_id='64a000000000000000000176'", &[]).await.unwrap();
    assert!(!row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), 0);
    worker.batch_execute("ALTER TABLE novu_rust_ws_events DROP CONSTRAINT test_api_event_write; UPDATE novu_rust_in_app_messages SET is_read=true,is_seen=false,is_archived=true WHERE environment_id='env-a' AND internal_id='64a000000000000000000176'").await.unwrap();
    let snooze_path = "/v2/subscribers/snooze-user/notifications/64a000000000000000000176/snooze";
    let stamp = (chrono::Utc::now() + chrono::Duration::minutes(3))
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        snooze_path,
        Some("ApiKey test-key"),
        json!({"snoozeUntil":"bad"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let too_far = (chrono::Utc::now() + chrono::Duration::hours(25)).to_rfc3339();
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        snooze_path,
        Some("ApiKey test-key"),
        json!({"snoozeUntil":too_far}),
    )
    .await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    worker.batch_execute("ALTER TABLE novu_queue_jobs ADD CONSTRAINT test_snooze_insert CHECK (kind <> 'experimental-in-app-unsnooze') NOT VALID").await.unwrap();
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        snooze_path,
        Some("ApiKey test-key"),
        json!({"snoozeUntil":stamp}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let unchanged: bool = worker.query_one("SELECT snoozed_until IS NULL AND is_archived AND NOT is_seen FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND internal_id='64a000000000000000000176'", &[]).await.unwrap().get(0);
    assert!(unchanged);
    worker
        .batch_execute("ALTER TABLE novu_queue_jobs DROP CONSTRAINT test_snooze_insert")
        .await
        .unwrap();
    let (status, result) = inbox_mutate(
        app.clone(),
        "PATCH",
        snooze_path,
        Some("ApiKey test-key"),
        json!({"snoozeUntil":stamp}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["data"]["snoozedUntil"], stamp);
    assert_eq!(result["data"]["isRead"], true);
    assert_eq!(result["data"]["isArchived"], false);
    let queued: i64 = worker.query_one("SELECT count(*) FROM novu_queue_jobs WHERE environment_id='env-a' AND kind='experimental-in-app-unsnooze' AND status='pending' AND payload->>'notificationId'='64a000000000000000000176'", &[]).await.unwrap().get(0);
    assert_eq!(queued, 1);
    let snooze_clear_path =
        "/v2/subscribers/snooze-user/notifications/64a000000000000000000176/unsnooze";
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        snooze_clear_path,
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    worker.execute("INSERT INTO novu_rust_organization_snooze_limits(organization_id,max_duration_ms) SELECT organization_id,108000000 FROM novu_rust_environments WHERE id='env-a'", &[]).await.unwrap();
    let longer = (chrono::Utc::now() + chrono::Duration::hours(25)).to_rfc3339();
    let (status, _) = inbox_mutate(
        app.clone(),
        "PATCH",
        snooze_path,
        Some("ApiKey test-key"),
        json!({"snoozeUntil":longer}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    worker.execute("DELETE FROM novu_rust_organization_snooze_limits WHERE organization_id=(SELECT organization_id FROM novu_rust_environments WHERE id='env-a')", &[]).await.unwrap();
    let context_token = subscriber_token(
        json!({"environmentId":"env-a","subscriberId":"seen-user","aud":"widget_user","contextKeys":["tenant:example"]}),
        secret,
    );
    let (status, _) = inbox_get(
        inbox_app.clone(),
        "/v1/inbox/notifications",
        Some(&format!("Bearer {context_token}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = get_subscriber(inbox_app.clone(), Some(&bearer), "seen-user").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let other_id: String = worker.query_one("SELECT internal_id FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND subscriber_id='counter' LIMIT 1", &[]).await.unwrap().get(0);
    let other_path = format!("/v1/inbox/notifications/{other_id}/read");
    let (status, _) = inbox_mutate(
        inbox_app.clone(),
        "PATCH",
        &other_path,
        Some(&bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let path = format!("/v1/inbox/notifications/{message_id}/read");
    for auth in [None, Some("ApiKey test-key"), Some("Bearer invalid")] {
        let (status, _) = inbox_mutate(inbox_app.clone(), "PATCH", &path, auth, json!({})).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    let (status, result) =
        inbox_mutate(inbox_app.clone(), "PATCH", &path, Some(&bearer), json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["data"]["isRead"], true);
    let (status, _) = inbox_mutate(
        inbox_app.clone(),
        "POST",
        "/v1/inbox/notifications/archive",
        Some(&bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = inbox_mutate(
        inbox_app.clone(),
        "POST",
        "/v1/inbox/notifications/seen",
        Some(&bearer),
        json!({"notificationIds":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    worker.execute("UPDATE novu_rust_in_app_messages SET is_read=false, is_seen=false, is_archived=false, first_seen_at=NULL,last_seen_at=NULL,last_read_at=NULL,archived_at=NULL,read_at_present=false,archived_at_present=false WHERE environment_id='env-a' AND subscriber_id='seen-user'", &[]).await.unwrap();
    worker.execute("UPDATE novu_rust_in_app_messages SET tags = CASE WHEN transaction_id='seen-workflow' THEN '[\"news\",\"red\"]'::jsonb ELSE '[\"blue\"]'::jsonb END WHERE environment_id='env-a' AND subscriber_id='seen-user'", &[]).await.unwrap();
    let filters = json!([{"tags":[]},{"tags":["news","blue"]},{"tags":{"or":["red"]}},{"tags":{"and":[{"or":["news","blue"]},{"or":["red"]}]}},{"tags":{"and":[]}},{"tags":["missing"]}]);
    let (status, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        &filters.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        counts["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["count"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![2, 2, 1, 1, 2, 0]
    );
    assert_eq!(counts["data"][3]["filter"], filters[3]);
    let (status, tagged) = list_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "?tags=news&tags=blue",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tagged["filter"]["tags"], json!(["news", "blue"]));
    assert_eq!(tagged["data"].as_array().unwrap().len(), 2);
    let (status, tagged) = list_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "?tags=red",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tagged["data"].as_array().unwrap().len(), 1);
    assert_eq!(tagged["data"][0]["id"], message_id);
    worker.execute("UPDATE novu_rust_in_app_messages SET data=CASE WHEN transaction_id='seen-workflow' THEN '{\"kind\":\"order\",\"order\":{\"state\":\"new\"},\"labels\":[\"red\",\"vip\"],\"priority\":2}'::jsonb ELSE '{\"kind\":\"comment\",\"priority\":1}'::jsonb END WHERE environment_id='env-a' AND subscriber_id='seen-user'", &[]).await.unwrap();
    let encoded_data = r#"%7B%22kind%22%3A%5B%22order%22%2C%22other%22%5D%2C%22order%22%3A%7B%22state%22%3A%22new%22%7D%7D"#;
    let (status, filtered) = list_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        &format!("?data={encoded_data}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(filtered["data"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["data"][0]["id"], message_id);
    assert_eq!(filtered["data"][0]["data"]["kind"], "order");
    assert_eq!(filtered["filter"]["data"]["order"]["state"], "new");
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "seen",
        json!({"data":"{\"labels\":\"vip\"}"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        r#"[{"seen":true},{"seen":false}]"#,
    )
    .await;
    assert_eq!(counts["data"][0]["count"], 1);
    assert_eq!(counts["data"][1]["count"], 1);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "read",
        json!({"tags":["red"],"data":"{\"priority\":{\"and\":[{\"or\":[1,2]},{\"or\":[2]}]}}"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        r#"[{"read":true}]"#,
    )
    .await;
    assert_eq!(counts["data"][0]["count"], 1);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "delete",
        json!({"data":"{\"kind\":\"no-match\"}"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) =
        count_notifications(app.clone(), Some("ApiKey test-key"), "seen-user", "[{}]").await;
    assert_eq!(counts["data"][0]["count"], 2);
    worker.execute("UPDATE novu_rust_in_app_messages SET data=NULL, is_read=false, is_seen=false, is_archived=false, first_seen_at=NULL,last_seen_at=NULL,last_read_at=NULL,archived_at=NULL,read_at_present=false,archived_at_present=false WHERE environment_id='env-a' AND subscriber_id='seen-user'", &[]).await.unwrap();
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "seen",
        json!({"tags":["red"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        r#"[{"seen":true},{"seen":false}]"#,
    )
    .await;
    assert_eq!(counts["data"][0]["count"], 1);
    assert_eq!(counts["data"][1]["count"], 1);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "read",
        json!({"tags":{"and":[{"or":["news"]},{"or":["red"]}]}}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        r#"[{"read":true},{"read":false}]"#,
    )
    .await;
    assert_eq!(counts["data"][0]["count"], 1);
    assert_eq!(counts["data"][1]["count"], 1);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "read-archive",
        json!({"tags":["red","blue"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        r#"[{"archived":true}]"#,
    )
    .await;
    assert_eq!(counts["data"][0]["count"], 1);
    worker.execute("UPDATE novu_rust_in_app_messages SET is_read=false, is_seen=false, is_archived=false, first_seen_at=NULL,last_seen_at=NULL,last_read_at=NULL,archived_at=NULL,read_at_present=false,archived_at_present=false WHERE environment_id='env-a' AND subscriber_id='seen-user'", &[]).await.unwrap();
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "seen",
        json!({"notificationIds":[message_id], "tags":["nonmatching"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        r#"[{"seen":true}]"#,
    )
    .await;
    assert_eq!(counts["data"][0]["count"], 1);
    worker.execute("UPDATE novu_rust_in_app_messages SET is_seen=false, first_seen_at=NULL,last_seen_at=NULL WHERE environment_id='env-a' AND subscriber_id='seen-user'", &[]).await.unwrap();
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "seen",
        json!({"notificationIds":[message_id.to_uppercase()]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, seen_counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        r#"[{"seen":true},{"seen":false},{"read":false}]"#,
    )
    .await;
    assert_eq!(
        seen_counts["data"],
        json!([{"count":1,"filter":{"seen":true}},{"count":1,"filter":{"seen":false}},{"count":2,"filter":{"read":false}}])
    );
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "missing",
        "seen",
        json!({"notificationIds":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, invalid) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "seen",
        json!({"notificationIds":"bad"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        invalid["errors"]["general"]["messages"],
        json!([
            "each value in notificationIds must be a mongodb id",
            "notificationIds must be an array"
        ])
    );
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key-b"),
        "seen-user",
        "seen",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "seen",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, seen_counts) = count_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        r#"[{"seen":true}]"#,
    )
    .await;
    assert_eq!(seen_counts["data"][0]["count"], 2);
    let (status, list) = list_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "?limit=1&seen=true",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["data"].as_array().unwrap().len(), 1);
    assert_eq!(list["hasMore"], true);
    assert_eq!(list["filter"], json!({"seen":true}));
    assert_eq!(
        list["data"][0]["workflow"],
        json!({"id":"64a000000000000000000004","identifier":"names","name":"Names","critical":false,"tags":[],"severity":"none"})
    );
    let (_, second_page) = list_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "?limit=1&offset=1",
    )
    .await;
    assert_eq!(second_page["hasMore"], false);
    assert_ne!(list["data"][0]["id"], second_page["data"][0]["id"]);
    let cursor = list["data"][0]["id"].as_str().unwrap();
    let (_, cursor_page) = list_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        &format!("?limit=1&offset=100&after={cursor}"),
    )
    .await;
    assert_eq!(cursor_page["data"][0]["id"], second_page["data"][0]["id"]);
    assert_eq!(cursor_page["hasMore"], false);
    let (status, _) =
        list_notifications(app.clone(), Some("ApiKey test-key-b"), "seen-user", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = list_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "?after=anything",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    worker.execute("UPDATE novu_rust_workflows SET inbox_metadata=NULL WHERE environment_id='env-a' AND identifier='names'", &[]).await.unwrap();
    let (status, _) = mark_one(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        &message_id,
        "archive",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let archived: bool = worker
        .query_one(
            "SELECT is_archived FROM novu_rust_in_app_messages WHERE internal_id=$1",
            &[&message_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!archived);
    worker.execute("UPDATE novu_rust_workflows SET inbox_metadata=$1 WHERE environment_id='env-a' AND identifier='names'", &[&metadata]).await.unwrap();
    for (action, read, archived) in [
        ("archive", true, true),
        ("unread", false, false),
        ("read", true, false),
        ("unarchive", true, false),
    ] {
        let (status, result) = mark_one(
            app.clone(),
            Some("ApiKey test-key"),
            "seen-user",
            &message_id,
            action,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["data"]["id"], message_id);
        assert_eq!(result["data"]["isRead"], read);
        assert_eq!(result["data"]["isArchived"], archived);
        assert_eq!(result["data"]["isSeen"], true);
        assert_eq!(result["data"]["readAt"].is_string(), read);
        assert_eq!(result["data"]["archivedAt"].is_string(), archived);
    }
    let (status, _) = mark_one(
        app.clone(),
        Some("ApiKey test-key-b"),
        "seen-user",
        &message_id,
        "read",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = mark_one(app.clone(), None, "seen-user", &message_id, "read").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = mark_one(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "bad",
        "read",
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = delete_notification(
        app.clone(),
        Some("ApiKey test-key-b"),
        "seen-user",
        &message_id,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, invalid) =
        delete_notification(app.clone(), Some("ApiKey test-key"), "seen-user", "bad").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(invalid["errors"]["notificationId"]["value"], "bad");
    let other_id: String = worker.query_one("SELECT internal_id FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND subscriber_id='counter' LIMIT 1", &[]).await.unwrap().get(0);
    let (status, _) =
        delete_notification(app.clone(), Some("ApiKey test-key"), "seen-user", &other_id).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = delete_notification(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        &message_id.to_uppercase(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, remaining) =
        count_notifications(app.clone(), Some("ApiKey test-key"), "seen-user", "[{}]").await;
    assert_eq!(remaining["data"][0]["count"], 1);
    let (status, _) = delete_notification(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        &message_id,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = bulk_notifications(
        app.clone(),
        Some("ApiKey test-key"),
        "seen-user",
        "delete",
        json!({"tags":{"and":[{"or":["news","blue"]},{"or":["blue"]}]}}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, remaining) =
        count_notifications(app.clone(), Some("ApiKey test-key"), "seen-user", "[{}]").await;
    assert_eq!(remaining["data"][0]["count"], 0);
    worker.execute("INSERT INTO novu_rust_in_app_messages (id, environment_id, subscriber_id, workflow_identifier, transaction_id, content, payload) SELECT md5('counter-fixture-' || n)::uuid, 'env-a', 'counter', 'fixture', 'bulk-' || n, 'x', '{}'::jsonb FROM generate_series(1, 110) AS n", &[]).await.unwrap();
    Box::pin(email_workflow_contract(app.clone(), &mut worker)).await;
    Box::pin(fcm_credentials_contract(app.clone(), &mut worker)).await;
    let (_, count) = count_notifications(app, Some("ApiKey test-key"), "counter", "[{}]").await;
    assert_eq!(count["data"][0]["count"], 100);
    worker.execute("INSERT INTO novu_rust_in_app_messages (id,internal_id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,tags) VALUES ('00000000-0000-4000-8000-000000000031','64a000000000000000000131','env-a','seen-user','names','jwt-delete-1','a','{}'::jsonb,'[\"red\"]'::jsonb),('00000000-0000-4000-8000-000000000032','64a000000000000000000132','env-a','seen-user','names','jwt-delete-2','b','{}'::jsonb,'[\"blue\"]'::jsonb)", &[]).await.unwrap();
    let (status, _) = inbox_mutate(
        inbox_app.clone(),
        "DELETE",
        "/v1/inbox/notifications/64a000000000000000000131/delete",
        Some(&bearer),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = inbox_mutate(
        inbox_app.clone(),
        "POST",
        "/v1/inbox/notifications/delete",
        Some(&bearer),
        json!({"tags":["missing"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let count: i64 = worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND subscriber_id='seen-user'", &[]).await.unwrap().get(0);
    assert_eq!(count, 1);
    let (status, _) = inbox_mutate(
        inbox_app.clone(),
        "POST",
        "/v1/inbox/notifications/delete",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = inbox_mutate(
        inbox_app,
        "POST",
        "/v1/inbox/notifications/delete",
        Some(&bearer),
        json!({"tags":["blue"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let count: i64 = worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND subscriber_id='seen-user'", &[]).await.unwrap().get(0);
    assert_eq!(count, 0);
}

async fn fcm_credentials_contract(app: axum::Router, db: &mut tokio_postgres::Client) {
    db.batch_execute("INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier) VALUES ('env-a','integration-test','fcm-test'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env-a','fcm-concurrent',novu_rust_new_objectid(),'{}'::jsonb)").await.unwrap();
    let path = "/v1/subscribers/fcm-concurrent/credentials";
    let initial = json!({"providerId":"fcm","credentials":{"deviceTokens":["initial"]}});
    let (status, _) =
        inbox_mutate(app.clone(), "PUT", path, Some("ApiKey test-key"), initial).await;
    assert_eq!(status, StatusCode::OK);
    db.execute("UPDATE novu_rust_subscribers SET updated_at='2000-01-01T00:00:00Z' WHERE environment_id='env-a' AND subscriber_id='fcm-concurrent'", &[]).await.unwrap();
    let (status, same) = inbox_mutate(
        app.clone(),
        "PUT",
        path,
        Some("ApiKey test-key"),
        json!({"providerId":"fcm","credentials":{"deviceTokens":["initial","initial"]}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(same["data"]["updatedAt"], "2000-01-01T00:00:00.000Z");
    let (second, driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let second_app = novu_rust_api::router(Arc::new(second));
    let one = json!({"providerId":"fcm","credentials":{"deviceTokens":["one"]}});
    let two = json!({"providerId":"fcm","credentials":{"deviceTokens":["two"]}});
    let (a, b) = tokio::join!(
        inbox_mutate(app.clone(), "PATCH", path, Some("ApiKey test-key"), one),
        inbox_mutate(
            second_app.clone(),
            "PATCH",
            path,
            Some("ApiKey test-key"),
            two
        )
    );
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.0, StatusCode::OK);
    let tokens:Vec<String>=db.query_one("SELECT device_tokens FROM novu_rust_fcm_devices WHERE environment_id='env-a' AND subscriber_id='fcm-concurrent'",&[]).await.unwrap().get(0);
    assert_eq!(tokens.len(), 3);
    for token in ["initial", "one", "two"] {
        assert!(tokens.iter().any(|v| v == token));
    }
    db.execute("UPDATE novu_rust_subscribers SET updated_at='2000-01-01T00:00:00Z' WHERE environment_id='env-a' AND subscriber_id='fcm-concurrent'", &[]).await.unwrap();
    let (status, subset) = inbox_mutate(
        app.clone(),
        "PATCH",
        path,
        Some("ApiKey test-key"),
        json!({"providerId":"fcm","credentials":{"deviceTokens":["initial"]}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(subset["data"]["updatedAt"], "2000-01-01T00:00:00.000Z");
    let overflow: Vec<String> = (0..101).map(|i| format!("device-{i}")).collect();
    let (status, overflow_body) = inbox_mutate(
        app.clone(),
        "PUT",
        path,
        Some("ApiKey test-key"),
        json!({"providerId":"fcm","credentials":{"deviceTokens":overflow}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(overflow_body["currentCount"], 101);
    assert_eq!(overflow_body["limit"], 100);
    assert_eq!(
        overflow_body["ctx"],
        json!({"currentCount":101,"limit":100})
    );
    let after:Vec<String>=db.query_one("SELECT device_tokens FROM novu_rust_fcm_devices WHERE environment_id='env-a' AND subscriber_id='fcm-concurrent'",&[]).await.unwrap().get(0);
    assert_eq!(tokens, after);
    for _ in 0..2 {
        db.execute("UPDATE novu_rust_subscribers SET updated_at='2000-01-01T00:00:00Z' WHERE environment_id='env-a' AND subscriber_id='fcm-concurrent'", &[]).await.unwrap();
        let (status, body) = inbox_mutate(
            app.clone(),
            "DELETE",
            "/v1/subscribers/fcm-concurrent/credentials/fcm",
            Some("ApiKey test-key"),
            json!({}),
        )
        .await;
        assert_eq!((status, body), (StatusCode::NO_CONTENT, Value::Null));
        let touched: bool = db.query_one("SELECT updated_at > '2000-01-01T00:00:00Z'::timestamptz FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='fcm-concurrent'", &[]).await.unwrap().get(0);
        assert!(touched);
    }
    db.batch_execute("INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier) VALUES ('env-a','integration-second','fcm-second'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env-a','fcm-multiple-race',novu_rust_new_objectid(),'{}')").await.unwrap();
    let multi_path = "/v1/subscribers/fcm-multiple-race/credentials";
    let (first, second) = tokio::join!(
        inbox_mutate(
            app.clone(),
            "PUT",
            multi_path,
            Some("ApiKey test-key"),
            json!({"providerId":"fcm","integrationIdentifier":"fcm-test","credentials":{"deviceTokens":["token-a"]}})
        ),
        inbox_mutate(
            second_app,
            "PUT",
            multi_path,
            Some("ApiKey test-key"),
            json!({"providerId":"fcm","integrationIdentifier":"fcm-second","credentials":{"deviceTokens":["token-b"]}})
        )
    );
    assert_eq!((first.0, second.0), (StatusCode::OK, StatusCode::OK));
    let rows=db.query("SELECT integration_internal_id,device_tokens FROM novu_rust_fcm_devices WHERE environment_id='env-a' AND subscriber_id='fcm-multiple-race' ORDER BY integration_internal_id",&[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "integration-second");
    assert_eq!(rows[0].get::<_, Vec<String>>(1), vec!["token-b"]);
    assert_eq!(rows[1].get::<_, String>(0), "integration-test");
    assert_eq!(rows[1].get::<_, Vec<String>>(1), vec!["token-a"]);
    let (status, _) = inbox_mutate(
        app,
        "DELETE",
        "/v2/subscribers/fcm-concurrent",
        Some("ApiKey test-key"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(db.query_opt("SELECT 1 FROM novu_rust_fcm_devices WHERE environment_id='env-a' AND subscriber_id='fcm-concurrent'",&[]).await.unwrap().is_none());
}

async fn email_workflow_contract(app: axum::Router, worker: &mut tokio_postgres::Client) {
    // Email workflow fixture: the API queues the workflow, then the worker
    // atomically creates recipient jobs for channel-time evaluation.
    worker.batch_execute("INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html) VALUES ('env-a','email-only',true,true,'Hello {{subscriber.firstName}}','<p>{{name}}</p>'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env-a','email-disabled',novu_rust_new_objectid(),'{\"email\":\"disabled@example.test\"}'); INSERT INTO novu_rust_subscriber_preferences(environment_id,subscriber_id,channels) VALUES ('env-a','email-disabled','{\"email\":false}')").await.unwrap();
    let event = json!({"name":"email-only","to":[{"subscriberId":"email-yes","email":"yes@example.test","firstName":"Jane"},"email-disabled","email-missing"],"payload":{"name":"<Alice>"},"transactionId":"email-workflow-test"});
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), event.clone()).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = request(app.clone(), Some("ApiKey test-key"), event).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(worker)
            .await
            .unwrap()
    );
    let rows = worker.query("SELECT subscriber_id,status,email_job_id FROM novu_rust_email_step_outcomes WHERE transaction_id='email-workflow-test' ORDER BY subscriber_id", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect::<Vec<_>>(),
        vec![
            ("email-disabled".into(), "queued".into()),
            ("email-missing".into(), "queued".into()),
            ("email-yes".into(), "queued".into())
        ]
    );
    let email_job: uuid::Uuid = rows[2].get::<_, Option<uuid::Uuid>>(2).unwrap();
    let row = worker
        .query_one(
            "SELECT environment_id,kind,payload FROM novu_queue_jobs WHERE id=$1",
            &[&email_job],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "env-a");
    assert_eq!(
        row.get::<_, String>(1),
        novu_postgres_queue::sendgrid_queue::JOB_KIND
    );
    let email: Value = row.get(2);
    assert_eq!(email["to"], json!([]));
    assert_eq!(
        email["workflow_context"]["subject_template"],
        "Hello {{subscriber.firstName}}"
    );
    assert_eq!(email["transaction_id"], "email-workflow-test");
    let messages: i64 = worker.query_one("SELECT count(*) FROM novu_rust_email_messages WHERE environment_id='env-a' AND transaction_id='email-workflow-test'", &[]).await.unwrap().get(0);
    assert_eq!(messages, 0);
    let count: i64 = worker.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='email-workflow-test'", &[]).await.unwrap().get(0);
    assert_eq!(count, 0);
    let count: i64 = worker.query_one("SELECT count(*) FROM novu_rust_notifications WHERE transaction_id='email-workflow-test' AND channels=ARRAY['email']", &[]).await.unwrap().get(0);
    assert_eq!(count, 3);
    assert!(
        !novu_postgres_queue::in_app::process_one(worker)
            .await
            .unwrap()
    );
}
