//! Signed form webhook tests against a disposable PostgreSQL database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use http_body_util::BodyExt;
use novu_rust_api::twilio_webhook::Config;
use ring::hmac;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::NoTls;
use tower::ServiceExt;

const PATH: &str = "/webhooks/organizations/org-a/environments/env-a/sms/twilio";
const URL: &str =
    "https://events.example.test/webhooks/organizations/org-a/environments/env-a/sms/twilio";
const BODY: &str = "AccountSid=AC11111111111111111111111111111111&MessageSid=SM22222222222222222222222222222222&MessageStatus=delivered&Extra=z&Extra=a&Extra=a&To=%2B15555555555";
// Generated independently with the pinned Twilio Node SDK 4.20.0.
const SIGNATURE: &str = "KP5H8XYItiRooUTbGchLGWDP9H4=";

async fn post(
    router: axum::Router,
    path: &str,
    body: &str,
    signature: &str,
    content_type: &str,
) -> StatusCode {
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", content_type)
                .header("x-twilio-signature", signature)
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let _ = response.into_body().collect().await.unwrap();
    status
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn signed_status_events_are_scoped_deduplicated_and_private() {
    let (db, connection) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    db.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        .await
        .unwrap();
    for migration in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::in_app::SMS_MESSAGES_MIGRATION,
        include_str!("../migrations/0005_twilio_webhooks.sql"),
        include_str!("../migrations/0006_twilio_webhook_job_link.sql"),
        include_str!("../../../worker/rust-queue/migrations/0041_experimental_twilio_attempts.sql"),
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    db.batch_execute(
        "INSERT INTO novu_rust_environments VALUES ('env-a','org-a'),('env-b','org-b')",
    )
    .await
    .unwrap();
    let db = Arc::new(db);
    let config = Config::new(
        "env-a".into(),
        format!("AC{}", "1".repeat(32)),
        URL.into(),
        b"test-secret".to_vec(),
    )
    .unwrap();
    let router =
        novu_rust_api::router_with_twilio_config(db.clone(), None, HashMap::new(), Some(config));
    let form = "application/x-www-form-urlencoded";
    assert_eq!(
        post(router.clone(), PATH, BODY, SIGNATURE, form).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        post(router.clone(), PATH, BODY, SIGNATURE, form).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        post(
            router.clone(),
            PATH,
            &BODY.replace("delivered", "failed"),
            SIGNATURE,
            form
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(router.clone(), PATH, BODY, "bad", form).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(
            router.clone(),
            "/webhooks/organizations/org-b/environments/env-b/sms/twilio",
            BODY,
            SIGNATURE,
            form
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(
            router.clone(),
            &format!("{PATH}?ignored=1"),
            BODY,
            SIGNATURE,
            form
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(router.clone(), PATH, BODY, SIGNATURE, "application/json").await,
        StatusCode::BAD_REQUEST
    );
    let row=db.query_one("SELECT count(*),min(status),min(message_sid),min(error_code) FROM novu_rust_twilio_delivery_events",&[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, String>(1), "delivered");
    assert_eq!(row.get::<_, String>(2), format!("SM{}", "2".repeat(32)));
    assert_eq!(row.get::<_, String>(3), "");
    // A callback may arrive before the worker records the provider receipt.
    // Evidence remains joinable after the SID is stored, without changing state.
    db.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status,identifier) VALUES ('sms-1','env-a','s','s','n','w','t','sent',$1)", &[&format!("SM{}","2".repeat(32))]).await.unwrap();
    let row=db.query_one("SELECT count(*),min(m.status) FROM novu_rust_sms_messages m JOIN novu_rust_twilio_delivery_events e ON e.environment_id=m.environment_id AND e.message_sid=m.identifier", &[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, String>(1), "sent");
    let stored: String = db
        .query_one(
            "SELECT row_to_json(e)::text FROM novu_rust_twilio_delivery_events e",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        !stored.contains("15555555555")
            && !stored.contains("test-secret")
            && !stored.contains("Extra")
    );
    // A signed unknown account must not be admitted even with a valid MAC.
    let bad_body = BODY.replace(
        "AC11111111111111111111111111111111",
        "AC33333333333333333333333333333333",
    );
    let signed = format!(
        "{URL}AccountSidAC{}ExtraaExtrazMessageSidSM{}MessageStatusdeliveredTo+15555555555",
        "3".repeat(32),
        "2".repeat(32)
    );
    let sig = STANDARD.encode(
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, b"test-secret"),
            signed.as_bytes(),
        )
        .as_ref(),
    );
    assert_eq!(
        post(router, PATH, &bad_body, &sig, form).await,
        StatusCode::BAD_REQUEST
    );
    let count: i64 = db
        .query_one("SELECT count(*) FROM novu_rust_twilio_delivery_events", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn signed_job_callbacks_bind_evidence_without_changing_attempt_state() {
    let (db, connection) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    db.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        .await
        .unwrap();
    for migration in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        include_str!("../migrations/0005_twilio_webhooks.sql"),
        include_str!("../migrations/0006_twilio_webhook_job_link.sql"),
        include_str!("../../../worker/rust-queue/migrations/0041_experimental_twilio_attempts.sql"),
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    db.batch_execute(
        "INSERT INTO novu_rust_environments VALUES ('env-a','org-a'),('env-b','org-b')",
    )
    .await
    .unwrap();
    let job = uuid::Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let other = uuid::Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    for id in [job, other] {
        novu_postgres_queue::enqueue(
            &db,
            id,
            "env-a",
            "experimental_twilio_sms",
            &serde_json::json!({}),
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state) VALUES ($1,'env-a','dispatching')", &[&id]).await.unwrap();
    }
    let db = Arc::new(db);
    let config = Config::new(
        "env-a".into(),
        format!("AC{}", "1".repeat(32)),
        URL.into(),
        b"test-secret".to_vec(),
    )
    .unwrap();
    let router =
        novu_rust_api::router_with_twilio_config(db.clone(), None, HashMap::new(), Some(config));
    let form = "application/x-www-form-urlencoded";
    // An earlier unlinked callback can be promoted, but never reassigned to another job.
    assert_eq!(
        post(router.clone(), PATH, BODY, SIGNATURE, form).await,
        StatusCode::NO_CONTENT
    );
    let signature_for = |id: uuid::Uuid| {
        let input = format!(
            "{URL}/jobs/{id}AccountSidAC{}ExtraaExtrazMessageSidSM{}MessageStatusdeliveredTo+15555555555",
            "1".repeat(32),
            "2".repeat(32)
        );
        STANDARD.encode(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, b"test-secret"),
                input.as_bytes(),
            )
            .as_ref(),
        )
    };
    let path = format!("{PATH}/jobs/{job}");
    let sig = signature_for(job);
    // Independently generated by the pinned Twilio Node SDK 4.20.0.
    assert_eq!(sig, "kH9uQ9DJg+3i6uMF7TqprvFuPg4=");
    assert_eq!(
        post(router.clone(), &path, BODY, SIGNATURE, form).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(router.clone(), &path, BODY, &sig, form).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        post(router.clone(), &path, BODY, &sig, form).await,
        StatusCode::NO_CONTENT
    );
    let other_path = format!("{PATH}/jobs/{other}");
    assert_eq!(
        post(router.clone(), &other_path, BODY, &sig, form).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(
            router.clone(),
            &other_path,
            BODY,
            &signature_for(other),
            form
        )
        .await,
        StatusCode::NOT_FOUND
    );
    let missing = uuid::Uuid::new_v4();
    assert_eq!(
        post(
            router,
            &format!("{PATH}/jobs/{missing}"),
            BODY,
            &signature_for(missing),
            form
        )
        .await,
        StatusCode::NOT_FOUND
    );
    let row = db.query_one("SELECT job_id,(SELECT state FROM novu_rust_twilio_attempts WHERE job_id=$1) FROM novu_rust_twilio_delivery_events", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, uuid::Uuid>(0), job);
    assert_eq!(row.get::<_, String>(1), "dispatching");
}
