//! Uses and resets a disposable PostgreSQL database.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::NoTls;
use tower::ServiceExt;

fn signature(secret: &[u8], id: &str, timestamp: &str, body: &[u8]) -> String {
    let mut data = format!("{id}.{timestamp}.").into_bytes();
    data.extend_from_slice(body);
    format!(
        "v1,{}",
        STANDARD.encode(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, secret), &data).as_ref())
    )
}
async fn call(
    router: Router,
    path: &str,
    id: &str,
    timestamp: &str,
    signature: &str,
    body: &[u8],
) -> StatusCode {
    router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("svix-id", id)
                .header("svix-timestamp", timestamp)
                .header("svix-signature", signature)
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn signed_events_are_scoped_deduplicated_and_private() {
    let (db, connection) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        .await
        .unwrap();
    db.batch_execute(novu_rust_api::MIGRATION).await.unwrap();
    db.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::resend_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(include_str!("../migrations/0010_resend_webhooks.sql"))
        .await
        .unwrap();
    db.batch_execute(include_str!(
        "../migrations/0012_resend_event_fingerprint.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!(
        "../migrations/0012_resend_event_fingerprint.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!("../migrations/0010_resend_webhooks.sql"))
        .await
        .unwrap();
    db.batch_execute(
        "INSERT INTO novu_rust_environments VALUES ('env-a','org-a'),('env-b','org-b')",
    )
    .await
    .unwrap();
    let raw_secret = b"test-secret-is-32-bytes-long!!";
    let key = format!("whsec_{}", STANDARD.encode(raw_secret));
    let config = novu_rust_api::resend_webhook::Config::new("env-a".into(), &key).unwrap();
    let db = Arc::new(db);
    let app = novu_rust_api::router_with_webhook_configs(
        db.clone(),
        None,
        HashMap::new(),
        None,
        Some(config),
    );
    let path = "/webhooks/organizations/org-a/environments/env-a/email/resend";
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body=br#"{"type":"email.delivered","data":{"email_id":"email-123","to":["private@example.test"]}}"#;
    let body = std::str::from_utf8(body)
        .unwrap()
        .replace("\\\"", "\"")
        .into_bytes();
    let sig = signature(raw_secret, "msg_test", &timestamp, &body);
    assert_eq!(
        call(app.clone(), path, "msg_test", &timestamp, &sig, &body).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(app.clone(), path, "msg_test", &timestamp, &sig, &body).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(app.clone(), path, "msg_test", &timestamp, &sig, b"{}").await,
        StatusCode::UNAUTHORIZED
    );
    let changed_recipient = br#"{"type":"email.delivered","data":{"email_id":"email-123","to":["other@example.test"]}}"#;
    assert_eq!(
        call(
            app.clone(),
            path,
            "msg_test",
            &timestamp,
            &signature(raw_secret, "msg_test", &timestamp, changed_recipient),
            changed_recipient
        )
        .await,
        StatusCode::CONFLICT
    );
    let future = (timestamp.parse::<u64>().unwrap() + 301).to_string();
    assert_eq!(
        call(
            app.clone(),
            path,
            "msg_future",
            &future,
            &signature(raw_secret, "msg_future", &future, &body),
            &body
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            app.clone(),
            path,
            "msg_test",
            &timestamp,
            "v1,not-base64",
            &body
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            app.clone(),
            "/webhooks/organizations/org-b/environments/env-b/email/resend",
            "msg_test",
            &timestamp,
            &sig,
            &body
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            app.clone(),
            "/webhooks/organizations/wrong/environments/env-a/email/resend",
            "msg_test",
            &timestamp,
            &sig,
            &body
        )
        .await,
        StatusCode::CONFLICT
    );
    let stale = (timestamp.parse::<u64>().unwrap() - 301).to_string();
    assert_eq!(
        call(
            app.clone(),
            path,
            "msg_stale",
            &stale,
            &signature(raw_secret, "msg_stale", &stale, &body),
            &body
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    let conflict = br#"{"type":"email.failed","data":{"email_id":"other-email"}}"#;
    let conflict = std::str::from_utf8(conflict)
        .unwrap()
        .replace("\\\"", "\"")
        .into_bytes();
    assert_eq!(
        call(
            app.clone(),
            path,
            "msg_test",
            &timestamp,
            &signature(raw_secret, "msg_test", &timestamp, &conflict),
            &conflict
        )
        .await,
        StatusCode::CONFLICT
    );
    let row = db
        .query_one(
            "SELECT count(*),min(email_id),min(event_type) FROM novu_rust_resend_delivery_events",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, String>(1), "email-123");
    assert_eq!(row.get::<_, String>(2), "email.delivered");
    let job = uuid::Uuid::new_v4();
    novu_postgres_queue::enqueue(
        db.as_ref(),
        job,
        "env-a",
        novu_postgres_queue::resend_queue::JOB_KIND,
        &serde_json::json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state) VALUES ($1,'env-a','unknown')", &[&job]).await.unwrap();
    let evidence = novu_postgres_queue::reconciliation::resend_evidence(&db, "env-a", job)
        .await
        .unwrap();
    assert_eq!(evidence["providerIdKnown"], false);
    assert_eq!(evidence["events"], serde_json::json!({}));
    db.execute(
        "UPDATE novu_rust_resend_attempts SET provider_message_id='email-123' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let evidence = novu_postgres_queue::reconciliation::resend_evidence(&db, "env-a", job)
        .await
        .unwrap();
    assert_eq!(evidence["providerIdKnown"], true);
    assert_eq!(evidence["events"], serde_json::json!({"email.delivered":1}));
    assert!(
        novu_postgres_queue::reconciliation::resend_evidence(&db, "env-b", job)
            .await
            .is_err()
    );
}
