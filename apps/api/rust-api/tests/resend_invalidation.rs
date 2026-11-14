//! Synthetic signed Resend callbacks and projector lock ordering.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;

async fn event(app: Router, secret: &[u8], id: &str, kind: &str, email: &str) -> StatusCode {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body = serde_json::to_vec(
        &json!({"type":kind,"data":{"email_id":email,"to":["one@example.test"]}}),
    )
    .unwrap();
    let mut signed = format!("{id}.{timestamp}.").into_bytes();
    signed.extend_from_slice(&body);
    let signature = format!(
        "v1,{}",
        STANDARD.encode(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, secret), &signed).as_ref())
    );
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/webhooks/organizations/org-a/environments/env-a/email/resend")
            .header("svix-id", id)
            .header("svix-timestamp", timestamp)
            .header("svix-signature", signature)
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn events_invalidate_only_matching_results_and_serialize_with_projector() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    for sql in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::resend_queue::MIGRATION,
        novu_postgres_queue::in_app::EMAIL_MESSAGES_MIGRATION,
        novu_postgres_queue::resend_projection::MIGRATION,
        include_str!("../migrations/0010_resend_webhooks.sql"),
        include_str!("../migrations/0012_resend_event_fingerprint.sql"),
        include_str!("../migrations/0013_resend_event_recipient.sql"),
        include_str!(
            "../../../worker/rust-queue/migrations/0102_resend_projection_invalidation.sql"
        ),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(
        "INSERT INTO novu_rust_environments VALUES ('env-a','org-a'),('env-b','org-b')",
    )
    .await
    .unwrap();
    let job = Uuid::new_v4();
    let other = Uuid::new_v4();
    for (j, env) in [(job, "env-a"), (other, "env-b")] {
        novu_postgres_queue::enqueue(
            &db,
            j,
            env,
            novu_postgres_queue::resend_queue::JOB_KIND,
            &json!({}),
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,$2,'accepted','email-123')", &[&j,&env]).await.unwrap();
        db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,resend_delivery_result) VALUES ($1,$2,'s','s','n','w','t','subject','sent',$3,'delivered')", &[&j.to_string(),&env,&j]).await.unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
            &[&j],
        )
        .await
        .unwrap();
    }
    let (webdb, driver) = tokio_postgres::connect(
        &format!("{url}?application_name=resend_invalidation_webhook"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let secret = b"test-secret-is-32-bytes-long!!";
    let config = novu_rust_api::resend_webhook::Config::new(
        "env-a".into(),
        &format!("whsec_{}", STANDARD.encode(secret)),
    )
    .unwrap();
    let app = novu_rust_api::router_with_webhook_configs(
        Arc::new(webdb),
        None,
        HashMap::new(),
        None,
        Some(config),
    );
    assert_eq!(
        event(app.clone(), secret, "evt-1", "email.bounced", "email-123").await,
        StatusCode::NO_CONTENT
    );
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    let other_result: String = db
        .query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(other_result, "delivered");
    db.execute(
        "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert_eq!(
        event(app.clone(), secret, "evt-1", "email.bounced", "email-123").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    assert_eq!(
        event(
            app.clone(),
            secret,
            "evt-unrelated",
            "email.failed",
            "different"
        )
        .await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    // The webhook must lock even when the committed value is already unknown.
    db.execute(
        "UPDATE novu_queue_jobs SET resend_delivery_result='unknown' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    let task = tokio::spawn(event(
        app.clone(),
        secret,
        "evt-race",
        "email.failed",
        "email-123",
    ));
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let mut waiting = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='resend_invalidation_webhook' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(waiting, "callback did not serialize with job lock");
    tx.execute(
        "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(task.await.unwrap(), StatusCode::NO_CONTENT);
    assert_eq!(
        observer
            .query_one(
                "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
                &[&job]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        "unknown"
    );
}
