//! Signed Twilio callbacks, event association and current-result invalidation.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use serde_json::json;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;
const PATH: &str = "/webhooks/organizations/org-a/environments/env-a/sms/twilio";
const URL: &str =
    "https://events.example.test/webhooks/organizations/org-a/environments/env-a/sms/twilio";
const ACCOUNT: &str = "AC11111111111111111111111111111111";
const SID: &str = "SM22222222222222222222222222222222";
async fn callback(app: Router, suffix: &str, status: &str) -> StatusCode {
    let text = format!("{URL}{suffix}AccountSid{ACCOUNT}MessageSid{SID}MessageStatus{status}");
    let sig = STANDARD.encode(
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, b"test-secret"),
            text.as_bytes(),
        )
        .as_ref(),
    );
    let body = format!("AccountSid={ACCOUNT}&MessageSid={SID}&MessageStatus={status}");
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("{PATH}{suffix}"))
            .header("content-type", "application/x-www-form-urlencoded")
            .header("x-twilio-signature", sig)
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn current_events_invalidate_and_linked_duplicates_are_idempotent() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    for sql in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::twilio_queue::MIGRATION,
        novu_postgres_queue::twilio_queue::DISPATCH_ID_MIGRATION,
        novu_postgres_queue::in_app::SMS_MESSAGES_MIGRATION,
        novu_postgres_queue::twilio_projection::MIGRATION,
        include_str!("../migrations/0005_twilio_webhooks.sql"),
        include_str!("../migrations/0006_twilio_webhook_job_link.sql"),
        include_str!("../migrations/0007_twilio_dispatch_identity.sql"),
        include_str!(
            "../../../worker/rust-queue/migrations/0103_twilio_projection_invalidation.sql"
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
    let dispatch = Uuid::new_v4();
    let old = Uuid::new_v4();
    for (j, env) in [(job, "env-a"), (other, "env-b")] {
        novu_postgres_queue::enqueue(
            &db,
            j,
            env,
            novu_postgres_queue::twilio_queue::JOB_KIND,
            &json!({}),
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status,sms_job_id,twilio_delivery_result) VALUES ($1,$2,'s','s','n','w','t','sent',$3,'delivered')", &[&j.to_string(),&env,&j]).await.unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET twilio_delivery_result='delivered' WHERE id=$1",
            &[&j],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES ($1,$3,'env-a'),($2,$3,'env-a')", &[&dispatch,&old,&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,provider_message_id,dispatch_id) VALUES ($1,'env-a','accepted',$2,$3)", &[&job,&SID,&dispatch]).await.unwrap();
    let (webdb, driver) = tokio_postgres::connect(
        &format!("{url}?application_name=twilio_invalidation_webhook"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let config = novu_rust_api::twilio_webhook::Config::new(
        "env-a".into(),
        ACCOUNT.into(),
        URL.into(),
        b"test-secret".to_vec(),
    )
    .unwrap();
    let app = novu_rust_api::router_with_twilio_config(
        Arc::new(webdb),
        None,
        HashMap::new(),
        Some(config),
    );
    let path = format!("/jobs/{job}/attempts/{dispatch}");
    // A previously unbound event can be promoted by a verified callback.
    assert_eq!(
        callback(app.clone(), "", "delivered").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT twilio_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    assert_eq!(
        callback(app.clone(), &path, "delivered").await,
        StatusCode::NO_CONTENT
    );
    let row=db.query_one("SELECT j.twilio_delivery_result,m.twilio_delivery_result FROM novu_queue_jobs j JOIN novu_rust_sms_messages m ON m.sms_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    db.execute(
        "UPDATE novu_queue_jobs SET twilio_delivery_result='delivered' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert_eq!(
        callback(app.clone(), &path, "delivered").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT twilio_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    assert_eq!(
        callback(
            app.clone(),
            &format!("/jobs/{job}/attempts/{old}"),
            "failed"
        )
        .await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT twilio_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    assert_eq!(
        callback(app.clone(), &path, "sent").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT twilio_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    assert_eq!(
        db.query_one(
            "SELECT twilio_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&other]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    let task = tokio::spawn(async move { callback(app, &path, "queued").await });
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let mut waiting = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='twilio_invalidation_webhook' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(waiting);
    tx.execute(
        "UPDATE novu_queue_jobs SET twilio_delivery_result='delivered' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(task.await.unwrap(), StatusCode::NO_CONTENT);
    assert_eq!(
        observer
            .query_one(
                "SELECT twilio_delivery_result FROM novu_queue_jobs WHERE id=$1",
                &[&job]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        "unknown"
    );
}
