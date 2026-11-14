//! Mock Resend accepts a tagged request but closes before returning an ID.
//! A separately signed webhook is delivered before or after the lost response.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use novu_postgres_queue as q;
use ring::hmac;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot};
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;

async fn callback(
    app: axum::Router,
    secret: &[u8],
    event_id: &str,
    tag: &str,
    email_id: &str,
) -> StatusCode {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body=json!({"type":"email.delivered","data":{"email_id":email_id,"to":["receiver@example.test"],"tags":{"swallow_dispatch":tag}}}).to_string();
    let mut data = format!("{event_id}.{timestamp}.").into_bytes();
    data.extend_from_slice(body.as_bytes());
    let signature = format!(
        "v1,{}",
        STANDARD.encode(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, secret), &data).as_ref())
    );
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/webhooks/organizations/org/environments/env/email/resend")
            .header("svix-id", event_id)
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
#[ignore = "requires disposable PostgreSQL and loopback"]
async fn lost_provider_response_is_recovered_from_signed_tag_before_or_after_response() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES('env','org')").await.unwrap();
    for sql in [
        q::MIGRATION,
        q::resend_queue::MIGRATION,
        q::resend_queue::RECIPIENT_MIGRATION,
        q::in_app::EMAIL_MESSAGES_MIGRATION,
        q::in_app::EMAIL_PROVIDER_RESULT_MIGRATION,
        "ALTER TABLE novu_rust_email_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'",
        q::resend_projection::MIGRATION,
        include_str!("../migrations/0010_resend_webhooks.sql"),
        include_str!("../migrations/0012_resend_event_fingerprint.sql"),
        include_str!("../migrations/0013_resend_event_recipient.sql"),
        include_str!("../migrations/0047_resend_event_correlation.sql"),
        q::reconciliation::MIGRATION,
        q::reconciliation::RETRY_DISPATCH_MIGRATION,
        "ALTER TABLE novu_rust_delivery_reconciliations ADD COLUMN retry_provider_message_id text",
        include_str!("../../../worker/rust-queue/migrations/0134_resend_correlation.sql"),
        include_str!("../../../worker/rust-queue/migrations/0137_resend_auto_correlation.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let (webdb, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let secret = b"test-secret-is-32-bytes-long!!";
    let key = format!("whsec_{}", STANDARD.encode(secret));
    let config = novu_rust_api::resend_webhook::Config::new("env".into(), &key)
        .unwrap()
        .with_correlation_capture(true);
    let app = novu_rust_api::router_with_webhook_configs(
        Arc::new(webdb),
        None,
        HashMap::new(),
        None,
        Some(config),
    );
    for early in [true, false] {
        let job = Uuid::new_v4();
        let message = job.to_string();
        q::enqueue(&db,job,"env",q::resend_queue::JOB_KIND,&json!({"to":["receiver@example.test"],"subject":"test","html":"Hi","message_id":message})).await.unwrap();
        db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id) VALUES ($1,'env','s','s','n','w','t','subject','sent',$2)", &[&message,&job]).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let (tag_tx, tag_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let mock = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(pos) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..pos]);
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + len {
                        let body: Value =
                            serde_json::from_slice(&bytes[pos + 4..pos + 4 + len]).unwrap();
                        assert_eq!(body["tags"][0]["name"], "swallow_dispatch");
                        assert_eq!(body["to"][0], "receiver@example.test");
                        tag_tx
                            .send(body["tags"][0]["value"].as_str().unwrap().to_owned())
                            .unwrap();
                        break;
                    }
                }
            }
            finish_rx.await.unwrap();
            // Drop the connection: the request was received but its result is unknown.
        });
        let provider = q::resend::Resend::test_endpoint(
            q::resend::Config {
                api_key: "fixture".into(),
                from: "sender@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        let (mut dispatch, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move {
            driver.await.unwrap();
        });
        let task = tokio::spawn(async move {
            q::resend_queue::process_one_with_correlation_guard(
                &mut dispatch,
                "env",
                &provider,
                false,
                q::email_step_policy::Policy::Snapshot,
                true,
                false,
            )
            .await
            .unwrap()
        });
        let tag = tokio::time::timeout(std::time::Duration::from_secs(5), tag_rx)
            .await
            .unwrap()
            .unwrap();
        let tag_id = Uuid::parse_str(&tag).unwrap();
        let row=db.query_one("SELECT state,correlation_id,provider_message_id FROM novu_rust_resend_attempts WHERE job_id=$1", &[&job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "dispatching");
        assert_eq!(row.get::<_, Uuid>(1), tag_id);
        assert!(row.get::<_, Option<String>>(2).is_none());
        let event_id = format!("evt_{}", job.simple());
        let provider_id = format!("email-{job}");
        if early {
            assert_eq!(
                callback(app.clone(), secret, &event_id, &tag, &provider_id).await,
                StatusCode::NO_CONTENT
            );
        }
        finish_tx.send(()).unwrap();
        assert!(task.await.unwrap());
        mock.await.unwrap();
        let row=db.query_one("SELECT j.status,a.state,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert!(row.get::<_, Option<String>>(2).is_none());
        if !early {
            assert_eq!(
                callback(app.clone(), secret, &event_id, &tag, &provider_id).await,
                StatusCode::NO_CONTENT
            );
        }
        assert!(
            q::resend_projection::project_with_auto_correlation(&mut db, "env", job, true)
                .await
                .unwrap()
        );
        let row=db.query_one("SELECT j.status,j.resend_delivery_result,a.state,a.provider_message_id,m.delivery_state,m.resend_delivery_result,(SELECT count(*) FROM novu_rust_resend_auto_correlations c WHERE c.job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "delivered");
        assert_eq!(row.get::<_, String>(2), "accepted");
        assert_eq!(row.get::<_, String>(3), provider_id);
        assert_eq!(row.get::<_, String>(4), "accepted");
        assert_eq!(row.get::<_, String>(5), "delivered");
        assert_eq!(row.get::<_, i64>(6), 1);
    }
}
