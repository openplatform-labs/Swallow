//! Uses a fresh disposable PostgreSQL database.
use novu_postgres_queue as queue;
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn resend_check_exits_for_unmatched_and_missing_evidence() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        queue::resend_queue::MIGRATION,
        queue::resend_queue::RECIPIENT_MIGRATION,
        include_str!("../../../api/rust-api/migrations/0010_resend_webhooks.sql"),
        include_str!("../../../api/rust-api/migrations/0012_resend_event_fingerprint.sql"),
        include_str!("../../../api/rust-api/migrations/0013_resend_event_recipient.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let job = Uuid::new_v4();
    queue::enqueue(&db, job, "env", queue::resend_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'env','accepted','id')", &[&job]).await.unwrap();
    db.execute(
        "INSERT INTO novu_rust_resend_recipients VALUES ($1,'env','to',0,'hash')",
        &[&job],
    )
    .await
    .unwrap();
    let check = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(["resend-check", "env", &job.to_string()])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert_eq!(check().status.code(), Some(2));
    db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash) VALUES ('env','event1','id','email.delivered','hash')", &[]).await.unwrap();
    let healthy = check();
    assert_eq!(healthy.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&healthy.stdout).unwrap()["needsAttention"],
        false
    );
    db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ('env','event2','id','email.bounced')", &[]).await.unwrap();
    let alert = check();
    assert_eq!(alert.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&alert.stdout).unwrap()["evidence"]["unmatchedEvents"],
        1
    );
}
