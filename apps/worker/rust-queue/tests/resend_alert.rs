//! Uses a fresh disposable PostgreSQL database.
use novu_postgres_queue as queue;
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn resend_check_exits_for_unmatched_and_missing_evidence() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        queue::resend_queue::MIGRATION,
        queue::resend_queue::RECIPIENT_MIGRATION,
        queue::in_app::EMAIL_MESSAGES_MIGRATION,
        queue::resend_projection::MIGRATION,
        queue::resend_projection::HEARTBEAT_MIGRATION,
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
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '10 minutes' WHERE id=$1", &[&job]).await.unwrap();
    let no_heartbeat = queue::reconciliation::resend_health(&db, "env", 300, 120)
        .await
        .unwrap();
    assert_eq!(no_heartbeat["projectorStale"], true);
    assert_eq!(no_heartbeat["overdueUndelivered"], 1);
    assert!(
        queue::reconciliation::resend_health(&db, "other", 300, 120)
            .await
            .is_err()
    );
    assert!(
        queue::reconciliation::resend_health(&db, "env", 0, 120)
            .await
            .is_err()
    );
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'env','accepted','id')", &[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id) VALUES ('message','env','s','s','n','w','t','subject','sent',$1)", &[&job]).await.unwrap();
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
    assert!(
        queue::resend_projection::project(&mut db, "env", job)
            .await
            .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    assert!(
        !queue::resend_projection::project(&mut db, "other", job)
            .await
            .unwrap()
    );
    let (mut second, second_driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { second_driver.await.unwrap() });
    let lock = second.transaction().await.unwrap();
    lock.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    assert!(
        !queue::resend_projection::project(&mut db, "env", job)
            .await
            .unwrap()
    );
    lock.rollback().await.unwrap();
    assert_eq!(
        queue::resend_projection::page(&db, "env", None)
            .await
            .unwrap(),
        vec![job]
    );
    assert!(
        queue::resend_projection::page(&db, "other", None)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash) VALUES ('env','event1','id','email.delivered','hash')", &[]).await.unwrap();
    let healthy = check();
    assert_eq!(healthy.status.code(), Some(0));
    let mut worker = std::process::Command::new(env!("CARGO_BIN_EXE_resend-projection-worker"))
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
        .spawn()
        .unwrap();
    let mut observed = false;
    for _ in 0..50 {
        let row = db
            .query_one(
                "SELECT resend_delivery_result,EXISTS(SELECT 1 FROM novu_rust_resend_projection_heartbeat WHERE environment_id='env') FROM novu_queue_jobs WHERE id=$1",
                &[&job],
            )
            .await
            .unwrap();
        let result: Option<String> = row.get(0);
        if result.as_deref() == Some("delivered") && row.get::<_, bool>(1) {
            observed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let _ = worker.kill();
    let _ = worker.wait();
    assert!(observed, "projection worker did not update the job");
    let row = db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result,j.status,m.status FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "delivered");
    assert_eq!(row.get::<_, String>(1), "delivered");
    assert_eq!(row.get::<_, String>(2), "pending");
    assert_eq!(row.get::<_, String>(3), "sent");
    db.execute("INSERT INTO novu_rust_resend_projection_heartbeat VALUES ('env',clock_timestamp()) ON CONFLICT(environment_id) DO UPDATE SET last_completed_at=EXCLUDED.last_completed_at", &[]).await.unwrap();
    let check_environment = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(["check-resend", "env", "300", "120"])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert_eq!(check_environment().status.code(), Some(0));
    let metrics = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(["check-resend-metrics", "env", "300", "120"])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let healthy_metrics = metrics();
    assert_eq!(healthy_metrics.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&healthy_metrics.stdout)
            .contains("swallow_resend_projector_stale{environment=\"env\"} 0")
    );
    db.execute("UPDATE novu_rust_resend_projection_heartbeat SET last_completed_at=clock_timestamp()-interval '5 minutes' WHERE environment_id='env'", &[]).await.unwrap();
    assert_eq!(check_environment().status.code(), Some(2));
    let stale_metrics = metrics();
    assert_eq!(stale_metrics.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&stale_metrics.stdout)
            .contains("swallow_resend_projector_stale{environment=\"env\"} 1")
    );
    db.execute("UPDATE novu_rust_resend_projection_heartbeat SET last_completed_at=clock_timestamp() WHERE environment_id='env'", &[]).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&healthy.stdout).unwrap()["needsAttention"],
        false
    );
    db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ('env','event2','id','email.bounced')", &[]).await.unwrap();
    let alert = check();
    assert_eq!(alert.status.code(), Some(2));
    let mut restarted = std::process::Command::new(env!("CARGO_BIN_EXE_resend-projection-worker"))
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
        .spawn()
        .unwrap();
    let mut observed = false;
    for _ in 0..50 {
        let result: Option<String> = db
            .query_one(
                "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
                &[&job],
            )
            .await
            .unwrap()
            .get(0);
        if result.as_deref() == Some("unknown") {
            observed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let _ = restarted.kill();
    let _ = restarted.wait();
    assert!(
        observed,
        "restarted projection worker did not refresh the job"
    );
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    assert_eq!(check_environment().status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&metrics().stdout)
            .contains("swallow_resend_overdue_undelivered{environment=\"env\"} 1")
    );
    let special = "env\"\\\nname";
    db.execute(
        "INSERT INTO novu_rust_environments(id,organization_id) VALUES ($1,'org')",
        &[&special],
    )
    .await
    .unwrap();
    let escaped = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["check-resend-metrics", special, "300", "120"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert_eq!(escaped.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&escaped.stdout).contains("environment=\"env\\\"\\\\\\nname\"")
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&alert.stdout).unwrap()["evidence"]["unmatchedEvents"],
        1
    );
}
