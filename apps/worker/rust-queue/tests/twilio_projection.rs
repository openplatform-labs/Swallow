//! Disposable PostgreSQL; events are synthetic, not live Twilio callbacks.
use novu_postgres_queue::{self as queue, twilio_projection as projection};
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn get(db: &tokio_postgres::Client, job: Uuid) -> tokio_postgres::Row {
    db.query_one("SELECT j.twilio_delivery_result,m.twilio_delivery_result FROM novu_queue_jobs j JOIN novu_rust_sms_messages m ON m.sms_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap()
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn projects_only_matching_current_attempt_and_recovers_on_rescan() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        queue::twilio_queue::MIGRATION,
        queue::twilio_queue::DISPATCH_ID_MIGRATION,
        queue::in_app::SMS_MESSAGES_MIGRATION,
        projection::MIGRATION,
        include_str!("../../../api/rust-api/migrations/0005_twilio_webhooks.sql"),
        include_str!("../../../api/rust-api/migrations/0006_twilio_webhook_job_link.sql"),
        include_str!("../../../api/rust-api/migrations/0007_twilio_dispatch_identity.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(projection::MIGRATION).await.unwrap();
    let job = Uuid::new_v4();
    let dispatch = Uuid::new_v4();
    queue::enqueue(&db, job, "a", queue::twilio_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,provider_message_id,dispatch_id) VALUES ($1,'a','accepted','SMmatch',$2)", &[&job,&dispatch]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status,sms_job_id) VALUES ('m','a','s','s','n','w','t','sent',$1)", &[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('a','ACtest','SMmatch','delivered',$1,$2)", &[&job,&dispatch]).await.unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    let row = get(&db, job).await;
    assert_eq!(row.get::<_, String>(0), "delivered");
    assert_eq!(row.get::<_, String>(1), "delivered");
    let duplicate = Uuid::new_v4();
    queue::enqueue(
        &db,
        duplicate,
        "a",
        queue::twilio_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'a','accepted','SMmatch')", &[&duplicate]).await.unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "conflicting");
    db.execute("DELETE FROM novu_queue_jobs WHERE id=$1", &[&duplicate])
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('a','ACtest','SMold','failed',$1,$2)", &[&job,&Uuid::new_v4()]).await.unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "delivered");
    assert!(!projection::project(&mut db, "b", job).await.unwrap());
    db.execute(
        "UPDATE novu_rust_twilio_attempts SET provider_message_id='SMother' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "conflicting");
    db.execute(
        "UPDATE novu_rust_twilio_attempts SET provider_message_id='SMmatch' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('a','ACtest','SMmatch','failed',$1,$2)", &[&job,&dispatch]).await.unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "conflicting");
    db.execute(
        "DELETE FROM novu_rust_twilio_delivery_events WHERE job_id=$1 AND status='failed'",
        &[&job],
    )
    .await
    .unwrap();
    for (status, expected) in [
        ("undelivered", "failed"),
        ("sent", "pending"),
        ("future_status", "unknown"),
        ("delivered", "delivered"),
    ] {
        db.execute("UPDATE novu_rust_twilio_delivery_events SET status=$3 WHERE job_id=$1 AND dispatch_id=$2", &[&job,&dispatch,&status]).await.unwrap();
        assert!(projection::project(&mut db, "a", job).await.unwrap());
        assert_eq!(get(&db, job).await.get::<_, String>(0), expected);
    }
    db.execute(
        "UPDATE novu_rust_twilio_attempts SET state='unknown' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "unknown");
    db.execute(
        "UPDATE novu_rust_twilio_attempts SET state='accepted' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let (mut other, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let lock = other.transaction().await.unwrap();
    lock.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    assert!(!projection::project(&mut db, "a", job).await.unwrap());
    lock.rollback().await.unwrap();
    let worker = std::process::Command::new(env!("CARGO_BIN_EXE_twilio-projection-worker"))
        .env("DATABASE_URL", &url)
        .env("NOVU_TWILIO_ENVIRONMENT_ID", "a")
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let _worker = Child(worker);
    let mut completed = false;
    for _ in 0..100 {
        let row=db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_twilio_projection_heartbeat WHERE environment_id='a'),twilio_delivery_result FROM novu_queue_jobs WHERE id=$1", &[&job]).await.unwrap();
        if row.get::<_, bool>(0) && row.get::<_, Option<String>>(1).as_deref() == Some("delivered")
        {
            completed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        completed,
        "worker did not complete projection and heartbeat"
    );
    drop(_worker);
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '10 minutes' WHERE id=$1", &[&job]).await.unwrap();
    let health = queue::reconciliation::twilio_health(&db, "a", 300, 120)
        .await
        .unwrap();
    assert_eq!(health["overdueUndelivered"], 0);
    assert_eq!(health["eligibleJobs"], 1);
    assert_eq!(health["projectorStale"], false);
    assert!(
        queue::reconciliation::twilio_health(&db, "missing", 300, 120)
            .await
            .is_err()
    );
    assert!(
        queue::reconciliation::twilio_health(&db, "a", 0, 120)
            .await
            .is_err()
    );
    assert_eq!(
        queue::reconciliation::twilio_health(&db, "b", 300, 120)
            .await
            .unwrap()["projectorStale"],
        true
    );
    let check = |command: &str, environment: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args([command, environment, "300", "120"])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert_eq!(check("check-twilio", "a").status.code(), Some(0));
    db.execute(
        "UPDATE novu_queue_jobs SET twilio_delivery_result='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert_eq!(check("check-twilio", "a").status.code(), Some(2));
    let metrics = check("check-twilio-metrics", "a");
    assert_eq!(metrics.status.code(), Some(0));
    assert!(
        String::from_utf8(metrics.stdout)
            .unwrap()
            .contains("swallow_twilio_overdue_undelivered{environment=\"a\"} 1")
    );
    assert_eq!(check("check-twilio", "missing").status.code(), Some(1));
    db.execute(
        "UPDATE novu_queue_jobs SET status='cancelled' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert_eq!(
        queue::reconciliation::twilio_health(&db, "a", 300, 120)
            .await
            .unwrap()["overdueUndelivered"],
        0
    );
    db.execute(
        "UPDATE novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    for sql in [
        queue::reconciliation::MIGRATION,
        queue::reconciliation::OPERATOR_GRANTS_MIGRATION,
        queue::alerts::MIGRATION,
        queue::alerts::RESOLUTION_MIGRATION,
        queue::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.execute(
        "INSERT INTO novu_rust_delivery_operator_grants VALUES ('a',session_user,'alert_send')",
        &[],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('a','slack','check','rejected')", &[]).await.unwrap();
    let alert = |resend: &str, twilio_heartbeat: Option<&str>| {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"));
        cmd.args(["a", "300", "120", "60"])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", "check")
            .env_remove("SWALLOW_ALERT_EMAIL_TO")
            .env("SWALLOW_ALERT_RESEND_ENABLED", resend)
            .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "false")
            .env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS")
            .env("SWALLOW_ALERT_TWILIO_GRACE_SECONDS", "300");
        if let Some(value) = twilio_heartbeat {
            cmd.env("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS", value);
        } else {
            cmd.env_remove("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS");
        }
        cmd.output().unwrap()
    };
    assert_eq!(alert("false", Some("120")).status.code(), Some(2));
    assert_eq!(alert("false", None).status.code(), Some(1));
    assert_eq!(alert("false", Some("0")).status.code(), Some(1));
    // Resend tables are absent; Twilio still reaches the blocked destination.
    let partial = alert("true", Some("120"));
    assert_eq!(partial.status.code(), Some(1));
    let log = String::from_utf8(partial.stderr).unwrap();
    assert!(
        log.contains("Resend health check failed")
            && log.contains("Slack alert requires operator review")
    );
    db.batch_execute("DROP TABLE novu_rust_twilio_projection_heartbeat")
        .await
        .unwrap();
    let failed = alert("false", Some("120"));
    assert_eq!(failed.status.code(), Some(1));
    let log = String::from_utf8(failed.stderr).unwrap();
    assert!(
        log.contains("Twilio health check failed")
            && log.contains("Slack alert requires operator review")
    );
}
