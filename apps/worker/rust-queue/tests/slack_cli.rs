//! Uses local PostgreSQL and a fixture secret; does not contact Slack.
use std::{
    fs,
    process::{Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn worker_secret_and_attention_status() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')").await.unwrap();
    db.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::slack_queue::MIGRATION)
        .await
        .unwrap();
    let check = |env: &str| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(["check-slack", env])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert_eq!(check("a").status.code(), Some(0));
    assert_eq!(check("missing").status.code(), Some(1));
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        job,
        "a",
        novu_postgres_queue::slack_queue::JOB_KIND,
        &serde_json::json!({"channel":"C","text":"private"}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'a','unknown')", &[&job]).await.unwrap();
    let out = check("a");
    assert_eq!(out.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&out.stdout).contains("private"));
    assert_eq!(check("b").status.code(), Some(0));
    for sql in [
        novu_postgres_queue::reconciliation::MIGRATION,
        novu_postgres_queue::reconciliation::OPERATOR_GRANTS_MIGRATION,
        novu_postgres_queue::alerts::MIGRATION,
        novu_postgres_queue::alerts::RESOLUTION_MIGRATION,
        novu_postgres_queue::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
        novu_postgres_queue::slack_queue::OPERATOR_MIGRATION,
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.execute(
        "INSERT INTO novu_rust_delivery_operator_grants VALUES ('a',session_user,'alert_send')",
        &[],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('a','slack','fixture','rejected')", &[]).await.unwrap();
    let alert = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .args(["a", "300", "120", "60"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "false")
        .env("SWALLOW_ALERT_SLACK_JOBS_ENABLED", "true")
        .env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS")
        .env_remove("SWALLOW_ALERT_TWILIO_GRACE_SECONDS")
        .env_remove("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "fixture")
        .env_remove("SWALLOW_ALERT_EMAIL_TO")
        .output()
        .unwrap();
    assert_eq!(alert.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&alert.stderr).contains("Slack alert requires operator review")
    );
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let resolve = |environment: &str, job: Uuid, decision: &str| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args([
                "slack-resolve",
                environment,
                &job.to_string(),
                decision,
                "operator",
                "ticket",
            ])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert!(!resolve("a", job, "retry").status.success());
    db.execute(
        "INSERT INTO novu_rust_delivery_operator_grants VALUES ('a',session_user,'retry')",
        &[],
    )
    .await
    .unwrap();
    assert!(!resolve("a", job, "retry").status.success());
    db.execute("INSERT INTO novu_rust_delivery_operator_grants VALUES ('a',session_user,'slack_retry'),('a',session_user,'slack_accepted')", &[]).await.unwrap();
    db.batch_execute(novu_postgres_queue::slack_queue::OPERATOR_MIGRATION)
        .await
        .unwrap();
    assert!(
        db.query_one(
            "SELECT public.novu_rust_lock_operator_grant('a','retry')",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    assert!(!resolve("b", job, "retry").status.success());
    assert!(resolve("a", job, "retry").status.success());
    assert_eq!(check("a").status.code(), Some(0));
    let row=db.query_one("SELECT previous_state,decision,database_session_user FROM novu_rust_slack_resolutions WHERE job_id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "retry");
    assert_eq!(
        row.get::<_, String>(2),
        db.query_one("SELECT session_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0)
    );
    let accepted = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        accepted,
        "a",
        novu_postgres_queue::slack_queue::JOB_KIND,
        &serde_json::json!({"channel":"C","text":"private"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&accepted],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'a','unknown')", &[&accepted]).await.unwrap();
    assert!(resolve("a", accepted, "accepted").status.success());
    let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_slack_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&accepted]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, String>(1), "accepted");
    let rejected = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        rejected,
        "a",
        novu_postgres_queue::slack_queue::JOB_KIND,
        &serde_json::json!({"channel":"C","text":"private"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&rejected],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'a','rejected')", &[&rejected]).await.unwrap();
    assert!(!resolve("a", rejected, "accepted").status.success());
    assert!(resolve("a", rejected, "retry").status.success());
    let fixture = Fixture(std::env::temp_dir().join(format!("swallow-slack-{}", Uuid::new_v4())));
    fs::write(&fixture.0, "fixture-token\n").unwrap();
    let command = || {
        let mut c = Command::new(env!("CARGO_BIN_EXE_slack-worker"));
        c.env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SLACK_ENVIRONMENT_ID", "b")
            .env_remove("NOVU_SLACK_BOT_TOKEN")
            .env("NOVU_SLACK_BOT_TOKEN_FILE", &fixture.0)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        c
    };
    let mut child = command().spawn().unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let alive = child.try_wait().unwrap().is_none();
    let _ = child.kill();
    let output = child.wait_with_output().unwrap();
    assert!(alive);
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-token"));
    let output = command()
        .env("NOVU_SLACK_BOT_TOKEN", "fixture-token")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let log = String::from_utf8_lossy(&output.stderr);
    assert!(!log.contains("fixture-token") && !log.contains(fixture.0.to_str().unwrap()));
}
