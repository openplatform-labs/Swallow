//! Real worker processes and SIGTERM; no external provider requests.
#![cfg(unix)]
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(url: &str, kind: &str, grace: &str) -> Worker {
    let bin = match kind {
        "resend" => env!("CARGO_BIN_EXE_resend-worker"),
        "sendgrid" => env!("CARGO_BIN_EXE_sendgrid-worker"),
        "twilio" => env!("CARGO_BIN_EXE_twilio-worker"),
        "fcm" => env!("CARGO_BIN_EXE_fcm-worker"),
        "slack" => env!("CARGO_BIN_EXE_slack-worker"),
        "webhook" => env!("CARGO_BIN_EXE_slack-webhook-worker"),
        "workflow" => env!("CARGO_BIN_EXE_slack-workflow-worker"),
        "inapp" => env!("CARGO_BIN_EXE_in-app-worker"),
        _ => unreachable!(),
    };
    let mut cmd = Command::new(bin);
    cmd.env(
        "DATABASE_URL",
        format!("{url}?application_name=shutdown_{kind}"),
    )
    .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
    .env("SWALLOW_WORKER_SHUTDOWN_GRACE_SECONDS", grace)
    .env("NOVU_RESEND_ENVIRONMENT_ID", "shutdown-env")
    .env("NOVU_RESEND_API_KEY", "fixture")
    .env_remove("NOVU_RESEND_API_KEY_FILE")
    .env("NOVU_RESEND_FROM", "from@example.test")
    .env("NOVU_SENDGRID_ENVIRONMENT_ID", "shutdown-env")
    .env("NOVU_SENDGRID_API_KEY", "fixture")
    .env_remove("NOVU_SENDGRID_API_KEY_FILE")
    .env("NOVU_SENDGRID_FROM", "from@example.test")
    .env("NOVU_SENDGRID_SENDER_NAME", "fixture")
    .env("NOVU_TWILIO_ENVIRONMENT_ID", "shutdown-env")
    .env("NOVU_TWILIO_ACCOUNT_SID", "ACfixture")
    .env("NOVU_TWILIO_AUTH_TOKEN", "fixture")
    .env_remove("NOVU_TWILIO_AUTH_TOKEN_FILE")
    .env_remove("NOVU_TWILIO_API_KEY_SID")
    .env_remove("NOVU_TWILIO_API_KEY_SECRET")
    .env_remove("NOVU_TWILIO_API_KEY_SECRET_FILE")
    .env_remove("NOVU_TWILIO_STATUS_CALLBACK_BASE_URL")
    .env("NOVU_TWILIO_FROM", "+15550000001")
    .env("NOVU_SLACK_ENVIRONMENT_ID", "shutdown-env")
    .env("NOVU_SLACK_BOT_TOKEN", "fixture")
    .env_remove("NOVU_SLACK_BOT_TOKEN_FILE")
    .env("NOVU_SLACK_WEBHOOK_DESTINATION_ID", "ops")
    .env(
        "NOVU_SLACK_WEBHOOK_URL",
        "https://hooks.slack.com/services/A/B/fixture",
    )
    .env_remove("NOVU_SLACK_WEBHOOK_URL_FILE")
    .env("NOVU_SLACK_WORKFLOW_TOKENS", r#"{"main":{"":"fixture"}}"#)
    .env_remove("NOVU_SLACK_WORKFLOW_TOKENS_FILE")
    .env("NOVU_FCM_ENVIRONMENT_ID", "shutdown-env")
    .env("NOVU_FCM_PROJECT_ID", "fixture")
    .env("NOVU_FCM_ACCESS_TOKEN", "fixture")
    .env_remove("NOVU_FCM_ACCESS_TOKEN_FILE")
    .env_remove("NOVU_FCM_INTEGRATIONS")
    .env_remove("NOVU_FCM_INTEGRATIONS_FILE")
    .env("HTTPS_PROXY", "http://127.0.0.1:1")
    .env("NOVU_RESEND_DB_TIMEOUT_SECONDS", "20")
    .env("NOVU_SENDGRID_DB_TIMEOUT_SECONDS", "20")
    .env("NOVU_TWILIO_DB_TIMEOUT_SECONDS", "20")
    .stdout(Stdio::null())
    .stderr(Stdio::null());
    Worker(cmd.spawn().unwrap())
}
fn terminate(worker: &Worker) {
    assert!(
        Command::new("kill")
            .args(["-TERM", &worker.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
}
async fn blocked(db: &tokio_postgres::Client, kind: &str) {
    for _ in 0..100 {
        let yes: bool = db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", &[&format!("shutdown_{kind}")]).await.unwrap().get(0);
        if yes {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{kind} never blocked on database");
}
async fn exited(child: &mut Child) -> std::process::ExitStatus {
    for _ in 0..100 {
        if let Some(s) = child.try_wait().unwrap() {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker did not exit");
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn workers_wait_for_inflight_operation_and_leave_new_jobs_unclaimed() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('shutdown-env','org')")
        .await
        .unwrap();
    for (kind, job_kind) in [
        ("resend", novu_postgres_queue::resend_queue::JOB_KIND),
        ("sendgrid", novu_postgres_queue::sendgrid_queue::JOB_KIND),
        ("twilio", novu_postgres_queue::twilio_queue::JOB_KIND),
        ("fcm", novu_postgres_queue::fcm_queue::JOB_KIND),
        ("slack", novu_postgres_queue::slack_queue::JOB_KIND),
        (
            "webhook",
            novu_postgres_queue::slack_webhook_queue::JOB_KIND,
        ),
        ("workflow", novu_postgres_queue::slack_workflow::FANOUT_KIND),
        ("inapp", novu_postgres_queue::in_app::JOB_KIND),
    ] {
        let job = Uuid::new_v4();
        novu_postgres_queue::enqueue(&db, job, "shutdown-env", job_kind, &serde_json::json!({}))
            .await
            .unwrap();
        let tx = db.transaction().await.unwrap();
        tx.batch_execute("LOCK TABLE novu_queue_jobs IN ACCESS EXCLUSIVE MODE")
            .await
            .unwrap();
        let mut worker = start(&url, kind, "5");
        blocked(&observer, kind).await;
        terminate(&worker);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(worker.0.try_wait().unwrap().is_none());
        tx.commit().await.unwrap();
        assert!(exited(&mut worker.0).await.success());
        let state: String = db
            .query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&job])
            .await
            .unwrap()
            .get(0);
        assert_eq!(state, "pending");
    }
    let tx = db.transaction().await.unwrap();
    tx.batch_execute("LOCK TABLE novu_queue_jobs IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let mut worker = start(&url, "resend", "1");
    blocked(&observer, "resend").await;
    terminate(&worker);
    assert!(!exited(&mut worker.0).await.success());
    tx.rollback().await.unwrap();
}
#[test]
fn invalid_grace_fails_before_worker_configuration() {
    for value in ["0", "301", "bad"] {
        for binary in [
            env!("CARGO_BIN_EXE_resend-worker"),
            env!("CARGO_BIN_EXE_sendgrid-worker"),
            env!("CARGO_BIN_EXE_twilio-worker"),
            env!("CARGO_BIN_EXE_fcm-worker"),
            env!("CARGO_BIN_EXE_slack-worker"),
            env!("CARGO_BIN_EXE_slack-webhook-worker"),
            env!("CARGO_BIN_EXE_slack-workflow-worker"),
            env!("CARGO_BIN_EXE_in-app-worker"),
        ] {
            let status = Command::new(binary)
                .env("SWALLOW_WORKER_SHUTDOWN_GRACE_SECONDS", value)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            assert!(!status.success());
        }
    }
}
