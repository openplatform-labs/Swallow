//! Uses a disposable PostgreSQL container and never contacts Slack.
use novu_postgres_queue as queue;
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    for _ in 0..100 {
        if let Ok((db, driver)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = driver.await;
            });
            return db;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("database did not become ready");
}
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(url: &str, application: &str) -> Worker {
    Worker(
        Command::new(env!("CARGO_BIN_EXE_sendgrid-worker"))
            .env(
                "DATABASE_URL",
                format!("{url}?application_name={application}"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SENDGRID_ENVIRONMENT_ID", "env")
            .env("NOVU_SENDGRID_API_KEY", "fixture-token")
            .env("NOVU_SENDGRID_FROM", "from@example.test")
            .env("NOVU_SENDGRID_SENDER_NAME", "fixture")
            .env_remove("NOVU_SENDGRID_API_KEY_FILE")
            .env_remove("NOVU_SENDGRID_DISPATCH_TRACKING")
            .env("NOVU_SENDGRID_DB_TIMEOUT_SECONDS", "2")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

#[test]
fn invalid_timeout_is_rejected_before_connecting() {
    for timeout in ["0", "301", "bad"] {
        let status = Command::new(env!("CARGO_BIN_EXE_sendgrid-worker"))
            .env("DATABASE_URL", "postgresql://localhost/unused")
            .env("NOVU_SENDGRID_ENVIRONMENT_ID", "env")
            .env("NOVU_SENDGRID_API_KEY", "fixture-token")
            .env("NOVU_SENDGRID_FROM", "from@example.test")
            .env("NOVU_SENDGRID_SENDER_NAME", "fixture")
            .env_remove("NOVU_SENDGRID_API_KEY_FILE")
            .env_remove("NOVU_SENDGRID_DISPATCH_TRACKING")
            .env("NOVU_SENDGRID_DB_TIMEOUT_SECONDS", timeout)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success());
    }
}

#[tokio::test]
#[ignore = "restarts a disposable PostgreSQL container"]
async fn workers_reconnect_and_preserve_uncertain_attempts() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let mut db = connect(&url).await;
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(queue::sendgrid_queue::MIGRATION)
        .await
        .unwrap();
    for sql in [
        queue::sendgrid_queue::DISPATCH_ID_MIGRATION,
        queue::sendgrid_queue::DISPATCH_PROOF_MIGRATION,
        queue::reconciliation::MIGRATION,
        queue::reconciliation::RETRY_DISPATCH_MIGRATION,
        include_str!("../../../api/rust-api/migrations/0004_sendgrid_webhooks.sql"),
        include_str!("../../../api/rust-api/migrations/0008_sendgrid_dispatch_identity.sql"),
        include_str!("../../../api/rust-api/migrations/0009_sendgrid_dispatch_proof.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(queue::in_app::EMAIL_MESSAGES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute("ALTER TABLE novu_rust_email_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
    let lock = db.transaction().await.unwrap();
    lock.batch_execute("LOCK TABLE novu_queue_jobs IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let observer = connect(&url).await;
    let mut first = worker(&url, "sendgrid_delivery_ha_1");
    let mut observed_lock = false;
    for _ in 0..100 {
        observed_lock = observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='sendgrid_delivery_ha_1' AND wait_event_type='Lock')", &[]).await.unwrap().get(0);
        if observed_lock {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(observed_lock, "worker did not encounter lock");
    let mut disconnected = false;
    for _ in 0..100 {
        disconnected = !observer
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='sendgrid_delivery_ha_1')",
                &[],
            )
            .await
            .unwrap()
            .get::<_, bool>(0);
        if disconnected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(disconnected, "worker did not abandon timed-out connection");
    lock.commit().await.unwrap();
    let mut second = worker(&url, "sendgrid_delivery_ha_2");
    let mut both = false;
    for _ in 0..100 {
        both = observer.query_one("SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('sendgrid_delivery_ha_1','sendgrid_delivery_ha_2')", &[]).await.unwrap().get(0);
        if both {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(both, "workers did not connect");
    assert!(
        Command::new("docker")
            .args(["restart", &container])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let mut db = connect(&url).await;
    let uncertain = Uuid::new_v4();
    let invalid = Uuid::new_v4();
    let transaction = db.transaction().await.unwrap();
    for job in [uncertain, invalid] {
        transaction.execute("INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ($1,'env',$2,$3)", &[&job,&queue::sendgrid_queue::JOB_KIND,&json!({"invalid_field":true})]).await.unwrap();
    }
    transaction.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&uncertain, &Uuid::new_v4()]).await.unwrap();
    transaction.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,'env','dispatching')", &[&uncertain]).await.unwrap();
    transaction.commit().await.unwrap();
    let mut recovered = false;
    for _ in 0..200 {
        let row = db.query_one("SELECT (SELECT status='failed' FROM novu_queue_jobs WHERE id=$1),(SELECT state='unknown' FROM novu_rust_sendgrid_attempts WHERE job_id=$1),(SELECT status='failed' FROM novu_queue_jobs WHERE id=$2),(SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('sendgrid_delivery_ha_1','sendgrid_delivery_ha_2'))", &[&uncertain,&invalid]).await.unwrap();
        recovered = (0..4).all(|i| row.get::<_, Option<bool>>(i) == Some(true));
        if recovered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recovered
            && first.0.try_wait().unwrap().is_none()
            && second.0.try_wait().unwrap().is_none(),
        "workers did not safely recover after restart"
    );
}
