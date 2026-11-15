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
        Command::new(env!("CARGO_BIN_EXE_resend-worker"))
            .env(
                "DATABASE_URL",
                format!("{url}?application_name={application}"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
            .env("NOVU_RESEND_API_KEY", "fixture-token")
            .env("NOVU_RESEND_FROM", "from@example.test")
            .env_remove("NOVU_RESEND_API_KEY_FILE")
            .env("NOVU_RESEND_DB_TIMEOUT_SECONDS", "2")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

#[test]
fn invalid_timeout_is_rejected_before_connecting() {
    for timeout in ["0", "301", "bad"] {
        let status = Command::new(env!("CARGO_BIN_EXE_resend-worker"))
            .env("DATABASE_URL", "postgresql://localhost/unused")
            .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
            .env("NOVU_RESEND_API_KEY", "fixture-token")
            .env("NOVU_RESEND_FROM", "from@example.test")
            .env_remove("NOVU_RESEND_API_KEY_FILE")
            .env("NOVU_RESEND_DB_TIMEOUT_SECONDS", timeout)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success());
    }
}

#[test]
fn invalid_retry_guard_is_rejected_before_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_resend-worker"))
        .env("DATABASE_URL", "postgresql://localhost/unused")
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
        .env("NOVU_RESEND_API_KEY", "fixture-token")
        .env("NOVU_RESEND_FROM", "from@example.test")
        .env_remove("NOVU_RESEND_API_KEY_FILE")
        .env("NOVU_RESEND_RETRY_EVIDENCE_GUARD", "invalid")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-token"));
}

#[test]
fn key_reload_requires_a_file_and_valid_setting() {
    for setting in ["true", "invalid"] {
        let output = Command::new(env!("CARGO_BIN_EXE_resend-worker"))
            .env("DATABASE_URL", "postgresql://localhost/unused")
            .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
            .env("NOVU_RESEND_API_KEY", "fixture-token")
            .env_remove("NOVU_RESEND_API_KEY_FILE")
            .env("NOVU_RESEND_FROM", "from@example.test")
            .env("NOVU_RESEND_RELOAD_API_KEY", setting)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-token"));
    }
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database"]
async fn key_reload_pauses_claims_until_file_is_valid() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0120_email_workflow_worker_policy.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(queue::resend_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(queue::resend_queue::RECIPIENT_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(queue::in_app::EMAIL_MESSAGES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute("ALTER TABLE novu_rust_email_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-resend-reload-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let key = dir.join("key");
    let log = dir.join("stderr");
    std::fs::write(&key, "fixture-key-before").unwrap();
    let mut process = Worker(
        Command::new(env!("CARGO_BIN_EXE_resend-worker"))
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
            .env_remove("NOVU_RESEND_API_KEY")
            .env("NOVU_RESEND_API_KEY_FILE", &key)
            .env("NOVU_RESEND_RELOAD_API_KEY", "true")
            .env("NOVU_RESEND_FROM", "from@example.test")
            .stdout(Stdio::null())
            .stderr(Stdio::from(std::fs::File::create(&log).unwrap()))
            .spawn()
            .unwrap(),
    );
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_email_workflow_worker_config WHERE environment_id='env')", &[]).await.unwrap().get::<_, bool>(0) { break; }
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "worker exited early"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    std::fs::write(&key, "invalid\nfixture-key").unwrap();
    let mut paused = false;
    for _ in 0..100 {
        let output = std::fs::read_to_string(&log).unwrap();
        assert!(!output.contains("fixture-key"));
        if output.contains("key reload failed") {
            paused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(paused, "worker did not pause on invalid key");
    let job = Uuid::new_v4();
    // Unknown field guarantees failure before any provider request.
    db.execute(
        "INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ($1,'env',$2,$3)",
        &[
            &job,
            &queue::resend_queue::JOB_KIND,
            &json!({"invalid":true}),
        ],
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&job])
            .await
            .unwrap()
            .get::<_, String>(0),
        "pending"
    );
    std::fs::write(dir.join("replacement"), "fixture-key-after").unwrap();
    std::fs::rename(dir.join("replacement"), &key).unwrap();
    let mut resumed = false;
    for _ in 0..100 {
        if db
            .query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&job])
            .await
            .unwrap()
            .get::<_, String>(0)
            == "failed"
        {
            resumed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(resumed, "worker did not resume after key replacement");
    assert!(process.0.try_wait().unwrap().is_none());
    drop(process);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
#[ignore = "restarts a disposable PostgreSQL container"]
async fn workers_reconnect_and_preserve_uncertain_attempts() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let mut db = connect(&url).await;
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0120_email_workflow_worker_policy.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(queue::resend_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(queue::resend_queue::RECIPIENT_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(queue::in_app::EMAIL_MESSAGES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute("ALTER TABLE novu_rust_email_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
    let lock = db.transaction().await.unwrap();
    lock.batch_execute("LOCK TABLE novu_queue_jobs IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let observer = connect(&url).await;
    let mut first = worker(&url, "resend_delivery_ha_1");
    let mut observed_lock = false;
    for _ in 0..100 {
        observed_lock = observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='resend_delivery_ha_1' AND wait_event_type='Lock')", &[]).await.unwrap().get(0);
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
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='resend_delivery_ha_1')",
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
    let mut second = worker(&url, "resend_delivery_ha_2");
    let mut both = false;
    for _ in 0..100 {
        both = observer.query_one("SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('resend_delivery_ha_1','resend_delivery_ha_2')", &[]).await.unwrap().get(0);
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
        transaction.execute("INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ($1,'env',$2,$3)", &[&job,&queue::resend_queue::JOB_KIND,&json!({})]).await.unwrap();
    }
    transaction.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&uncertain, &Uuid::new_v4()]).await.unwrap();
    transaction.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state) VALUES ($1,'env','dispatching')", &[&uncertain]).await.unwrap();
    transaction.commit().await.unwrap();
    let mut recovered = false;
    for _ in 0..200 {
        let row = db.query_one("SELECT (SELECT status='failed' FROM novu_queue_jobs WHERE id=$1),(SELECT state='unknown' FROM novu_rust_resend_attempts WHERE job_id=$1),(SELECT status='failed' FROM novu_queue_jobs WHERE id=$2),(SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('resend_delivery_ha_1','resend_delivery_ha_2'))", &[&uncertain,&invalid]).await.unwrap();
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
