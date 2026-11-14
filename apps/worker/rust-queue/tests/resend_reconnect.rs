//! Restarts the disposable PostgreSQL container; never use with a shared database.
use novu_postgres_queue as queue;
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

async fn connect(url: &str) -> tokio_postgres::Client {
    for _ in 0..100 {
        if let Ok((db, driver)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = driver.await;
            });
            return db;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("PostgreSQL did not become ready")
}

struct Worker(std::process::Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "restarts a disposable PostgreSQL container"]
async fn projector_reconnects_after_database_restart() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let db = connect(&url).await;
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
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'env','accepted','id')", &[&job]).await.unwrap();
    db.execute(
        "INSERT INTO novu_rust_resend_recipients VALUES ($1,'env','to',0,'hash')",
        &[&job],
    )
    .await
    .unwrap();
    let mut worker = Worker(
        std::process::Command::new(env!("CARGO_BIN_EXE_resend-projection-worker"))
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut ready = false;
    for _ in 0..100 {
        let result: Option<String> = db
            .query_one(
                "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
                &[&job],
            )
            .await
            .unwrap()
            .get(0);
        if result.as_deref() == Some("pending") {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ready);
    assert!(
        std::process::Command::new("docker")
            .args(["restart", &container])
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let db = connect(&url).await;
    db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash) VALUES ('env','event','id','email.delivered','hash')", &[]).await.unwrap();
    let mut delivered = false;
    for _ in 0..150 {
        let result: Option<String> = db
            .query_one(
                "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
                &[&job],
            )
            .await
            .unwrap()
            .get(0);
        if result.as_deref() == Some("delivered") {
            delivered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let alive = worker.0.try_wait().unwrap().is_none();
    assert!(
        alive && delivered,
        "projector did not recover after PostgreSQL restart"
    );
}
