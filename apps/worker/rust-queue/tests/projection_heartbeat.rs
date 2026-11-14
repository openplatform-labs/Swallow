//! Selectable strict heartbeats against a disposable PostgreSQL database.
use novu_postgres_queue as queue;
use serde_json::json;
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

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn strict_heartbeat_waits_for_skipped_jobs_in_each_channel() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('resend','org'),('twilio','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        queue::resend_queue::MIGRATION,
        queue::resend_queue::RECIPIENT_MIGRATION,
        queue::in_app::EMAIL_MESSAGES_MIGRATION,
        queue::resend_projection::MIGRATION,
        queue::resend_projection::HEARTBEAT_MIGRATION,
        queue::twilio_queue::MIGRATION,
        queue::twilio_queue::DISPATCH_ID_MIGRATION,
        queue::in_app::SMS_MESSAGES_MIGRATION,
        queue::twilio_projection::MIGRATION,
        include_str!("../../../api/rust-api/migrations/0010_resend_webhooks.sql"),
        include_str!("../../../api/rust-api/migrations/0012_resend_event_fingerprint.sql"),
        include_str!("../../../api/rust-api/migrations/0013_resend_event_recipient.sql"),
        include_str!("../../../api/rust-api/migrations/0005_twilio_webhooks.sql"),
        include_str!("../../../api/rust-api/migrations/0006_twilio_webhook_job_link.sql"),
        include_str!("../../../api/rust-api/migrations/0007_twilio_dispatch_identity.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    for (name, bin, env_key, full_key, interval_key, job_kind, attempts, heartbeat) in [
        (
            "resend",
            env!("CARGO_BIN_EXE_resend-projection-worker"),
            "NOVU_RESEND_ENVIRONMENT_ID",
            "NOVU_RESEND_PROJECTION_REQUIRE_FULL_SCAN",
            "NOVU_RESEND_PROJECTION_SCAN_INTERVAL_SECONDS",
            queue::resend_queue::JOB_KIND,
            "novu_rust_resend_attempts",
            "novu_rust_resend_projection_heartbeat",
        ),
        (
            "twilio",
            env!("CARGO_BIN_EXE_twilio-projection-worker"),
            "NOVU_TWILIO_ENVIRONMENT_ID",
            "NOVU_TWILIO_PROJECTION_REQUIRE_FULL_SCAN",
            "NOVU_TWILIO_PROJECTION_SCAN_INTERVAL_SECONDS",
            queue::twilio_queue::JOB_KIND,
            "novu_rust_twilio_attempts",
            "novu_rust_twilio_projection_heartbeat",
        ),
    ] {
        for (setting, value) in [
            (full_key, "bad"),
            (interval_key, "0"),
            (interval_key, "301"),
        ] {
            let output = Command::new(bin)
                .env("DATABASE_URL", &url)
                .env(env_key, name)
                .env_remove(full_key)
                .env_remove(interval_key)
                .env(setting, value)
                .output()
                .unwrap();
            assert!(
                !output.status.success(),
                "{name} accepted invalid {setting}"
            );
        }
        let job = Uuid::new_v4();
        queue::enqueue(&db, job, name, job_kind, &json!({}))
            .await
            .unwrap();
        db.execute(
            &format!(
                "INSERT INTO {attempts}(job_id,environment_id,state) VALUES ($1,$2,'accepted')"
            ),
            &[&job, &name],
        )
        .await
        .unwrap();
        let lock = db.transaction().await.unwrap();
        lock.query_one(
            "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
            &[&job],
        )
        .await
        .unwrap();
        let log_path = std::env::temp_dir().join(format!("swallow-{name}-{}.log", Uuid::new_v4()));
        let log = std::fs::File::create(&log_path).unwrap();
        let mut worker = Worker(
            Command::new(bin)
                .env("DATABASE_URL", &url)
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .env(env_key, name)
                .env(full_key, "true")
                .env(interval_key, "1")
                .stdout(Stdio::null())
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
        let mut skipped = false;
        for _ in 0..100 {
            if std::fs::read_to_string(&log_path)
                .unwrap()
                .contains("projection scan skipped jobs")
            {
                skipped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            skipped && worker.0.try_wait().unwrap().is_none(),
            "{name} did not skip locked job"
        );
        let exists: bool = observer
            .query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM {heartbeat} WHERE environment_id=$1)"),
                &[&name],
            )
            .await
            .unwrap()
            .get(0);
        assert!(!exists, "{name} advanced heartbeat on a skipped scan");
        lock.rollback().await.unwrap();
        let mut recovered = false;
        for _ in 0..100 {
            if observer
                .query_one(
                    &format!("SELECT EXISTS(SELECT 1 FROM {heartbeat} WHERE environment_id=$1)"),
                    &[&name],
                )
                .await
                .unwrap()
                .get::<_, bool>(0)
            {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            recovered && worker.0.try_wait().unwrap().is_none(),
            "{name} did not recover after unlock"
        );
        drop(worker);
        std::fs::remove_file(log_path).unwrap();
    }
}
