//! Restarts only a dedicated disposable PostgreSQL container.
use novu_postgres_queue as queue;
use serde_json::json;
use std::time::Duration;
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
struct Worker(std::process::Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(url: &str, app: &str) -> Worker {
    Worker(
        std::process::Command::new(env!("CARGO_BIN_EXE_twilio-projection-worker"))
            .env("DATABASE_URL", format!("{url}?application_name={app}"))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_TWILIO_ENVIRONMENT_ID", "env")
            .env("NOVU_TWILIO_PROJECTION_DB_TIMEOUT_SECONDS", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    )
}

#[tokio::test]
#[ignore = "restarts a disposable PostgreSQL container"]
async fn workers_reconnect_after_timeout_and_database_restart() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let mut db = connect(&url).await;
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        queue::twilio_queue::MIGRATION,
        queue::twilio_queue::DISPATCH_ID_MIGRATION,
        queue::in_app::SMS_MESSAGES_MIGRATION,
        queue::twilio_projection::MIGRATION,
        include_str!("../../../api/rust-api/migrations/0005_twilio_webhooks.sql"),
        include_str!("../../../api/rust-api/migrations/0006_twilio_webhook_job_link.sql"),
        include_str!("../../../api/rust-api/migrations/0007_twilio_dispatch_identity.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let job = Uuid::new_v4();
    let dispatch = Uuid::new_v4();
    queue::enqueue(&db, job, "env", queue::twilio_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,provider_message_id,dispatch_id) VALUES ($1,'env','accepted','SMfixture',$2)", &[&job,&dispatch]).await.unwrap();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('env','ACfixture','SMfixture','sent',$1,$2)", &[&job,&dispatch]).await.unwrap();
    db.execute("INSERT INTO novu_rust_twilio_projection_heartbeat VALUES ('env',clock_timestamp()-interval '1 hour')", &[]).await.unwrap();
    let lock = db.transaction().await.unwrap();
    lock.query_one("SELECT environment_id FROM novu_rust_twilio_projection_heartbeat WHERE environment_id='env' FOR UPDATE", &[]).await.unwrap();
    let observer = connect(&url).await;
    let mut first = worker(&url, "swallow_twilio_ha_1");
    let mut blocked = false;
    for _ in 0..100 {
        blocked=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='swallow_twilio_ha_1' AND wait_event_type='Lock')", &[]).await.unwrap().get(0);
        if blocked {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(blocked, "worker did not wait on heartbeat lock");
    let mut disconnected = false;
    for _ in 0..100 {
        disconnected = !observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='swallow_twilio_ha_1')", &[]).await.unwrap().get::<_,bool>(0);
        if disconnected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        disconnected,
        "worker did not abandon the timed-out DB connection"
    );
    lock.commit().await.unwrap();
    let mut recovered = false;
    for _ in 0..150 {
        recovered=observer.query_one("SELECT last_completed_at>clock_timestamp()-interval '10 seconds' FROM novu_rust_twilio_projection_heartbeat WHERE environment_id='env'", &[]).await.unwrap().get(0);
        if recovered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(recovered, "worker did not reconnect after timeout");
    let mut second = worker(&url, "swallow_twilio_ha_2");
    let mut both = false;
    for _ in 0..100 {
        let count:i64=observer.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('swallow_twilio_ha_1','swallow_twilio_ha_2')", &[]).await.unwrap().get(0);
        if count == 2 {
            both = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(both, "two worker connections did not start");
    assert!(
        std::process::Command::new("docker")
            .args(["restart", &container])
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let db = connect(&url).await;
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('env','ACfixture','SMfixture','delivered',$1,$2)", &[&job,&dispatch]).await.unwrap();
    let mut delivered = false;
    let mut reconnected = false;
    for _ in 0..400 {
        delivered = db
            .query_one(
                "SELECT twilio_delivery_result='delivered' FROM novu_queue_jobs WHERE id=$1",
                &[&job],
            )
            .await
            .unwrap()
            .get::<_, Option<bool>>(0)
            == Some(true);
        let count: i64 = db.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('swallow_twilio_ha_1','swallow_twilio_ha_2')", &[]).await.unwrap().get(0);
        reconnected = count == 2;
        if delivered && reconnected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        delivered
            && reconnected
            && first.0.try_wait().unwrap().is_none()
            && second.0.try_wait().unwrap().is_none(),
        "workers did not recover after database restart"
    );
}
