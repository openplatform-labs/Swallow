//! SIGTERM during an interrupted scan must not advertise a complete heartbeat.
#![cfg(unix)]
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(url: &str, kind: &str, grace: &str) -> Worker {
    let (bin, env_var) = match kind {
        "resend" => (
            env!("CARGO_BIN_EXE_resend-projection-worker"),
            "NOVU_RESEND_ENVIRONMENT_ID",
        ),
        "sendgrid" => (
            env!("CARGO_BIN_EXE_sendgrid-projection-worker"),
            "NOVU_SENDGRID_ENVIRONMENT_ID",
        ),
        "twilio" => (
            env!("CARGO_BIN_EXE_twilio-projection-worker"),
            "NOVU_TWILIO_ENVIRONMENT_ID",
        ),
        _ => unreachable!(),
    };
    Worker(
        Command::new(bin)
            .env(
                "DATABASE_URL",
                format!("{url}?application_name=projection_shutdown_{kind}"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env(env_var, "projection-shutdown-env")
            .env("SWALLOW_WORKER_SHUTDOWN_GRACE_SECONDS", grace)
            .env("NOVU_RESEND_PROJECTION_DB_TIMEOUT_SECONDS", "20")
            .env("NOVU_SENDGRID_PROJECTION_DB_TIMEOUT_SECONDS", "20")
            .env("NOVU_TWILIO_PROJECTION_DB_TIMEOUT_SECONDS", "20")
            .env("NOVU_RESEND_PROJECTION_SCAN_INTERVAL_SECONDS", "300")
            .env("NOVU_SENDGRID_PROJECTION_SCAN_INTERVAL_SECONDS", "300")
            .env("NOVU_TWILIO_PROJECTION_SCAN_INTERVAL_SECONDS", "300")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
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
        let yes: bool = db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", &[&format!("projection_shutdown_{kind}")]).await.unwrap().get(0);
        if yes {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{kind} did not block on database");
}
async fn exited(child: &mut Child) -> std::process::ExitStatus {
    for _ in 0..100 {
        if let Some(s) = child.try_wait().unwrap() {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("projection worker did not exit");
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn interrupted_scans_leave_heartbeat_stale_and_restart_recovers() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('projection-shutdown-env','org')")
        .await
        .unwrap();
    for kind in ["resend", "sendgrid", "twilio"] {
        let table = format!("public.novu_rust_{kind}_projection_heartbeat");
        db.execute(&format!("INSERT INTO {table}(environment_id,last_completed_at) VALUES ('projection-shutdown-env',clock_timestamp()-interval '1 hour')"),&[]).await.unwrap();
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
        let stale: bool = db.query_one(&format!("SELECT last_completed_at<clock_timestamp()-interval '30 minutes' FROM {table} WHERE environment_id='projection-shutdown-env'"),&[]).await.unwrap().get(0);
        assert!(stale, "interrupted {kind} scan advanced heartbeat");
        let mut worker = start(&url, kind, "5");
        let mut fresh = false;
        for _ in 0..100 {
            fresh = db.query_one(&format!("SELECT last_completed_at>clock_timestamp()-interval '1 minute' FROM {table} WHERE environment_id='projection-shutdown-env'"),&[]).await.unwrap().get(0);
            if fresh {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(fresh, "{kind} did not complete after restart");
        terminate(&worker);
        assert!(exited(&mut worker.0).await.success());
    }
    // A projection already inside its transaction is allowed to commit, but
    // stopping before completing the scan must not advance the heartbeat.
    let job = uuid::Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        job,
        "projection-shutdown-env",
        novu_postgres_queue::resend_queue::JOB_KIND,
        &serde_json::json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'projection-shutdown-env','accepted','fixture-id')", &[&job]).await.unwrap();
    db.execute("UPDATE public.novu_rust_resend_projection_heartbeat SET last_completed_at=clock_timestamp()-interval '1 hour' WHERE environment_id='projection-shutdown-env'",&[]).await.unwrap();
    let tx = db.transaction().await.unwrap();
    tx.batch_execute("LOCK TABLE novu_rust_email_messages IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let mut worker = start(&url, "resend", "5");
    blocked(&observer, "resend").await;
    terminate(&worker);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(worker.0.try_wait().unwrap().is_none());
    tx.commit().await.unwrap();
    assert!(exited(&mut worker.0).await.success());
    let row = db.query_one("SELECT j.resend_delivery_result, h.last_completed_at<clock_timestamp()-interval '30 minutes' FROM novu_queue_jobs j CROSS JOIN novu_rust_resend_projection_heartbeat h WHERE j.id=$1 AND h.environment_id='projection-shutdown-env'", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, Option<String>>(0).as_deref(), Some("unknown"));
    assert!(row.get::<_, bool>(1));
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
