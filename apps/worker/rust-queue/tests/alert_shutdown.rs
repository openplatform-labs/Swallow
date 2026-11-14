//! Real scheduled commands and a black-hole proxy. No external traffic is sent.
#![cfg(unix)]
use novu_postgres_queue::{
    alerts::{self, Reservation},
    enqueue,
};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::{net::TcpListener, time::timeout};
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(url: &str, kind: &str, target: &str, proxy: &str, grace: &str) -> Process {
    let (bin, args): (&str, Vec<&str>) = match kind {
        "delivery" => (
            env!("CARGO_BIN_EXE_send-delivery-alert"),
            vec!["alert-shutdown-env", "300", "120", "60"],
        ),
        "database" => (
            env!("CARGO_BIN_EXE_send-database-alert"),
            vec!["alert-shutdown-env", "fixture", "60"],
        ),
        _ => unreachable!(),
    };
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
        .env_remove("SWALLOW_ALERT_EMAIL_TO")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", target)
        .env("SWALLOW_DB_ALERT_SLACK_TARGET_ID", target)
        .env(
            "SWALLOW_DB_PROBE_URL",
            "postgres://postgres@127.0.0.1:1/postgres",
        )
        .env("SWALLOW_DB_PROBE_TIMEOUT_SECONDS", "1")
        .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "10")
        .env("SWALLOW_DB_ALERT_OPERATION_TIMEOUT_SECONDS", "10")
        .env("SWALLOW_ALERT_SHUTDOWN_GRACE_SECONDS", grace)
        .env("HTTPS_PROXY", proxy)
        .env("https_proxy", proxy)
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Process(cmd.spawn().unwrap())
}
fn terminate(p: &Process) {
    assert!(
        Command::new("kill")
            .args(["-TERM", &p.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
}
async fn exited(child: &mut Child) -> std::process::ExitStatus {
    for _ in 0..120 {
        if let Some(s) = child.try_wait().unwrap() {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("alert process did not exit");
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn signal_waits_for_active_alert_and_timeout_preserves_unknown_outcome() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('alert-shutdown-env','org'); INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'alert-shutdown-env',rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user").await.unwrap();
    let job = Uuid::new_v4();
    enqueue(
        &db,
        job,
        "alert-shutdown-env",
        "fixture",
        &serde_json::json!({}),
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1", &[&job]).await.unwrap();
    for kind in ["delivery", "database"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!("http://{}", listener.local_addr().unwrap());
        let target = format!("{kind}-wait");
        let mut process = start(&url, kind, &target, &proxy, "5");
        let (socket, _) = timeout(Duration::from_secs(8), listener.accept())
            .await
            .expect("provider request did not reach proxy")
            .unwrap();
        let state: String=db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='alert-shutdown-env' AND channel='slack' AND target_id=$1", &[&target]).await.unwrap().get(0);
        assert_eq!(state, "dispatching");
        terminate(&process);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(process.0.try_wait().unwrap().is_none());
        drop(socket);
        assert_eq!(exited(&mut process.0).await.code(), Some(2));
        let state: String=db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='alert-shutdown-env' AND channel='slack' AND target_id=$1", &[&target]).await.unwrap().get(0);
        assert_eq!(state, "unknown");
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let mut process = start(&url, "delivery", "timeout", &proxy, "1");
    let (socket, _) = timeout(Duration::from_secs(8), listener.accept())
        .await
        .unwrap()
        .unwrap();
    terminate(&process);
    assert_eq!(exited(&mut process.0).await.code(), Some(1));
    drop(socket);
    let state: String=db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='alert-shutdown-env' AND channel='slack' AND target_id='timeout'",&[]).await.unwrap().get(0);
    assert_eq!(state, "dispatching");
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id='alert-shutdown-env' AND channel='slack' AND target_id='timeout'",&[]).await.unwrap();
    assert_eq!(
        alerts::reserve(&mut db, "alert-shutdown-env", "slack", "timeout", 60)
            .await
            .unwrap(),
        Reservation::Blocked
    );
    let state: String=db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='alert-shutdown-env' AND channel='slack' AND target_id='timeout'",&[]).await.unwrap().get(0);
    assert_eq!(state, "unknown");
}
