//! Real scheduler, real fanout and a held provider connection across local PostgreSQL promotion.
#![cfg(unix)]
use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(12);
        while Instant::now() < deadline {
            if self.0.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db
}
#[tokio::test]
#[ignore = "requires disposable synchronously replicated local PostgreSQL; stops its primary"]
async fn scheduler_recovers_unknown_alert_after_synchronous_promotion() {
    let primary = connect(&std::env::var("TEST_PRIMARY_URL").unwrap()).await;
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let env = format!("scheduler-failover-{}", Uuid::new_v4().simple());
    primary
        .execute(
            "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
            &[&env],
        )
        .await
        .unwrap();
    primary.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user",&[&env]).await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&primary, job, &env, "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    primary.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let dir =
        Fixture(std::env::temp_dir().join(format!("swallow-scheduler-promote-{}", Uuid::new_v4())));
    std::fs::create_dir(&dir.0).unwrap();
    let webhook = dir.0.join("webhook");
    let config = dir.0.join("config.json");
    let log = dir.0.join("output");
    std::fs::write(&webhook, "https://hooks.slack.com/services/A/B/C").unwrap();
    std::fs::write(
        &config,
        serde_json::to_vec(
            &serde_json::json!({"targets":[{"channel":"slack","id":"ops","webhook_file":webhook}]}),
        )
        .unwrap(),
    )
    .unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    proxy.set_nonblocking(true).unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let output = std::fs::File::create(&log).unwrap();
    let mut scheduler = Process(
        Command::new(env!("CARGO_BIN_EXE_alert-scheduler"))
            .args(["1", "delivery", &env, "300", "120", "60"])
            .arg(&config)
            .env_clear()
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "30")
            .env("SWALLOW_ALERT_SCHEDULER_SHUTDOWN_GRACE_SECONDS", "10")
            .env("HTTPS_PROXY", &proxy_url)
            .env("https_proxy", &proxy_url)
            .env("NO_PROXY", "")
            .env("no_proxy", "")
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let socket = loop {
        match proxy.accept() {
            Ok((socket, _)) => break socket,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "provider connection missing: {}",
                    std::fs::read_to_string(&log).unwrap_or_default()
                );
                tokio::time::sleep(Duration::from_millis(25)).await
            }
            Err(e) => panic!("proxy: {e}"),
        }
    };
    let standby = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        let row=standby.query_one("SELECT pg_is_in_recovery(),EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='ops' AND state='dispatching'),EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND channel='slack' AND target_id='ops' AND state='dispatching')",&[&env]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        if row.get::<_, bool>(1) && row.get::<_, bool>(2) {
            replicated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated, "dispatch reservation did not replicate");
    let ctl = std::env::var("TEST_LOCAL_PG_CTL").unwrap();
    assert!(
        Command::new(&ctl)
            .args([
                "-D",
                &std::env::var("TEST_LOCAL_PRIMARY_DATA").unwrap(),
                "-m",
                "immediate",
                "stop"
            ])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new(&ctl)
            .args([
                "-D",
                &std::env::var("TEST_LOCAL_STANDBY_DATA").unwrap(),
                "promote",
                "-w"
            ])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    drop(socket);
    let promoted = connect(&standby_url).await;
    assert!(
        !promoted
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    // Accelerate the documented 60-second lease recovery on the promoted database.
    promoted.execute("UPDATE public.novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id=$1 AND channel='slack' AND target_id='ops'",&[&env]).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut recovered = false;
    while Instant::now() < deadline {
        let row=promoted.query_one("SELECT (SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='ops'),(SELECT count(*) FROM public.novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND channel='slack' AND target_id='ops')",&[&env]).await.unwrap();
        if row.get::<_, String>(0) == "unknown"
            && row.get::<_, i64>(1) == 1
            && std::fs::read_to_string(&log)
                .unwrap_or_default()
                .matches("alert scheduler: attention required")
                .count()
                >= 2
        {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        recovered,
        "scheduler did not recover unknown state: {}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    let attempt_state:String=promoted.query_one("SELECT state FROM public.novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND channel='slack' AND target_id='ops'",&[&env]).await.unwrap().get(0);
    assert_eq!(attempt_state, "unknown");
    assert!(
        matches!(proxy.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock),
        "unexpected duplicate provider connection"
    );
    assert!(
        Command::new("kill")
            .args(["-TERM", &scheduler.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(scheduler.0.wait().unwrap().success());
}
