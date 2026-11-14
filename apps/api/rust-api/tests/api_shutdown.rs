//! Real SIGTERM, HTTP and PostgreSQL on a disposable migrated database.
#![cfg(unix)]
use sha2::{Digest, Sha256};
use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio_postgres::NoTls;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn start(url: &str, port: u16, drain: &str, grace: &str) -> Process {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"));
    cmd.env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
        .env("SWALLOW_API_DRAIN_SECONDS", drain)
        .env("SWALLOW_API_SHUTDOWN_GRACE_SECONDS", grace)
        .env("SWALLOW_API_DB_HEALTH_INTERVAL_SECONDS", "30")
        .env_remove("SWALLOW_API_STRICT_SECURITY_CONFIG")
        .env_remove("SWALLOW_API_REQUIRED_SECURITY")
        .env_remove("JWT_SECRET")
        .env_remove("JWT_SECRET_FILE")
        .env_remove("INBOX_HMAC_KEYS_JSON")
        .env_remove("INBOX_HMAC_KEYS_JSON_FILE")
        .env_remove("NOVU_RESEND_WEBHOOK_SECRET")
        .env_remove("NOVU_RESEND_WEBHOOK_SECRET_FILE")
        .env_remove("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN")
        .env_remove("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN_FILE")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Process(cmd.spawn().unwrap())
}
fn get(port: u16, path: &str) -> Option<u16> {
    let out = Command::new("curl")
        .args([
            "-s",
            "--max-time",
            "2",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("http://127.0.0.1:{port}{path}"),
        ])
        .output()
        .ok()?;
    std::str::from_utf8(&out.stdout).ok()?.parse().ok()
}
async fn until_status(port: u16, path: &str, expected: u16) {
    for _ in 0..80 {
        if get(port, path) == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{path} did not return {expected}");
}
fn signal(child: &Child) {
    // SAFETY: this is the child process started and owned by this test.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
}
fn request(port: u16) -> Child {
    Command::new("curl")
        .args([
            "-s",
            "--max-time",
            "10",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            &format!("Authorization: SwallowConsole {}", "a".repeat(64)),
            &format!("http://127.0.0.1:{port}/v1/experimental/console/alerts"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}
async fn wait_for_blocked(db: &tokio_postgres::Client) {
    for _ in 0..100 {
        let n: i64 = db.query_one("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_console_alerts_page%'", &[]).await.unwrap().get(0);
        if n > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("HTTP query did not reach lock");
}
async fn wait_exit(child: &mut Child) -> std::process::ExitStatus {
    for _ in 0..100 {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not stop in time");
}
#[tokio::test]
#[ignore = "requires disposable migrated PostgreSQL and curl"]
async fn sigterm_drains_requests_and_bounds_shutdown() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let (observer, observer_driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = observer_driver.await;
    });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('shutdown-env','org')")
        .await
        .unwrap();
    let hash = format!("{:x}", Sha256::digest("a".repeat(64).as_bytes()));
    db.execute("INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at) VALUES ($1,'shutdown-env',clock_timestamp()+interval '1 hour')", &[&hash]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_console_alert_grants SELECT credential_id FROM public.novu_rust_console_credentials WHERE token_hash=$1", &[&hash]).await.unwrap();
    let p = port();
    let mut server = start(&url, p, "2", "4");
    let peer_port = port();
    let _peer = start(&url, peer_port, "0", "4");
    until_status(p, "/health/ready", 200).await;
    until_status(peer_port, "/health/ready", 200).await;
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "UPDATE public.novu_rust_console_credentials SET enabled=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    let pending = request(p);
    wait_for_blocked(&observer).await;
    signal(&server.0);
    until_status(p, "/health/ready", 503).await;
    assert_eq!(get(p, "/v1/experimental/console/alerts"), Some(503));
    assert_eq!(get(p, "/health/live"), Some(200));
    assert_eq!(get(peer_port, "/health/ready"), Some(200));
    tx.commit().await.unwrap();
    let out = pending.wait_with_output().unwrap();
    assert_eq!(out.stdout, b"200");
    assert!(wait_exit(&mut server.0).await.success());
    let p = port();
    let mut server = start(&url, p, "0", "1");
    until_status(p, "/health/ready", 200).await;
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "UPDATE public.novu_rust_console_credentials SET enabled=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    let pending = request(p);
    wait_for_blocked(&observer).await;
    let before = Instant::now();
    signal(&server.0);
    assert!(!wait_exit(&mut server.0).await.success());
    assert!(before.elapsed() < Duration::from_secs(4));
    tx.rollback().await.unwrap();
    assert_ne!(pending.wait_with_output().unwrap().stdout, b"200");
}

#[test]
fn shutdown_configuration_rejects_out_of_range_values() {
    for (name, value) in [
        ("SWALLOW_API_DRAIN_SECONDS", "301"),
        ("SWALLOW_API_DRAIN_SECONDS", "invalid"),
        ("SWALLOW_API_SHUTDOWN_GRACE_SECONDS", "0"),
        ("SWALLOW_API_SHUTDOWN_GRACE_SECONDS", "301"),
    ] {
        let status = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", "postgresql://localhost/unused")
            .env_remove("SWALLOW_API_DRAIN_SECONDS")
            .env_remove("SWALLOW_API_SHUTDOWN_GRACE_SECONDS")
            .env(name, value)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success(), "accepted {name}={value}");
    }
}
