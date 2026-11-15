//! Actual HTTP readiness against a disposable PostgreSQL server.
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(url: &str, writable: &str) -> (Server, u16) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
        .env("SWALLOW_API_READY_REQUIRE_WRITABLE", writable)
        .env_remove("SWALLOW_API_STRICT_SECURITY_CONFIG")
        .env_remove("SWALLOW_API_REQUIRED_SECURITY")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    (Server(child), port)
}
async fn status(port: u16, path: &str, expected: &str) {
    for _ in 0..100 {
        let output = Command::new("curl")
            .args([
                "-s",
                "--max-time",
                "1",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code}",
                &format!("http://127.0.0.1:{port}{path}"),
            ])
            .output()
            .unwrap();
        if output.stdout == expected.as_bytes() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{path} did not return {expected}");
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL and curl"]
async fn writable_policy_distinguishes_read_only_sessions() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    assert!(!url.contains('?'));
    let read_only = format!("{url}?options=-c%20default_transaction_read_only%3Don");
    let (_write, wp) = start(&url, "true");
    let (_read, rp) = start(&read_only, "true");
    let (_legacy, lp) = start(&read_only, "false");
    status(wp, "/health/ready", "200").await;
    status(rp, "/health/live", "200").await;
    status(rp, "/health/ready", "503").await;
    status(lp, "/health/ready", "200").await;
    let (mut invalid, _) = start(&url, "invalid");
    for _ in 0..100 {
        if let Some(result) = invalid.0.try_wait().unwrap() {
            assert!(!result.success());
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("invalid configuration was accepted");
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL standby; promotes standby"]
async fn standby_is_unready_until_promoted() {
    let url = std::env::var("TEST_STANDBY_URL").unwrap();
    let data = std::env::var("TEST_STANDBY_DATA").unwrap();
    let pg_ctl = std::env::var("TEST_PG_CTL").unwrap();
    let guarded_url = format!("{url}?application_name=readiness_standby_guard");
    let (_server, port) = start(&guarded_url, "true");
    let (_legacy, legacy_port) = start(&url, "false");
    status(legacy_port, "/health/ready", "200").await;
    let (observer, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let mut connected = false;
    for _ in 0..100 {
        connected = observer.query_one("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = 'readiness_standby_guard')", &[]).await.unwrap().get(0);
        if connected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(connected, "guarded API never connected to the standby");
    status(port, "/health/live", "200").await;
    let mut probed = false;
    for _ in 0..100 {
        status(port, "/health/ready", "503").await;
        probed = observer.query_one("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = 'readiness_standby_guard' AND query LIKE 'SELECT NOT pg_is_in_recovery()%')", &[]).await.unwrap().get(0);
        if probed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(probed, "readiness did not probe the connected standby");
    assert!(
        Command::new(pg_ctl)
            .args(["promote", "-D", &data, "-w"])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    status(port, "/health/ready", "200").await;
}
