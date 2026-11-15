//! Real HTTP requests and a disposable PostgreSQL instance.
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
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
fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn start(url: &str, port: u16, strict: bool, connections: &str) -> Server {
    start_policy(url, port, strict, connections, "any")
}
fn start_policy(url: &str, port: u16, strict: bool, connections: &str, policy: &str) -> Server {
    Server(
        Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
            .env("SWALLOW_API_DB_CONNECTIONS", connections)
            .env("SWALLOW_API_READY_CONNECTION_POLICY", policy)
            .env("SWALLOW_API_MAX_INFLIGHT_PER_CONNECTION", "1")
            .env(
                "SWALLOW_API_READY_REQUIRE_CAPACITY",
                if strict { "true" } else { "false" },
            )
            .env("SWALLOW_API_DB_HEALTH_INTERVAL_SECONDS", "30")
            .env("SWALLOW_API_DB_HEALTH_TIMEOUT_SECONDS", "2")
            .env_remove("SWALLOW_API_REQUEST_TIMEOUT_SECONDS")
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
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
fn status(port: u16, path: &str) -> Option<u16> {
    let output = Command::new("curl")
        .args([
            "--max-time",
            "3",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("http://127.0.0.1:{port}{path}"),
        ])
        .output()
        .ok()?;
    std::str::from_utf8(&output.stdout).ok()?.parse().ok()
}
async fn wait_status(port: u16, path: &str, expected: u16) {
    for _ in 0..100 {
        if status(port, path) == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{path} did not return {expected}");
}
fn pending_request(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.write_all(b"POST /v1/events/trigger HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{").unwrap();
    stream
}
#[test]
fn readiness_capacity_rejects_invalid_configuration() {
    for (mode, limit) in [("invalid", "1"), ("true", "0")] {
        let result = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", "postgresql://localhost/unused")
            .env("SWALLOW_API_READY_REQUIRE_CAPACITY", mode)
            .env("SWALLOW_API_MAX_INFLIGHT_PER_CONNECTION", limit)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("readiness capacity"));
    }
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL and curl"]
async fn readiness_can_follow_saturated_request_capacity() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let default_port = port();
    let strict_port = port();
    let _default = start(&url, default_port, false, "1");
    let _strict = start(&url, strict_port, true, "1");
    wait_status(default_port, "/health/ready", 200).await;
    wait_status(strict_port, "/health/ready", 200).await;
    let default_pending = pending_request(default_port);
    let strict_pending = pending_request(strict_port);
    wait_status(default_port, "/unmatched", 503).await;
    wait_status(strict_port, "/unmatched", 503).await;
    assert_eq!(status(default_port, "/health/ready"), Some(200));
    assert_eq!(status(strict_port, "/health/ready"), Some(503));
    assert_eq!(status(strict_port, "/health/live"), Some(200));
    drop(default_pending);
    drop(strict_pending);
    wait_status(strict_port, "/health/ready", 200).await;
    wait_status(default_port, "/unmatched", 404).await;

    let pool_port = port();
    let pool_url = format!("{url}?application_name=capacity_pool");
    let _pool = start(&pool_url, pool_port, true, "2");
    wait_status(pool_port, "/health/ready", 200).await;
    let (db, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let mut both_connected = false;
    for _ in 0..100 {
        both_connected = db
            .query_one(
                "SELECT count(*)=2 FROM pg_stat_activity WHERE application_name='capacity_pool'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if both_connected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(both_connected);
    let first = pending_request(pool_port);
    let second = pending_request(pool_port);
    wait_status(pool_port, "/unmatched", 503).await;
    assert_eq!(status(pool_port, "/health/ready"), Some(503));
    drop(first);
    wait_status(pool_port, "/health/ready", 200).await;
    assert_eq!(status(pool_port, "/unmatched"), Some(404));
    drop(second);

    let all_port = port();
    let _all = start_policy(&url, all_port, true, "2", "all");
    wait_status(all_port, "/health/ready", 200).await;
    let pending = pending_request(all_port);
    wait_status(all_port, "/health/ready", 503).await;
    assert_eq!(status(all_port, "/health/live"), Some(200));
    drop(pending);
    wait_status(all_port, "/health/ready", 200).await;
}
