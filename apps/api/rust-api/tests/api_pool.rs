//! Multiple real API database connections, one deliberately blocked.
#![cfg(unix)]
use sha2::{Digest, Sha256};
use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio_postgres::NoTls;
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn http(port: u16, path: &str, token: Option<&str>, max: &str) -> Option<u16> {
    let mut command = Command::new("curl");
    command.args([
        "-s",
        "--max-time",
        max,
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
    ]);
    if let Some(value) = token {
        command.args(["-H", &format!("Authorization: SwallowConsole {value}")]);
    }
    let out = command
        .arg(format!("http://127.0.0.1:{port}{path}"))
        .output()
        .ok()?;
    std::str::from_utf8(&out.stdout).ok()?.parse().ok()
}
#[test]
fn invalid_database_connection_counts_are_rejected() {
    for count in ["0", "17", "bad"] {
        let output = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", "postgres://localhost/unused")
            .env("SWALLOW_API_DB_CONNECTIONS", count)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("invalid API database connection count")
        );
    }
}
#[test]
fn invalid_database_routing_is_rejected() {
    let output = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
        .env("DATABASE_URL", "postgres://localhost/unused")
        .env("SWALLOW_API_DB_ROUTING", "random-policy")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("invalid API database routing policy")
    );
}
#[test]
fn invalid_inflight_limits_are_rejected() {
    for limit in ["-1", "1001", "bad"] {
        let output = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", "postgres://localhost/unused")
            .env("SWALLOW_API_MAX_INFLIGHT_PER_CONNECTION", limit)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("invalid API inflight limit"));
    }
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and curl"]
async fn one_locked_connection_does_not_block_every_request_or_readiness() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('pool-env','org')")
        .await
        .unwrap();
    let a = "d".repeat(64);
    let b = "e".repeat(64);
    for token in [&a, &b] {
        let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
        db.execute("INSERT INTO novu_rust_console_credentials(token_hash,environment_id,expires_at) VALUES($1,'pool-env',clock_timestamp()+interval '1 hour')",&[&hash]).await.unwrap();
        db.execute("INSERT INTO novu_rust_console_alert_grants SELECT credential_id FROM novu_rust_console_credentials WHERE token_hash=$1",&[&hash]).await.unwrap();
    }
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut child = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"));
    child
        .env(
            "DATABASE_URL",
            format!("{url}?application_name=api_pool_test"),
        )
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
        .env("SWALLOW_API_DB_CONNECTIONS", "2")
        .env(
            "SWALLOW_API_DB_ROUTING",
            std::env::var("TEST_API_ROUTING").unwrap_or_else(|_| "least-inflight".into()),
        )
        .env("SWALLOW_API_MAX_INFLIGHT_PER_CONNECTION", "1")
        .env("SWALLOW_API_DB_HEALTH_INTERVAL_SECONDS", "30")
        .env("SWALLOW_API_DB_HEALTH_TIMEOUT_SECONDS", "2")
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
    let mut server = Server(child.spawn().unwrap());
    let mut connected = false;
    for _ in 0..100 {
        connected = observer
            .query_one(
                "SELECT count(*)=2 FROM pg_stat_activity WHERE application_name='api_pool_test'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if connected && http(port, "/health/ready", None, "1") == Some(200) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(connected);
    assert_eq!(http(port, "/health/ready", None, "1"), Some(200));
    let hash = format!("{:x}", Sha256::digest(a.as_bytes()));
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "UPDATE novu_rust_console_credentials SET enabled=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    let mut pending = Command::new("curl")
        .args([
            "-s",
            "--max-time",
            "10",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            &format!("Authorization: SwallowConsole {a}"),
            &format!("http://127.0.0.1:{port}/v1/experimental/console/alerts"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut blocked = false;
    for _ in 0..100 {
        blocked=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='api_pool_test' AND wait_event_type='Lock')",&[]).await.unwrap().get(0);
        if blocked {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(blocked);
    let before = Instant::now();
    assert_eq!(http(port, "/health/ready", None, "1"), Some(200));
    assert!(before.elapsed() < Duration::from_secs(2));
    for _ in 0..5 {
        assert_eq!(
            http(port, "/v1/experimental/console/alerts", Some(&b), "1"),
            Some(200),
            "an idle connection should serve repeated requests while the other is blocked"
        );
    }
    assert!(pending.try_wait().unwrap().is_none());
    tx.commit().await.unwrap();
    assert_eq!(pending.wait_with_output().unwrap().stdout, b"200");
    // Concurrent arrivals should occupy different idle connections rather
    // than both selecting the same one before the counters are incremented.
    let hash_b = format!("{:x}", Sha256::digest(b.as_bytes()));
    let tx = db.transaction().await.unwrap();
    tx.execute("UPDATE novu_rust_console_credentials SET enabled=true WHERE token_hash=$1 OR token_hash=$2", &[&hash,&hash_b]).await.unwrap();
    let spawn_request = |token: &str| {
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
                &format!("Authorization: SwallowConsole {token}"),
                &format!("http://127.0.0.1:{port}/v1/experimental/console/alerts"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    };
    let one = spawn_request(&a);
    let two = spawn_request(&b);
    let mut both_blocked = false;
    for _ in 0..100 {
        both_blocked=observer.query_one("SELECT count(*)=2 FROM pg_stat_activity WHERE application_name='api_pool_test' AND wait_event_type='Lock'",&[]).await.unwrap().get(0);
        if both_blocked {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        both_blocked,
        "concurrent requests did not use both connections"
    );
    let busy = Command::new("curl")
        .args([
            "-s",
            "-i",
            "--max-time",
            "1",
            "-H",
            &format!("Authorization: SwallowConsole {b}"),
            &format!("http://127.0.0.1:{port}/v1/experimental/console/alerts"),
        ])
        .output()
        .unwrap();
    assert!(busy.status.success());
    let response = String::from_utf8(busy.stdout).unwrap().to_ascii_lowercase();
    assert!(response.contains("503 service unavailable"));
    assert!(response.contains("retry-after: 1"));
    assert!(response.contains("cache-control: no-store"));
    assert!(response.contains("server busy"));
    assert_eq!(http(port, "/health/live", None, "1"), Some(200));
    tx.commit().await.unwrap();
    assert_eq!(one.wait_with_output().unwrap().stdout, b"200");
    assert_eq!(two.wait_with_output().unwrap().stdout, b"200");
    assert_eq!(
        http(port, "/v1/experimental/console/alerts", Some(&b), "1"),
        Some(200)
    );
    assert!(
        Command::new("kill")
            .args(["-TERM", &server.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    for _ in 0..100 {
        if let Some(status) = server.0.try_wait().unwrap() {
            assert!(status.success());
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("API did not shut down");
}
