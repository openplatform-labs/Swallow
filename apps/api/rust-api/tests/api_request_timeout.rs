//! Real API processes against disposable PostgreSQL; deliberately blocks a write.
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;

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
fn status(port: u16, path: &str) -> Option<u16> {
    let output = Command::new("curl")
        .args([
            "-s",
            "--max-time",
            "3",
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
#[test]
fn invalid_request_timeout_is_rejected() {
    for value in ["0", "301", "invalid"] {
        let out = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", "postgres://localhost/unused")
            .env("SWALLOW_API_REQUEST_TIMEOUT_SECONDS", value)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains("invalid API request timeout"));
    }
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL and curl"]
async fn times_out_slow_body_and_reports_indeterminate_blocked_write() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    for sql in [
        novu_rust_api::MIGRATION,
        include_str!("../../../worker/rust-queue/migrations/0061_workflow_email_provider.sql"),
        include_str!("../migrations/0011_workflow_provider_permissions.sql"),
        include_str!("../migrations/0014_workflow_provider_auth_lock.sql"),
        include_str!("../migrations/0015_workflow_provider_audit.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute("INSERT INTO novu_rust_users VALUES ('u'); INSERT INTO novu_rust_environments VALUES ('e','org'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps) VALUES ('e','w',true,true)").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"fixture-key"));
    db.execute(
        "INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'e','u')",
        &[&hash],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO novu_rust_workflow_provider_permissions VALUES ($1,'write')",
        &[&hash],
    )
    .await
    .unwrap();
    let port = port();
    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env(
                "DATABASE_URL",
                format!("{url}?application_name=timeout_test"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
            .env("SWALLOW_API_DB_CONNECTIONS", "1")
            .env("SWALLOW_API_MAX_INFLIGHT_PER_CONNECTION", "1")
            .env("SWALLOW_API_READY_REQUIRE_CAPACITY", "true")
            .env("SWALLOW_API_REQUEST_TIMEOUT_SECONDS", "1")
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
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_status(port, "/health/ready", 200).await;
    let mut slow = TcpStream::connect(("127.0.0.1", port)).unwrap();
    slow.set_read_timeout(Some(Duration::from_secs(4))).unwrap();
    slow.write_all(b"POST /v1/events/trigger HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{").unwrap();
    wait_status(port, "/health/ready", 503).await;
    let mut buffer = [0u8; 1024];
    let n = slow.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]).to_ascii_lowercase();
    assert!(response.contains("504 gateway timeout"));
    assert!(response.contains("cache-control: no-store"));
    wait_status(port, "/health/ready", 200).await;

    let tx = db.transaction().await.unwrap();
    tx.execute("UPDATE novu_rust_workflows SET email_provider='sendgrid' WHERE environment_id='e' AND identifier='w'",&[]).await.unwrap();
    let write = Command::new("curl")
        .args([
            "-s",
            "--max-time",
            "5",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-X",
            "PUT",
            "-H",
            "Authorization: ApiKey fixture-key",
            "-H",
            "Content-Type: application/json",
            "--data",
            r#"{"provider":"resend"}"#,
            &format!("http://127.0.0.1:{port}/v1/experimental/workflows/w/email-provider"),
        ])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut locked = false;
    for _ in 0..100 {
        locked=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='timeout_test' AND wait_event_type='Lock')",&[]).await.unwrap().get(0);
        if locked {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(locked);
    let output = write.wait_with_output().unwrap();
    assert_eq!(output.stdout, b"504");
    tx.commit().await.unwrap();
    let mut committed = false;
    for _ in 0..100 {
        committed=observer.query_one("SELECT email_provider='resend' FROM novu_rust_workflows WHERE environment_id='e' AND identifier='w'",&[]).await.unwrap().get(0);
        if committed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        committed,
        "a timed-out request can still commit after its DB lock is released"
    );
    assert_eq!(
        observer
            .query_one(
                "SELECT count(*) FROM novu_rust_workflow_provider_audit WHERE environment_id='e'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    wait_status(port, "/health/ready", 200).await;
}
