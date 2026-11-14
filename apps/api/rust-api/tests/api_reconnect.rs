//! Uses and restarts only the dedicated disposable PostgreSQL container.
use sha2::{Digest, Sha256};
use std::{
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
struct Paused(String);
impl Drop for Paused {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["unpause", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
fn status(port: u16) -> Option<u16> {
    let out = Command::new("curl")
        .args([
            "--max-time",
            "1",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            "Authorization: ApiKey fixture-key",
            &format!("http://127.0.0.1:{port}/v1/experimental/workflows/w/email-provider"),
        ])
        .output()
        .ok()?;
    std::str::from_utf8(&out.stdout).ok()?.parse().ok()
}
fn put_status(port: u16) -> Option<u16> {
    let out = Command::new("curl")
        .args([
            "--max-time",
            "1",
            "-s",
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
        .output()
        .ok()?;
    std::str::from_utf8(&out.stdout).ok()?.parse().ok()
}
async fn await_status(port: u16, expected: u16) {
    let mut last = None;
    for _ in 0..200 {
        last = status(port);
        if last == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("API did not return expected HTTP status {expected}; last={last:?}");
}
#[tokio::test]
#[ignore = "pauses and restarts a disposable PostgreSQL container"]
async fn api_reconnects_without_replaying_requests() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
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
        "INSERT INTO novu_rust_workflow_provider_permissions VALUES ($1,'read'),($1,'write')",
        &[&hash],
    )
    .await
    .unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    assert!(
        Command::new("docker")
            .args(["pause", &container])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let initially_paused = Paused(container.clone());
    let child = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("SWALLOW_API_DB_HEALTH_INTERVAL_SECONDS", "1")
        .env("SWALLOW_API_DB_HEALTH_TIMEOUT_SECONDS", "2")
        .env_remove("JWT_SECRET")
        .env_remove("JWT_SECRET_FILE")
        .env_remove("INBOX_HMAC_KEYS_JSON")
        .env_remove("NOVU_RESEND_WEBHOOK_SECRET")
        .env_remove("NOVU_RESEND_WEBHOOK_SECRET_FILE")
        .env_remove("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN")
        .env_remove("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN_FILE")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut server = Server(child);
    await_status(port, 503).await;
    drop(initially_paused);
    await_status(port, 200).await;
    assert!(
        Command::new("docker")
            .args(["pause", &container])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let paused = Paused(container.clone());
    await_status(port, 503).await;
    assert_eq!(put_status(port), Some(503));
    drop(paused);
    await_status(port, 200).await;
    let row=db.query_one("SELECT email_provider,(SELECT count(*) FROM public.novu_rust_workflow_provider_audit) FROM novu_rust_workflows WHERE environment_id='e' AND identifier='w'", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "sendgrid");
    assert_eq!(row.get::<_, i64>(1), 0);
    assert!(
        Command::new("docker")
            .args(["restart", &container])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    await_status(port, 200).await;
    assert!(server.0.try_wait().unwrap().is_none());
}
