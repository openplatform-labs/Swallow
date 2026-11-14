//! Exercises secret selection with a disposable database; sends no email.
use std::{
    fs,
    process::{Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;

struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn file_secret_starts_and_conflicts_are_rejected() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::sendgrid_queue::MIGRATION)
        .await
        .unwrap();
    let fixture = Fixture(
        std::env::temp_dir().join(format!("swallow-sendgrid-fixture-{}", uuid::Uuid::new_v4())),
    );
    fs::write(&fixture.0, "fixture-secret\n").unwrap();
    let command = || {
        let mut c = Command::new(env!("CARGO_BIN_EXE_sendgrid-worker"));
        c.env(
            "DATABASE_URL",
            format!("{url}?application_name=sendgrid_secret_fixture"),
        )
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_SENDGRID_ENVIRONMENT_ID", "env")
        .env("NOVU_SENDGRID_FROM", "from@example.test")
        .env("NOVU_SENDGRID_SENDER_NAME", "Fixture")
        .env_remove("NOVU_SENDGRID_API_KEY")
        .env_remove("NOVU_SENDGRID_API_KEY_FILE")
        .env_remove("NOVU_SENDGRID_REGION")
        .env_remove("NOVU_SENDGRID_DISPATCH_TRACKING")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
        c
    };
    let mut child = command()
        .env("NOVU_SENDGRID_API_KEY_FILE", &fixture.0)
        .spawn()
        .unwrap();
    let mut connected = false;
    for _ in 0..100 {
        connected = db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='sendgrid_secret_fixture')", &[]).await.unwrap().get(0);
        if connected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let alive = child.try_wait().unwrap().is_none();
    let _ = child.kill();
    let output = child.wait_with_output().unwrap();
    assert!(
        connected && alive,
        "worker did not connect with secret file"
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-secret"));
    let output = command()
        .env("NOVU_SENDGRID_API_KEY", "fixture-secret")
        .env("NOVU_SENDGRID_API_KEY_FILE", &fixture.0)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-secret"));
}
