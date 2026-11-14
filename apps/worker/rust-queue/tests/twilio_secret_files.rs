//! Uses an isolated PostgreSQL database and fixture credentials; sends no SMS.
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
async fn twilio_worker_accepts_files_and_rejects_conflicts() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    db.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY); INSERT INTO novu_rust_environments VALUES ('env')").await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0121_sms_workflow_worker_policy.sql"
    ))
    .await
    .unwrap();
    let path =
        std::env::temp_dir().join(format!("swallow-twilio-fixture-{}", uuid::Uuid::new_v4()));
    let fixture = Fixture(path);
    fs::write(&fixture.0, "fixture-token\n").unwrap();
    let command = || {
        let mut c = Command::new(env!("CARGO_BIN_EXE_twilio-worker"));
        c.env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_TWILIO_ENVIRONMENT_ID", "env")
            .env(
                "NOVU_TWILIO_ACCOUNT_SID",
                "AC11111111111111111111111111111111",
            )
            .env("NOVU_TWILIO_FROM", "+15550000000")
            .env_remove("NOVU_TWILIO_AUTH_TOKEN")
            .env_remove("NOVU_TWILIO_AUTH_TOKEN_FILE")
            .env_remove("NOVU_TWILIO_API_KEY_SID")
            .env_remove("NOVU_TWILIO_API_KEY_SECRET")
            .env_remove("NOVU_TWILIO_API_KEY_SECRET_FILE")
            .env_remove("NOVU_TWILIO_STATUS_CALLBACK_BASE_URL")
            .env_remove("NOVU_TWILIO_REGION")
            .env_remove("SWALLOW_SMS_WORKFLOW_STEP_POLICY")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        c
    };
    for api_key in [false, true] {
        let mut c = command();
        if api_key {
            c.env("NOVU_TWILIO_API_KEY_SID", "SKfixture")
                .env("NOVU_TWILIO_API_KEY_SECRET_FILE", &fixture.0);
        } else {
            c.env("NOVU_TWILIO_AUTH_TOKEN_FILE", &fixture.0);
        }
        let mut child = c.spawn().unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let alive = child.try_wait().unwrap().is_none();
        let _ = child.kill();
        let output = child.wait_with_output().unwrap();
        assert!(alive, "worker failed to start with a fixture secret file");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-token"));
    }
    let output = command()
        .env("NOVU_TWILIO_AUTH_TOKEN", "fixture-token")
        .env("NOVU_TWILIO_AUTH_TOKEN_FILE", &fixture.0)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-token"));
}
