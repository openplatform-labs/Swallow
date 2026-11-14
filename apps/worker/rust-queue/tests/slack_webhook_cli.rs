//! Fixture URLs are never contacted; only destination registration is exercised.
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn duplicate_destination_with_different_url_is_rejected() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
    for migration in [
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::slack_queue::MIGRATION,
        novu_postgres_queue::slack_webhook_queue::MIGRATION,
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    let command = |webhook: &str| {
        let mut c = Command::new(env!("CARGO_BIN_EXE_slack-webhook-worker"));
        c.env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SLACK_ENVIRONMENT_ID", "env")
            .env("NOVU_SLACK_WEBHOOK_DESTINATION_ID", "ops")
            .env("NOVU_SLACK_WEBHOOK_URL", webhook)
            .env_remove("NOVU_SLACK_WEBHOOK_URL_FILE")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        c
    };
    let mut first = Worker(
        command("https://hooks.slack.com/services/A/B/secretone")
            .spawn()
            .unwrap(),
    );
    let mut registered = false;
    for _ in 0..100 {
        registered=db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_slack_webhook_destinations WHERE environment_id='env' AND destination_id='ops')", &[]).await.unwrap().get(0);
        if registered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(registered && first.0.try_wait().unwrap().is_none());
    let output = command("https://hooks.slack.com/services/A/B/secrettwo")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("different endpoint"));
    assert!(!error.contains("secrettwo"));
    let stored: String=db.query_one("SELECT endpoint_hash FROM novu_rust_slack_webhook_destinations WHERE environment_id='env' AND destination_id='ops'",&[]).await.unwrap().get(0);
    assert!(!stored.contains("secretone"));
}
