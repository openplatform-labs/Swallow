//! Uses fixture credentials and no external requests.
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
async fn workers_reject_different_maps_without_disclosing_tokens() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org'); CREATE TABLE novu_rust_workflows(environment_id text,identifier text); CREATE TABLE novu_rust_workflow_preferences(environment_id text,workflow_identifier text)").await.unwrap();
    for sql in [
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::slack_queue::MIGRATION,
        novu_postgres_queue::slack_workflow::MIGRATION,
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let command = |tokens: &str| {
        let mut c = Command::new(env!("CARGO_BIN_EXE_slack-workflow-worker"));
        c.env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SLACK_ENVIRONMENT_ID", "env")
            .env("NOVU_SLACK_WORKFLOW_TOKENS", tokens)
            .env_remove("NOVU_SLACK_WORKFLOW_TOKENS_FILE")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        c
    };
    let mut first = Worker(
        command(r#"{"main":{"":"fixture-secret","conn":"fixture-two"}}"#)
            .spawn()
            .unwrap(),
    );
    let mut registered = false;
    for _ in 0..100 {
        registered=db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_slack_workflow_worker_config WHERE environment_id='env')",&[]).await.unwrap().get(0);
        if registered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(registered && first.0.try_wait().unwrap().is_none());
    let mut same = Worker(
        command(r#"{"main":{"conn":"fixture-two","":"fixture-secret"}}"#)
            .spawn()
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(same.0.try_wait().unwrap().is_none());
    let output = command(r#"{"main":{"":"different-secret"}}"#)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("different credential maps"));
    assert!(!error.contains("different-secret"));
    let stored:String=db.query_one("SELECT credential_map_hash FROM novu_rust_slack_workflow_worker_config WHERE environment_id='env'",&[]).await.unwrap().get(0);
    assert!(!stored.contains("fixture-secret"));
}
