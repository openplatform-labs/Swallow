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
        include_str!("../migrations/0113_slack_workflow_policy.sql"),
        include_str!("../migrations/0115_slack_endpoint_policy.sql"),
        include_str!("../migrations/0148_slack_revision_policy.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let command = |tokens: &str| {
        let mut c = Command::new(env!("CARGO_BIN_EXE_slack-workflow-worker"));
        c.env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SLACK_ENVIRONMENT_ID", "env")
            .env("NOVU_SLACK_WORKFLOW_TOKENS", tokens)
            .env_remove("NOVU_SLACK_WORKFLOW_STEP_POLICY")
            .env_remove("NOVU_SLACK_WORKFLOW_ENDPOINT_POLICY")
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
    let mismatch = command(r#"{"main":{"":"fixture-secret","conn":"fixture-two"}}"#)
        .env("NOVU_SLACK_WORKFLOW_STEP_POLICY", "active")
        .output()
        .unwrap();
    assert!(!mismatch.status.success());
    assert!(String::from_utf8_lossy(&mismatch.stderr).contains("workflow policies"));
    let endpoint_mismatch = command(r#"{"main":{"":"fixture-secret","conn":"fixture-two"}}"#)
        .env("NOVU_SLACK_WORKFLOW_ENDPOINT_POLICY", "snapshot")
        .output()
        .unwrap();
    assert!(!endpoint_mismatch.status.success());
    assert!(String::from_utf8_lossy(&endpoint_mismatch.stderr).contains("workflow policies"));
    let invalid = command(r#"{"main":{"":"fixture-secret"}}"#)
        .env("NOVU_SLACK_WORKFLOW_STEP_POLICY", "invalid")
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    assert!(
        String::from_utf8_lossy(&invalid.stderr).contains("invalid Slack workflow step policy")
    );
    let stored:String=db.query_one("SELECT credential_map_hash FROM novu_rust_slack_workflow_worker_config WHERE environment_id='env'",&[]).await.unwrap().get(0);
    assert!(!stored.contains("fixture-secret"));
    first.0.kill().unwrap();
    first.0.wait().unwrap();
    same.0.kill().unwrap();
    same.0.wait().unwrap();
    db.execute("UPDATE novu_rust_slack_workflow_worker_config SET step_policy=NULL WHERE environment_id='env'", &[]).await.unwrap();
    let mut active = Worker(
        command(r#"{"main":{"":"fixture-secret","conn":"fixture-two"}}"#)
            .env("NOVU_SLACK_WORKFLOW_STEP_POLICY", "active")
            .spawn()
            .unwrap(),
    );
    let mut selected = false;
    for _ in 0..100 {
        selected=db.query_one("SELECT step_policy='active' FROM novu_rust_slack_workflow_worker_config WHERE environment_id='env'", &[]).await.unwrap().get::<_,Option<bool>>(0)==Some(true);
        if selected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(selected && active.0.try_wait().unwrap().is_none());
    active.0.kill().unwrap();
    active.0.wait().unwrap();
    db.execute("UPDATE novu_rust_slack_workflow_worker_config SET endpoint_policy=NULL WHERE environment_id='env'", &[]).await.unwrap();
    let mut snapshot = Worker(
        command(r#"{"main":{"":"fixture-secret","conn":"fixture-two"}}"#)
            .env("NOVU_SLACK_WORKFLOW_STEP_POLICY", "active")
            .env("NOVU_SLACK_WORKFLOW_ENDPOINT_POLICY", "snapshot")
            .spawn()
            .unwrap(),
    );
    let mut selected = false;
    for _ in 0..100 {
        selected=db.query_one("SELECT endpoint_policy='snapshot' FROM novu_rust_slack_workflow_worker_config WHERE environment_id='env'", &[]).await.unwrap().get::<_,Option<bool>>(0)==Some(true);
        if selected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(selected && snapshot.0.try_wait().unwrap().is_none());
    let mismatch = command(r#"{"main":{"":"fixture-secret","conn":"fixture-two"}}"#)
        .env("NOVU_SLACK_WORKFLOW_STEP_POLICY", "active")
        .env("NOVU_SLACK_WORKFLOW_ENDPOINT_POLICY", "snapshot-revision")
        .output()
        .unwrap();
    assert!(!mismatch.status.success());
    snapshot.0.kill().unwrap();
    snapshot.0.wait().unwrap();
    db.execute("UPDATE novu_rust_slack_workflow_worker_config SET endpoint_policy=NULL WHERE environment_id='env'", &[]).await.unwrap();
    let mut strict = Worker(
        command(r#"{"main":{"":"fixture-secret","conn":"fixture-two"}}"#)
            .env("NOVU_SLACK_WORKFLOW_STEP_POLICY", "active")
            .env("NOVU_SLACK_WORKFLOW_ENDPOINT_POLICY", "snapshot-revision")
            .spawn()
            .unwrap(),
    );
    let mut selected = false;
    for _ in 0..100 {
        selected=db.query_one("SELECT endpoint_policy='snapshot-revision' FROM novu_rust_slack_workflow_worker_config WHERE environment_id='env'",&[]).await.unwrap().get::<_,Option<bool>>(0)==Some(true);
        if selected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(selected && strict.0.try_wait().unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn strict_worker_rejects_legacy_jobs_without_external_dispatch() {
    use std::path::Path;
    use uuid::Uuid;
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (dir, start, end) in [
        (root.join("../../api/rust-api/migrations"), 1, 17),
        (root.join("migrations"), 1, u32::MAX),
        (root.join("../../api/rust-api/migrations"), 18, u32::MAX),
    ] {
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect();
        files.sort();
        for file in files {
            let name = file.file_name().unwrap().to_str().unwrap();
            let version = name[..4].parse::<u32>().unwrap();
            if (start..=end).contains(&version) {
                db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
            }
        }
    }
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('strict-env','org'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('strict-env','alice','alice-id'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,inbox_metadata,active,has_steps) VALUES ('strict-env','workflow','{\"id\":\"workflow-id\"}',true,true)").await.unwrap();
    let endpoint = Uuid::new_v4();
    let context = serde_json::json!({"subscriber_id":"alice","subscriber_internal_id":"alice-id","workflow_identifier":"workflow","workflow_internal_id":"workflow-id","content_template":"hello","payload":{},"context_keys":[],"actor":null,"actor_internal_id":null});
    let mut legacy_parent = context.clone();
    legacy_parent["endpoint_snapshot"] = serde_json::json!([{"endpoint_id":endpoint,"endpoint_type":"slack_channel","endpoint":{"channelId":"C123"},"integration":"main","connection":null}]);
    let legacy_child = serde_json::json!({"context":context,"endpoint_id":endpoint,"target":"C123","integration":"main","connection":null});
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        parent,
        "strict-env",
        novu_postgres_queue::slack_workflow::FANOUT_KIND,
        &legacy_parent,
    )
    .await
    .unwrap();
    novu_postgres_queue::enqueue(
        &db,
        child,
        "strict-env",
        novu_postgres_queue::slack_workflow::DELIVERY_KIND,
        &legacy_child,
    )
    .await
    .unwrap();
    let mut worker = Worker(
        Command::new(env!("CARGO_BIN_EXE_slack-workflow-worker"))
            .env_clear()
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SLACK_ENVIRONMENT_ID", "strict-env")
            .env(
                "NOVU_SLACK_WORKFLOW_TOKENS",
                r#"{"main":{"":"fixture-token"}}"#,
            )
            .env("NOVU_SLACK_WORKFLOW_ENDPOINT_POLICY", "snapshot-revision")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut done = false;
    for _ in 0..150 {
        done=db.query_one("SELECT count(*)=2 FROM public.novu_queue_jobs WHERE id=ANY($1) AND status='failed' AND last_error='Slack endpoint revision missing'",&[&vec![parent,child]]).await.unwrap().get(0);
        if done {
            break;
        }
        assert!(worker.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(done, "strict worker did not reject both legacy jobs");
    assert_eq!(
        db.query_one("SELECT count(*) FROM public.novu_rust_slack_attempts", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert!(
        novu_postgres_queue::slack_queue::health(&db, "strict-env")
            .await
            .unwrap()["preDispatchFailures"]
            .as_i64()
            .unwrap()
            >= 2
    );
}
