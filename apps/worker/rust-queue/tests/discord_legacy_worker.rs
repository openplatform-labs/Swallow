//! Uses only a disposable PostgreSQL container and local worker binaries.
use novu_postgres_queue as queue;
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(binary: &str, url: &str, name: &str) -> Worker {
    Worker(
        Command::new(binary)
            .env_clear()
            .env("DATABASE_URL", format!("{url}?application_name={name}"))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_DISCORD_WORKFLOW_ENABLED", "true")
            .env("NOVU_IN_APP_DB_TIMEOUT_SECONDS", "10")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
#[tokio::test]
#[ignore = "requires migrated disposable Docker PostgreSQL and pre-fence worker binary"]
async fn legacy_process_is_fenced_and_current_process_creates_discord_job() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let old = std::env::var("TEST_OLD_IN_APP_BINARY").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let env = format!("discord-legacy-{}", Uuid::new_v4());
    let job = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE public.novu_rust_discord_workflow_policy SET enabled=true WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active,chat_content,inbox_metadata) VALUES($1,'workflow',true,true,false,'Hello {{name}}','{\"id\":\"workflow-id\"}')",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES($1,'alice','original')",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_destinations(environment_id,destination_id,endpoint_hash) VALUES($1,'ops','fixture')",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_workflow_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,destination_id) VALUES($1,'endpoint','alice','original','ops')",&[&env]).await.unwrap();
    let payload = json!({"workflow":"workflow","transactionId":job.to_string(),"content":"","payload":{"name":"Jane"},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"original"},"severityOverride":null});
    queue::enqueue(&db, job, &env, queue::in_app::JOB_KIND, &payload)
        .await
        .unwrap();
    let mut previous = worker(&old, &url, "discord_legacy_old");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    loop {
        let logs = Command::new("docker")
            .args(["logs", &container])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&logs.stderr)
            .contains("Discord workflow requires a compatible enabled worker")
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "legacy completion was not fenced"
        );
        assert!(
            previous.0.try_wait().unwrap().is_none(),
            "legacy worker exited early"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let row=db.query_one("SELECT status,(SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id=$2 AND kind=$3),(SELECT count(*) FROM public.novu_rust_notifications WHERE environment_id=$2) FROM public.novu_queue_jobs WHERE id=$1",&[&job,&env,&queue::discord_queue::WORKFLOW_KIND]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "running");
    assert_eq!(row.get::<_, i64>(1), 0);
    assert_eq!(row.get::<_, i64>(2), 0);
    previous.0.kill().unwrap();
    previous.0.wait().unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
    let mut current = worker(
        env!("CARGO_BIN_EXE_in-app-worker"),
        &url,
        "discord_legacy_current",
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    loop {
        let row=db.query_one("SELECT status,(SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id=$2 AND kind=$3) FROM public.novu_queue_jobs WHERE id=$1",&[&job,&env,&queue::discord_queue::WORKFLOW_KIND]).await.unwrap();
        if row.get::<_, String>(0) == "completed" && row.get::<_, i64>(1) == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "current worker did not recover"
        );
        assert!(
            current.0.try_wait().unwrap().is_none(),
            "current worker exited early"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_notifications WHERE environment_id=$1",
            &[&env]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
}
