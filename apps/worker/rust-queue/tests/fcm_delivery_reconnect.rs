//! Uses a disposable PostgreSQL container and never contacts Slack.
use novu_postgres_queue as queue;
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    for _ in 0..100 {
        if let Ok((db, driver)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = driver.await;
            });
            return db;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("database did not become ready");
}
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(url: &str, application: &str) -> Worker {
    Worker(
        Command::new(env!("CARGO_BIN_EXE_fcm-worker"))
            .env(
                "DATABASE_URL",
                format!("{url}?application_name={application}"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_FCM_ENVIRONMENT_ID", "env")
            .env("NOVU_FCM_PROJECT_ID", "fixture")
            .env("NOVU_FCM_ACCESS_TOKEN", "fixture-token")
            .env("SWALLOW_FCM_ENFORCE_ROUTING_CONFIG", "true")
            .env_remove("SWALLOW_FCM_ALLOW_SHARED_PROVIDER")
            .env_remove("SWALLOW_PUSH_WORKFLOW_STEP_POLICY")
            .env_remove("SWALLOW_PUSH_PREFERENCE_POLICY")
            .env_remove("SWALLOW_PUSH_TARGET_POLICY")
            .env_remove("NOVU_FCM_ACCESS_TOKEN_FILE")
            .env_remove("NOVU_FCM_INTEGRATIONS")
            .env_remove("NOVU_FCM_INTEGRATIONS_FILE")
            .env("HTTPS_PROXY", "http://127.0.0.1:1")
            .env("NOVU_FCM_DB_TIMEOUT_SECONDS", "2")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

#[test]
fn invalid_timeout_is_rejected_before_connecting() {
    for timeout in ["0", "301", "bad"] {
        let status = Command::new(env!("CARGO_BIN_EXE_fcm-worker"))
            .env("DATABASE_URL", "postgresql://localhost/unused")
            .env("NOVU_FCM_ENVIRONMENT_ID", "env")
            .env("NOVU_FCM_PROJECT_ID", "fixture")
            .env("NOVU_FCM_ACCESS_TOKEN", "fixture-token")
            .env_remove("NOVU_FCM_ACCESS_TOKEN_FILE")
            .env_remove("NOVU_FCM_INTEGRATIONS")
            .env_remove("NOVU_FCM_INTEGRATIONS_FILE")
            .env("HTTPS_PROXY", "http://127.0.0.1:1")
            .env("NOVU_FCM_DB_TIMEOUT_SECONDS", timeout)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success());
    }
}

#[tokio::test]
#[ignore = "restarts a disposable PostgreSQL container"]
async fn workers_reconnect_and_preserve_uncertain_attempts() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let mut db = connect(&url).await;
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0124_push_workflow_worker_policy.sql"
    ))
    .await
    .unwrap();
    db.batch_execute("ALTER TABLE public.novu_rust_push_workflow_worker_config ADD COLUMN preference_policy text DEFAULT 'snapshot', ADD COLUMN target_policy text DEFAULT 'reference'").await.unwrap();
    db.batch_execute("CREATE TABLE novu_rust_operator_grant_admins(environment_id text,database_principal text,principal_oid oid)").await.unwrap();
    db.batch_execute(include_str!("../migrations/0095_fcm_routing_guard.sql"))
        .await
        .unwrap();
    db.batch_execute("INSERT INTO novu_rust_fcm_routing_workers SELECT 'env',rolname,oid FROM pg_roles WHERE rolname=session_user").await.unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(queue::fcm_queue::MIGRATION).await.unwrap();
    db.batch_execute(queue::in_app::PUSH_MESSAGES_MIGRATION)
        .await
        .unwrap();
    db.batch_execute("ALTER TABLE novu_rust_push_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
    let lock = db.transaction().await.unwrap();
    lock.batch_execute("LOCK TABLE novu_queue_jobs IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let observer = connect(&url).await;
    let mut first = worker(&url, "fcm_delivery_ha_1");
    let mut observed_lock = false;
    for _ in 0..100 {
        observed_lock = observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='fcm_delivery_ha_1' AND wait_event_type='Lock')", &[]).await.unwrap().get(0);
        if observed_lock {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(observed_lock, "worker did not encounter lock");
    let mut disconnected = false;
    for _ in 0..100 {
        disconnected = !observer
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='fcm_delivery_ha_1')",
                &[],
            )
            .await
            .unwrap()
            .get::<_, bool>(0);
        if disconnected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(disconnected, "worker did not abandon timed-out connection");
    lock.commit().await.unwrap();
    let mut second = worker(&url, "fcm_delivery_ha_2");
    let mut both = false;
    for _ in 0..100 {
        both = observer.query_one("SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('fcm_delivery_ha_1','fcm_delivery_ha_2')", &[]).await.unwrap().get(0);
        if both {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(both, "workers did not connect");
    assert!(
        Command::new("docker")
            .args(["restart", &container])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let mut db = connect(&url).await;
    let uncertain = Uuid::new_v4();
    let invalid = Uuid::new_v4();
    let transaction = db.transaction().await.unwrap();
    for job in [uncertain, invalid] {
        let payload = if job == uncertain {
            json!({"title":"Hi", "content":"Hello", "targets":["fake-a", "fake-b"]})
        } else {
            json!({})
        };
        transaction.execute("INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ($1,'env',$2,$3)", &[&job,&queue::fcm_queue::JOB_KIND,&payload]).await.unwrap();
    }
    transaction.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&uncertain, &Uuid::new_v4()]).await.unwrap();
    transaction.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,'env',0,'dispatching'),($1,'env',1,'accepted')", &[&uncertain]).await.unwrap();
    transaction.commit().await.unwrap();
    let mut recovered = false;
    for _ in 0..200 {
        let row = db.query_one("SELECT (SELECT status='failed' FROM novu_queue_jobs WHERE id=$1),(SELECT count(*)=2 AND count(*) FILTER (WHERE state='unknown')=1 AND count(*) FILTER (WHERE state='accepted')=1 FROM novu_rust_fcm_attempts WHERE job_id=$1),(SELECT status='failed' FROM novu_queue_jobs WHERE id=$2),(SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('fcm_delivery_ha_1','fcm_delivery_ha_2')),(SELECT count(*)=2 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name IN ('fcm_delivery_ha_1','fcm_delivery_ha_2') AND l.locktype='advisory' AND l.mode='ShareLock' AND l.granted)", &[&uncertain,&invalid]).await.unwrap();
        recovered = (0..5).all(|i| row.get::<_, Option<bool>>(i) == Some(true));
        if recovered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recovered
            && first.0.try_wait().unwrap().is_none()
            && second.0.try_wait().unwrap().is_none(),
        "workers did not safely recover after restart"
    );
}
