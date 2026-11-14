//! Two workers reconnect after a synchronous PostgreSQL standby is promoted.
use novu_postgres_queue::{self as queue, in_app};
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
    panic!("database unavailable")
}
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(url: &str, name: &str) -> Worker {
    Worker(
        Command::new(env!("CARGO_BIN_EXE_in-app-worker"))
            .env("DATABASE_URL", format!("{url}&application_name={name}"))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_ALLOW_RECREATED_SUBSCRIBER", "false")
            .env("NOVU_IN_APP_DB_TIMEOUT_SECONDS", "10")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
#[tokio::test]
#[ignore = "requires disposable synchronously replicated PostgreSQL containers; destroys the primary"]
async fn synchronous_standby_promotion_recovers_pending_and_leased_work() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let standby = std::env::var("TEST_STANDBY_CONTAINER").unwrap();
    let db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('ha-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES('ha-env','ha-workflow',true,true,'Hello {{subscriber.firstName}}','{\"id\":\"64a000000000000000000051\",\"name\":\"Fixture\",\"critical\":false,\"tags\":[]}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES('ha-env','alice','64a000000000000000000054','{\"firstName\":\"Alice\"}')").await.unwrap();
    let pending = Uuid::new_v4();
    let leased = Uuid::new_v4();
    for (id, txn) in [(pending, "ha-pending"), (leased, "ha-lease")] {
        let body = json!({"workflow":"ha-workflow","transactionId":txn,"content":"Hello {{subscriber.firstName}}","payload":{},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"64a000000000000000000054"}});
        queue::enqueue(&db, id, "ha-env", in_app::JOB_KIND, &body)
            .await
            .unwrap();
    }
    let lease = Uuid::new_v4();
    db.execute(
        "UPDATE public.novu_queue_jobs SET available_at='infinity' WHERE id=$1",
        &[&pending],
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '1 hour',attempts=1 WHERE id=$1",&[&leased,&lease]).await.unwrap();
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        let row=replica.query_one("SELECT pg_is_in_recovery(),(SELECT count(*) FROM public.novu_queue_jobs WHERE id=ANY($1::uuid[]))",&[&vec![pending,leased]]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        replicated = row.get::<_, i64>(1) == 2;
        if replicated {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(replicated, "standby did not replay committed jobs");
    // The standby is listed first in cluster_url and must be skipped while read-only.
    let mut first = worker(&cluster_url, "failover_worker_1");
    let mut second = worker(&cluster_url, "failover_worker_2");
    let mut started = false;
    for _ in 0..150 {
        started=db.query_one("SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('failover_worker_1','failover_worker_2')",&[]).await.unwrap().get(0);
        if started {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(started, "workers did not connect to writable primary");
    assert!(
        Command::new("docker")
            .args(["kill", &primary])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("docker")
            .args([
                "exec",
                "-u",
                "postgres",
                &standby,
                "pg_ctl",
                "promote",
                "-D",
                "/var/lib/postgresql/data",
                "-w"
            ])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let promoted = connect(&standby_url).await;
    assert!(
        !promoted
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let rows=promoted.query_one("SELECT (SELECT count(*) FROM public.novu_queue_jobs WHERE id=ANY($1::uuid[])),(SELECT count(*) FROM public.novu_rust_in_app_messages WHERE environment_id='ha-env')",&[&vec![pending,leased]]).await.unwrap();
    assert_eq!(rows.get::<_, i64>(0), 2);
    assert_eq!(rows.get::<_, i64>(1), 0);
    let lease_state = promoted
        .query_one(
            "SELECT status,lease_token FROM public.novu_queue_jobs WHERE id=$1",
            &[&leased],
        )
        .await
        .unwrap();
    assert_eq!(lease_state.get::<_, String>(0), "running");
    assert_eq!(lease_state.get::<_, Option<Uuid>>(1), Some(lease));
    let pending_state=promoted.query_one("SELECT status,available_at='infinity'::timestamptz FROM public.novu_queue_jobs WHERE id=$1",&[&pending]).await.unwrap();
    assert_eq!(pending_state.get::<_, String>(0), "pending");
    assert!(pending_state.get::<_, bool>(1));
    promoted
        .execute(
            "UPDATE public.novu_queue_jobs SET available_at=clock_timestamp() WHERE id=$1",
            &[&pending],
        )
        .await
        .unwrap();
    promoted.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&leased]).await.unwrap();
    let mut recovered = false;
    for _ in 0..250 {
        let row=promoted.query_one("SELECT (SELECT count(*)=2 FROM public.novu_queue_jobs WHERE id=ANY($1::uuid[]) AND status='completed'),(SELECT count(*)=2 FROM public.novu_rust_in_app_messages WHERE environment_id='ha-env' AND subscriber_id='alice'),(SELECT count(*)=2 FROM public.novu_rust_notifications WHERE job_id=ANY($1::uuid[])),(SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('failover_worker_1','failover_worker_2'))",&[&vec![pending,leased]]).await.unwrap();
        recovered = (0..4).all(|i| row.get::<_, bool>(i));
        if recovered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recovered
            && first.0.try_wait().unwrap().is_none()
            && second.0.try_wait().unwrap().is_none(),
        "workers failed to recover after promotion"
    );
}
