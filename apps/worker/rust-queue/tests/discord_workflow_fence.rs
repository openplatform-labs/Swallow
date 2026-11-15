//! Policy changes serialize with legacy completion and enabled workers fence commits.
use novu_postgres_queue as queue;
use serde_json::json;
use std::time::Duration;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(driver);
    db
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn policy_fences_legacy_workers_and_serializes_enablement() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO public.novu_rust_environments VALUES('a','org')").await.unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0206_discord_workflow_fence.sql"
    ))
    .await
    .unwrap();
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('b','org')")
        .await
        .unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_discord_workflow_policy WHERE NOT enabled",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
    let role = format!("discord_fence_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} NOLOGIN; GRANT SELECT,UPDATE ON public.novu_queue_jobs TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_workflow_guard(text,uuid,uuid) TO {role}")).await.unwrap();
    let mut worker = connect(&url).await;
    worker
        .batch_execute(&format!("SET ROLE {role}"))
        .await
        .unwrap();
    assert!(
        worker
            .query(
                "SELECT * FROM public.novu_rust_discord_workflow_policy",
                &[]
            )
            .await
            .is_err()
    );
    let old = Uuid::new_v4();
    queue::enqueue(&db, old, "a", queue::in_app::JOB_KIND, &json!({}))
        .await
        .unwrap();
    let lease = Uuid::new_v4();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '60 seconds' WHERE id=$1",&[&old,&lease]).await.unwrap();
    let tx = worker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_queue_jobs SET status='completed',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&old]).await.unwrap();
    let admin = connect(&url).await;
    let enabling = tokio::spawn(async move {
        admin.execute("UPDATE public.novu_rust_discord_workflow_policy SET enabled=true WHERE environment_id='a'",&[]).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !enabling.is_finished(),
        "enablement should wait for the old completion"
    );
    tx.commit().await.unwrap();
    assert_eq!(enabling.await.unwrap(), 1);
    let job = Uuid::new_v4();
    queue::enqueue(&db, job, "a", queue::in_app::JOB_KIND, &json!({}))
        .await
        .unwrap();
    let token = Uuid::new_v4();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '60 seconds' WHERE id=$1",&[&job,&token]).await.unwrap();
    assert!(worker.execute("UPDATE public.novu_queue_jobs SET status='completed',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job]).await.is_err());
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "running"
    );
    assert!(
        worker
            .query_one(
                "SELECT public.novu_rust_discord_workflow_guard($1,$2,$3)",
                &[&"a", &job, &Uuid::new_v4()]
            )
            .await
            .is_err()
    );
    worker
        .query_one(
            "SELECT public.novu_rust_discord_workflow_guard($1,$2,$3)",
            &[&"a", &job, &token],
        )
        .await
        .unwrap();
    assert!(worker.execute("UPDATE public.novu_queue_jobs SET status='completed',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job]).await.is_err());
    let tx = worker.transaction().await.unwrap();
    tx.query_one(
        "SELECT public.novu_rust_discord_workflow_guard($1,$2,$3)",
        &[&"a", &job, &token],
    )
    .await
    .unwrap();
    tx.execute("UPDATE public.novu_queue_jobs SET status='completed',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job]).await.unwrap();
    tx.commit().await.unwrap();
    let off = Uuid::new_v4();
    queue::enqueue(&db, off, "b", queue::in_app::JOB_KIND, &json!({}))
        .await
        .unwrap();
    let off_token = Uuid::new_v4();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '60 seconds' WHERE id=$1",&[&off,&off_token]).await.unwrap();
    assert!(
        !worker
            .query_one(
                "SELECT public.novu_rust_discord_workflow_guard($1,$2,$3)",
                &[&"b", &off, &off_token]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(worker.execute("UPDATE public.novu_queue_jobs SET status='completed',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&off]).await.unwrap(),1);
}
