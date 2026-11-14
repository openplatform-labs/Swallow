//! Scoped failure review with concurrent changes on a disposable PostgreSQL instance.
use novu_postgres_queue as queue;
use serde_json::json;
use std::{path::Path, process::Command, time::Duration};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and roles"]
async fn reviewed_failure_revision_fences_acknowledgement() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("../../api/rust-api/migrations");
    let worker = root.join("migrations");
    for args in [
        vec!["apply", "api", api.to_str().unwrap(), "--through", "17"],
        vec!["apply", "worker", worker.to_str().unwrap()],
        vec!["apply", "api", api.to_str().unwrap()],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('a','org'),('b','org')")
        .await
        .unwrap();
    let role = format!("failure_review_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_failed_queue_count(text),public.novu_rust_failed_queue_list_v2(text,uuid),public.novu_rust_ack_failed_queue_v2(text,uuid,bigint,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) p WHERE rolname::text=$1", &[&role]).await.unwrap();
    let job = Uuid::new_v4();
    queue::enqueue(&db, job, "a", "fixture", &json!({"private":"payload"}))
        .await
        .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='private diagnostic' WHERE id=$1", &[&job]).await.unwrap();
    let (actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let first = queue::reconciliation::failed_queue_list_v2(&actor, "a", None)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert!(!format!("{first:?}").contains("private"));
    assert!(
        queue::reconciliation::failed_queue_list_v2(&actor, "b", None)
            .await
            .is_err()
    );
    assert!(
        queue::reconciliation::acknowledge_failed_queue_v2(
            &actor, "b", job, 1, "operator", "review"
        )
        .await
        .is_err()
    );
    assert!(
        actor
            .query("SELECT * FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    let revision = first[0]["revision"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    // Hold the job lock and replace the failure while the operator is waiting.
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='private diagnostic' WHERE id=$1", &[&job]).await.unwrap();
    let pending = tokio::spawn(async move {
        let result = queue::reconciliation::acknowledge_failed_queue_v2(
            &actor,
            "a",
            job,
            revision,
            "operator",
            "stale review",
        )
        .await;
        (actor, result)
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    let (actor, result) = pending.await.unwrap();
    assert_eq!(
        result
            .unwrap_err()
            .downcast_ref::<tokio_postgres::Error>()
            .unwrap()
            .code()
            .unwrap()
            .code(),
        "40001"
    );
    assert_eq!(
        queue::reconciliation::failed_queue_count(&actor, "a")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_queue_failure_acknowledgements WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let current = queue::reconciliation::failed_queue_list_v2(&actor, "a", None)
        .await
        .unwrap();
    let revision2 = current[0]["revision"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(revision2 > revision);
    assert!(
        queue::reconciliation::acknowledge_failed_queue_v2(
            &actor, "a", job, revision2, "operator", "reviewed"
        )
        .await
        .unwrap()
    );
    assert!(
        !queue::reconciliation::acknowledge_failed_queue_v2(
            &actor,
            "a",
            job,
            revision2,
            "operator",
            "duplicate"
        )
        .await
        .unwrap()
    );
    assert_eq!(
        queue::reconciliation::failed_queue_count(&actor, "a")
            .await
            .unwrap(),
        0
    );
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let actor_url = url.replacen(
        "postgres://postgres@",
        &format!("postgres://{role}:fixture@"),
        1,
    );
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let listing = run(&["list-failed-jobs-v2", "a"]);
    assert!(listing.status.success());
    let jobs: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    let revision3 = jobs[0]["revision"].as_str().unwrap();
    assert!(
        !run(&[
            "ack-failed-job-v2",
            "a",
            &job.to_string(),
            &revision2.to_string(),
            "operator",
            "stale"
        ])
        .status
        .success()
    );
    assert!(
        run(&[
            "ack-failed-job-v2",
            "a",
            &job.to_string(),
            revision3,
            "operator",
            "reviewed"
        ])
        .status
        .success()
    );
    assert_eq!(
        queue::reconciliation::failed_queue_count(&actor, "a")
            .await
            .unwrap(),
        0
    );
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) SELECT gen_random_uuid(),'a','fixture','{}'::jsonb,'failed' FROM generate_series(1,101)", &[]).await.unwrap();
    let page = queue::reconciliation::failed_queue_list_v2(&actor, "a", None)
        .await
        .unwrap();
    assert_eq!(page.len(), 100);
    let cursor = Uuid::parse_str(page[99]["jobId"].as_str().unwrap()).unwrap();
    assert_eq!(
        queue::reconciliation::failed_queue_list_v2(&actor, "a", Some(cursor))
            .await
            .unwrap()
            .len(),
        1
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=$1 AND decision='queue_ack'", &[&role]).await.unwrap();
    assert!(
        queue::reconciliation::acknowledge_failed_queue_v2(
            &actor,
            "a",
            job,
            revision3.parse().unwrap(),
            "operator",
            "revoked"
        )
        .await
        .is_err()
    );
}
