//! Resolution of uncertain FCM attempts never resumes cancelled work.
use novu_postgres_queue as queue;
use serde_json::{Value, json};
use std::{path::Path, process::Command, time::Duration};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and roles"]
async fn resolutions_are_scoped_fenced_audited_and_do_not_dispatch() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("../../api/rust-api/migrations");
    let worker = root.join("migrations");
    for args in [
        vec!["apply", "api", api.to_str().unwrap(), "--through", "17"],
        vec!["apply", "worker", worker.to_str().unwrap()],
        vec!["apply", "api", api.to_str().unwrap()],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('a','org'),('b','org')")
        .await
        .unwrap();
    let role = format!("push_resolution_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture-password'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_cancelled_push(text,uuid,bigint,integer,text,text,text),public.novu_rust_list_cancelled_push_resolutions(text,uuid,bigint,integer),public.novu_rust_push_policy_evidence(text,uuid,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,p.permission,r.oid FROM pg_roles r CROSS JOIN (VALUES ('alert_view'),('accepted'),('retry')) p(permission) WHERE r.rolname::text=$1", &[&role]).await.unwrap();
    let id = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    for (job, env) in [(id, "a"), (foreign, "b")] {
        queue::enqueue(
            &db,
            job,
            env,
            queue::fcm_queue::JOB_KIND,
            &json!({"prepared_messages":[{},{},{}],"secret":"private"}),
        )
        .await
        .unwrap();
        db.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,$2,0,'unknown'),($1,$2,1,'unknown')", &[&job,&env]).await.unwrap();
        db.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='push preference recheck disabled' WHERE id=$1", &[&job]).await.unwrap();
    }
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("fixture-password")).unwrap();
    let (operator, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    assert!(
        operator
            .query("SELECT * FROM public.novu_rust_fcm_attempts", &[])
            .await
            .is_err()
    );
    assert!(
        queue::reconciliation::resolve_cancelled_push(
            &operator, "b", foreign, 1, 0, "accepted", "operator", "ticket"
        )
        .await
        .is_err()
    );
    assert!(
        queue::reconciliation::resolve_cancelled_push(
            &operator, "a", id, 0, 0, "accepted", "operator", "ticket"
        )
        .await
        .is_err()
    );
    assert!(
        queue::reconciliation::resolve_cancelled_push(
            &operator, "a", id, 1, 2, "accepted", "operator", "ticket"
        )
        .await
        .is_err()
    );
    assert!(
        queue::reconciliation::resolve_cancelled_push(
            &operator, "a", id, 1, 0, "invalid", "operator", "ticket"
        )
        .await
        .is_err()
    );
    let cli = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let out = cli(&[
        "resolve-cancelled-push",
        "a",
        &id.to_string(),
        "1",
        "0",
        "accepted",
        "operator",
        "ticket",
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        queue::reconciliation::resolve_cancelled_push(
            &operator, "a", id, 1, 0, "accepted", "operator", "ticket"
        )
        .await
        .is_err()
    );
    let tx = db.transaction().await.unwrap();
    tx.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=$1 AND decision='retry'", &[&role]).await.unwrap();
    let waiting = tokio::spawn(async move {
        queue::reconciliation::resolve_cancelled_push(
            &operator,
            "a",
            id,
            1,
            1,
            "not_accepted",
            "operator",
            "ticket",
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    tx.commit().await.unwrap();
    assert!(waiting.await.unwrap().is_err());
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'retry',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let (operator, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE FUNCTION public.test_reject_push_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test failure'; END; $$; CREATE TRIGGER test_reject_push_audit BEFORE INSERT ON public.novu_rust_cancelled_push_resolutions FOR EACH ROW EXECUTE FUNCTION public.test_reject_push_audit()").await.unwrap();
    assert!(
        queue::reconciliation::resolve_cancelled_push(
            &operator,
            "a",
            id,
            1,
            1,
            "not_accepted",
            "operator",
            "ticket"
        )
        .await
        .is_err()
    );
    assert_eq!(
        db.query_one(
            "SELECT state FROM public.novu_rust_fcm_attempts WHERE job_id=$1 AND target_index=1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    db.batch_execute(
        "DROP TRIGGER test_reject_push_audit ON public.novu_rust_cancelled_push_resolutions",
    )
    .await
    .unwrap();
    assert!(
        queue::reconciliation::resolve_cancelled_push(
            &operator,
            "a",
            id,
            1,
            1,
            "not_accepted",
            "operator",
            "ticket"
        )
        .await
        .is_ok()
    );
    let rows = queue::reconciliation::list_cancelled_push_resolutions(&operator, "a", id, 1, -1)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["decision"], "accepted");
    assert_eq!(rows[1]["decision"], "not_accepted");
    assert_eq!(
        queue::reconciliation::list_cancelled_push_resolutions(&operator, "a", id, 1, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    let listing = cli(&["list-cancelled-push-resolutions", "a", &id.to_string(), "1"]);
    assert!(listing.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&listing.stdout)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let row=db.query_one("SELECT (SELECT status FROM public.novu_queue_jobs WHERE id=$1),(SELECT count(*) FROM public.novu_rust_fcm_attempts WHERE job_id=$1),(SELECT count(*) FROM public.novu_rust_cancelled_push_resolutions WHERE job_id=$1 AND database_session_user=$2)", &[&id,&role]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "cancelled");
    assert_eq!(row.get::<_, i64>(1), 2);
    assert_eq!(row.get::<_, i64>(2), 2);
    let evidence = queue::reconciliation::push_policy_evidence(&operator, "a", id, 1)
        .await
        .unwrap();
    assert_eq!(
        evidence,
        json!({"accepted":1,"rejected":1,"unknown":0,"dispatching":0,"unattempted":1})
    );
    let race = Uuid::new_v4();
    queue::enqueue(&db, race, "a", queue::fcm_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,'a',0,'unknown')", &[&race]).await.unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='push workflow step disabled' WHERE id=$1", &[&race]).await.unwrap();
    let (operator2, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let (first, second) = tokio::join!(
        queue::reconciliation::resolve_cancelled_push(
            &operator, "a", race, 1, 0, "accepted", "first", "ticket"
        ),
        queue::reconciliation::resolve_cancelled_push(
            &operator2,
            "a",
            race,
            1,
            0,
            "not_accepted",
            "second",
            "ticket"
        )
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_cancelled_push_resolutions WHERE job_id=$1",
            &[&race]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&id],
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='push workflow step disabled' WHERE id=$1", &[&id]).await.unwrap();
    assert!(
        queue::reconciliation::list_cancelled_push_resolutions(&operator, "a", id, 1, -1)
            .await
            .is_err()
    );
    let prior = queue::reconciliation::list_cancelled_push_resolutions(&operator, "a", id, 2, -1)
        .await
        .unwrap();
    assert_eq!(prior.len(), 2);
    assert_eq!(prior[0]["revision"], "1");
}
