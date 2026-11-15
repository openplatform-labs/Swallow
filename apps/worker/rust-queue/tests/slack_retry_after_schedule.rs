//! Explicit Slack 429 delayed retries using a restricted role and mock provider.
#![cfg(feature = "test-mocks")]
use novu_postgres_queue as queue;
use serde_json::json;
use std::process::{Command, Stdio};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db
}
async fn failed(db: &Client, env: &str, kind: &str, delay: Option<i32>) -> (Uuid, Uuid) {
    let job = Uuid::new_v4();
    queue::enqueue(db, job, env, kind, &json!({"channel":"C1","text":"hello"}))
        .await
        .unwrap();
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before) VALUES($1,$2,'rejected',429,$3,CASE WHEN $3::int IS NULL THEN NULL ELSE clock_timestamp()+make_interval(secs=>$3::int::float8) END) RETURNING attempt_id",&[&job,&env,&delay]).await.unwrap().get(0);
    (job, attempt)
}
fn cli(url: &str, env: &str, job: Uuid, attempt: Uuid) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"));
    c.env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "slack-retry-after",
            env,
            &job.to_string(),
            &attempt.to_string(),
            "test-operator",
            "ticket",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL with role creation"]
async fn slack_delayed_retry_is_fenced_scoped_and_atomic() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    let env = format!("slack-delay-{}", Uuid::new_v4());
    let role = format!("slackdelay_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_slack_after(text,uuid,uuid,text,text) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let (job, attempt) = failed(&db, &env, queue::slack_queue::JOB_KIND, Some(120)).await;
    assert!(
        !cli(role_url.as_str(), &env, job, attempt)
            .status()
            .unwrap()
            .success()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'slack_retry',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let (missing, missing_attempt) = failed(&db, &env, queue::slack_queue::JOB_KIND, None).await;
    assert!(
        !cli(role_url.as_str(), &env, missing, missing_attempt)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        !cli(role_url.as_str(), &env, job, Uuid::new_v4())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        !cli(role_url.as_str(), "wrong-env", job, attempt)
            .status()
            .unwrap()
            .success()
    );
    let (rollback, rollback_attempt) =
        failed(&db, &env, queue::slack_queue::JOB_KIND, Some(120)).await;
    db.batch_execute("ALTER TABLE public.novu_rust_slack_resolutions ADD CONSTRAINT test_slack_delay_reject CHECK(retry_not_before IS NULL) NOT VALID").await.unwrap();
    assert!(
        !cli(role_url.as_str(), &env, rollback, rollback_attempt)
            .status()
            .unwrap()
            .success()
    );
    db.batch_execute(
        "ALTER TABLE public.novu_rust_slack_resolutions DROP CONSTRAINT test_slack_delay_reject",
    )
    .await
    .unwrap();
    let result=db.query_one("SELECT j.status,(SELECT count(*) FROM public.novu_rust_slack_attempts WHERE job_id=j.id),(SELECT count(*) FROM public.novu_rust_slack_resolutions WHERE job_id=j.id) FROM public.novu_queue_jobs j WHERE id=$1",&[&rollback]).await.unwrap();
    assert_eq!(result.get::<_, String>(0), "failed");
    assert_eq!(result.get::<_, i64>(1), 1);
    assert_eq!(result.get::<_, i64>(2), 0);
    let before: String = db
        .query_one(
            "SELECT retry_not_before::text FROM public.novu_rust_slack_attempts WHERE job_id=$1",
            &[&job],
        )
        .await
        .unwrap()
        .get(0);
    let first = cli(role_url.as_str(), &env, job, attempt).spawn().unwrap();
    let second = cli(role_url.as_str(), &env, job, attempt).spawn().unwrap();
    let first = first.wait_with_output().unwrap();
    let second = second.wait_with_output().unwrap();
    assert_ne!(first.status.success(), second.status.success());
    let result=db.query_one("SELECT status,available_at>clock_timestamp()+interval '100 seconds',(SELECT retry_not_before::text FROM public.novu_rust_slack_resolutions WHERE job_id=$1),(SELECT count(*) FROM public.novu_rust_slack_resolutions WHERE job_id=$1) FROM public.novu_queue_jobs WHERE id=$1",&[&job]).await.unwrap();
    assert_eq!(result.get::<_, String>(0), "pending");
    assert!(result.get::<_, bool>(1));
    assert_eq!(result.get::<_, String>(2), before);
    assert_eq!(result.get::<_, i64>(3), 1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/", listener.local_addr().unwrap());
    let provider = queue::slack::Slack::test_endpoint("test-token".into(), &endpoint).unwrap();
    assert!(
        !queue::slack_queue::process_one(&mut db, &env, &provider)
            .await
            .unwrap()
    );
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0; 4096];
        assert!(socket.read(&mut buf).await.unwrap() > 0);
        let body = r#"{"ok":true,"channel":"C1","ts":"1"}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });
    assert!(
        queue::slack_queue::process_one(&mut db, &env, &provider)
            .await
            .unwrap()
    );
    server.await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "completed"
    );
    for kind in [
        queue::slack_webhook_queue::JOB_KIND,
        queue::slack_workflow::DELIVERY_KIND,
    ] {
        let (other, other_attempt) = failed(&db, &env, kind, Some(120)).await;
        assert!(
            cli(role_url.as_str(), &env, other, other_attempt)
                .status()
                .unwrap()
                .success()
        );
        assert!(db.query_one("SELECT available_at>clock_timestamp()+interval '100 seconds' FROM public.novu_queue_jobs WHERE id=$1",&[&other]).await.unwrap().get::<_,bool>(0));
    }
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='slack_retry'",&[&env,&role]).await.unwrap();
    let (revoked, revoked_attempt) =
        failed(&db, &env, queue::slack_queue::JOB_KIND, Some(120)).await;
    assert!(
        !cli(role_url.as_str(), &env, revoked, revoked_attempt)
            .status()
            .unwrap()
            .success()
    );
}
