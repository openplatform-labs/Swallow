//! Operator-selected delayed retry against a migrated disposable PostgreSQL database.
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
    let (client, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    client
}

async fn rejected(db: &Client, env: &str, delay: Option<i32>) -> Uuid {
    let job = Uuid::new_v4();
    queue::enqueue(
        db,
        job,
        env,
        queue::resend_queue::JOB_KIND,
        &json!({"to":["test@example.test"],"subject":"test","html":"test"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before) VALUES ($1,$2,'rejected',429,$3,CASE WHEN $3::int IS NULL THEN NULL ELSE clock_timestamp()+make_interval(secs=>$3::int::float8) END)", &[&job,&env,&delay]).await.unwrap();
    job
}

fn cli(url: &str, env: &str, job: Uuid) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"));
    command
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "resend-retry-after",
            env,
            &job.to_string(),
            "test-operator",
            "fixture-ticket",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL with role creation"]
async fn delayed_retry_is_scoped_atomic_audited_and_concurrency_safe() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    let env = format!("resend-delay-{}", Uuid::new_v4());
    let role = format!("delay_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_resend_after(text,uuid,text,text) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let job = rejected(&db, &env, Some(120)).await;
    assert!(
        !cli(role_url.as_str(), &env, job)
            .status()
            .unwrap()
            .success()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'retry',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    let missing = rejected(&db, &env, None).await;
    assert!(
        !cli(role_url.as_str(), &env, missing)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        !cli(role_url.as_str(), "wrong-environment", job)
            .status()
            .unwrap()
            .success()
    );
    // A signed callback for the provider ID blocks a retry through the same
    // evidence checks as the existing immediate-resolution path.
    let evidenced = rejected(&db, &env, Some(120)).await;
    db.execute("UPDATE public.novu_rust_resend_attempts SET provider_message_id='provider-proof' WHERE job_id=$1", &[&evidenced]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ($1,'fixture-event','provider-proof','email.delivered')", &[&env]).await.unwrap();
    assert!(
        !cli(role_url.as_str(), &env, evidenced)
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&evidenced]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    // Failure to record the added audit field must roll back the whole decision.
    let rollback = rejected(&db, &env, Some(120)).await;
    db.batch_execute("ALTER TABLE public.novu_rust_delivery_reconciliations ADD CONSTRAINT fixture_reject_delay CHECK (resend_retry_not_before IS NULL) NOT VALID").await.unwrap();
    assert!(
        !cli(role_url.as_str(), &env, rollback)
            .status()
            .unwrap()
            .success()
    );
    db.batch_execute("ALTER TABLE public.novu_rust_delivery_reconciliations DROP CONSTRAINT fixture_reject_delay").await.unwrap();
    let rolled_back=db.query_one("SELECT j.status,(SELECT count(*) FROM public.novu_rust_resend_attempts WHERE job_id=j.id),(SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id=j.id) FROM public.novu_queue_jobs j WHERE j.id=$1", &[&rollback]).await.unwrap();
    assert_eq!(rolled_back.get::<_, String>(0), "failed");
    assert_eq!(rolled_back.get::<_, i64>(1), 1);
    assert_eq!(rolled_back.get::<_, i64>(2), 0);
    let original: String = db
        .query_one(
            "SELECT retry_not_before::text FROM public.novu_rust_resend_attempts WHERE job_id=$1",
            &[&job],
        )
        .await
        .unwrap()
        .get(0);
    let first = cli(role_url.as_str(), &env, job).spawn().unwrap();
    let second = cli(role_url.as_str(), &env, job).spawn().unwrap();
    let first = first.wait_with_output().unwrap();
    let second = second.wait_with_output().unwrap();
    assert_ne!(first.status.success(), second.status.success());
    let result=db.query_one("SELECT j.status,j.available_at>clock_timestamp(),(SELECT count(*) FROM public.novu_rust_resend_attempts WHERE job_id=j.id),(SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id=j.id),(SELECT resend_retry_not_before::text FROM public.novu_rust_delivery_reconciliations WHERE job_id=j.id) FROM public.novu_queue_jobs j WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(result.get::<_, String>(0), "pending");
    assert!(result.get::<_, bool>(1));
    assert_eq!(result.get::<_, i64>(2), 0);
    assert_eq!(result.get::<_, i64>(3), 1);
    assert_eq!(result.get::<_, String>(4), original);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id=$1 AND id=$2 AND status='pending' AND available_at<=now()", &[&env,&job]).await.unwrap().get::<_,i64>(0),0);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = queue::resend::Resend::test_endpoint(
        queue::resend::Config {
            api_key: "re_test".into(),
            from: "sender@example.test".into(),
            sender_name: String::new(),
        },
        &format!("http://{}/emails", listener.local_addr().unwrap()),
    )
    .unwrap();
    assert!(
        !queue::resend_queue::process_one(&mut db, &env, &provider)
            .await
            .unwrap()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 4096];
        assert!(socket.read(&mut buffer).await.unwrap() > 0);
        let body = r#"{"id":"mock-delayed"}"#;
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
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1", &[&job]).await.unwrap();
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id=$1 AND id=$2 AND status='pending' AND available_at<=now()", &[&env,&job]).await.unwrap().get::<_,i64>(0),1);
    assert!(
        queue::resend_queue::process_one(&mut db, &env, &provider)
            .await
            .unwrap()
    );
    server.await.unwrap();
    let row=db.query_one("SELECT j.status,a.state,a.provider_message_id FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, String>(1), "accepted");
    assert_eq!(
        row.get::<_, Option<String>>(2).as_deref(),
        Some("mock-delayed")
    );
}
