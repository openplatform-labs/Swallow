//! Opt-in automatic 429 policy against a migrated disposable PostgreSQL database.
#![cfg(feature = "test-mocks")]
use novu_postgres_queue::{
    self as queue,
    resend::{Config, Resend},
};
use serde_json::json;
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
async fn automatic(db: &Client, env: &str) -> Result<bool, tokio_postgres::Error> {
    Ok(db
        .query_one("SELECT public.novu_rust_resend_auto_retry($1)", &[&env])
        .await?
        .get(0))
}
#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL with role creation"]
async fn automatic_retries_are_opt_in_bounded_and_serialized() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    let env = format!("auto-429-{}", Uuid::new_v4());
    let role = format!("auto_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resend_auto_retry(text) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let worker = connect(role_url.as_str()).await;
    let worker2 = connect(role_url.as_str()).await;
    assert!(automatic(&worker, &env).await.is_err());
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'retry',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    assert!(!automatic(&worker, &env).await.unwrap());
    db.execute(
        "INSERT INTO public.novu_rust_resend_auto_retry_policy(environment_id,enabled,max_retries) VALUES ($1,true,2)",
        &[&env],
    )
    .await
    .unwrap();
    let old_job = Uuid::new_v4();
    queue::enqueue(
        &db,
        old_job,
        &env,
        queue::resend_queue::JOB_KIND,
        &json!({"to":["recipient@example.test"],"subject":"old","html":"test"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&old_job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before,created_at) VALUES ($1,$2,'rejected',429,0,clock_timestamp(),clock_timestamp()-interval '1 day')", &[&old_job,&env]).await.unwrap();
    assert!(!automatic(&worker, &env).await.unwrap());
    let job = Uuid::new_v4();
    queue::enqueue(
        &db,
        job,
        &env,
        queue::resend_queue::JOB_KIND,
        &json!({"to":["recipient@example.test"],"subject":"test","html":"test"}),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = Resend::test_endpoint(
        Config {
            api_key: "re_test".into(),
            from: "sender@example.test".into(),
            sender_name: String::new(),
        },
        &format!("http://{}/emails", listener.local_addr().unwrap()),
    )
    .unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0; 4096];
            loop {
                let n = socket.read(&mut tmp).await.unwrap();
                assert!(n > 0);
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = buf.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buf[..pos]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap();
                    if buf.len() >= pos + 4 + length {
                        break;
                    }
                }
            }
            requests.push(String::from_utf8(buf).unwrap());
            socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        }
        requests
    });
    for index in 0..3 {
        assert!(
            queue::resend_queue::process_one_with_retry_key(
                &mut db,
                &env,
                &provider,
                true,
                queue::email_step_policy::Policy::Snapshot,
                true,
                true,
                true
            )
            .await
            .unwrap()
        );
        if index == 0 {
            let (a, b) = tokio::join!(automatic(&worker, &env), automatic(&worker2, &env));
            assert_ne!(a.unwrap(), b.unwrap());
        } else if index == 1 {
            assert!(automatic(&worker, &env).await.unwrap());
        } else {
            assert!(!automatic(&worker, &env).await.unwrap());
        }
    }
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 3);
    let row=db.query_one("SELECT j.status,a.state,a.http_status,(SELECT count(*) FROM public.novu_rust_delivery_reconciliations r WHERE r.job_id=j.id AND r.operator_name='auto:resend-429' AND r.resend_retry_not_before IS NOT NULL) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "rejected");
    assert_eq!(row.get::<_, i32>(2), 429);
    assert_eq!(row.get::<_, i64>(3), 2);
    // Older retry correlations remain relevant after later 429 decisions.
    db.execute("UPDATE public.novu_rust_resend_auto_retry_policy SET max_retries=3 WHERE environment_id=$1", &[&env]).await.unwrap();
    let old_tag: Uuid=db.query_one("SELECT retry_correlation_id FROM public.novu_rust_delivery_reconciliations WHERE job_id=$1 ORDER BY decision_order LIMIT 1", &[&job]).await.unwrap().get(0);
    db.execute("INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,correlation_id) VALUES ($1,'old-proof','provider-proof','email.delivered',$2)", &[&env,&old_tag]).await.unwrap();
    assert!(!automatic(&worker, &env).await.unwrap());
    db.execute("DELETE FROM public.novu_rust_resend_delivery_events WHERE environment_id=$1 AND event_id='old-proof'", &[&env]).await.unwrap();
    assert!(automatic(&worker, &env).await.unwrap());
    db.execute("INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,correlation_id) VALUES ($1,'old-proof','provider-proof','email.delivered',$2)", &[&env,&old_tag]).await.unwrap();
    // A callback arriving after scheduling is checked again at dispatch.
    assert!(
        queue::resend_queue::process_one_with_retry_key(
            &mut db,
            &env,
            &provider,
            true,
            queue::email_step_policy::Policy::Snapshot,
            true,
            true,
            true
        )
        .await
        .unwrap()
    );
    let row=db.query_one("SELECT j.status,a.state,j.last_error FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(
        row.get::<_, String>(2),
        "Resend retry blocked by tagged callback"
    );
    // An earlier ambiguous decision disqualifies automatic retries, even if
    // the latest response happens to be a 429 with a valid delay.
    let uncertain_job = Uuid::new_v4();
    queue::enqueue(
        &db,
        uncertain_job,
        &env,
        queue::resend_queue::JOB_KIND,
        &json!({"to":["recipient@example.test"],"subject":"test","html":"test"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&uncertain_job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before) VALUES ($1,$2,'rejected',429,0,clock_timestamp())", &[&uncertain_job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,previous_http_status) VALUES ($1,$2,$3,'retry','operator','fixture',500)", &[&Uuid::new_v4(),&env,&uncertain_job]).await.unwrap();
    assert!(!automatic(&worker, &env).await.unwrap());
    assert!(automatic(&worker, "wrong-env").await.is_err());
    db.execute("UPDATE public.novu_rust_resend_auto_retry_policy SET enabled=false WHERE environment_id=$1", &[&env]).await.unwrap();
    assert!(!automatic(&worker, &env).await.unwrap());
}

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and a local worker process"]
async fn worker_automatically_schedules_a_rejected_429_when_enabled() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("auto-worker-{}", Uuid::new_v4());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,session_user,'retry',oid FROM pg_roles WHERE rolname=session_user", &[&env]).await.unwrap();
    db.execute(
        "INSERT INTO public.novu_rust_resend_auto_retry_policy(environment_id,enabled,max_retries) VALUES ($1,true,1)",
        &[&env],
    )
    .await
    .unwrap();
    let job = Uuid::new_v4();
    queue::enqueue(
        &db,
        job,
        &env,
        queue::resend_queue::JOB_KIND,
        &json!({"to":["recipient@example.test"],"subject":"test","html":"test"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before,correlation_id) VALUES ($1,$2,'rejected',429,120,clock_timestamp()+interval '120 seconds',$3)", &[&job,&env,&Uuid::new_v4()]).await.unwrap();
    let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_resend-worker"))
        .env_clear()
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_RESEND_ENVIRONMENT_ID", &env)
        .env("NOVU_RESEND_API_KEY", "re_test")
        .env("NOVU_RESEND_FROM", "sender@example.test")
        .env("NOVU_RESEND_AUTO_RETRY_429", "true")
        .env("NOVU_RESEND_RETRY_EVIDENCE_GUARD", "true")
        .env("NOVU_RESEND_CORRELATION_TAG", "true")
        .env("NOVU_RESEND_RETRY_CORRELATION_GUARD", "true")
        .env("NOVU_RESEND_IDEMPOTENCY_SCOPE", "retry")
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15),async {
        loop {
            let row=db.query_one("SELECT status,available_at>clock_timestamp() FROM public.novu_queue_jobs WHERE id=$1", &[&job]).await.unwrap();
            if row.get::<_,String>(0)=="pending" {assert!(row.get::<_,bool>(1));break;}
            assert!(process.try_wait().unwrap().is_none(),"worker stopped unexpectedly");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }).await.expect("worker did not schedule the retry");
    process.kill().await.unwrap();
    let _ = process.wait().await;
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id=$1 AND operator_name='auto:resend-429'", &[&job]).await.unwrap().get::<_,i64>(0),1);
}

#[test]
fn worker_rejects_automatic_retry_without_required_evidence_guards() {
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_resend-worker"))
        .env_clear()
        .env("DATABASE_URL", "postgres://localhost/unused")
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_RESEND_ENVIRONMENT_ID", "fixture")
        .env("NOVU_RESEND_API_KEY", "re_test")
        .env("NOVU_RESEND_FROM", "sender@example.test")
        .env("NOVU_RESEND_AUTO_RETRY_429", "true")
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr)
            .contains("automatic 429 retries require both evidence guards")
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL with role creation"]
async fn automatic_retry_dispatch_is_fenced_even_when_policy_is_disabled() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    let env = format!("auto-fence-{}", Uuid::new_v4());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,session_user,'retry',oid FROM pg_roles WHERE rolname=session_user", &[&env]).await.unwrap();
    assert!(
        queue::resend_queue::register_idempotency_scope(&db, &env, true)
            .await
            .unwrap()
    );
    db.execute("INSERT INTO public.novu_rust_resend_auto_retry_policy(environment_id,enabled,max_retries) VALUES ($1,true,1)", &[&env]).await.unwrap();
    let job = Uuid::new_v4();
    queue::enqueue(
        &db,
        job,
        &env,
        queue::resend_queue::JOB_KIND,
        &json!({"to":["recipient@example.test"],"subject":"test","html":"test"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before,correlation_id) VALUES ($1,$2,'rejected',429,0,clock_timestamp(),$3)", &[&job,&env,&Uuid::new_v4()]).await.unwrap();
    assert!(automatic(&db, &env).await.unwrap());
    assert!(db.query_one("SELECT resend_auto_retry FROM public.novu_rust_delivery_reconciliations WHERE job_id=$1", &[&job]).await.unwrap().get::<_,bool>(0));
    db.execute("UPDATE public.novu_rust_resend_auto_retry_policy SET enabled=false WHERE environment_id=$1", &[&env]).await.unwrap();
    for flags in [
        (false, true, true, true),
        (true, false, true, true),
        (true, true, false, true),
        (true, true, true, false),
    ] {
        let row = db
            .query_one(
                "SELECT public.novu_rust_resend_auto_dispatch_allowed($1,$2,$3,$4,$5,$6)",
                &[&env, &job, &flags.0, &flags.1, &flags.2, &flags.3],
            )
            .await
            .unwrap();
        assert!(!row.get::<_, bool>(0));
    }
    let role = format!("fence_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resend_auto_dispatch_allowed(text,uuid,boolean,boolean,boolean,boolean) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let restricted = connect(role_url.as_str()).await;
    assert!(
        restricted
            .query(
                "SELECT * FROM public.novu_rust_delivery_reconciliations",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        !restricted
            .query_one(
                "SELECT public.novu_rust_resend_auto_dispatch_allowed($1,$2,false,true,true,true)",
                &[&env, &job]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = Resend::test_endpoint(
        Config {
            api_key: "re_test".into(),
            from: "sender@example.test".into(),
            sender_name: String::new(),
        },
        &format!("http://{}/emails", listener.local_addr().unwrap()),
    )
    .unwrap();
    assert!(
        queue::resend_queue::process_one_with_registered_policy(
            &mut db,
            &env,
            &provider,
            false,
            queue::email_step_policy::Policy::Snapshot,
            false,
            false,
            true
        )
        .await
        .unwrap()
    );
    let row=db.query_one("SELECT j.status,j.last_error,(SELECT count(*) FROM public.novu_rust_resend_attempts WHERE job_id=j.id) FROM public.novu_queue_jobs j WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(
        row.get::<_, String>(1),
        "Resend automatic retry requires evidence guards"
    );
    assert_eq!(row.get::<_, i64>(2), 0);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1", &[&job]).await.unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        assert!(socket.read(&mut bytes).await.unwrap() > 0);
        let body = r#"{"id":"guarded-ok"}"#;
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
        queue::resend_queue::process_one_with_registered_policy(
            &mut db,
            &env,
            &provider,
            true,
            queue::email_step_policy::Policy::Snapshot,
            true,
            true,
            true
        )
        .await
        .unwrap()
    );
    server.await.unwrap();
    let row=db.query_one("SELECT j.status,a.state FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, String>(1), "accepted");
}
