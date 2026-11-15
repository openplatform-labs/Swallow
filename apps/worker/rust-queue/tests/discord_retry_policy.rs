use novu_postgres_queue::{self as queue, discord::Webhook, discord_queue};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and test-mocks"]
async fn selected_429_retry_is_bounded_fenced_and_preserves_unknown() {
    let (mut db, driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(driver);
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY); INSERT INTO novu_rust_environments VALUES('env')").await.unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(discord_queue::MIGRATION).await.unwrap();
    db.batch_execute(discord_queue::RETRY_POLICY_MIGRATION)
        .await
        .unwrap();
    db.batch_execute("INSERT INTO public.novu_rust_discord_retry_policy VALUES('env','known','known',2,20),('env','fallback','fallback',1,7),('env','off','off',3,10)").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/webhook", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for body in [
            Some((429, r#"{"retry_after":1.25}"#)),
            Some((200, r#"{"id":"123","timestamp":"2026-01-01Z"}"#)),
            Some((429, "{}")),
            Some((429, "{}")),
            None,
            Some((429, r#"{"retry_after":2}"#)),
            Some((429, r#"{"retry_after":2}"#)),
            Some((429, "{}")),
            Some((429, r#"{"retry_after":2}"#)),
            Some((429, r#"{"retry_after":2}"#)),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await.unwrap();
            if let Some((code, body)) = body {
                socket.write_all(format!("HTTP/1.1 {code} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        }
    });
    let provider = Webhook::test_endpoint(&url).unwrap();
    let known = Uuid::new_v4();
    queue::enqueue(
        &db,
        known,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"known","content":"test"}),
    )
    .await
    .unwrap();
    let lease = Uuid::new_v4();
    db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '1 minute' WHERE id=$1",&[&known,&lease]).await.unwrap();
    assert!(db.execute("INSERT INTO public.novu_rust_discord_attempts(job_id,environment_id,state) VALUES($1,'env','dispatching')",&[&known]).await.is_err());
    db.execute(
        "UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1",
        &[&known],
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "known", &provider)
            .await
            .unwrap()
    );
    let row=db.query_one("SELECT j.status,j.available_at>clock_timestamp(),(SELECT count(*) FROM novu_rust_discord_attempts WHERE job_id=j.id),(SELECT scheduled_delay_seconds FROM novu_rust_discord_retry_history WHERE job_id=j.id) FROM novu_queue_jobs j WHERE j.id=$1",&[&known]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, i64>(2), 0);
    assert_eq!(row.get::<_, i32>(3), 2);
    assert!(
        !discord_queue::process_one(&mut db, "env", "known", &provider)
            .await
            .unwrap()
    );
    db.execute(
        "UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1",
        &[&known],
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "known", &provider)
            .await
            .unwrap()
    );
    let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&known]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, String>(1), "accepted");
    let fallback = Uuid::new_v4();
    queue::enqueue(
        &db,
        fallback,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"fallback","content":"test"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "fallback", &provider)
            .await
            .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT scheduled_delay_seconds FROM novu_rust_discord_retry_history WHERE job_id=$1",
            &[&fallback]
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        7
    );
    db.execute(
        "UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1",
        &[&fallback],
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "fallback", &provider)
            .await
            .unwrap()
    );
    let row=db.query_one("SELECT j.status,a.state,a.http_status FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&fallback]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "rejected");
    assert_eq!(row.get::<_, Option<i32>>(2), Some(429));
    let unknown = Uuid::new_v4();
    queue::enqueue(
        &db,
        unknown,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"known","content":"test"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "known", &provider)
            .await
            .unwrap()
    );
    let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&unknown]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    let off = Uuid::new_v4();
    queue::enqueue(
        &db,
        off,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"off","content":"test"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "off", &provider)
            .await
            .unwrap()
    );
    let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&off]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "rejected");
    db.batch_execute("CREATE FUNCTION public.reject_retry_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER reject_retry_history BEFORE INSERT ON public.novu_rust_discord_retry_history FOR EACH ROW EXECUTE FUNCTION public.reject_retry_history()").await.unwrap();
    let rollback = Uuid::new_v4();
    queue::enqueue(
        &db,
        rollback,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"known","content":"test"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "known", &provider)
            .await
            .is_err()
    );
    let row=db.query_one("SELECT j.status,a.state,(SELECT count(*) FROM public.novu_rust_discord_retry_history WHERE job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&rollback]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "running");
    assert_eq!(row.get::<_, String>(1), "dispatching");
    assert_eq!(row.get::<_, i64>(2), 0);
    db.execute(
        "UPDATE novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
        &[&rollback],
    )
    .await
    .unwrap();
    assert_eq!(
        discord_queue::recover_expired(&mut db, "env", "known")
            .await
            .unwrap(),
        1
    );
    let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&rollback]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    let no_guidance = Uuid::new_v4();
    queue::enqueue(
        &db,
        no_guidance,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"known","content":"test"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "known", &provider)
            .await
            .unwrap()
    );
    let row=db.query_one("SELECT j.status,a.state,a.retry_after_seconds FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&no_guidance]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "rejected");
    assert_eq!(row.get::<_, Option<i32>>(2), None);
    db.batch_execute("DROP TRIGGER reject_retry_history ON public.novu_rust_discord_retry_history; DROP FUNCTION public.reject_retry_history(); INSERT INTO public.novu_rust_discord_retry_policy VALUES('env','concurrent','known',1,10)").await.unwrap();
    let concurrent = Uuid::new_v4();
    queue::enqueue(
        &db,
        concurrent,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"concurrent","content":"test"}),
    )
    .await
    .unwrap();
    let (mut second, driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(driver);
    let (a, b) = tokio::join!(
        discord_queue::process_one(&mut db, "env", "concurrent", &provider),
        discord_queue::process_one(&mut second, "env", "concurrent", &provider)
    );
    assert_ne!(a.unwrap(), b.unwrap());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_discord_retry_history WHERE job_id=$1",
            &[&concurrent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );

    let role = format!("discord_retry_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT,UPDATE ON public.novu_queue_jobs TO {role}; GRANT SELECT,INSERT,UPDATE,DELETE ON public.novu_rust_discord_attempts TO {role}; GRANT SELECT,UPDATE ON public.novu_rust_discord_retry_policy TO {role}; GRANT SELECT,INSERT ON public.novu_rust_discord_retry_history TO {role}; GRANT USAGE ON SEQUENCE public.novu_rust_discord_retry_history_id_seq TO {role}; INSERT INTO public.novu_rust_discord_retry_policy VALUES('env','restricted','known',1,10)")).await.unwrap();
    let restricted = Uuid::new_v4();
    queue::enqueue(
        &db,
        restricted,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"restricted","content":"test"}),
    )
    .await
    .unwrap();
    let (mut limited, driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(driver);
    limited
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        discord_queue::process_one(&mut limited, "env", "restricted", &provider)
            .await
            .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_discord_retry_history WHERE job_id=$1",
            &[&restricted]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );

    // Two actual workers for one destination must agree on retry settings.
    let worker = || {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_discord-webhook-worker"));
        cmd.env_clear()
            .env("DATABASE_URL", std::env::var("TEST_DATABASE_URL").unwrap())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_DISCORD_ENVIRONMENT_ID", "env")
            .env("NOVU_DISCORD_WEBHOOK_DESTINATION_ID", "registered")
            .env(
                "NOVU_DISCORD_WEBHOOK_URL",
                "https://discord.com/api/webhooks/123/fixture",
            )
            .kill_on_drop(true);
        cmd
    };
    let mut first = worker()
        .env("NOVU_DISCORD_429_RETRY_MODE", "known")
        .env("NOVU_DISCORD_429_MAX_RETRIES", "2")
        .env("NOVU_DISCORD_429_FALLBACK_SECONDS", "20")
        .spawn()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            if db.query_opt("SELECT 1 FROM public.novu_rust_discord_retry_policy WHERE environment_id='env' AND destination_id='registered' AND mode='known'",&[]).await.unwrap().is_some() {break;}
            assert!(first.try_wait().unwrap().is_none());tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(10), worker().status())
        .await
        .unwrap()
        .unwrap();
    assert!(!status.success());
    first.kill().await.unwrap();
    let _ = first.wait().await;
    server.await.unwrap();
}
