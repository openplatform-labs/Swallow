//! Real previous/current workers against a disposable database and local proxy.
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::TcpListener};
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(binary: &str, url: &str, env: &str, proxy: &str) -> Worker {
    Worker(
        Command::new(binary)
            .env_clear()
            .env("DATABASE_URL", url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_TWILIO_ENVIRONMENT_ID", env)
            .env(
                "NOVU_TWILIO_ACCOUNT_SID",
                "AC11111111111111111111111111111111",
            )
            .env("NOVU_TWILIO_AUTH_TOKEN", "test-only")
            .env("NOVU_TWILIO_FROM", "+15550000001")
            .env("HTTPS_PROXY", proxy)
            .env("https_proxy", proxy)
            .env("HTTP_PROXY", proxy)
            .env("http_proxy", proxy)
            .env("ALL_PROXY", proxy)
            .env("all_proxy", proxy)
            .env("NO_PROXY", "")
            .env("no_proxy", "")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and TEST_OLD_TWILIO_WORKER_BINARY"]
async fn previous_worker_is_blocked_and_current_worker_can_dispatch() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let old = std::env::var("TEST_OLD_TWILIO_WORKER_BINARY").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("twilio-version-{}", Uuid::new_v4());
    let job = Uuid::new_v4();
    let dispatch = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'retry',oid FROM pg_roles WHERE rolname=current_user",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_twilio_sms','{\"to\":\"+15550000002\",\"content\":\"fixture\"}','failed')",&[&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES($1,$2,$3)",&[&dispatch,&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_twilio_attempts(job_id,environment_id,state,http_status,dispatch_id,retry_after_seconds,retry_not_before) VALUES($1,$2,'rejected',429,$3,0,clock_timestamp()-interval '1 second')",&[&job,&env,&dispatch]).await.unwrap();
    db.query_one(
        "SELECT public.novu_rust_resolve_twilio_after($1,$2,$3,'fixture','ticket')",
        &[&env, &job, &dispatch],
    )
    .await
    .unwrap();
    let log_path = std::env::var("TEST_DISPOSABLE_POSTGRES_LOG").unwrap();
    let log_offset = std::fs::metadata(&log_path).unwrap().len() as usize;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let mut previous = worker(&old, &url, &env, &proxy);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let row = db
            .query_one(
                "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
                &[&job],
            )
            .await
            .unwrap();
        if row.get::<_, String>(0) == "running" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "old worker did not claim"
        );
        assert!(
            previous.0.try_wait().unwrap().is_none(),
            "old worker exited"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let contents = std::fs::read(&log_path).unwrap();
        if contents.get(log_offset..).is_some_and(|tail| {
            String::from_utf8_lossy(tail)
                .contains("Twilio delayed retry requires a compatible worker")
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "database did not report the version fence rejection"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .is_err(),
        "old worker attempted an outbound connection"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_twilio_attempts WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    previous.0.kill().unwrap();
    previous.0.wait().unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
    let _current = worker(env!("CARGO_BIN_EXE_twilio-worker"), &url, &env, &proxy);
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept())
        .await
        .expect("current worker did not dispatch")
        .unwrap();
    let mut bytes = [0u8; 2048];
    let n = socket.read(&mut bytes).await.unwrap();
    assert!(
        std::str::from_utf8(&bytes[..n])
            .unwrap()
            .starts_with("CONNECT ")
    );
    drop(socket);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let row = db
            .query_opt(
                "SELECT state FROM public.novu_rust_twilio_attempts WHERE job_id=$1",
                &[&job],
            )
            .await
            .unwrap();
        if row.is_some_and(|r| r.get::<_, String>(0) == "unknown") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "current attempt did not become unknown"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
