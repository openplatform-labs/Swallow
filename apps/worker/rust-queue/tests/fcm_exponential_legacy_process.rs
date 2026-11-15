//! Real old and current workers against disposable PostgreSQL and a local proxy.
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
            .env("NOVU_FCM_ENVIRONMENT_ID", env)
            .env("NOVU_FCM_PROJECT_ID", "fixture-project")
            .env("NOVU_FCM_ACCESS_TOKEN", "fixture-token")
            .env("SWALLOW_FCM_MULTICAST_503_POLICY", "exponential")
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
#[ignore = "requires migrated disposable PostgreSQL, server log, and old FCM worker"]
async fn old_exponential_worker_is_fenced_and_new_worker_recovers() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let old = std::env::var("TEST_OLD_FCM_WORKER_BINARY").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("fcm-version-{}", Uuid::new_v4());
    let job = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_push_workflow_worker_config(environment_id,multicast_503_policy) VALUES($1,'exponential')", &[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_fcm_push','{\"title\":\"Hi\",\"content\":\"Body\",\"targets\":[\"fixture-device\"]}','pending')", &[&job,&env]).await.unwrap();
    let log_path = std::env::var("TEST_DISPOSABLE_POSTGRES_LOG").unwrap();
    let offset = std::fs::metadata(&log_path).unwrap().len() as usize;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let mut previous = worker(&old, &url, &env, &proxy);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let log = std::fs::read(&log_path).unwrap();
        if log.get(offset..).is_some_and(|tail| {
            String::from_utf8_lossy(tail)
                .contains("FCM exponential policy requires a compatible worker")
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "legacy worker was not rejected by database"
        );
        assert!(
            previous.0.try_wait().unwrap().is_none(),
            "legacy worker exited before fencing"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .is_err(),
        "legacy worker opened a provider connection"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_fcm_attempts WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    previous.0.kill().unwrap();
    previous.0.wait().unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&job]).await.unwrap();
    let _current = worker(env!("CARGO_BIN_EXE_fcm-worker"), &url, &env, &proxy);
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept())
        .await
        .expect("current worker did not dispatch")
        .unwrap();
    let mut bytes = [0; 2048];
    let n = socket.read(&mut bytes).await.unwrap();
    assert!(
        std::str::from_utf8(&bytes[..n])
            .unwrap()
            .starts_with("CONNECT ")
    );
    drop(socket);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if db
            .query_opt(
                "SELECT state FROM public.novu_rust_fcm_attempts WHERE job_id=$1",
                &[&job],
            )
            .await
            .unwrap()
            .is_some_and(|r| r.get::<_, String>(0) == "unknown")
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "current worker attempt did not become unknown"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
