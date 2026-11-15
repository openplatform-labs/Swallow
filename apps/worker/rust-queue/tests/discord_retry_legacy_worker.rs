//! Actual historical and current workers against a local proxy and disposable DB.
use novu_postgres_queue::{self as queue, discord_queue};
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(binary: &str, url: &str, env: &str, proxy: &str, current: bool) -> Worker {
    let mut command = Command::new(binary);
    command
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_DISCORD_ENVIRONMENT_ID", env)
        .env("NOVU_DISCORD_WEBHOOK_DESTINATION_ID", "ops")
        .env(
            "NOVU_DISCORD_WEBHOOK_URL",
            "https://discord.com/api/webhooks/123/fixture",
        )
        .env("NOVU_DISCORD_WEBHOOK_DB_TIMEOUT_SECONDS", "10")
        .env("HTTPS_PROXY", proxy)
        .env("https_proxy", proxy)
        .env("HTTP_PROXY", proxy)
        .env("http_proxy", proxy)
        .env("ALL_PROXY", proxy)
        .env("all_proxy", proxy)
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if current {
        command
            .env("NOVU_DISCORD_429_RETRY_MODE", "known")
            .env("NOVU_DISCORD_429_MAX_RETRIES", "2")
            .env("NOVU_DISCORD_429_FALLBACK_SECONDS", "20");
    }
    Worker(command.spawn().unwrap())
}
#[tokio::test]
#[ignore = "requires migrated disposable Docker PostgreSQL and pre-retry worker binary"]
async fn old_worker_is_fenced_before_network_and_current_recovers() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let old = std::env::var("TEST_OLD_DISCORD_BINARY").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let env = format!("discord-retry-legacy-{}", Uuid::new_v4());
    let job = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO public.novu_rust_discord_retry_policy VALUES($1,'ops','known',2,20)",
        &[&env],
    )
    .await
    .unwrap();
    queue::enqueue(
        &db,
        job,
        &env,
        discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"fixture"}),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let proxy_task = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            seen.fetch_add(1, Ordering::SeqCst);
            drop(socket);
        }
    });
    let mut previous = worker(&old, &url, &env, &proxy, false);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    loop {
        let logs = Command::new("docker")
            .args(["logs", &container])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&logs.stderr)
            .contains("Discord retry policy requires a compatible worker")
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "old worker was not fenced"
        );
        assert!(
            previous.0.try_wait().unwrap().is_none(),
            "old worker exited"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let row=db.query_one("SELECT status,(SELECT count(*) FROM public.novu_rust_discord_attempts WHERE job_id=$1),(SELECT count(*) FROM public.novu_rust_discord_retry_history WHERE job_id=$1) FROM public.novu_queue_jobs WHERE id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "running");
    assert_eq!(row.get::<_, i64>(1), 0);
    assert_eq!(row.get::<_, i64>(2), 0);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    previous.0.kill().unwrap();
    previous.0.wait().unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
    let mut current = worker(
        env!("CARGO_BIN_EXE_discord-webhook-worker"),
        &url,
        &env,
        &proxy,
        true,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    loop {
        let row=db.query_opt("SELECT j.status,a.state FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
        if row.is_some_and(|r| {
            r.get::<_, String>(0) == "failed" && r.get::<_, String>(1) == "unknown"
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "current worker did not recover"
        );
        assert!(
            current.0.try_wait().unwrap().is_none(),
            "current worker exited"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    proxy_task.abort();
}
