//! Controlled synchronous DB promotion after killing a worker during proxy CONNECT.
use novu_postgres_queue as queue;
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    for _ in 0..100 {
        if let Ok((db, driver)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(driver);
            return db;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("database unavailable")
}
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(url: &str, name: &str, proxy: &str) -> Worker {
    Worker(
        Command::new(env!("CARGO_BIN_EXE_discord-webhook-worker"))
            .env_clear()
            .env("DATABASE_URL", format!("{url}&application_name={name}"))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_DISCORD_ENVIRONMENT_ID", "discord-ha")
            .env("NOVU_DISCORD_WEBHOOK_DESTINATION_ID", "ops")
            .env(
                "NOVU_DISCORD_WEBHOOK_URL",
                "https://discord.com/api/webhooks/123/fixture",
            )
            .env("NOVU_DISCORD_WEBHOOK_DB_TIMEOUT_SECONDS", "30")
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
#[ignore = "requires disposable synchronous PostgreSQL containers; kills the primary"]
async fn discord_worker_crash_and_db_promotion_do_not_retry_uncertain_attempt() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let standby = std::env::var("TEST_STANDBY_CONTAINER").unwrap();
    let db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('discord-ha','org')")
        .await
        .unwrap();
    let first = Uuid::new_v4();
    queue::enqueue(
        &db,
        first,
        "discord-ha",
        queue::discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"first"}),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let seen_count = count.clone();
    let (seen_tx, seen_rx) = oneshot::channel();
    let proxy_task = tokio::spawn(async move {
        let (mut first_socket, _) = listener.accept().await.unwrap();
        seen_count.fetch_add(1, Ordering::SeqCst);
        let mut request = Vec::new();
        let mut bytes = [0u8; 1024];
        loop {
            let n = first_socket.read(&mut bytes).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&bytes[..n]);
            if request.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        assert!(String::from_utf8_lossy(&request).starts_with("CONNECT discord.com:443 "));
        seen_tx.send(()).unwrap();
        // Never establish TLS or contact Discord; the worker waits at CONNECT.
        tokio::spawn(async move {
            let _ = first_socket.read(&mut bytes).await;
        });
        while let Ok((socket, _)) = listener.accept().await {
            seen_count.fetch_add(1, Ordering::SeqCst);
            drop(socket);
        }
    });
    let mut original = worker(
        &format!("{primary_url}?connect_timeout=2"),
        "discord_original",
        &proxy,
    );
    tokio::time::timeout(Duration::from_secs(20), seen_rx)
        .await
        .unwrap()
        .unwrap();
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        replicated=replica.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1 AND j.status='running' AND a.state='dispatching'",&[&first]).await.unwrap().is_some();
        if replicated {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated, "dispatch reservation did not reach standby");
    original.0.kill().unwrap();
    original.0.wait().unwrap();
    assert!(
        Command::new("docker")
            .args(["kill", &primary])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("docker")
            .args([
                "exec",
                "-u",
                "postgres",
                &standby,
                "pg_ctl",
                "promote",
                "-D",
                "/var/lib/postgresql/data",
                "-w"
            ])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let promoted = connect(&standby_url).await;
    promoted.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&first]).await.unwrap();
    let second = Uuid::new_v4();
    queue::enqueue(
        &promoted,
        second,
        "discord-ha",
        queue::discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"second"}),
    )
    .await
    .unwrap();
    let mut survivor1 = worker(&cluster_url, "discord_survivor_1", &proxy);
    let mut survivor2 = worker(&cluster_url, "discord_survivor_2", &proxy);
    let mut recovered = false;
    for _ in 0..200 {
        let row=promoted.query_one("SELECT (SELECT j.status='failed' AND a.state='unknown' FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1), (SELECT j.status='failed' AND a.state='unknown' FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$2), (SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('discord_survivor_1','discord_survivor_2'))",&[&first,&second]).await.unwrap();
        recovered = row.get::<_, Option<bool>>(0) == Some(true)
            && row.get::<_, Option<bool>>(1) == Some(true)
            && row.get::<_, i64>(2) == 2;
        if recovered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recovered
            && survivor1.0.try_wait().unwrap().is_none()
            && survivor2.0.try_wait().unwrap().is_none(),
        "survivors failed to recover and process"
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        promoted
            .query_one(
                "SELECT count(*) FROM public.novu_rust_discord_attempts WHERE job_id=ANY($1)",
                &[&vec![first, second]]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        2,
        "an uncertain job was retried"
    );
    proxy_task.abort();
}
