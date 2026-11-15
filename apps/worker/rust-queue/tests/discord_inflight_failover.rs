//! In-flight Discord dispatch across synchronous PostgreSQL promotion.
use novu_postgres_queue::{self as queue, discord::Webhook, discord_queue};
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
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
            .env("NOVU_DISCORD_ENVIRONMENT_ID", "discord-inflight-ha")
            .env("NOVU_DISCORD_WEBHOOK_DESTINATION_ID", "ops")
            .env(
                "NOVU_DISCORD_WEBHOOK_URL",
                "https://discord.com/api/webhooks/123/fixture",
            )
            .env("NOVU_DISCORD_429_RETRY_MODE", "off")
            .env("NOVU_DISCORD_429_MAX_RETRIES", "2")
            .env("NOVU_DISCORD_429_FALLBACK_SECONDS", "60")
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
#[ignore = "requires disposable synchronous local PostgreSQL pair; stops primary"]
async fn inflight_attempt_becomes_unknown_after_promotion_without_retry() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_LOCAL_PRIMARY_DATA").unwrap();
    let standby = std::env::var("TEST_LOCAL_STANDBY_DATA").unwrap();
    let pgctl = std::env::var("TEST_LOCAL_PG_CTL").unwrap();
    let mut db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('discord-inflight-ha','org'); INSERT INTO public.novu_rust_discord_retry_policy(environment_id,destination_id,mode,max_retries,fallback_seconds) VALUES('discord-inflight-ha','ops','off',2,60)").await.unwrap();
    let job = Uuid::new_v4();
    queue::enqueue(
        &db,
        job,
        "discord-inflight-ha",
        discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"first"}),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/webhook", listener.local_addr().unwrap());
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let mock = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = [0u8; 4096];
        stream.read(&mut bytes).await.unwrap();
        received_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let body = r#"{"id":"123","timestamp":"2026-01-01Z"}"#;
        stream
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
    let processor = tokio::spawn(async move {
        discord_queue::process_one(
            &mut db,
            "discord-inflight-ha",
            "ops",
            &Webhook::test_endpoint(&url).unwrap(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), received_rx)
        .await
        .unwrap()
        .unwrap();
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        replicated=replica.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1 AND j.status='running' AND a.state='dispatching'",&[&job]).await.unwrap().is_some();
        if replicated {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated);
    assert!(
        Command::new(&pgctl)
            .args(["-D", &primary, "-m", "immediate", "stop", "-w"])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new(&pgctl)
            .args(["-D", &standby, "promote", "-w"])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    release_tx.send(()).unwrap();
    mock.await.unwrap();
    assert!(processor.await.unwrap().is_err());
    let promoted = connect(&standby_url).await;
    promoted.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", proxy_listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let proxy_task = tokio::spawn(async move {
        while let Ok((socket, _)) = proxy_listener.accept().await {
            seen.fetch_add(1, Ordering::SeqCst);
            drop(socket);
        }
    });
    let mut one = worker(&cluster_url, "discord_inflight_ha_1", &proxy);
    let mut two = worker(&cluster_url, "discord_inflight_ha_2", &proxy);
    let mut recovered = false;
    for _ in 0..200 {
        recovered=promoted.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1 AND j.status='failed' AND a.state='unknown'",&[&job]).await.unwrap().is_some();
        if recovered {
            break;
        }
        assert!(one.0.try_wait().unwrap().is_none() && two.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(recovered);
    let mut connected = false;
    for _ in 0..200 {
        connected=promoted.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('discord_inflight_ha_1','discord_inflight_ha_2')",&[]).await.unwrap().get::<_,i64>(0)==2;
        if connected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(connected);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let second = Uuid::new_v4();
    queue::enqueue(
        &promoted,
        second,
        "discord-inflight-ha",
        discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"second"}),
    )
    .await
    .unwrap();
    let mut processed = false;
    for _ in 0..200 {
        processed=promoted.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1 AND j.status='failed' AND a.state='unknown'",&[&second]).await.unwrap().is_some();
        if processed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(processed);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    proxy_task.abort();
}
