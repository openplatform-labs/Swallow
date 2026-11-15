//! Controlled synchronous failover after a persisted mock Discord 429.
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
            .env("NOVU_DISCORD_ENVIRONMENT_ID", "discord-retry-ha")
            .env("NOVU_DISCORD_WEBHOOK_DESTINATION_ID", "ops")
            .env(
                "NOVU_DISCORD_WEBHOOK_URL",
                "https://discord.com/api/webhooks/123/fixture",
            )
            .env("NOVU_DISCORD_429_RETRY_MODE", "known")
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
#[ignore = "requires disposable synchronous PostgreSQL containers; kills the primary"]
async fn scheduled_429_and_unknown_survive_promotion_without_duplicate_dispatch() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let standby = std::env::var("TEST_STANDBY_CONTAINER").unwrap();
    let mut db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('discord-retry-ha','org'); INSERT INTO public.novu_rust_discord_retry_policy VALUES('discord-retry-ha','ops','known',2,60)").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_url = format!("http://{}/webhook", listener.local_addr().unwrap());
    let mock = tokio::spawn(async move {
        for result in [true, false] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0u8; 4096];
            let _ = socket.read(&mut bytes).await.unwrap();
            if result {
                let body = r#"{"retry_after":120.25}"#;
                socket.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        }
    });
    let provider = Webhook::test_endpoint(&mock_url).unwrap();
    let scheduled = Uuid::new_v4();
    let unknown = Uuid::new_v4();
    queue::enqueue(
        &db,
        scheduled,
        "discord-retry-ha",
        discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"scheduled"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "discord-retry-ha", "ops", &provider)
            .await
            .unwrap()
    );
    queue::enqueue(
        &db,
        unknown,
        "discord-retry-ha",
        discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"unknown"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "discord-retry-ha", "ops", &provider)
            .await
            .unwrap()
    );
    mock.await.unwrap();
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        let row=replica.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_queue_jobs WHERE id=$1 AND status='pending' AND available_at>clock_timestamp()+interval '100 seconds'),EXISTS(SELECT 1 FROM public.novu_rust_discord_retry_history WHERE job_id=$1 AND scheduled_delay_seconds=121),EXISTS(SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$2 AND j.status='failed' AND a.state='unknown')",&[&scheduled,&unknown]).await.unwrap();
        if row.get::<_, bool>(0) && row.get::<_, bool>(1) && row.get::<_, bool>(2) {
            replicated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated, "retry and unknown outcomes not replicated");
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
    let mut one = worker(&cluster_url, "discord_retry_ha_1", &proxy);
    let mut two = worker(&cluster_url, "discord_retry_ha_2", &proxy);
    let mut connected = false;
    for _ in 0..200 {
        if promoted.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('discord_retry_ha_1','discord_retry_ha_2')",&[]).await.unwrap().get::<_,i64>(0)==2 {connected=true;break;}
        assert!(one.0.try_wait().unwrap().is_none() && two.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(connected);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let row=promoted.query_one("SELECT status='pending' AND available_at>clock_timestamp()+interval '90 seconds',(SELECT count(*) FROM public.novu_rust_discord_retry_history WHERE job_id=$1) FROM public.novu_queue_jobs WHERE id=$1",&[&scheduled]).await.unwrap();
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), 1);
    promoted.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1",&[&scheduled]).await.unwrap();
    let mut done = false;
    for _ in 0..200 {
        done=promoted.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1 AND j.status='failed' AND a.state='unknown'",&[&scheduled]).await.unwrap().is_some();
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(done);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(
        promoted
            .query_one(
                "SELECT count(*) FROM public.novu_rust_discord_retry_history WHERE job_id=$1",
                &[&scheduled]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(promoted.query_one("SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=ANY($1) AND j.status='failed' AND a.state='unknown'",&[&vec![scheduled,unknown]]).await.unwrap().get::<_,i64>(0),2);
    proxy_task.abort();
}
