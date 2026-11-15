//! Shared cooldown across synchronous PostgreSQL promotion.
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
            .env("NOVU_DISCORD_ENVIRONMENT_ID", "discord-cooldown-ha")
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
async fn shared_cooldown_survives_promotion_and_fences_competing_workers() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_LOCAL_PRIMARY_DATA").unwrap();
    let standby = std::env::var("TEST_LOCAL_STANDBY_DATA").unwrap();
    let pgctl = std::env::var("TEST_LOCAL_PG_CTL").unwrap();
    let mut db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('discord-cooldown-ha','org'); INSERT INTO public.novu_rust_discord_retry_policy(environment_id,destination_id,mode,max_retries,fallback_seconds) VALUES('discord-cooldown-ha','ops','off',2,60); INSERT INTO public.novu_rust_discord_shared_cooldown(environment_id,destination_id) VALUES('discord-cooldown-ha','ops')").await.unwrap();
    let role = format!("cooldown_ha_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_cooldown_health(text,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT 'discord-cooldown-ha',rolname,'alert_view',oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/webhook", listener.local_addr().unwrap());
    let mock = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        socket.read(&mut bytes).await.unwrap();
        let body = r#"{"retry_after":120.25}"#;
        socket.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    });
    let rejected = Uuid::new_v4();
    queue::enqueue(
        &db,
        rejected,
        "discord-cooldown-ha",
        discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"first"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(
            &mut db,
            "discord-cooldown-ha",
            "ops",
            &Webhook::test_endpoint(&url).unwrap()
        )
        .await
        .unwrap()
    );
    mock.await.unwrap();
    let deferred = Uuid::new_v4();
    queue::enqueue(
        &db,
        deferred,
        "discord-cooldown-ha",
        discord_queue::JOB_KIND,
        &json!({"destination":"ops","content":"second"}),
    )
    .await
    .unwrap();
    let replica = connect(&standby_url).await;
    let mut seen = false;
    for _ in 0..100 {
        let row=replica.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_rust_discord_shared_cooldown WHERE environment_id='discord-cooldown-ha' AND destination_id='ops' AND not_before>clock_timestamp()+interval '100 seconds'),EXISTS(SELECT 1 FROM public.novu_queue_jobs WHERE id=$1 AND status='pending'),EXISTS(SELECT 1 FROM public.novu_rust_discord_attempts WHERE job_id=$2 AND state='rejected' AND http_status=429)",&[&deferred,&rejected]).await.unwrap();
        if row.get::<_, bool>(0) && row.get::<_, bool>(1) && row.get::<_, bool>(2) {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(seen, "cooldown and queue state were not replicated");
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
    let mut one = worker(&cluster_url, "discord_cooldown_ha_1", &proxy);
    let mut two = worker(&cluster_url, "discord_cooldown_ha_2", &proxy);
    let mut ready = false;
    for _ in 0..200 {
        ready=promoted.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_queue_jobs WHERE id=$1 AND status='pending' AND last_error='Discord shared cooldown active' AND available_at>clock_timestamp()+interval '90 seconds')",&[&deferred]).await.unwrap().get(0);
        if ready {
            break;
        }
        assert!(one.0.try_wait().unwrap().is_none() && two.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready);
    let mut connected = false;
    for _ in 0..200 {
        connected=promoted.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('discord_cooldown_ha_1','discord_cooldown_ha_2')",&[]).await.unwrap().get::<_,i64>(0)==2;
        if connected {
            break;
        }
        assert!(one.0.try_wait().unwrap().is_none() && two.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(connected);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let operator =
        connect(&cluster_url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1))
            .await;
    let health = operator
        .query_one(
            "SELECT * FROM public.novu_rust_discord_cooldown_health('discord-cooldown-ha',60)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        (
            health.get::<_, i64>(0),
            health.get::<_, i64>(1),
            health.get::<_, i64>(2)
        ),
        (1, 1, 0)
    );
    assert_eq!(
        promoted
            .query_one(
                "SELECT attempts FROM public.novu_queue_jobs WHERE id=$1",
                &[&deferred]
            )
            .await
            .unwrap()
            .get::<_, i32>(0),
        0
    );
    assert_eq!(
        promoted
            .query_one(
                "SELECT count(*) FROM public.novu_rust_discord_attempts WHERE job_id=$1",
                &[&deferred]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    promoted.execute("UPDATE public.novu_rust_discord_shared_cooldown SET not_before=clock_timestamp()-interval '1 second' WHERE environment_id='discord-cooldown-ha'",&[]).await.unwrap();
    promoted.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1",&[&deferred]).await.unwrap();
    let mut done = false;
    for _ in 0..200 {
        done=promoted.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1 AND j.status='failed' AND a.state='unknown'",&[&deferred]).await.unwrap().is_some();
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(done);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    proxy_task.abort();
}
