//! Controlled synchronous failover while a mock FCM request is in flight.
use crate::{enqueue, fcm::Fcm, fcm_queue};
use serde_json::{Value, json};
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
    sync::oneshot,
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    for _ in 0..100 {
        if let Ok((db, driver)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = driver.await;
            });
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
        Command::new(std::env::var("TEST_FCM_WORKER_BINARY").unwrap())
            .env_clear()
            .env("DATABASE_URL", format!("{url}&application_name={name}"))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_FCM_ENVIRONMENT_ID", "fcm-ha")
            .env("NOVU_FCM_PROJECT_ID", "fixture-project")
            .env("NOVU_FCM_ACCESS_TOKEN", "fixture-token")
            .env("NOVU_FCM_DB_TIMEOUT_SECONDS", "10")
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
#[ignore = "requires disposable synchronously replicated PostgreSQL containers; destroys the primary"]
async fn synchronous_failover_during_fcm_dispatch_preserves_unknown() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let standby = std::env::var("TEST_STANDBY_CONTAINER").unwrap();
    let db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('fcm-ha','org'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('fcm-ha','alice','subscriber','{}')").await.unwrap();
    let job = Uuid::new_v4();
    let multiple = std::env::var("TEST_FCM_FAILOVER_MULTIPLE").as_deref() == Ok("true");
    let mut payload = json!({"prepared_messages":[{"message":{"token":"fixture-device","notification":{"title":"Hello","body":"Body"}}}],"prepared_context":{"subscriber_id":"alice","subscriber_internal_id":"subscriber","notification_internal_id":"notification","workflow_identifier":"workflow","transaction_id":job.to_string(),"title":"Hello","content":"Body","target_metadata":[{"message_id":"fixture-message","device_tokens":["fixture-device"]}]}});
    if multiple {
        payload["prepared_messages"].as_array_mut().unwrap().push(json!({"message":{"token":"fixture-second","notification":{"title":"Hello","body":"Body"}}}));
        payload["prepared_context"]["target_metadata"]
            .as_array_mut()
            .unwrap()
            .push(json!({"message_id":"fixture-message-2","device_tokens":["fixture-second"]}));
    }
    enqueue(&db, job, "fcm-ha", fcm_queue::JOB_KIND, &payload)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        let start = loop {
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..pos]);
                let len = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|v| v.parse::<usize>().ok())
                    })
                    .unwrap();
                if bytes.len() >= pos + 4 + len {
                    break pos + 4;
                }
            }
        };
        let body: Value = serde_json::from_slice(&bytes[start..]).unwrap();
        assert_eq!(body["message"]["token"], "fixture-device");
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let body = r#"{"name":"projects/fixture-project/messages/fixture"}"#;
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
    let provider = Fcm::with_origin("fixture-project", "fixture-token".into(), &origin).unwrap();
    let mut send_db = connect(&primary_url).await;
    let sending =
        tokio::spawn(
            async move { fcm_queue::process_one(&mut send_db, "fcm-ha", &provider).await },
        );
    tokio::time::timeout(Duration::from_secs(10), seen_rx)
        .await
        .unwrap()
        .unwrap();
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        replicated = replica.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_fcm_attempts a ON a.job_id=j.id JOIN public.novu_rust_push_messages m ON m.push_job_id=j.id WHERE j.id=$1 AND j.status='running' AND a.state='dispatching' AND m.delivery_state='dispatching'", &[&job]).await.unwrap().is_some();
        if replicated {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        replicated,
        "in-flight attempt must reach the synchronous replica"
    );
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
    release_tx.send(()).unwrap();
    server.await.unwrap();
    assert!(
        sending.await.unwrap().is_err(),
        "old primary must not record acceptance"
    );
    promoted.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&job]).await.unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let proxy_task = tokio::spawn(async move {
        while let Ok((socket, _)) = proxy.accept().await {
            observed.fetch_add(1, Ordering::SeqCst);
            drop(socket);
        }
    });
    let mut first = worker(&cluster_url, "fcm_failover_1", &proxy_url);
    let mut second = worker(&cluster_url, "fcm_failover_2", &proxy_url);
    let mut recovered = false;
    for _ in 0..200 {
        let row=promoted.query_one("SELECT j.status,a.state,m.delivery_state,(SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('fcm_failover_1','fcm_failover_2')) FROM public.novu_queue_jobs j JOIN public.novu_rust_fcm_attempts a ON a.job_id=j.id AND a.target_index=0 JOIN public.novu_rust_push_messages m ON m.push_job_id=j.id AND m.target_index=0 WHERE j.id=$1", &[&job]).await.unwrap();
        recovered = row.get::<_, String>(0) == "failed"
            && row.get::<_, String>(1) == "unknown"
            && row.get::<_, String>(2) == "unknown"
            && row.get::<_, i64>(3) == 2;
        if recovered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recovered
            && first.0.try_wait().unwrap().is_none()
            && second.0.try_wait().unwrap().is_none(),
        "workers failed to preserve the ambiguous attempt"
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    let rows = promoted.query("SELECT target_index,state FROM public.novu_rust_fcm_attempts WHERE job_id=$1 ORDER BY target_index", &[&job]).await.unwrap();
    assert_eq!(rows.len(), if multiple { 2 } else { 1 });
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i as i32);
        assert_eq!(row.get::<_, String>(1), "unknown");
    }
    let messages = promoted.query("SELECT target_index,delivery_state FROM public.novu_rust_push_messages WHERE push_job_id=$1 ORDER BY target_index", &[&job]).await.unwrap();
    assert_eq!(messages.len(), if multiple { 2 } else { 1 });
    for (i, row) in messages.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i as i32);
        assert_eq!(row.get::<_, String>(1), "unknown");
    }
    assert_eq!(
        count.load(Ordering::SeqCst),
        if multiple { 1 } else { 0 },
        "workers must send only the unattempted target, never retry the ambiguous target"
    );
    proxy_task.abort();
}
