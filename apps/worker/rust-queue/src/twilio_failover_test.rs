//! Controlled synchronous failover while a mock Twilio request is in flight.
use crate::{
    enqueue, in_app,
    twilio::{Config, Region, Twilio},
    twilio_queue,
};
use serde_json::json;
use std::{
    collections::HashMap,
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
        Command::new(std::env::var("TEST_TWILIO_WORKER_BINARY").unwrap())
            .env_clear()
            .env("DATABASE_URL", format!("{url}&application_name={name}"))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_TWILIO_ENVIRONMENT_ID", "sms-ha")
            .env("NOVU_TWILIO_ACCOUNT_SID", "ACfixture")
            .env("NOVU_TWILIO_AUTH_TOKEN", "fixture")
            .env("NOVU_TWILIO_FROM", "+15550000001")
            .env("NOVU_TWILIO_DB_TIMEOUT_SECONDS", "10")
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
async fn synchronous_failover_during_twilio_dispatch_preserves_unknown() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let standby = std::env::var("TEST_STANDBY_CONTAINER").unwrap();
    let mut db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('sms-ha','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,sms_content,inbox_metadata) VALUES ('sms-ha','sms-workflow',true,true,'Hello fixture','{\"id\":\"64a000000000000000000051\",\"name\":\"Fixture\",\"critical\":false,\"tags\":[]}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('sms-ha','alice','64a000000000000000000054','{\"phone\":\"+15550000002\"}')").await.unwrap();
    let trigger = Uuid::new_v4();
    let payload = json!({"workflow":"sms-workflow","transactionId":"sms-ha-transaction","content":"","payload":{},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"64a000000000000000000054"},"severityOverride":null});
    enqueue(&db, trigger, "sms-ha", in_app::JOB_KIND, &payload)
        .await
        .unwrap();
    assert!(in_app::process_one(&mut db).await.unwrap());
    let job:Uuid=db.query_one("SELECT sms_job_id FROM public.novu_rust_sms_step_outcomes WHERE environment_id='sms-ha' AND transaction_id='sms-ha-transaction'",&[]).await.unwrap().get(0);
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
        let form: HashMap<String, String> = serde_urlencoded::from_bytes(&bytes[start..]).unwrap();
        assert_eq!(form.get("To").map(String::as_str), Some("+15550000002"));
        assert_eq!(form.get("Body").map(String::as_str), Some("Hello fixture"));
        let callback = form.get("StatusCallback").unwrap();
        let dispatch = Uuid::parse_str(callback.rsplit('/').next().unwrap()).unwrap();
        seen_tx.send(dispatch).unwrap();
        release_rx.await.unwrap();
        let body = r#"{"sid":"SMfixture","date_created":"Wed, 28 Oct 2026 10:00:00 +0000"}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 201 Created\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });
    let provider = Twilio::test_origin(
        Config {
            account_sid: "ACfixture".into(),
            auth_token: "fixture".into(),
            api_key_sid: None,
            api_key_secret: None,
            from: "+15550000001".into(),
            region: Region::Us,
        },
        &origin,
    )
    .unwrap()
    .with_status_callback(
        "https://events.example.test/webhooks/organizations/org/environments/sms-ha/sms/twilio",
        "sms-ha",
    )
    .unwrap();
    let mut send_db = connect(&primary_url).await;
    let sending =
        tokio::spawn(
            async move { twilio_queue::process_one(&mut send_db, "sms-ha", &provider).await },
        );
    let dispatch = tokio::time::timeout(Duration::from_secs(10), seen_rx)
        .await
        .unwrap()
        .unwrap();
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        replicated=replica.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_twilio_attempts a ON a.job_id=j.id JOIN public.novu_rust_sms_messages m ON m.sms_job_id=j.id JOIN public.novu_rust_twilio_dispatch_ids d ON d.id=a.dispatch_id WHERE j.id=$1 AND j.status='running' AND a.state='dispatching' AND m.delivery_state='dispatching' AND a.dispatch_id=$2",&[&job,&dispatch]).await.unwrap().is_some();
        if replicated {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        replicated,
        "in-flight SMS attempt and callback identity must be replicated"
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
        "old primary must not record provider acceptance"
    );
    promoted.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
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
    let mut first = worker(&cluster_url, "sms_failover_1", &proxy_url);
    let mut second = worker(&cluster_url, "sms_failover_2", &proxy_url);
    let mut recovered = false;
    for _ in 0..200 {
        let row=promoted.query_one("SELECT j.status,a.state,m.delivery_state,(SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('sms_failover_1','sms_failover_2')) FROM public.novu_queue_jobs j JOIN public.novu_rust_twilio_attempts a ON a.job_id=j.id JOIN public.novu_rust_sms_messages m ON m.sms_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
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
        "workers failed to recover the ambiguous SMS"
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    let row=promoted.query_one("SELECT count(*),min(state),min(dispatch_id::text) FROM public.novu_rust_twilio_attempts WHERE job_id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some("unknown"));
    assert_eq!(
        row.get::<_, Option<String>>(2).as_deref(),
        Some(dispatch.to_string().as_str())
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "workers must not repeat an uncertain SMS"
    );
    proxy_task.abort();
}
