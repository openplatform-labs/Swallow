//! Synchronous DB promotion with a mock Slack request in flight.
use crate::{
    enqueue, in_app,
    slack::Slack,
    slack_workflow::{self, EndpointPolicy, Providers, StepPolicy},
};
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
        Command::new(std::env::var("TEST_SLACK_WORKER_BINARY").unwrap())
            .env_clear()
            .env("DATABASE_URL", format!("{url}&application_name={name}"))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SLACK_ENVIRONMENT_ID", "slack-ha")
            .env(
                "NOVU_SLACK_WORKFLOW_TOKENS",
                r#"{"main":{"":"fixture-token"}}"#,
            )
            .env("NOVU_SLACK_WORKFLOW_ENDPOINT_POLICY", "snapshot-revision")
            .env("NOVU_SLACK_WORKFLOW_DB_TIMEOUT_SECONDS", "10")
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
async fn make_delivery(db: &mut Client, transaction: &str) -> Uuid {
    let trigger = Uuid::new_v4();
    let payload = json!({"workflow":"slack-workflow","transactionId":transaction,"content":"","payload":{},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"64a000000000000000000054"},"severityOverride":null});
    enqueue(db, trigger, "slack-ha", in_app::JOB_KIND, &payload)
        .await
        .unwrap();
    assert!(
        in_app::process_one_with_slack_snapshot(db, Duration::from_secs(60), true)
            .await
            .unwrap()
    );
    let parent: Uuid = db
        .query_one(
            "SELECT id FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'",
            &[&slack_workflow::FANOUT_KIND],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        slack_workflow::fanout_one_with_policies(
            db,
            "slack-ha",
            StepPolicy::Snapshot,
            EndpointPolicy::SnapshotRevision
        )
        .await
        .unwrap()
    );
    db.query_one(
        "SELECT delivery_job_id FROM public.novu_rust_slack_workflow_fanout WHERE parent_job_id=$1",
        &[&parent],
    )
    .await
    .unwrap()
    .get(0)
}
#[tokio::test]
#[ignore = "requires disposable synchronously replicated PostgreSQL containers; destroys primary"]
async fn synchronous_failover_fences_uncertain_and_changed_slack_jobs() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let standby = std::env::var("TEST_STANDBY_CONTAINER").unwrap();
    let mut db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('slack-ha','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active,chat_content,inbox_metadata) VALUES ('slack-ha','slack-workflow',true,true,false,'Hello fixture','{\"id\":\"64a000000000000000000051\",\"name\":\"Fixture\",\"critical\":false,\"tags\":[]}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('slack-ha','alice','64a000000000000000000054'); INSERT INTO public.novu_rust_slack_endpoint_integrations(environment_id,identifier) VALUES ('slack-ha','main'); INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('slack-ha','endpoint','alice','64a000000000000000000054','main','slack_channel','{\"channelId\":\"C123\"}')").await.unwrap();
    let job = make_delivery(&mut db, "slack-ha-one").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buf = [0; 4096];
        let start = loop {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
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
        assert_eq!(body, json!({"channel":"C123","text":"Hello fixture"}));
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let body = r#"{"ok":true,"channel":"C123","ts":"1.2"}"#;
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
    let mut providers = Providers::new();
    providers.insert(
        ("main".into(), String::new()),
        Slack::test_endpoint("fixture-token".into(), &url).unwrap(),
    );
    let mut send_db = connect(&primary_url).await;
    let sending = tokio::spawn(async move {
        slack_workflow::delivery_one_with_policies(
            &mut send_db,
            "slack-ha",
            &providers,
            StepPolicy::Snapshot,
            EndpointPolicy::SnapshotRevision,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), seen_rx)
        .await
        .unwrap()
        .unwrap();
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        replicated=replica.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_slack_attempts a ON a.job_id=j.id JOIN public.novu_rust_slack_channel_endpoints e ON e.environment_id=j.environment_id AND e.internal_id=(j.payload->>'endpoint_id')::uuid WHERE j.id=$1 AND j.status='running' AND a.state='dispatching' AND j.payload->>'endpoint_revision'=e.revision::text",&[&job]).await.unwrap().is_some();
        if replicated {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        replicated,
        "dispatch and destination revision must reach standby"
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
    let mut promoted = connect(&standby_url).await;
    release_tx.send(()).unwrap();
    server.await.unwrap();
    assert!(sending.await.unwrap().is_err());
    promoted.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
    let changed = make_delivery(&mut promoted, "slack-ha-two").await;
    promoted.batch_execute("UPDATE public.novu_rust_slack_channel_endpoints SET endpoint='{\"channelId\":\"COTHER\"}' WHERE environment_id='slack-ha' AND identifier='endpoint'; UPDATE public.novu_rust_slack_channel_endpoints SET endpoint='{\"channelId\":\"C123\"}' WHERE environment_id='slack-ha' AND identifier='endpoint'").await.unwrap();
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
    let mut first = worker(&cluster_url, "slack_failover_1", &proxy_url);
    let mut second = worker(&cluster_url, "slack_failover_2", &proxy_url);
    let mut recovered = false;
    for _ in 0..200 {
        let row=promoted.query_one("SELECT (SELECT status='failed' AND last_error='Slack outcome unknown' FROM public.novu_queue_jobs WHERE id=$1), (SELECT state='unknown' FROM public.novu_rust_slack_attempts WHERE job_id=$1), (SELECT status='failed' AND last_error='Slack endpoint changed or deleted' FROM public.novu_queue_jobs WHERE id=$2), (SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('slack_failover_1','slack_failover_2'))",&[&job,&changed]).await.unwrap();
        recovered = row.get::<_, Option<bool>>(0) == Some(true)
            && row.get::<_, Option<bool>>(1) == Some(true)
            && row.get::<_, Option<bool>>(2) == Some(true)
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
        "workers did not reconcile both jobs"
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        promoted
            .query_one(
                "SELECT count(*) FROM public.novu_rust_slack_attempts WHERE job_id=$1",
                &[&job]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        promoted
            .query_one(
                "SELECT count(*) FROM public.novu_rust_slack_attempts WHERE job_id=$1",
                &[&changed]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "no external retry or changed destination dispatch"
    );
    proxy_task.abort();
}
