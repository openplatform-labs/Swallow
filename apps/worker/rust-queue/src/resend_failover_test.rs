//! A mocked accepted request during a controlled synchronous PostgreSQL failover.
use crate::{
    enqueue, in_app,
    resend::{Config, Resend},
    resend_queue,
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
        Command::new(std::env::var("TEST_RESEND_WORKER_BINARY").unwrap())
            .env("DATABASE_URL", format!("{url}&application_name={name}"))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_RESEND_ENVIRONMENT_ID", "resend-ha")
            .env("NOVU_RESEND_API_KEY", "fixture-token")
            .env("NOVU_RESEND_FROM", "sender@example.test")
            .env_remove("NOVU_RESEND_API_KEY_FILE")
            .env("NOVU_RESEND_DB_TIMEOUT_SECONDS", "10")
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
async fn synchronous_failover_during_resend_dispatch_preserves_unknown() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let standby = std::env::var("TEST_STANDBY_CONTAINER").unwrap();
    let mut db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('resend-ha','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata) VALUES('resend-ha','resend-ha-workflow',true,true,'Hello','<p>Fixture</p>','resend','{\"id\":\"64a000000000000000000051\",\"name\":\"Fixture\",\"critical\":false,\"tags\":[]}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES('resend-ha','alice','64a000000000000000000054','{\"email\":\"receiver@example.test\"}')").await.unwrap();
    let trigger = Uuid::new_v4();
    let payload = json!({"workflow":"resend-ha-workflow","transactionId":"resend-ha-transaction","content":"","payload":{},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"64a000000000000000000054"},"severityOverride":null});
    enqueue(&db, trigger, "resend-ha", in_app::JOB_KIND, &payload)
        .await
        .unwrap();
    assert!(in_app::process_one(&mut db).await.unwrap());
    let job:Uuid=db.query_one("SELECT email_job_id FROM public.novu_rust_email_step_outcomes WHERE environment_id='resend-ha' AND transaction_id='resend-ha-transaction'",&[]).await.unwrap().get(0);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        let body_start = loop {
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..pos]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|v| v.parse::<usize>().ok())
                    })
                    .unwrap();
                if bytes.len() >= pos + 4 + length {
                    break pos + 4;
                }
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..body_start]);
        assert!(
            headers
                .to_ascii_lowercase()
                .contains(&format!("idempotency-key: swallow/{job}"))
        );
        let body: Value = serde_json::from_slice(&bytes[body_start..]).unwrap();
        assert_eq!(body["to"][0], "receiver@example.test");
        assert_eq!(body["tags"][0]["name"], "swallow_dispatch");
        let tag = Uuid::parse_str(body["tags"][0]["value"].as_str().unwrap()).unwrap();
        seen_tx.send(tag).unwrap();
        release_rx.await.unwrap();
        let response = r#"{"id":"fixture-accepted"}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });
    let provider = Resend::test_endpoint(
        Config {
            api_key: "fixture".into(),
            from: "sender@example.test".into(),
            sender_name: String::new(),
        },
        &endpoint,
    )
    .unwrap();
    let primary_db = connect(&primary_url).await;
    let running = tokio::spawn(async move {
        let mut client = primary_db;
        resend_queue::process_one_with_correlation(
            &mut client,
            "resend-ha",
            &provider,
            false,
            crate::email_step_policy::Policy::Snapshot,
            true,
        )
        .await
    });
    let sent_tag = tokio::time::timeout(Duration::from_secs(10), seen_rx)
        .await
        .unwrap()
        .unwrap();
    // Seed a different ambiguous delivery and resolve it from synthetic tagged
    // evidence before failover. A late collision after promotion must still
    // invalidate both replicated projections.
    let auto_job = Uuid::new_v4();
    let auto_tag = Uuid::new_v4();
    enqueue(
        &db,
        auto_job,
        "resend-ha",
        resend_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='Resend outcome unknown' WHERE id=$1",&[&auto_job]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,correlation_id) VALUES ($1,'resend-ha','unknown',$2)",&[&auto_job,&auto_tag]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_recipients VALUES ($1,'resend-ha','to',0,'fixture-hash')",&[&auto_job]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ($1,'resend-ha','alice','64a000000000000000000054','fixture-notification','resend-ha-workflow','auto-ha','subject','error',$2,'unknown')",&[&auto_job.to_string(),&auto_job]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash,correlation_id) VALUES ('resend-ha','auto-before-failover','auto-provider','email.delivered','fixture-hash',$1)",&[&auto_tag]).await.unwrap();
    assert!(
        crate::resend_projection::project_with_auto_correlation(
            &mut db,
            "resend-ha",
            auto_job,
            true
        )
        .await
        .unwrap()
    );
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result,(SELECT count(*) FROM public.novu_rust_resend_auto_correlations WHERE job_id=j.id) FROM public.novu_queue_jobs j JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&auto_job]).await.unwrap();
    assert_eq!(
        row.get::<_, Option<String>>(0).as_deref(),
        Some("delivered")
    );
    assert_eq!(
        row.get::<_, Option<String>>(1).as_deref(),
        Some("delivered")
    );
    assert_eq!(row.get::<_, i64>(2), 1);
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        let state: Option<String> = replica
            .query_opt(
                "SELECT state FROM public.novu_rust_resend_attempts WHERE job_id=$1",
                &[&job],
            )
            .await
            .unwrap()
            .map(|r| r.get(0));
        let auto_replayed:bool=replica.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id JOIN public.novu_rust_resend_auto_correlations c ON c.job_id=j.id WHERE j.id=$1 AND j.resend_delivery_result='delivered' AND m.resend_delivery_result='delivered')",&[&auto_job]).await.unwrap().get(0);
        if state.as_deref() == Some("dispatching") && auto_replayed {
            replicated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated, "dispatch marker not replayed");
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
    promoted.execute("INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash,correlation_id) VALUES ('resend-ha','auto-after-failover','different-provider','email.delivered','fixture-hash',$1)",&[&auto_tag]).await.unwrap();
    let row=promoted.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result,public.novu_rust_resend_auto_evidence_conflicts('resend-ha',j.id),(SELECT count(*) FROM public.novu_rust_resend_auto_correlations c WHERE c.job_id=j.id) FROM public.novu_queue_jobs j JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&auto_job]).await.unwrap();
    assert_eq!(row.get::<_, Option<String>>(0).as_deref(), Some("unknown"));
    assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some("unknown"));
    assert!(row.get::<_, bool>(2));
    assert_eq!(row.get::<_, i64>(3), 1);
    let row=promoted.query_one("SELECT j.status,a.state,m.delivery_state FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "running");
    assert_eq!(row.get::<_, String>(1), "dispatching");
    assert_eq!(row.get::<_, String>(2), "dispatching");
    release_tx.send(()).unwrap();
    server.await.unwrap();
    assert!(
        running.await.unwrap().is_err(),
        "old primary must not record a successful response"
    );
    promoted.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let connections = Arc::new(AtomicUsize::new(0));
    let observed = connections.clone();
    let proxy_task = tokio::spawn(async move {
        while let Ok((socket, _)) = proxy.accept().await {
            observed.fetch_add(1, Ordering::SeqCst);
            drop(socket);
        }
    });
    let mut first = worker(&cluster_url, "resend_failover_1", &proxy_url);
    let mut second = worker(&cluster_url, "resend_failover_2", &proxy_url);
    let mut recovered = false;
    for _ in 0..200 {
        let row=promoted.query_one("SELECT j.status,a.state,m.delivery_state,(SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('resend_failover_1','resend_failover_2')) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
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
        "workers failed to recover the ambiguous dispatch"
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    let row=promoted.query_one("SELECT j.status,a.state,m.delivery_state,(SELECT count(*) FROM public.novu_rust_resend_attempts WHERE job_id=$1) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(row.get::<_, String>(2), "unknown");
    assert_eq!(row.get::<_, i64>(3), 1);
    assert_eq!(
        connections.load(Ordering::SeqCst),
        0,
        "worker attempted another external request"
    );
    let stored_tag: Uuid = promoted
        .query_one(
            "SELECT correlation_id FROM public.novu_rust_resend_attempts WHERE job_id=$1",
            &[&job],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored_tag, sent_tag);
    let recipient:String=promoted.query_one("SELECT recipient_hash FROM public.novu_rust_resend_recipients WHERE job_id=$1 AND recipient_kind='to' AND recipient_index=0",&[&job]).await.unwrap().get(0);
    promoted.execute("INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash,correlation_id) VALUES ('resend-ha','dispatch-after-failover','fixture-accepted','email.delivered',$1,$2)",&[&recipient,&sent_tag]).await.unwrap();
    assert!(
        crate::resend_projection::project_with_auto_correlation(
            &mut promoted,
            "resend-ha",
            job,
            true
        )
        .await
        .unwrap()
    );
    let row=promoted.query_one("SELECT j.status,j.resend_delivery_result,a.state,a.provider_message_id,m.delivery_state,m.resend_delivery_result,(SELECT count(*) FROM public.novu_rust_resend_auto_correlations c WHERE c.job_id=j.id) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(
        row.get::<_, Option<String>>(1).as_deref(),
        Some("delivered")
    );
    assert_eq!(row.get::<_, String>(2), "accepted");
    assert_eq!(
        row.get::<_, Option<String>>(3).as_deref(),
        Some("fixture-accepted")
    );
    assert_eq!(row.get::<_, String>(4), "accepted");
    assert_eq!(
        row.get::<_, Option<String>>(5).as_deref(),
        Some("delivered")
    );
    assert_eq!(row.get::<_, i64>(6), 1);
    assert_eq!(
        connections.load(Ordering::SeqCst),
        0,
        "automatic correlation must not resend"
    );
    proxy_task.abort();
}
