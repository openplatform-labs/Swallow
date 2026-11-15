//! Signed webhook through the API and conservative automatic Resend projection.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use novu_postgres_queue as queue;
use ring::hmac;
use serde_json::json;
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
async fn migrate(db: &Client, dir: &Path, range: std::ops::RangeInclusive<u32>) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        if range.contains(&name[..4].parse::<u32>().unwrap()) {
            db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}
async fn send(
    router: axum::Router,
    id: &str,
    kind: &str,
    email: &str,
    tag: Uuid,
    recipient: &str,
    secret: &[u8],
) -> StatusCode {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body=json!({"type":kind,"created_at":"2026-01-22T23:41:12Z","data":{"email_id":email,"message_id":"<fixture@example.test>","to":[recipient],"tags":{"swallow_dispatch":tag.to_string()}}}).to_string();
    let signed = format!("{id}.{timestamp}.{body}");
    let signature = format!(
        "v1,{}",
        STANDARD.encode(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, secret),
                signed.as_bytes()
            )
            .as_ref()
        )
    );
    router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/webhooks/organizations/org/environments/env/email/resend")
                .header("svix-id", id)
                .header("svix-timestamp", timestamp)
                .header("svix-signature", signature)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL"]
async fn signed_recipient_events_resolve_unknown_and_preserve_conflicts() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let mut projection = connect(&url).await;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    db.execute(
        "INSERT INTO novu_rust_environments VALUES ('env','org')",
        &[],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_resend_webhook_lock_grants SELECT 'env',session_user,oid FROM pg_roles WHERE rolname=session_user",&[]).await.unwrap();
    let job = Uuid::new_v4();
    let tag = Uuid::new_v4();
    queue::enqueue(&db, job, "env", queue::resend_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend outcome unknown' WHERE id=$1",&[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,correlation_id) VALUES ($1,'env','unknown',$2)",&[&job,&tag]).await.unwrap();
    for (index, recipient) in ["alice@example.test", "bob@example.test"]
        .iter()
        .enumerate()
    {
        let hash = queue::resend_queue::recipient_hash(recipient);
        db.execute(
            "INSERT INTO novu_rust_resend_recipients VALUES ($1,'env','to',$2,$3)",
            &[&job, &(index as i32), &hash],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ($1,'env','s','s','n','w','t','subject','error',$2,'unknown')",&[&job.to_string(),&job]).await.unwrap();
    let secret = b"fixture-secret-012345678901234567";
    let key = format!("whsec_{}", STANDARD.encode(secret));
    let config = novu_rust_api::resend_webhook::Config::new("env".into(), &key)
        .unwrap()
        .with_retry_job_lock(true)
        .with_correlation_capture(true)
        .with_correlation_job_lock(true);
    let app = novu_rust_api::router_with_webhook_configs(
        Arc::new(db),
        None,
        HashMap::new(),
        None,
        Some(config),
    );
    assert_eq!(
        send(
            app.clone(),
            "msg_forged",
            "email.delivered",
            "provider-1",
            tag,
            "alice@example.test",
            b"incorrect-secret-012345678901234"
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert!(
        queue::resend_projection::project_with_auto_correlation(&mut projection, "env", job, true)
            .await
            .unwrap()
    );
    let row=projection.query_one("SELECT j.status,a.state,(SELECT count(*) FROM novu_rust_resend_delivery_events WHERE environment_id='env') FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(row.get::<_, i64>(2), 0);
    assert_eq!(
        send(
            app.clone(),
            "msg_first",
            "email.delivered",
            "provider-1",
            tag,
            "alice@example.test",
            secret
        )
        .await,
        StatusCode::NO_CONTENT
    );
    assert!(
        queue::resend_projection::project_with_auto_correlation(&mut projection, "env", job, true)
            .await
            .unwrap()
    );
    let row=projection.query_one("SELECT j.status,j.resend_delivery_result,a.state,m.delivery_state,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, String>(1), "pending");
    assert_eq!(row.get::<_, String>(2), "accepted");
    assert_eq!(row.get::<_, String>(3), "accepted");
    assert_eq!(row.get::<_, String>(4), "pending");
    assert_eq!(
        send(
            app.clone(),
            "msg_second",
            "email.bounced",
            "provider-1",
            tag,
            "bob@example.test",
            secret
        )
        .await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        send(
            app.clone(),
            "msg_second",
            "email.bounced",
            "provider-1",
            tag,
            "bob@example.test",
            secret
        )
        .await,
        StatusCode::NO_CONTENT
    );
    queue::resend_projection::project_with_auto_correlation(&mut projection, "env", job, true)
        .await
        .unwrap();
    let row=projection.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result,(SELECT count(*) FROM novu_rust_resend_auto_correlations WHERE job_id=j.id),(SELECT count(*) FROM novu_rust_resend_delivery_events WHERE environment_id='env') FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "partial");
    assert_eq!(row.get::<_, String>(1), "partial");
    assert_eq!(row.get::<_, i64>(2), 1);
    assert_eq!(row.get::<_, i64>(3), 2);
    assert_eq!(
        send(
            app,
            "msg_conflict",
            "email.delivered",
            "provider-2",
            tag,
            "alice@example.test",
            secret
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let row=projection.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
}

struct Process(std::process::Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn send_http(
    origin: &str,
    id: &str,
    kind: &str,
    email: &str,
    tag: Uuid,
    recipient: &str,
    secret: &[u8],
) -> reqwest::StatusCode {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body=json!({"type":kind,"created_at":"2026-01-22T23:41:12Z","data":{"email_id":email,"message_id":"<fixture@example.test>","to":[recipient],"tags":{"swallow_dispatch":tag.to_string()}}}).to_string();
    let signed = format!("{id}.{timestamp}.{body}");
    let signature = format!(
        "v1,{}",
        STANDARD.encode(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, secret),
                signed.as_bytes()
            )
            .as_ref()
        )
    );
    reqwest::Client::new()
        .post(format!(
            "{origin}/webhooks/organizations/org/environments/env/email/resend"
        ))
        .header("svix-id", id)
        .header("svix-timestamp", timestamp)
        .header("svix-signature", signature)
        .body(body)
        .send()
        .await
        .unwrap()
        .status()
}
async fn wait_for_result(db: &Client, job: Uuid, expected: &str) {
    for _ in 0..150 {
        let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
        if row.get::<_, Option<String>>(0).as_deref() == Some(expected)
            && row.get::<_, Option<String>>(1).as_deref() == Some(expected)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("projection did not reach {expected}");
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and a separately built Resend projection worker"]
async fn real_http_and_worker_project_signed_events() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    db.execute(
        "INSERT INTO novu_rust_environments VALUES ('env','org')",
        &[],
    )
    .await
    .unwrap();
    let role = format!("resend_web_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.novu_rust_environments TO {role}; GRANT SELECT,INSERT,UPDATE ON public.novu_rust_resend_delivery_events TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_resend_webhook_jobs_with_correlation(text,text,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_webhook_lock_grants SELECT 'env',$1,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let mut api_db = reqwest::Url::parse(&url).unwrap();
    api_db.set_username(&role).unwrap();
    let job = Uuid::new_v4();
    let tag = Uuid::new_v4();
    queue::enqueue(&db, job, "env", queue::resend_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend outcome unknown' WHERE id=$1",&[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,correlation_id) VALUES ($1,'env','unknown',$2)",&[&job,&tag]).await.unwrap();
    for (index, recipient) in ["alice@example.test", "bob@example.test"]
        .iter()
        .enumerate()
    {
        let hash = queue::resend_queue::recipient_hash(recipient);
        db.execute(
            "INSERT INTO novu_rust_resend_recipients VALUES ($1,'env','to',$2,$3)",
            &[&job, &(index as i32), &hash],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ($1,'env','s','s','n','w','t','subject','error',$2,'unknown')",&[&job.to_string(),&job]).await.unwrap();
    let secret = b"fixture-secret-012345678901234567";
    let key = format!("whsec_{}", STANDARD.encode(secret));
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let origin = format!("http://127.0.0.1:{port}");
    let mut api = Process(
        std::process::Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env_clear()
            .env("DATABASE_URL", api_db.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
            .env("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env")
            .env("NOVU_RESEND_WEBHOOK_SECRET", &key)
            .env("NOVU_RESEND_WEBHOOK_LOCK_RETRY_JOBS", "true")
            .env("NOVU_RESEND_WEBHOOK_CAPTURE_CORRELATION", "true")
            .env("NOVU_RESEND_WEBHOOK_LOCK_CORRELATION_JOBS", "true")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let mut ready = false;
    for _ in 0..100 {
        if client
            .get(format!("{origin}/health/ready"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            ready = true;
            break;
        }
        assert!(api.0.try_wait().unwrap().is_none(), "API exited");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ready, "API not ready");
    let mut worker = Process(
        std::process::Command::new(std::env::var("TEST_RESEND_PROJECTION_BINARY").unwrap())
            .env_clear()
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
            .env("NOVU_RESEND_PROJECTION_AUTO_CORRELATE", "true")
            .env("NOVU_RESEND_PROJECTION_SCAN_INTERVAL_SECONDS", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for_result(&db, job, "unknown").await;
    assert!(
        worker.0.try_wait().unwrap().is_none(),
        "projection worker exited"
    );
    db.execute("DELETE FROM novu_rust_resend_webhook_lock_grants WHERE environment_id='env' AND database_principal::text=$1",&[&role]).await.unwrap();
    assert_eq!(
        send_http(
            &origin,
            "msg_no_grant",
            "email.delivered",
            "provider-1",
            tag,
            "alice@example.test",
            secret
        )
        .await,
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM novu_rust_resend_delivery_events WHERE environment_id='env'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.execute("INSERT INTO novu_rust_resend_webhook_lock_grants SELECT 'env',$1,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    assert_eq!(
        send_http(
            &origin,
            "msg_first",
            "email.delivered",
            "provider-1",
            tag,
            "alice@example.test",
            secret
        )
        .await,
        reqwest::StatusCode::NO_CONTENT
    );
    wait_for_result(&db, job, "pending").await;
    assert_eq!(
        send_http(
            &origin,
            "msg_second",
            "email.bounced",
            "provider-1",
            tag,
            "bob@example.test",
            secret
        )
        .await,
        reqwest::StatusCode::NO_CONTENT
    );
    wait_for_result(&db, job, "partial").await;
    assert_eq!(
        send_http(
            &origin,
            "msg_conflict",
            "email.delivered",
            "provider-2",
            tag,
            "alice@example.test",
            secret
        )
        .await,
        reqwest::StatusCode::NO_CONTENT
    );
    wait_for_result(&db, job, "unknown").await;
    let row=db.query_one("SELECT j.status,a.state,(SELECT count(*) FROM novu_rust_resend_auto_correlations WHERE job_id=j.id),(SELECT count(*) FROM novu_rust_resend_projection_heartbeat WHERE environment_id='env') FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, String>(1), "accepted");
    assert_eq!(row.get::<_, i64>(2), 1);
    assert_eq!(row.get::<_, i64>(3), 1);
    assert!(worker.0.try_wait().unwrap().is_none());
}
