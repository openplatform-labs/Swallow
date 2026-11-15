//! Atomic Resend webhook secret rotation with a disposable database.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;
fn key(raw: &[u8]) -> String {
    format!("whsec_{}", STANDARD.encode(raw))
}
fn bundle(path: &Path, current: &str, previous: Option<&str>) {
    let data = serde_json::json!({"current":current,"previous":previous}).to_string();
    let tmp = path.with_extension("next");
    std::fs::write(&tmp, data).unwrap();
    std::fs::rename(tmp, path).unwrap();
}
async fn call(app: Router, raw: &[u8], id: &str) -> StatusCode {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body = br#"{"type":"email.delivered","data":{"email_id":"email-test"}}"#;
    let mut signed = format!("{id}.{timestamp}.").into_bytes();
    signed.extend_from_slice(body);
    let sig = format!(
        "v1,{}",
        STANDARD.encode(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, raw), &signed).as_ref())
    );
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/webhooks/organizations/org/environments/env/email/resend")
            .header("svix-id", id)
            .header("svix-timestamp", timestamp)
            .header("svix-signature", sig)
            .body(Body::from(body.as_slice()))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn atomic_bundle_reload_accepts_overlap_and_fails_closed() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES('env','org')",
        &[],
    )
    .await
    .unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-resend-bundle-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("bundle.json");
    let old = b"old-test-secret-32-bytes-long!!!";
    let new = b"new-test-secret-32-bytes-long!!!";
    bundle(&path, &key(old), None);
    let config =
        novu_rust_api::resend_webhook::Config::new_reloading("env".into(), path.clone()).unwrap();
    let app = novu_rust_api::router_with_webhook_configs(
        Arc::new(db),
        None,
        std::collections::HashMap::new(),
        None,
        Some(config),
    );
    assert_eq!(
        call(app.clone(), old, "msg_old").await,
        StatusCode::NO_CONTENT
    );
    bundle(&path, &key(new), Some(&key(old)));
    assert_eq!(
        call(app.clone(), old, "msg_overlap_old").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(app.clone(), new, "msg_overlap_new").await,
        StatusCode::NO_CONTENT
    );
    bundle(&path, &key(new), None);
    assert_eq!(
        call(app.clone(), old, "msg_retired").await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(app.clone(), new, "msg_new").await,
        StatusCode::NO_CONTENT
    );
    std::fs::write(&path, "{invalid").unwrap();
    assert_eq!(
        call(app.clone(), new, "msg_invalid").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    std::fs::remove_file(&path).unwrap();
    assert_eq!(
        call(app.clone(), new, "msg_missing").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    bundle(&path, &key(new), Some(&key(new)));
    assert_eq!(
        call(app.clone(), new, "msg_duplicate").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    bundle(&path, &key(new), None);
    assert_eq!(
        call(app, new, "msg_recovered").await,
        StatusCode::NO_CONTENT
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn process_reports_bundle_failure_and_recovery_without_leaking_secrets() {
    use std::{
        net::TcpListener,
        process::{Child, Command, Stdio},
        time::Duration,
    };
    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let dir =
        Directory(std::env::temp_dir().join(format!("swallow-resend-probe-{}", Uuid::new_v4())));
    std::fs::create_dir(&dir.0).unwrap();
    let path = dir.0.join("bundle");
    let secret = key(b"secret-fixture-32-bytes-long!!!!");
    bundle(&path, &secret, None);
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env_clear()
            .env("DATABASE_URL", "postgres://postgres@127.0.0.1:1/postgres")
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
            .env("SWALLOW_API_RESEND_WEBHOOK_BUNDLE_HEALTH_ENABLED", "true")
            .env("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env")
            .env("NOVU_RESEND_WEBHOOK_SECRET_BUNDLE_FILE", &path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let url = format!("http://127.0.0.1:{port}/health/resend-webhook-bundle");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if client.get(&url).send().await.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "health endpoint unavailable"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let good = client.get(&url).send().await.unwrap();
    assert_eq!(good.status(), StatusCode::OK);
    assert_eq!(good.headers().get("cache-control").unwrap(), "no-store");
    let body = good.text().await.unwrap();
    assert!(!body.contains("whsec_"));
    assert!(!body.contains(path.to_str().unwrap()));
    std::fs::write(&path, "invalid").unwrap();
    assert_eq!(
        client.get(&url).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    bundle(&path, &secret, None);
    assert_eq!(
        client.get(&url).send().await.unwrap().status(),
        StatusCode::OK
    );
}
