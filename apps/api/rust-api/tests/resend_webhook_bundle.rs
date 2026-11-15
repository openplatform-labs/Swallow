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
