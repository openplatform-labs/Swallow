//! Synthetic Twilio callbacks with atomic token replacement.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;
const PATH: &str = "/webhooks/organizations/org-a/environments/env-a/sms/twilio";
const URL: &str =
    "https://events.example.test/webhooks/organizations/org-a/environments/env-a/sms/twilio";
const BODY: &str = "AccountSid=AC11111111111111111111111111111111&MessageSid=SM22222222222222222222222222222222&MessageStatus=delivered&Extra=z&Extra=a&Extra=a&To=%2B15555555555";
fn bundle(path: &Path, current: &str, previous: Option<&str>) {
    let temp = path.with_extension("next");
    std::fs::write(
        &temp,
        serde_json::json!({"current":current,"previous":previous}).to_string(),
    )
    .unwrap();
    std::fs::rename(temp, path).unwrap();
}
async fn call(app: Router, token: &str) -> StatusCode {
    let data = format!(
        "{URL}AccountSidAC{}ExtraaExtrazMessageSidSM{}MessageStatusdeliveredTo+15555555555",
        "1".repeat(32),
        "2".repeat(32)
    );
    let sig = STANDARD.encode(
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, token.as_bytes()),
            data.as_bytes(),
        )
        .as_ref(),
    );
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(PATH)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("x-twilio-signature", sig)
            .body(Body::from(BODY))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn atomic_twilio_token_rotation_accepts_overlap_and_fails_closed() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES('env-a','org-a')",
        &[],
    )
    .await
    .unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-twilio-bundle-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("bundle");
    bundle(&path, "old-fixture-token", None);
    let config = novu_rust_api::twilio_webhook::Config::new_reloading(
        "env-a".into(),
        format!("AC{}", "1".repeat(32)),
        URL.into(),
        path.clone(),
    )
    .unwrap();
    let app =
        novu_rust_api::router_with_twilio_config(Arc::new(db), None, HashMap::new(), Some(config));
    assert_eq!(
        call(app.clone(), "old-fixture-token").await,
        StatusCode::NO_CONTENT
    );
    bundle(&path, "new-fixture-token", Some("old-fixture-token"));
    assert_eq!(
        call(app.clone(), "old-fixture-token").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(app.clone(), "new-fixture-token").await,
        StatusCode::NO_CONTENT
    );
    bundle(&path, "new-fixture-token", None);
    assert_eq!(
        call(app.clone(), "old-fixture-token").await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(app.clone(), "new-fixture-token").await,
        StatusCode::NO_CONTENT
    );
    std::fs::write(&path, "bad").unwrap();
    assert_eq!(
        call(app.clone(), "new-fixture-token").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    std::fs::remove_file(&path).unwrap();
    assert_eq!(
        call(app.clone(), "new-fixture-token").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    bundle(&path, "new-fixture-token", Some("new-fixture-token"));
    assert_eq!(
        call(app.clone(), "new-fixture-token").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    bundle(&path, "new-fixture-token", None);
    assert_eq!(call(app, "new-fixture-token").await, StatusCode::NO_CONTENT);
    std::fs::remove_dir_all(dir).unwrap();
}
