//! HTTP and PostgreSQL integration for the supported Slack endpoint subset.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tower::ServiceExt;

async fn request(
    app: axum::Router,
    method: &str,
    path: &str,
    key: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("ApiKey {key}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn scoped_slack_endpoint_crud_and_subscriber_deletion() {
    let (db, driver) = tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    for migration in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::in_app::MIGRATION,
        include_str!("../../../worker/rust-queue/migrations/0075_slack_channel_endpoints.sql"),
        include_str!("../../../worker/rust-queue/migrations/0076_slack_endpoint_integrations.sql"),
        include_str!("../migrations/0016_slack_endpoint_permissions.sql"),
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    db.batch_execute("INSERT INTO novu_rust_users VALUES ('u'); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('a','alice','old'),('b','alice','other'); INSERT INTO novu_rust_slack_endpoint_integrations VALUES ('a','main'),('b','main'); INSERT INTO novu_rust_slack_endpoint_connections VALUES ('a','main','connection')").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"key-a"));
    let hash_b = format!("{:x}", Sha256::digest(b"key-b"));
    db.execute("INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'a','u'),($2,'b','u')", &[&hash,&hash_b]).await.unwrap();
    let api_pid: i32 = db
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let payload = json!({"identifier":"endpoint","subscriberId":"alice","integrationIdentifier":"main","connectionIdentifier":"connection","type":"slack_channel","endpoint":{"channelId":"C123"}});
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/channel-endpoints",
            "key-a",
            payload.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (mut setup, driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    setup.execute("INSERT INTO novu_rust_slack_endpoint_permissions VALUES ($1,'read'),($1,'write'),($2,'read')", &[&hash,&hash_b]).await.unwrap();
    let (status, body) = request(
        app.clone(),
        "POST",
        "/v1/channel-endpoints",
        "key-a",
        payload.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["endpoint"], json!({"channelId":"C123"}));
    assert_eq!(body["data"]["subscriberId"], "alice");
    assert_eq!(body["data"]["contextKeys"], json!([]));
    if let Ok(source) = std::env::var("NOVU_REFERENCE_SOURCE") {
        let output = std::process::Command::new("node")
            .arg("tests/reference_slack_endpoint_mapper.cjs")
            .env("NOVU_REFERENCE_SOURCE", source)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let reference: Value = serde_json::from_slice(&output.stdout).unwrap();
        let mut actual = body["data"].clone();
        actual.as_object_mut().unwrap().remove("createdAt");
        actual.as_object_mut().unwrap().remove("updatedAt");
        assert_eq!(actual, reference);
    }
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/channel-endpoints",
            "key-a",
            payload.clone()
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(
            app.clone(),
            "GET",
            "/v1/channel-endpoints/endpoint",
            "key-b",
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            app.clone(),
            "GET",
            "/v1/channel-endpoints/endpoint",
            "key-a",
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        request(
            app.clone(),
            "DELETE",
            "/v1/channel-endpoints/endpoint",
            "key-b",
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let mut invalid = payload.clone();
    invalid["identifier"] = json!("invalid");
    invalid["endpoint"]["token"] = json!("secret");
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/channel-endpoints",
            "key-a",
            invalid
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let mut missing = payload.clone();
    missing["identifier"] = json!("missing");
    missing["connectionIdentifier"] = json!("other");
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/channel-endpoints",
            "key-a",
            missing
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            app.clone(),
            "DELETE",
            "/v1/channel-endpoints/endpoint",
            "key-a",
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        setup
            .query_one("SELECT count(*) FROM novu_rust_slack_endpoint_audit", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/channel-endpoints",
            "key-a",
            payload
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let lock = setup.transaction().await.unwrap();
    lock.query_one("SELECT 1 FROM novu_rust_subscribers WHERE environment_id='a' AND subscriber_id='alice' FOR UPDATE",&[]).await.unwrap();
    let mut race_payload = json!({"identifier":"race","subscriberId":"alice","integrationIdentifier":"main","type":"slack_user","endpoint":{"userId":"U123"}});
    let race_app = app.clone();
    let create_task = tokio::spawn(async move {
        request(
            race_app,
            "POST",
            "/v1/channel-endpoints",
            "key-a",
            race_payload.take(),
        )
        .await
        .0
    });
    let (observer, driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let mut waiting = false;
    for _ in 0..200 {
        waiting = observer
            .query_one(
                "SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1",
                &[&api_pid],
            )
            .await
            .unwrap()
            .get::<_, Option<bool>>(0)
            == Some(true);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(waiting, "request must wait for subscriber row");
    let revoke_hash = hash.clone();
    let revoke = tokio::spawn(async move {
        observer.execute("DELETE FROM novu_rust_slack_endpoint_permissions WHERE key_hash=$1 AND permission='write'", &[&revoke_hash]).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !revoke.is_finished(),
        "permission revocation must wait for the in-flight write"
    );
    lock.commit().await.unwrap();
    assert_eq!(create_task.await.unwrap(), StatusCode::CREATED);
    assert_eq!(revoke.await.unwrap().unwrap(), 1);
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/channel-endpoints",
            "key-a",
            json!({})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    setup
        .execute(
            "DELETE FROM novu_rust_subscribers WHERE environment_id='a' AND subscriber_id='alice'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        request(
            app.clone(),
            "GET",
            "/v1/channel-endpoints/endpoint",
            "key-a",
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    setup
        .execute(
            "DELETE FROM novu_rust_slack_endpoint_permissions WHERE key_hash=$1",
            &[&hash],
        )
        .await
        .unwrap();
    assert_eq!(
        request(
            app,
            "GET",
            "/v1/channel-endpoints/endpoint",
            "key-a",
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}
