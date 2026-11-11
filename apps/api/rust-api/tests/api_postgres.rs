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

async fn request(app: axum::Router, auth: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri("/v1/events/trigger")
        .header("content-type", "application/json");
    if let Some(value) = auth {
        req = req.header("authorization", value);
    }
    let res = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

// This drops tables: only run against a dedicated disposable database.
#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database in TEST_DATABASE_URL"]
async fn trigger_api_contract() {
    let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let (db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db.batch_execute("DROP TABLE IF EXISTS novu_rust_in_app_messages, novu_rust_subscribers, novu_queue_jobs, novu_rust_trigger_transactions, novu_rust_workflows, novu_rust_api_keys, novu_rust_users, novu_rust_environments CASCADE")
        .await
        .unwrap();
    db.batch_execute(novu_rust_api::MIGRATION).await.unwrap();
    db.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(novu_postgres_queue::in_app::MIGRATION)
        .await
        .unwrap();
    let hash = format!("{:x}", Sha256::digest(b"test-key"));
    db.batch_execute("INSERT INTO novu_rust_users VALUES ('user-a'); INSERT INTO novu_rust_environments VALUES ('env-a', 'org-a'), ('env-b', 'org-b')").await.unwrap();
    db.execute(
        "INSERT INTO novu_rust_api_keys (key_hash, environment_id, user_id) VALUES ($1, 'env-a', 'user-a')",
        &[&hash],
    )
    .await
    .unwrap();
    let orphan_hash = format!("{:x}", Sha256::digest(b"orphan-key"));
    db.execute("INSERT INTO novu_rust_api_keys (key_hash, environment_id, user_id) VALUES ($1, 'env-a', 'missing-user')", &[&orphan_hash]).await.unwrap();
    let second_hash = format!("{:x}", Sha256::digest(b"test-key-b"));
    db.execute("INSERT INTO novu_rust_api_keys (key_hash, environment_id, user_id) VALUES ($1, 'env-b', 'user-a')", &[&second_hash]).await.unwrap();
    let disabled_hash = format!("{:x}", Sha256::digest(b"disabled-key"));
    db.execute("INSERT INTO novu_rust_api_keys (key_hash, environment_id, user_id, enabled) VALUES ($1, 'env-a', 'user-a', false)", &[&disabled_hash]).await.unwrap();
    db.batch_execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-a', 'inactive', false, true, NULL), ('env-a', 'empty', true, false, NULL), ('env-a', 'active', true, true, NULL), ('env-a', 'inbox', true, true, 'Hello'), ('env-b', 'private', false, true, NULL)").await.unwrap();
    let app = novu_rust_api::router(Arc::new(db));
    let valid = json!({"name":"inactive", "to":"subscriber", "transactionId":"tx-1"});
    let (status, body) = request(app.clone(), None, valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "Missing authorization header");
    let (status, body) = request(app.clone(), Some("ApiKey invalid"), valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "API Key not found");
    let (status, body) = request(app.clone(), Some("ApiKey disabled-key"), valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "API Key not found");
    let (status, body) = request(app.clone(), Some("ApiKey orphan-key"), valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "User not found");
    for auth in ["ApiKey", "Bearer", "Bearer invalid"] {
        let (status, body) = request(app.clone(), Some(auth), valid.clone()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["message"], "Unauthorized");
    }
    let (status, body) = request(app.clone(), Some("Invalid invalid_value"), valid.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        body["message"],
        "Invalid authentication scheme: \"Invalid\""
    );
    let (status, body) = request(app.clone(), Some("ApiKey test-key"), valid).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        body,
        json!({"data":{"acknowledged":true,"status":"trigger_not_active","transactionId":"tx-1"}})
    );
    let (status, body) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"empty","to":["s"],"transactionId":"tx-2"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["status"], "no_workflow_steps_defined");
    assert_eq!(body["data"]["transactionId"], "tx-2");
    for name in ["missing", "private"] {
        let (status, body) = request(
            app.clone(),
            Some("ApiKey test-key"),
            json!({"name":name,"to":"s"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["message"], "workflow_not_found");
    }
    let (status, body) = request(
        app.clone(),
        Some("ApiKey test-key-b"),
        json!({"name":"private","to":"s", "transactionId":"env-b-txn"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["status"], "trigger_not_active");
    let (status, body) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inactive","to":{"subscriberId":""}}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["message"], "Validation Error");
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"active","to":"s"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"inactive","to":"s","tenant":"a"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);

    let trigger = json!({"name":"inbox","to":["alice", "bob", "alice"],"transactionId":"test-inbox","payload":{"case":1}});
    let (first, second) = tokio::join!(
        request(app.clone(), Some("ApiKey test-key"), trigger.clone()),
        request(app.clone(), Some("ApiKey test-key"), trigger)
    );
    let (status, response) = first;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(response["data"]["status"], "processed");
    assert_eq!(response["data"]["transactionId"], "test-inbox");
    let (status, _) = second;
    assert_eq!(status, StatusCode::CREATED);
    let (mut worker, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    assert!(
        !novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let rows = worker.query("SELECT subscriber_id, content, payload, environment_id FROM novu_rust_in_app_messages ORDER BY subscriber_id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip(["alice", "bob"]) {
        assert_eq!(row.get::<_, String>(0), expected);
        assert_eq!(row.get::<_, String>(1), "Hello");
        assert_eq!(row.get::<_, Value>(2), json!({"case":1}));
        assert_eq!(row.get::<_, String>(3), "env-a");
    }
    assert_eq!(
        worker
            .query_one("SELECT COUNT(*) FROM novu_rust_subscribers", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    worker.execute("INSERT INTO novu_rust_workflows (environment_id, identifier, active, has_steps, in_app_content) VALUES ('env-a', 'templated', true, true, $1), ('env-a', 'unsupported', true, true, $2)", &[&"Hello {{subscriber.subscriberId}}: {{name}}", &"{{#if name}}yes{{/if}}"] ).await.unwrap();
    let (status, _) = request(
        app.clone(),
        Some("ApiKey test-key"),
        json!({"name":"unsupported", "to":"alice", "payload":{"name":"x"}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let (status, _) = request(app, Some("ApiKey test-key"), json!({"name":"templated", "to":"alice", "payload":{"name":"<A & B>"}, "transactionId":"template-test"})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let content: String = worker.query_one("SELECT content FROM novu_rust_in_app_messages WHERE environment_id = 'env-a' AND transaction_id = 'template-test'", &[]).await.unwrap().get(0);
    assert_eq!(content, "Hello alice: &lt;A &amp; B&gt;");
}
