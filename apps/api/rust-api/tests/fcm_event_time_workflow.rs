//! Trigger-to-mocked-FCM verification of the optional JSON eventTime extension.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, process::Command, sync::Arc};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
const ENV: &str = "64a000000000000000000002";
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
async fn trigger(app: axum::Router, txn: &str, time: Value) -> StatusCode {
    let body = json!({"name":"event-time-workflow","to":"fixture-subscriber","transactionId":txn,"payload":{},"overrides":{"fcm":{"android":{"notification":{"eventTime":time},"fcmOptions":{"analyticsLabel":"workflow-campaign"}}}}});
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events/trigger")
                .header("authorization", "ApiKey event-time-key")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    status
}
async fn capture(url: &str, count: usize) -> Vec<Value> {
    let path = std::env::temp_dir().join(format!("swallow-fcm-capture-{}.json", Uuid::new_v4()));
    let path2 = path.clone();
    let url = url.to_owned();
    let output = tokio::task::spawn_blocking(move || {
        let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../worker/rust-queue");
        let mut cmd = Command::new("cargo");
        cmd.current_dir(worker)
            .args([
                "test",
                "--locked",
                "--offline",
                "--lib",
                "capture_reference_workflow_push",
                "--",
                "--ignored",
            ])
            .env("TEST_DATABASE_URL", url)
            .env("TEST_PUSH_EXPECTED_COUNT", count.to_string())
            .env("TEST_PUSH_CAPTURE_PATH", path2)
            .env("TEST_PUSH_ALLOW_SHARED_PROVIDER", "true");
        for name in [
            "TEST_PUSH_INVALID_TARGET",
            "TEST_PUSH_HTTP_STATUS",
            "TEST_PUSH_ERROR_BODY",
            "TEST_PUSH_BARRIER_DIR",
            "TEST_PUSH_AUTH_CAPTURE_PATH",
            "TEST_PUSH_CLEANUP_GUARD",
            "TEST_PUSH_INTEGRATIONS",
            "TEST_PUSH_STEP_POLICY",
        ] {
            cmd.env_remove(name);
        }
        if let Ok(target) = std::env::var("SWALLOW_WORKER_TEST_TARGET_DIR") {
            cmd.env("CARGO_TARGET_DIR", target);
        }
        cmd.output().unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let values = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    std::fs::remove_file(path).unwrap();
    values
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and loopback"]
async fn event_time_flows_from_trigger_through_queue_to_fcm() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut worker = connect(&url).await;
    worker.batch_execute("INSERT INTO novu_rust_users VALUES('event-user'); INSERT INTO novu_rust_environments VALUES('64a000000000000000000002','event-org'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,push_title,push_content,push_step_id,inbox_metadata) VALUES('64a000000000000000000002','event-time-workflow',true,true,'Hi','Hello','push-step','{\"id\":\"64a000000000000000000051\",\"name\":\"Fixture\",\"critical\":false,\"tags\":[]}'); INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,push_enabled) VALUES('64a000000000000000000002','event-time-workflow',true,false,true); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES('64a000000000000000000002','fixture-subscriber','64a000000000000000000054','{\"firstName\":\"Test\"}'); INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) VALUES('64a000000000000000000002','64a000000000000000000053','fixture-fcm',true); INSERT INTO novu_rust_fcm_devices(environment_id,subscriber_id,integration_internal_id,device_tokens) VALUES('64a000000000000000000002','fixture-subscriber','64a000000000000000000053',ARRAY['fixture-device']);").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"event-time-key"));
    worker.execute("INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,$2,'event-user')",&[&hash,&ENV]).await.unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(connect(&url).await), None);
    trigger(
        app.clone(),
        "event-time-valid",
        json!("2026-11-14T21:34:56.999987+09:00"),
    )
    .await;
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    let rows = capture(&url, 1).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["message"]["android"]["notification"]["event_time"],
        "2026-11-14T12:34:56.999Z"
    );
    assert_eq!(rows[0]["message"]["token"], "fixture-device");
    assert_eq!(
        rows[0]["message"]["android"]["fcmOptions"]["analyticsLabel"],
        "workflow-campaign"
    );
    let row=worker.query_one("SELECT j.status,(SELECT count(*) FROM public.novu_rust_fcm_attempts a WHERE a.job_id=j.id AND a.state='accepted') FROM public.novu_queue_jobs j JOIN public.novu_rust_push_step_outcomes o ON o.push_job_id=j.id WHERE o.transaction_id='event-time-valid'",&[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, i64>(1), 1);
    trigger(app, "event-time-invalid", json!("bad timestamp")).await;
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    assert!(capture(&url, 0).await.is_empty());
    let row=worker.query_one("SELECT j.status,(SELECT count(*) FROM public.novu_rust_fcm_attempts a WHERE a.job_id=j.id),(SELECT status FROM public.novu_rust_push_step_outcomes o WHERE o.transaction_id='event-time-invalid') FROM public.novu_queue_jobs j WHERE j.kind='experimental_fcm_push' AND j.payload->'workflow_context'->>'transaction_id'='event-time-invalid'",&[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, i64>(1), 0);
    assert_eq!(row.get::<_, String>(2), "failed");
}
