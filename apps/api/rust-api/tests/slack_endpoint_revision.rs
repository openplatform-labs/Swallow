//! HTTP trigger to PostgreSQL snapshot fanout and delivery, with no external sends.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
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
async fn trigger(app: axum::Router, id: &str) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events/trigger")
                .header("authorization", "ApiKey revision-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name":"workflow","to":"alice","transactionId":id,"payload":{}})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&body)
    );
}
async fn change_and_restore(db: &Client) {
    db.batch_execute("UPDATE public.novu_rust_slack_channel_endpoints SET endpoint='{\"channelId\":\"COTHER\"}' WHERE environment_id='a' AND identifier='endpoint'; UPDATE public.novu_rust_slack_channel_endpoints SET endpoint='{\"channelId\":\"C123\"}' WHERE environment_id='a' AND identifier='endpoint'").await.unwrap();
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn snapshot_revisions_fence_changes_before_fanout_and_before_delivery() {
    use novu_postgres_queue::slack_workflow::{self, EndpointPolicy, StepPolicy};
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('u'); INSERT INTO public.novu_rust_environments VALUES ('a','org'); INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES (encode(sha256(convert_to('revision-key','UTF8')),'hex'),'a','u'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active,chat_content,inbox_metadata) VALUES ('a','workflow',true,true,false,'hello','{\"id\":\"workflow-id\"}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('a','alice','alice-id'); INSERT INTO public.novu_rust_slack_endpoint_integrations(environment_id,identifier) VALUES ('a','main'); INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('a','endpoint','alice','alice-id','main','slack_channel','{\"channelId\":\"C123\"}')").await.unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    trigger(app.clone(), "before-fanout").await;
    assert!(
        novu_postgres_queue::in_app::process_one_with_slack_snapshot(
            &mut db,
            Duration::from_secs(60),
            true
        )
        .await
        .unwrap()
    );
    let original:Value=db.query_one("SELECT payload->'endpoint_snapshot' FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'",&[&slack_workflow::FANOUT_KIND]).await.unwrap().get(0);
    assert!(original[0]["revision"].as_str().is_some());
    change_and_restore(&db).await;
    assert!(
        slack_workflow::fanout_one_with_policies(
            &mut db,
            "a",
            StepPolicy::Snapshot,
            EndpointPolicy::Snapshot
        )
        .await
        .unwrap()
    );
    assert_eq!(db.query_one("SELECT last_error FROM public.novu_queue_jobs WHERE kind=$1 ORDER BY created_at DESC LIMIT 1",&[&slack_workflow::FANOUT_KIND]).await.unwrap().get::<_,String>(0),"Slack endpoint snapshot changed");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1",
            &[&slack_workflow::DELIVERY_KIND]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    trigger(app.clone(), "before-delivery").await;
    assert!(
        novu_postgres_queue::in_app::process_one_with_slack_snapshot(
            &mut db,
            Duration::from_secs(60),
            true
        )
        .await
        .unwrap()
    );
    assert!(
        slack_workflow::fanout_one_with_policies(
            &mut db,
            "a",
            StepPolicy::Snapshot,
            EndpointPolicy::Snapshot
        )
        .await
        .unwrap()
    );
    let child: Value = db
        .query_one(
            "SELECT payload FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'",
            &[&slack_workflow::DELIVERY_KIND],
        )
        .await
        .unwrap()
        .get(0);
    assert!(child["endpoint_revision"].as_str().is_some());
    change_and_restore(&db).await;
    assert!(
        slack_workflow::delivery_one(&mut db, "a", &slack_workflow::Providers::new())
            .await
            .unwrap()
    );
    assert_eq!(db.query_one("SELECT last_error FROM public.novu_queue_jobs WHERE kind=$1 ORDER BY created_at DESC LIMIT 1",&[&slack_workflow::DELIVERY_KIND]).await.unwrap().get::<_,String>(0),"Slack endpoint changed or deleted");
    assert_eq!(
        db.query_one("SELECT count(*) FROM public.novu_rust_slack_attempts", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    // Pre-upgrade snapshot payloads lack a revision and retain their previous behavior.
    trigger(app, "legacy-snapshot").await;
    assert!(
        novu_postgres_queue::in_app::process_one_with_slack_snapshot(
            &mut db,
            Duration::from_secs(60),
            true
        )
        .await
        .unwrap()
    );
    db.execute("UPDATE public.novu_queue_jobs SET payload=jsonb_set(payload,'{endpoint_snapshot}',(SELECT jsonb_agg(item - 'revision') FROM jsonb_array_elements(payload->'endpoint_snapshot') item)) WHERE kind=$1 AND status='pending'",&[&slack_workflow::FANOUT_KIND]).await.unwrap();
    change_and_restore(&db).await;
    assert!(
        slack_workflow::fanout_one_with_policies(
            &mut db,
            "a",
            StepPolicy::Snapshot,
            EndpointPolicy::Snapshot
        )
        .await
        .unwrap()
    );
    let child: Value = db
        .query_one(
            "SELECT payload FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'",
            &[&slack_workflow::DELIVERY_KIND],
        )
        .await
        .unwrap()
        .get(0);
    assert!(child.get("endpoint_revision").is_none());
    assert!(
        slack_workflow::delivery_one(&mut db, "a", &slack_workflow::Providers::new())
            .await
            .unwrap()
    );
    assert_eq!(db.query_one("SELECT last_error FROM public.novu_queue_jobs WHERE kind=$1 AND status='failed' AND last_error='Slack integration credentials not configured'",&[&slack_workflow::DELIVERY_KIND]).await.unwrap().get::<_,String>(0),"Slack integration credentials not configured");
}
