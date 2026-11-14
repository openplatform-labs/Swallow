//! Conditional channel-step writes against an isolated PostgreSQL instance.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
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
async fn call(app: axum::Router, path: &str, body: Value) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri(path)
            .header("authorization", "ApiKey fixture-key")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn conditional_writes_lock_compare_and_audit() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    db.batch_execute("INSERT INTO novu_rust_users VALUES ('u'); INSERT INTO novu_rust_environments VALUES ('a','org'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active) VALUES ('a','workflow',true,true,false)").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"fixture-key"));
    db.execute(
        "INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'a','u')",
        &[&hash],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_channel_workflow_permissions SELECT $1,c,p FROM unnest(ARRAY['email','sms','push']) c CROSS JOIN unnest(ARRAY['read','write']) p",&[&hash]).await.unwrap();
    db.execute(
        "INSERT INTO novu_rust_slack_workflow_permissions VALUES ($1,'read'),($1,'write')",
        &[&hash],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO novu_rust_workflow_provider_permissions VALUES ($1,'read'),($1,'write')",
        &[&hash],
    )
    .await
    .unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let (mut setup, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let provider_path = "/v1/experimental/workflows/workflow/email-provider";
    assert_eq!(
        call(
            app.clone(),
            provider_path,
            json!({"provider":"resend","expected":"sendgrid"})
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            provider_path,
            json!({"provider":"sendgrid","expected":"sendgrid"})
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            app.clone(),
            provider_path,
            json!({"provider":"sendgrid","expected":null})
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    let tx = setup.transaction().await.unwrap();
    tx.execute("UPDATE novu_rust_workflows SET email_provider='sendgrid' WHERE environment_id='a' AND identifier='workflow'",&[]).await.unwrap();
    let pending = tokio::spawn(call(
        app.clone(),
        provider_path,
        json!({"provider":"sendgrid","expected":"resend"}),
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::CONFLICT);
    assert_eq!(setup.query_one("SELECT email_provider,(SELECT count(*) FROM novu_rust_workflow_provider_audit) FROM novu_rust_workflows WHERE environment_id='a' AND identifier='workflow'",&[]).await.unwrap().get::<_,String>(0),"sendgrid");
    assert_eq!(
        setup
            .query_one(
                "SELECT count(*) FROM novu_rust_workflow_provider_audit",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    for channel in ["email", "sms", "push", "slack"] {
        let path = if channel == "slack" {
            "/v1/experimental/workflows/workflow/slack-step".to_string()
        } else {
            format!("/v1/experimental/workflows/workflow/channel-steps/{channel}")
        };
        let titled = channel == "email" || channel == "push";
        let expected = if channel == "slack" {
            json!({"content":null,"active":true})
        } else {
            json!({"title":null,"content":null,"active":true})
        };
        let mut body = if channel == "slack" {
            json!({"content":"new","active":false,"expected":expected})
        } else {
            json!({"title":if titled {Some("title")} else {None},"content":"new","active":false,"expected":expected})
        };
        assert_eq!(
            call(app.clone(), &path, body.clone()).await,
            StatusCode::OK,
            "{channel}"
        );
        assert_eq!(
            call(app.clone(), &path, body.clone()).await,
            StatusCode::CONFLICT,
            "{channel}"
        );
        body["expected"] = json!({});
        assert_eq!(
            call(app.clone(), &path, body).await,
            StatusCode::BAD_REQUEST,
            "{channel}"
        );
    }
    assert_eq!(setup.query_one("SELECT (SELECT count(*) FROM novu_rust_channel_workflow_audit),(SELECT count(*) FROM novu_rust_slack_workflow_audit)",&[]).await.unwrap().get::<_,i64>(0),3);
    // Block the comparison on the workflow row, commit a competing change, then verify the waiting request rechecks it.
    let tx = setup.transaction().await.unwrap();
    tx.execute("UPDATE novu_rust_workflows SET email_html='competitor' WHERE environment_id='a' AND identifier='workflow'",&[]).await.unwrap();
    let pending = tokio::spawn(call(
        app.clone(),
        "/v1/experimental/workflows/workflow/channel-steps/email",
        json!({"title":"mine","content":"mine","active":true,"expected":{"title":"title","content":"new","active":false}}),
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::CONFLICT);
    let row=setup.query_one("SELECT email_subject,email_html FROM novu_rust_workflows WHERE environment_id='a' AND identifier='workflow'",&[]).await.unwrap();
    assert_eq!(row.get::<_, Option<String>>(0).as_deref(), Some("title"));
    assert_eq!(
        row.get::<_, Option<String>>(1).as_deref(),
        Some("competitor")
    );
    assert_eq!(
        setup
            .query_one("SELECT count(*) FROM novu_rust_channel_workflow_audit", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        3
    );
    let tx = setup.transaction().await.unwrap();
    tx.execute("UPDATE novu_rust_workflows SET chat_content='competitor' WHERE environment_id='a' AND identifier='workflow'",&[]).await.unwrap();
    let pending = tokio::spawn(call(
        app.clone(),
        "/v1/experimental/workflows/workflow/slack-step",
        json!({"content":"mine","active":true,"expected":{"content":"new","active":false}}),
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::CONFLICT);
    assert_eq!(setup.query_one("SELECT chat_content FROM novu_rust_workflows WHERE environment_id='a' AND identifier='workflow'",&[]).await.unwrap().get::<_,Option<String>>(0).as_deref(),Some("competitor"));
    assert_eq!(
        setup
            .query_one("SELECT count(*) FROM novu_rust_slack_workflow_audit", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    setup.execute("DELETE FROM novu_rust_channel_workflow_permissions WHERE key_hash=$1 AND channel='email' AND permission='write'",&[&hash]).await.unwrap();
    assert_eq!(call(app.clone(),"/v1/experimental/workflows/workflow/channel-steps/email",json!({"title":"mine","content":"mine","active":true,"expected":{"title":"title","content":"competitor","active":false}})).await,StatusCode::FORBIDDEN);
}
