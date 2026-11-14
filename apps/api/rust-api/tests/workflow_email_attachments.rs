//! Trigger-to-email-queue attachment handling against a migrated disposable database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tower::ServiceExt;

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn trigger_preserves_bounded_attachments_for_both_email_providers() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('attachment-user'); INSERT INTO public.novu_rust_environments VALUES ('attachment-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata) VALUES ('attachment-env','resend-attachment',true,true,'Hello','<p>hello</p>','resend','{\"id\":\"workflow-r\"}'),('attachment-env','sendgrid-attachment',true,true,'Hello','<p>hello</p>','sendgrid','{\"id\":\"workflow-s\"}')").await.unwrap();
    db.batch_execute("UPDATE public.novu_rust_workflows SET in_app_content='Hello' WHERE environment_id='attachment-env'").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"attachment-key"));
    db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'attachment-env','attachment-user')",&[&hash]).await.unwrap();
    let (api, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let router = novu_rust_api::router(Arc::new(api));
    let payload = json!({"name":"Alice","attachments":[{"file":"aGVsbG8=","name":"hello.txt","mime":"text/plain"},{"file":{"type":"Buffer","data":[0,1,255]},"name":"logo.png","mime":"image/png","cid":"logo","channels":["email"]}]});
    for provider in ["resend", "sendgrid"] {
        let body = json!({"name":format!("{provider}-attachment"),"to":{"subscriberId":format!("{provider}-person"),"email":"recipient@example.test"},"payload":payload,"transactionId":format!("attachment-{provider}")});
        let req = Request::builder()
            .method("POST")
            .uri("/v1/events/trigger")
            .header("authorization", "ApiKey attachment-key")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        assert_eq!(
            router.clone().oneshot(req).await.unwrap().status(),
            StatusCode::CREATED
        );
        let row=db.query_one("SELECT kind FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND payload->>'transactionId'=$1", &[&format!("attachment-{provider}")]).await.unwrap();
        assert_eq!(
            row.get::<_, String>(0),
            novu_postgres_queue::in_app::ATTACHMENT_JOB_KIND
        );
        assert!(
            novu_postgres_queue::claim(
                &db,
                novu_postgres_queue::in_app::JOB_KIND,
                std::time::Duration::from_secs(60)
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            novu_postgres_queue::in_app::process_one(&mut db)
                .await
                .unwrap()
        );
        let row=db.query_one("SELECT payload FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND payload->>'transaction_id'=$1", &[&format!("attachment-{provider}")]).await.unwrap();
        let queue: Value = row.get(0);
        assert_eq!(
            queue["attachments"][0]["file"],
            json!([104, 101, 108, 108, 111])
        );
        assert_eq!(queue["attachments"][1]["file"], json!([0, 1, 255]));
        assert!(
            queue["workflow_context"]["payload"]["attachments"][0]
                .get("file")
                .is_none()
        );
        let row=db.query_one("SELECT payload FROM public.novu_rust_notifications WHERE environment_id='attachment-env' AND transaction_id=$1", &[&format!("attachment-{provider}")]).await.unwrap();
        let notification: Value = row.get(0);
        assert!(notification["attachments"][0].get("file").is_none());
        let row=db.query_one("SELECT payload FROM public.novu_rust_in_app_messages WHERE environment_id='attachment-env' AND transaction_id=$1", &[&format!("attachment-{provider}")]).await.unwrap();
        let message: Value = row.get(0);
        assert!(message.get("attachments").is_none());
    }
    let invalid = json!({"name":"resend-attachment","to":{"subscriberId":"bad"},"transactionId":"bad-attachment","payload":{"attachments":[{"file":"invalid?","name":"bad","mime":"text/plain"}]}});
    let req = Request::builder()
        .method("POST")
        .uri("/v1/events/trigger")
        .header("authorization", "ApiKey attachment-key")
        .header("content-type", "application/json")
        .body(Body::from(invalid.to_string()))
        .unwrap();
    assert_eq!(
        router.oneshot(req).await.unwrap().status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE transaction_id='bad-attachment'",&[]).await.unwrap().get::<_,i64>(0),0);
}
