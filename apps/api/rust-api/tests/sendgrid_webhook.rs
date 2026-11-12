//! Uses and resets a disposable PostgreSQL database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use http_body_util::BodyExt;
use ring::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::NoTls;
use tower::ServiceExt;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn signed_events_are_scoped_idempotent_and_private() {
    let (db, connection) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    db.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        .await
        .unwrap();
    for sql in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::in_app::EMAIL_MESSAGES_MIGRATION,
        novu_postgres_queue::sendgrid_queue::MIGRATION,
        novu_postgres_queue::sendgrid_queue::DISPATCH_ID_MIGRATION,
        novu_postgres_queue::sendgrid_queue::DISPATCH_PROOF_MIGRATION,
        novu_postgres_queue::reconciliation::MIGRATION,
        novu_postgres_queue::reconciliation::RETRY_DISPATCH_MIGRATION,
        include_str!("../migrations/0004_sendgrid_webhooks.sql"),
        include_str!("../migrations/0008_sendgrid_dispatch_identity.sql"),
        include_str!("../migrations/0009_sendgrid_dispatch_proof.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng).unwrap();
    let key =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let mut spki = vec![
        0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08,
        0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
    ];
    spki.extend_from_slice(key.public_key().as_ref());
    let public = STANDARD.encode(spki);
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('env-a','org-a'),('env-b','org-b'); INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status) VALUES ('64a000000000000000000099','env-a','s','s','n','w','t','subject','sent'),('64a000000000000000000098','env-b','s','s','n','w','t','subject','sent')").await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_webhook_keys(environment_id,key_id,public_key_base64) VALUES ('env-a','key',$1)", &[&public]).await.unwrap();
    assert!(db.execute("INSERT INTO novu_rust_sendgrid_webhook_keys(environment_id,key_id,public_key_base64) VALUES ('env-b','reused',$1)", &[&public]).await.is_err());
    let job = uuid::Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        job,
        "env-a",
        novu_postgres_queue::sendgrid_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute("UPDATE novu_rust_email_messages SET email_job_id=$1 WHERE internal_id='64a000000000000000000099'", &[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,'env-a','unknown')", &[&job]).await.unwrap();
    let db = Arc::new(db);
    let router = novu_rust_api::router_with_subscriber_secret(db.clone(), None);
    let event = json!([{"id":"64a000000000000000000099","event":"delivered","sg_event_id":"event-1","timestamp":1730000000,"email":"private@example.test"}]);
    let raw = serde_json::to_vec(&event).unwrap();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let mut signed = timestamp.as_bytes().to_vec();
    signed.extend_from_slice(&raw);
    let signature = STANDARD.encode(key.sign(&rng, &signed).unwrap().as_ref());
    async fn call(
        router: axum::Router,
        path: &str,
        ts: &str,
        sig: &str,
        body: Vec<u8>,
    ) -> (StatusCode, Value) {
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("x-twilio-email-event-webhook-timestamp", ts)
                    .header("x-twilio-email-event-webhook-signature", sig)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    let path = "/webhooks/organizations/org-a/environments/env-a/email/sendgrid";
    let (status, body) = call(router.clone(), path, &timestamp, &signature, raw.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["recorded"], 1);
    let (mut operator_db, operator_driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        operator_driver.await.unwrap();
    });
    let err = novu_postgres_queue::reconciliation::resolve(
        &mut operator_db,
        "env-a",
        job,
        None,
        "retry",
        "operator",
        "ticket",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("authenticated provider callback"));
    let row = db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    // Signed batch insertion locks matching jobs, serializing against retry.
    operator_db.batch_execute("BEGIN").await.unwrap();
    operator_db
        .query_one(
            "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
            &[&job],
        )
        .await
        .unwrap();
    let waiting_router = router.clone();
    let waiting_timestamp = timestamp.clone();
    let waiting_signature = signature.clone();
    let waiting_raw = raw.clone();
    let callback = tokio::spawn(async move {
        call(
            waiting_router,
            path,
            &waiting_timestamp,
            &waiting_signature,
            waiting_raw,
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!callback.is_finished());
    operator_db.batch_execute("COMMIT").await.unwrap();
    assert_eq!(callback.await.unwrap().0, StatusCode::OK);

    let (status, body) = call(router.clone(), path, &timestamp, &signature, raw.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["recorded"], 0);
    assert_eq!(
        call(router.clone(), path, &timestamp, &signature, b"[]".to_vec())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            router.clone(),
            "/webhooks/organizations/org-b/environments/env-a/email/sendgrid",
            &timestamp,
            &signature,
            raw.clone()
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            router.clone(),
            "/webhooks/organizations/org-a/environments/env-b/email/sendgrid",
            &timestamp,
            &signature,
            raw.clone()
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(router.clone(), path, "1", &signature, raw.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let malformed = json!([{"id":"64a000000000000000000099","event":"delivered","sg_event_id":"event-3","timestamp":1730000000,"email":"private@example.test"},{"id":"bad","event":"delivered","sg_event_id":"event-4","timestamp":1730000000,"email":"private@example.test"}]);
    let malformed = serde_json::to_vec(&malformed).unwrap();
    let mut input = timestamp.as_bytes().to_vec();
    input.extend_from_slice(&malformed);
    let malformed_signature = STANDARD.encode(key.sign(&rng, &input).unwrap().as_ref());
    assert_eq!(
        call(
            router.clone(),
            path,
            &timestamp,
            &malformed_signature,
            malformed
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let cross = json!([{"id":"64a000000000000000000098","event":"delivered","sg_event_id":"event-2","timestamp":1730000000,"email":"private@example.test"}]);
    let cross = serde_json::to_vec(&cross).unwrap();
    let mut signed = timestamp.as_bytes().to_vec();
    signed.extend_from_slice(&cross);
    let signature = STANDARD.encode(key.sign(&rng, &signed).unwrap().as_ref());
    let (status, body) = call(router.clone(), path, &timestamp, &signature, cross).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["recorded"], 0);
    let row=db.query_one("SELECT count(*),bool_or(recipient_hash LIKE '%private%') FROM novu_rust_sendgrid_delivery_events", &[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert!(!row.get::<_, bool>(1));
    let tracked_job = uuid::Uuid::new_v4();
    let tracked_dispatch = uuid::Uuid::new_v4();
    let proof = "a".repeat(64);
    let proof_hash = format!("{:x}", Sha256::digest(proof.as_bytes()));
    assert_eq!(
        proof_hash,
        "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb"
    );
    novu_postgres_queue::enqueue(
        db.as_ref(),
        tracked_job,
        "env-a",
        novu_postgres_queue::sendgrid_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&tracked_job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id) VALUES ('64a000000000000000000097','env-a','s','s','n','w','t','subject','sent',$1)", &[&tracked_job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_dispatch_ids(id,job_id,environment_id,proof_hash) VALUES ($1,$2,'env-a',$3)", &[&tracked_dispatch,&tracked_job,&proof_hash]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,'env-a','unknown',$2)", &[&tracked_job,&tracked_dispatch]).await.unwrap();
    let tracked = json!([{"id":"64a000000000000000000097","event":"delivered","sg_event_id":"tracked","timestamp":1730000000,"email":"private@example.test","swallow_dispatch_id":tracked_dispatch.to_string(),"swallow_dispatch_proof":proof}]);
    let tracked = serde_json::to_vec(&tracked).unwrap();
    let mut signed = timestamp.as_bytes().to_vec();
    signed.extend_from_slice(&tracked);
    let signature = STANDARD.encode(key.sign(&rng, &signed).unwrap().as_ref());
    let result = call(router.clone(), path, &timestamp, &signature, tracked).await;
    assert_eq!(result.0, StatusCode::OK);
    assert_eq!(result.1["recorded"], 1);
    let saved: Option<uuid::Uuid> = db.query_one("SELECT dispatch_id FROM novu_rust_sendgrid_delivery_events WHERE environment_id='env-a' AND event_id='tracked'", &[]).await.unwrap().get(0);
    assert_eq!(saved, Some(tracked_dispatch));
    let stored = db.query_one("SELECT row_to_json(e)::text FROM novu_rust_sendgrid_delivery_events e WHERE event_id='tracked'", &[]).await.unwrap().get::<_,String>(0);
    assert!(!stored.contains(&proof) && !stored.contains(&proof_hash));
    let err = novu_postgres_queue::reconciliation::resolve(
        &mut operator_db,
        "env-a",
        tracked_job,
        None,
        "retry",
        "operator",
        "ticket",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("authenticated provider callback"));
    let other_dispatch = uuid::Uuid::new_v4();
    db.execute("INSERT INTO novu_rust_sendgrid_dispatch_ids(id,job_id,environment_id,proof_hash) VALUES ($1,$2,'env-a',$3)", &[&other_dispatch,&job,&proof_hash]).await.unwrap();
    let cross_dispatch = json!([{"id":"64a000000000000000000097","event":"delivered","sg_event_id":"unmatched","timestamp":1730000000,"email":"private@example.test","swallow_dispatch_id":other_dispatch.to_string(),"swallow_dispatch_proof":proof}]);
    let raw = serde_json::to_vec(&cross_dispatch).unwrap();
    let mut signed = timestamp.as_bytes().to_vec();
    signed.extend_from_slice(&raw);
    let signature = STANDARD.encode(key.sign(&rng, &signed).unwrap().as_ref());
    assert_eq!(
        call(router.clone(), path, &timestamp, &signature, raw)
            .await
            .0,
        StatusCode::OK
    );
    let unbound: Option<uuid::Uuid> = db.query_one("SELECT dispatch_id FROM novu_rust_sendgrid_delivery_events WHERE environment_id='env-a' AND event_id='unmatched'", &[]).await.unwrap().get(0);
    assert_eq!(unbound, None);
    // A known UUID alone or an incorrect proof can never be associated.
    for (event_id, presented_proof) in [
        ("missing-proof", None),
        ("wrong-proof", Some("b".repeat(64))),
    ] {
        let value = json!([{"id":"64a000000000000000000097","event":"delivered","sg_event_id":event_id,"timestamp":1730000000,"email":"private@example.test","swallow_dispatch_id":tracked_dispatch.to_string(),"swallow_dispatch_proof":presented_proof}]);
        let raw = serde_json::to_vec(&value).unwrap();
        let mut input = timestamp.as_bytes().to_vec();
        input.extend_from_slice(&raw);
        let signature = STANDARD.encode(key.sign(&rng, &input).unwrap().as_ref());
        assert_eq!(
            call(router.clone(), path, &timestamp, &signature, raw)
                .await
                .0,
            StatusCode::OK
        );
        let saved:Option<uuid::Uuid>=db.query_one("SELECT dispatch_id FROM novu_rust_sendgrid_delivery_events WHERE event_id=$1 AND environment_id='env-a'", &[&event_id]).await.unwrap().get(0);
        assert_eq!(saved, None);
    }
    let malformed_id = json!([{"id":"64a000000000000000000097","event":"delivered","sg_event_id":"bad-dispatch","timestamp":1730000000,"email":"private@example.test","swallow_dispatch_id":"not-a-uuid"}]);
    let raw = serde_json::to_vec(&malformed_id).unwrap();
    let mut signed = timestamp.as_bytes().to_vec();
    signed.extend_from_slice(&raw);
    let signature = STANDARD.encode(key.sign(&rng, &signed).unwrap().as_ref());
    assert_eq!(
        call(router, path, &timestamp, &signature, raw).await.0,
        StatusCode::OK
    );
    let unbound: Option<uuid::Uuid> = db.query_one("SELECT dispatch_id FROM novu_rust_sendgrid_delivery_events WHERE environment_id='env-a' AND event_id='bad-dispatch'", &[]).await.unwrap().get(0);
    assert_eq!(unbound, None);
}
