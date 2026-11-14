//! Signed SendGrid callbacks projected against disposable PostgreSQL.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn callback(app: axum::Router, key: &EcdsaKeyPair, events: Value) -> StatusCode {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body = serde_json::to_vec(&events).unwrap();
    let mut signed = ts.as_bytes().to_vec();
    signed.extend_from_slice(&body);
    let signature = STANDARD.encode(key.sign(&SystemRandom::new(), &signed).unwrap().as_ref());
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/webhooks/organizations/org-a/environments/env-a/email/sendgrid")
            .header("x-twilio-email-event-webhook-timestamp", ts)
            .header("x-twilio-email-event-webhook-signature", signature)
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn signed_events_update_current_recipient_and_job_results() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    for sql in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::in_app::EMAIL_MESSAGES_MIGRATION,
        novu_postgres_queue::sendgrid_queue::MIGRATION,
        novu_postgres_queue::sendgrid_queue::DISPATCH_ID_MIGRATION,
        novu_postgres_queue::sendgrid_queue::DISPATCH_PROOF_MIGRATION,
        novu_postgres_queue::sendgrid_queue::RECIPIENT_MIGRATION,
        novu_postgres_queue::sendgrid_projection::MIGRATION,
        include_str!("../migrations/0004_sendgrid_webhooks.sql"),
        include_str!("../migrations/0008_sendgrid_dispatch_identity.sql"),
        include_str!("../migrations/0009_sendgrid_dispatch_proof.sql"),
        include_str!("../migrations/0020_sendgrid_bounce_type.sql"),
        include_str!(
            "../../../worker/rust-queue/migrations/0100_sendgrid_projection_invalidation.sql"
        ),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(novu_postgres_queue::sendgrid_projection::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(
        "INSERT INTO novu_rust_environments VALUES ('env-a','org-a'),('env-b','org-b')",
    )
    .await
    .unwrap();
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng).unwrap();
    let key =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let mut spki = vec![
        0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08,
        0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
    ];
    spki.extend_from_slice(key.public_key().as_ref());
    db.execute(
        "INSERT INTO novu_rust_sendgrid_webhook_keys VALUES ('env-a','fixture',$1,true)",
        &[&STANDARD.encode(spki)],
    )
    .await
    .unwrap();
    let job = Uuid::new_v4();
    let dispatch = Uuid::new_v4();
    let old = Uuid::new_v4();
    let proof = "a".repeat(64);
    let proof_hash = format!("{:x}", Sha256::digest(proof.as_bytes()));
    novu_postgres_queue::enqueue(
        &db,
        job,
        "env-a",
        novu_postgres_queue::sendgrid_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id) VALUES ('64a000000000000000000097','env-a','s','s','n','w','t','subject','sent',$1)",&[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_dispatch_ids(id,job_id,environment_id,proof_hash) VALUES ($1,$3,'env-a',$4),($2,$3,'env-a',$4)",&[&dispatch,&old,&job,&proof_hash]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,'env-a','accepted',$2)",&[&job,&dispatch]).await.unwrap();
    for (index, address) in ["one@example.test", "two@example.test"].iter().enumerate() {
        let hash = format!("{:x}", Sha256::digest(address.as_bytes()));
        db.execute("INSERT INTO novu_rust_sendgrid_recipients(dispatch_id,environment_id,recipient_kind,recipient_index,recipient_hash) VALUES ($1,'env-a','to',$2,$3)",&[&dispatch,&(index as i32),&hash]).await.unwrap();
    }
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let state = |address: &str,
                 kind: &str,
                 ty: Option<&str>,
                 id: &str,
                 dispatch_id: Uuid,
                 valid: bool| json!({"id":"64a000000000000000000097","event":kind,"type":ty,"sg_event_id":id,"timestamp":1730000000,"email":address,"swallow_dispatch_id":dispatch_id.to_string(),"swallow_dispatch_proof":if valid {proof.clone()} else {"b".repeat(64)}});
    async fn result(db: &mut tokio_postgres::Client, job: Uuid) -> String {
        assert!(
            novu_postgres_queue::sendgrid_projection::project(db, "env-a", job)
                .await
                .unwrap()
        );
        db.query_one(
            "SELECT sendgrid_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job],
        )
        .await
        .unwrap()
        .get(0)
    }
    assert_eq!(result(&mut db, job).await, "pending");
    assert_eq!(
        callback(
            app.clone(),
            &key,
            json!([
                state(
                    "one@example.test",
                    "delivered",
                    None,
                    "wrong-proof",
                    dispatch,
                    false
                ),
                state(
                    "two@example.test",
                    "dropped",
                    None,
                    "old-dispatch",
                    old,
                    true
                )
            ])
        )
        .await,
        StatusCode::OK
    );
    let before: String = db
        .query_one(
            "SELECT sendgrid_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        before, "pending",
        "unverified and old dispatch events do not invalidate"
    );
    assert_eq!(result(&mut db, job).await, "pending");
    assert_eq!(
        callback(
            app.clone(),
            &key,
            json!([
                state(
                    "one@example.test",
                    "delivered",
                    None,
                    "one-delivered",
                    dispatch,
                    true
                ),
                state(
                    "two@example.test",
                    "bounce",
                    Some("blocked"),
                    "two-soft",
                    dispatch,
                    true
                )
            ])
        )
        .await,
        StatusCode::OK
    );
    let invalidated=db.query_one("SELECT j.sendgrid_delivery_result,m.sendgrid_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(invalidated.get::<_, String>(0), "unknown");
    assert_eq!(invalidated.get::<_, String>(1), "unknown");
    let pending_recipients: i64 = db.query_one("SELECT count(*) FROM novu_rust_sendgrid_recipients WHERE dispatch_id=$1 AND delivery_result<>'unknown'", &[&dispatch]).await.unwrap().get(0);
    assert_eq!(pending_recipients, 0);
    assert_eq!(result(&mut db, job).await, "pending");
    let rows=db.query("SELECT delivery_result FROM novu_rust_sendgrid_recipients WHERE dispatch_id=$1 ORDER BY recipient_index",&[&dispatch]).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>(),
        vec!["delivered", "pending"]
    );
    assert_eq!(
        callback(
            app.clone(),
            &key,
            json!([state(
                "two@example.test",
                "bounce",
                Some("bounce"),
                "two-hard",
                dispatch,
                true
            )])
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(result(&mut db, job).await, "partial");
    assert_eq!(
        callback(
            app.clone(),
            &key,
            json!([state(
                "two@example.test",
                "bounce",
                Some("bounce"),
                "two-hard",
                dispatch,
                true
            )])
        )
        .await,
        StatusCode::OK
    );
    let after_duplicate: String = db
        .query_one(
            "SELECT sendgrid_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        after_duplicate, "partial",
        "duplicate callback does not invalidate"
    );

    assert_eq!(
        callback(
            app.clone(),
            &key,
            json!([state(
                "two@example.test",
                "delivered",
                None,
                "two-delivered",
                dispatch,
                true
            )])
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(result(&mut db, job).await, "conflicting");
    assert_eq!(
        callback(
            app.clone(),
            &key,
            json!([state(
                "one@example.test",
                "bounce",
                None,
                "unknown-bounce",
                dispatch,
                true
            )])
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(result(&mut db, job).await, "unknown");
    db.execute(
        "DELETE FROM novu_rust_sendgrid_delivery_events WHERE event_id='unknown-bounce'",
        &[],
    )
    .await
    .unwrap();
    let duplicate_hash = format!("{:x}", Sha256::digest(b"one@example.test"));
    db.execute("INSERT INTO novu_rust_sendgrid_recipients(dispatch_id,environment_id,recipient_kind,recipient_index,recipient_hash) VALUES ($1,'env-a','cc',0,$2)", &[&dispatch,&duplicate_hash]).await.unwrap();
    assert_eq!(result(&mut db, job).await, "unknown");
    db.execute(
        "DELETE FROM novu_rust_sendgrid_recipients WHERE dispatch_id=$1 AND recipient_kind='cc'",
        &[&dispatch],
    )
    .await
    .unwrap();
    assert_eq!(
        callback(
            app.clone(),
            &key,
            json!([state(
                "other@example.test",
                "delivered",
                None,
                "unexpected-recipient",
                dispatch,
                true
            )])
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(result(&mut db, job).await, "unknown");
    let message: String = db
        .query_one(
            "SELECT sendgrid_delivery_result FROM novu_rust_email_messages WHERE email_job_id=$1",
            &[&job],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(message, "unknown");
    assert!(
        !novu_postgres_queue::sendgrid_projection::project(&mut db, "env-b", job)
            .await
            .unwrap()
    );
    // Remove only synthetic contradictory evidence and let the actual worker rescan.
    db.batch_execute("DELETE FROM novu_rust_sendgrid_delivery_events WHERE event_id IN ('unexpected-recipient','two-hard'); UPDATE novu_rust_sendgrid_attempts SET state='unknown'").await.unwrap();
    let mut worker = Worker(
        Command::new(std::env::var("TEST_SENDGRID_PROJECTION_BIN").unwrap())
            .env(
                "DATABASE_URL",
                format!("{url}?application_name=sendgrid_projection_test"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SENDGRID_ENVIRONMENT_ID", "env-a")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut projected = false;
    for _ in 0..100 {
        let r=db.query_one("SELECT j.sendgrid_delivery_result, EXISTS(SELECT 1 FROM novu_rust_sendgrid_projection_heartbeat WHERE environment_id='env-a') FROM novu_queue_jobs j WHERE j.id=$1",&[&job]).await.unwrap();
        if r.get::<_, String>(0) == "delivered" && r.get::<_, bool>(1) {
            projected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(projected && worker.0.try_wait().unwrap().is_none());
    let row=db.query_one("SELECT j.status,a.state,m.sendgrid_delivery_result FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(row.get::<_, String>(2), "delivered");
    let prior: String = db.query_one("SELECT last_completed_at::text FROM novu_rust_sendgrid_projection_heartbeat WHERE environment_id='env-a'", &[]).await.unwrap().get(0);
    assert_eq!(db.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='sendgrid_projection_test'", &[]).await.unwrap(),1);
    let mut reconnected = false;
    for _ in 0..150 {
        let r=db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_sendgrid_projection_heartbeat WHERE environment_id='env-a' AND last_completed_at>$1::text::timestamptz)", &[&prior]).await.unwrap();
        if r.get::<_, bool>(0) {
            reconnected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(reconnected && worker.0.try_wait().unwrap().is_none());
    drop(worker);
    // A strict heartbeat must not advance when a locked row is skipped.
    db.execute(
        "DELETE FROM novu_rust_sendgrid_projection_heartbeat WHERE environment_id='env-a'",
        &[],
    )
    .await
    .unwrap();
    let lock = db.transaction().await.unwrap();
    lock.query_one(
        "SELECT 1 FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    for (setting, value) in [
        ("NOVU_SENDGRID_PROJECTION_REQUIRE_FULL_SCAN", "invalid"),
        ("NOVU_SENDGRID_PROJECTION_SCAN_INTERVAL_SECONDS", "0"),
        ("NOVU_SENDGRID_PROJECTION_SCAN_INTERVAL_SECONDS", "301"),
    ] {
        let output = Command::new(std::env::var("TEST_SENDGRID_PROJECTION_BIN").unwrap())
            .env("DATABASE_URL", &url)
            .env("NOVU_SENDGRID_ENVIRONMENT_ID", "env-a")
            .env_remove("NOVU_SENDGRID_PROJECTION_REQUIRE_FULL_SCAN")
            .env_remove("NOVU_SENDGRID_PROJECTION_SCAN_INTERVAL_SECONDS")
            .env(setting, value)
            .output()
            .unwrap();
        assert!(!output.status.success());
    }
    let log_path = std::env::temp_dir().join(format!("swallow-projection-{}.log", Uuid::new_v4()));
    let log = std::fs::File::create(&log_path).unwrap();
    let mut strict = Worker(
        Command::new(std::env::var("TEST_SENDGRID_PROJECTION_BIN").unwrap())
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SENDGRID_ENVIRONMENT_ID", "env-a")
            .env("NOVU_SENDGRID_PROJECTION_REQUIRE_FULL_SCAN", "true")
            .env("NOVU_SENDGRID_PROJECTION_SCAN_INTERVAL_SECONDS", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    let mut observed_skip = false;
    for _ in 0..100 {
        if std::fs::read_to_string(&log_path)
            .unwrap()
            .contains("projection scan skipped jobs")
        {
            observed_skip = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(observed_skip && strict.0.try_wait().unwrap().is_none());
    assert!(!observer.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_sendgrid_projection_heartbeat WHERE environment_id='env-a')", &[]).await.unwrap().get::<_,bool>(0));
    lock.rollback().await.unwrap();
    let mut complete = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_sendgrid_projection_heartbeat WHERE environment_id='env-a')", &[]).await.unwrap().get::<_,bool>(0) { complete=true; break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(complete && strict.0.try_wait().unwrap().is_none());
    std::fs::remove_file(log_path).unwrap();
}
