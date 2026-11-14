//! Synthetic signed Resend callbacks and projector lock ordering.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;

async fn event(app: Router, secret: &[u8], id: &str, kind: &str, email: &str) -> StatusCode {
    let tag = match id {
        "evt-callback-first" => "96df49d1-0f4d-4bce-9290-666dd8fc908c",
        "evt-prior-tag" => "5ba1d539-0dd9-4ca6-a370-4622a972bb43",
        _ => "e93bc23b-d9c4-4df9-810f-e642a9659a60",
    };
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body = serde_json::to_vec(
        &json!({"type":kind,"data":{"email_id":email,"to":["one@example.test"],"tags":{"swallow_dispatch":tag}}}),
    )
    .unwrap();
    let mut signed = format!("{id}.{timestamp}.").into_bytes();
    signed.extend_from_slice(&body);
    let signature = format!(
        "v1,{}",
        STANDARD.encode(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, secret), &signed).as_ref())
    );
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/webhooks/organizations/org-a/environments/env-a/email/resend")
            .header("svix-id", id)
            .header("svix-timestamp", timestamp)
            .header("svix-signature", signature)
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn events_invalidate_only_matching_results_and_serialize_with_projector() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    for sql in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::resend_queue::MIGRATION,
        novu_postgres_queue::resend_queue::RECIPIENT_MIGRATION,
        novu_postgres_queue::in_app::EMAIL_MESSAGES_MIGRATION,
        novu_postgres_queue::in_app::EMAIL_PROVIDER_RESULT_MIGRATION,
        "ALTER TABLE novu_rust_email_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'",
        novu_postgres_queue::resend_projection::MIGRATION,
        include_str!("../migrations/0010_resend_webhooks.sql"),
        include_str!("../migrations/0012_resend_event_fingerprint.sql"),
        include_str!("../migrations/0013_resend_event_recipient.sql"),
        include_str!("../migrations/0047_resend_event_correlation.sql"),
        include_str!(
            "../../../worker/rust-queue/migrations/0102_resend_projection_invalidation.sql"
        ),
        novu_postgres_queue::reconciliation::MIGRATION,
        novu_postgres_queue::reconciliation::RETRY_DISPATCH_MIGRATION,
        novu_postgres_queue::reconciliation::OPERATOR_GRANTS_MIGRATION,
        novu_postgres_queue::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
        "ALTER TABLE novu_rust_delivery_reconciliations ADD COLUMN retry_provider_message_id text",
        include_str!("../../../worker/rust-queue/migrations/0111_resend_webhook_job_lock.sql"),
        include_str!("../../../worker/rust-queue/migrations/0134_resend_correlation.sql"),
        include_str!("../../../worker/rust-queue/migrations/0135_resend_correlation_retry.sql"),
        include_str!(
            "../../../worker/rust-queue/migrations/0136_resend_correlation_webhook_lock.sql"
        ),
        include_str!("../../../worker/rust-queue/migrations/0137_resend_auto_correlation.sql"),
        include_str!("../../../worker/rust-queue/migrations/0138_resend_auto_projection_guard.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(
        "INSERT INTO novu_rust_environments VALUES ('env-a','org-a'),('env-b','org-b')",
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_resend_webhook_lock_grants SELECT 'env-a',session_user,oid FROM pg_roles WHERE rolname=session_user", &[]).await.unwrap();
    let job = Uuid::new_v4();
    let other = Uuid::new_v4();
    for (j, env) in [(job, "env-a"), (other, "env-b")] {
        novu_postgres_queue::enqueue(
            &db,
            j,
            env,
            novu_postgres_queue::resend_queue::JOB_KIND,
            &json!({}),
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,$2,'accepted','email-123')", &[&j,&env]).await.unwrap();
        db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,resend_delivery_result) VALUES ($1,$2,'s','s','n','w','t','subject','sent',$3,'delivered')", &[&j.to_string(),&env,&j]).await.unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
            &[&j],
        )
        .await
        .unwrap();
    }
    let (webdb, driver) = tokio_postgres::connect(
        &format!("{url}?application_name=resend_invalidation_webhook"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let secret = b"test-secret-is-32-bytes-long!!";
    let config = if std::env::var_os("TEST_RESEND_LOCK_CONFIG").is_some() {
        novu_rust_api::resend_webhook::Config::from_env().unwrap()
    } else {
        novu_rust_api::resend_webhook::Config::new(
            "env-a".into(),
            &format!("whsec_{}", STANDARD.encode(secret)),
        )
        .unwrap()
        .with_retry_job_lock(true)
    }
    .with_correlation_capture(true)
    .with_correlation_job_lock(true);
    let app = novu_rust_api::router_with_webhook_configs(
        Arc::new(webdb),
        None,
        HashMap::new(),
        None,
        Some(config),
    );
    assert_eq!(
        event(app.clone(), secret, "evt-1", "email.bounced", "email-123").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(db.query_one("SELECT correlation_id::text FROM novu_rust_resend_delivery_events WHERE event_id='evt-1'", &[]).await.unwrap().get::<_,Option<String>>(0).as_deref(),Some("e93bc23b-d9c4-4df9-810f-e642a9659a60"));
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    let other_result: String = db
        .query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(other_result, "delivered");
    db.execute(
        "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert_eq!(
        event(app.clone(), secret, "evt-1", "email.bounced", "email-123").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    assert_eq!(
        event(
            app.clone(),
            secret,
            "evt-unrelated",
            "email.failed",
            "different"
        )
        .await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    // The webhook must lock even when the committed value is already unknown.
    db.execute(
        "UPDATE novu_queue_jobs SET resend_delivery_result='unknown' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    let task = tokio::spawn(event(
        app.clone(),
        secret,
        "evt-race",
        "email.failed",
        "email-123",
    ));
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let mut waiting = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='resend_invalidation_webhook' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(waiting, "callback did not serialize with job lock");
    tx.execute(
        "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(task.await.unwrap(), StatusCode::NO_CONTENT);
    assert_eq!(
        observer
            .query_one(
                "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
                &[&job]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        "unknown"
    );
    // A prior retry is still locked after its attempt is deleted.
    let retried = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        retried,
        "env-a",
        novu_postgres_queue::resend_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_provider_message_id) VALUES ($1,'env-a',$2,'retry','operator','ticket','old-retry-id')", &[&Uuid::new_v4(),&retried]).await.unwrap();
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&retried],
    )
    .await
    .unwrap();
    let waiting = tokio::spawn(event(
        app.clone(),
        secret,
        "evt-prior-retry",
        "email.delivered",
        "old-retry-id",
    ));
    let mut blocked = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='resend_invalidation_webhook' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0) {blocked=true;break;}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(blocked && !waiting.is_finished());
    assert!(!observer.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_resend_delivery_events WHERE event_id='evt-prior-retry')", &[]).await.unwrap().get::<_,bool>(0));
    tx.commit().await.unwrap();
    assert_eq!(waiting.await.unwrap(), StatusCode::NO_CONTENT);
    assert!(observer.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_resend_delivery_events WHERE environment_id='env-a' AND email_id='old-retry-id')", &[]).await.unwrap().get::<_,bool>(0));
    // The previous provider ID can be absent; the audit tag still locks the job.
    let retried_tag = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        retried_tag,
        "env-a",
        novu_postgres_queue::resend_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,correlation_id) VALUES ($1,'env-a','unknown','5ba1d539-0dd9-4ca6-a370-4622a972bb43')", &[&retried_tag]).await.unwrap();
    db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason) VALUES ($1,'env-a',$2,'retry','operator','ticket')", &[&Uuid::new_v4(),&retried_tag]).await.unwrap();
    db.execute(
        "DELETE FROM novu_rust_resend_attempts WHERE job_id=$1",
        &[&retried_tag],
    )
    .await
    .unwrap();
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&retried_tag],
    )
    .await
    .unwrap();
    let pending = tokio::spawn(event(
        app.clone(),
        secret,
        "evt-prior-tag",
        "email.delivered",
        "no-known-id",
    ));
    let mut blocked = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='resend_invalidation_webhook' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0) { blocked=true;break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(blocked && !pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::NO_CONTENT);
    // Restrict lock access to an explicitly granted API login; no direct queue read is needed.
    let role = format!("webhook_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_resend_webhook_jobs(text,text),public.novu_rust_lock_resend_webhook_jobs_with_correlation(text,text,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_webhook_lock_grants SELECT 'env-a',$1,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let (restricted, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    restricted
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert_eq!(restricted.query_one("SELECT public.novu_rust_lock_resend_webhook_jobs_with_correlation('env-a','unrelated','5ba1d539-0dd9-4ca6-a370-4622a972bb43')", &[]).await.unwrap().get::<_,i32>(0),1);
    assert!(restricted.query("SELECT public.novu_rust_lock_resend_webhook_jobs_with_correlation('env-b','unrelated','5ba1d539-0dd9-4ca6-a370-4622a972bb43')", &[]).await.is_err());
    db.batch_execute(&format!("REVOKE EXECUTE ON FUNCTION public.novu_rust_lock_resend_webhook_jobs_with_correlation(text,text,uuid) FROM {role}")).await.unwrap();
    assert!(restricted.query("SELECT public.novu_rust_lock_resend_webhook_jobs_with_correlation('env-a','unrelated','5ba1d539-0dd9-4ca6-a370-4622a972bb43')", &[]).await.is_err());
    assert_eq!(
        restricted
            .query_one(
                "SELECT public.novu_rust_lock_resend_webhook_jobs('env-a','old-retry-id')",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    assert!(
        restricted
            .query(
                "SELECT public.novu_rust_lock_resend_webhook_jobs('env-b','old-retry-id')",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        restricted
            .query(
                "SELECT * FROM public.novu_rust_resend_webhook_lock_grants",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        restricted
            .query("SELECT payload FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    // The HTTP handler can use the lock function without direct queue or audit reads.
    db.batch_execute(&format!("GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.novu_rust_environments TO {role}; GRANT SELECT,INSERT,UPDATE ON public.novu_rust_resend_delivery_events TO {role}")).await.unwrap();
    let (limited_web, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    limited_web
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let limited_config = novu_rust_api::resend_webhook::Config::new(
        "env-a".into(),
        &format!("whsec_{}", STANDARD.encode(secret)),
    )
    .unwrap()
    .with_retry_job_lock(true);
    let limited_app = novu_rust_api::router_with_webhook_configs(
        Arc::new(limited_web),
        None,
        HashMap::new(),
        None,
        Some(limited_config),
    );
    assert_eq!(
        event(
            limited_app,
            secret,
            "evt-limited-api",
            "email.delivered",
            "old-retry-id"
        )
        .await,
        StatusCode::NO_CONTENT
    );
    db.execute("DELETE FROM novu_rust_resend_webhook_lock_grants WHERE environment_id='env-a' AND database_principal=$1", &[&role]).await.unwrap();
    assert!(
        restricted
            .query(
                "SELECT public.novu_rust_lock_resend_webhook_jobs('env-a','old-retry-id')",
                &[]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO novu_rust_resend_webhook_lock_grants SELECT 'env-a',$1,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    db.batch_execute(&format!("REVOKE EXECUTE ON FUNCTION public.novu_rust_lock_resend_webhook_jobs(text,text) FROM {role}")).await.unwrap();
    assert!(
        restricted
            .query(
                "SELECT public.novu_rust_lock_resend_webhook_jobs('env-a','old-retry-id')",
                &[]
            )
            .await
            .is_err()
    );

    // A callback that acquired the job lock first must commit before a retry
    // decision can inspect its evidence. Pause the insert after the lock CTE.
    let race_job = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        race_job,
        "env-a",
        novu_postgres_queue::resend_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed',last_error='Resend outcome unknown' WHERE id=$1",
        &[&race_job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,correlation_id) VALUES ($1,'env-a','unknown','96df49d1-0f4d-4bce-9290-666dd8fc908c')", &[&race_job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_recipients(job_id,environment_id,recipient_kind,recipient_index,recipient_hash) VALUES ($1,'env-a','to',0,$2)", &[&race_job,&novu_postgres_queue::resend_queue::recipient_hash("one@example.test")]).await.unwrap();
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ('race-message','env-a','s','s','n','w','t','subject','error',$1,'unknown')", &[&race_job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision) VALUES ('env-a',session_user,'retry')", &[]).await.unwrap();
    db.batch_execute("CREATE FUNCTION public.test_pause_resend_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_id='evt-callback-first' THEN PERFORM pg_advisory_xact_lock(762019); END IF; RETURN NEW; END $$; CREATE TRIGGER test_pause_resend_event BEFORE INSERT ON public.novu_rust_resend_delivery_events FOR EACH ROW EXECUTE FUNCTION public.test_pause_resend_event();").await.unwrap();
    let (blocker, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    blocker
        .query_one("SELECT pg_advisory_lock(762019)", &[])
        .await
        .unwrap();
    let callback = tokio::spawn(event(
        app.clone(),
        secret,
        "evt-callback-first",
        "email.delivered",
        "race-provider-id",
    ));
    let mut callback_blocked = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='resend_invalidation_webhook' AND wait_event='advisory')", &[]).await.unwrap().get::<_,bool>(0) { callback_blocked = true; break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(callback_blocked, "callback did not reach the paused insert");
    let (mut retry_db, driver) =
        tokio_postgres::connect(&format!("{url}?application_name=resend_retry_race"), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let retry = tokio::spawn(async move {
        novu_postgres_queue::reconciliation::resolve(
            &mut retry_db,
            "env-a",
            race_job,
            None,
            "retry",
            "operator",
            "test",
        )
        .await
    });
    let mut retry_blocked = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='resend_retry_race' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0) { retry_blocked = true; break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        retry_blocked && !retry.is_finished(),
        "retry did not wait for callback's job lock"
    );
    blocker
        .query_one("SELECT pg_advisory_unlock(762019)", &[])
        .await
        .unwrap();
    assert_eq!(callback.await.unwrap(), StatusCode::NO_CONTENT);
    assert!(retry.await.unwrap().is_err());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM novu_rust_delivery_reconciliations WHERE job_id=$1",
            &[&race_job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert!(
        novu_postgres_queue::resend_projection::project_with_auto_correlation(
            &mut db, "env-a", race_job, true
        )
        .await
        .unwrap()
    );
    let row=db.query_one("SELECT j.status,j.resend_delivery_result,a.state,a.provider_message_id,m.delivery_state,m.resend_delivery_result,(SELECT count(*) FROM novu_rust_resend_auto_correlations c WHERE c.job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&race_job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, String>(1), "delivered");
    assert_eq!(row.get::<_, String>(2), "accepted");
    assert_eq!(row.get::<_, String>(3), "race-provider-id");
    assert_eq!(row.get::<_, String>(4), "accepted");
    assert_eq!(row.get::<_, String>(5), "delivered");
    assert_eq!(row.get::<_, i64>(6), 1);
    assert_eq!(
        event(
            app.clone(),
            secret,
            "evt-post-auto",
            "email.delivered",
            "race-provider-id"
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&race_job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert!(
        novu_postgres_queue::resend_projection::project(&mut db, "env-a", race_job)
            .await
            .unwrap()
    );
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&race_job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
}
