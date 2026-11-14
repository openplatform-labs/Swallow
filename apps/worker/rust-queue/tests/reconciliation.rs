//! Uses a disposable PostgreSQL database and destroys the experimental tables.
use novu_postgres_queue::{self as queue, reconciliation};
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn decisions_are_fenced_audited_and_environment_scoped() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    db.batch_execute("DROP TABLE IF EXISTS novu_rust_sendgrid_dispatch_ids,novu_rust_twilio_dispatch_ids,novu_rust_delivery_reconciliations,novu_rust_sendgrid_attempts,novu_rust_twilio_attempts,novu_rust_fcm_attempts,novu_rust_email_messages,novu_rust_sms_messages,novu_rust_push_messages,novu_queue_jobs CASCADE").await.unwrap();
    for migration in [
        queue::MIGRATION,
        queue::sendgrid_queue::MIGRATION,
        queue::twilio_queue::MIGRATION,
        queue::twilio_queue::DISPATCH_ID_MIGRATION,
        queue::fcm_queue::MIGRATION,
        queue::in_app::EMAIL_MESSAGES_MIGRATION,
        queue::in_app::EMAIL_PROVIDER_RESULT_MIGRATION,
        queue::in_app::SMS_MESSAGES_MIGRATION,
        queue::in_app::PUSH_MESSAGES_MIGRATION,
        queue::in_app::DELIVERY_STATE_MIGRATION,
        reconciliation::MIGRATION,
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    let legacy_email = Uuid::new_v4();
    queue::enqueue(
        &db,
        legacy_email,
        "legacy",
        queue::sendgrid_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,'legacy','unknown')", &[&legacy_email]).await.unwrap();
    db.batch_execute(queue::sendgrid_queue::DISPATCH_ID_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(queue::sendgrid_queue::DISPATCH_ID_MIGRATION)
        .await
        .unwrap();
    let legacy_registry = Uuid::new_v4();
    db.execute("INSERT INTO novu_rust_sendgrid_dispatch_ids(id,job_id,environment_id) VALUES ($1,$2,'legacy')", &[&legacy_registry,&legacy_email]).await.unwrap();
    db.batch_execute(queue::sendgrid_queue::DISPATCH_PROOF_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(queue::sendgrid_queue::DISPATCH_PROOF_MIGRATION)
        .await
        .unwrap();
    let legacy_dispatch: Option<Uuid> = db
        .query_one(
            "SELECT dispatch_id FROM novu_rust_sendgrid_attempts WHERE job_id=$1",
            &[&legacy_email],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(legacy_dispatch, None);
    let old_hash: Option<String> = db
        .query_one(
            "SELECT proof_hash FROM novu_rust_sendgrid_dispatch_ids WHERE id=$1",
            &[&legacy_registry],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(old_hash, None);
    // The new migration must preserve old decisions and be safely re-runnable.
    let legacy_job = Uuid::new_v4();
    queue::enqueue(
        &db,
        legacy_job,
        "legacy",
        queue::twilio_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason) VALUES ($1,'legacy',$2,'retry','operator','legacy')", &[&Uuid::new_v4(),&legacy_job]).await.unwrap();
    db.batch_execute(reconciliation::RETRY_DISPATCH_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(reconciliation::RETRY_DISPATCH_MIGRATION)
        .await
        .unwrap();
    let row = db.query_one("SELECT retry_dispatch_id,decision_order FROM novu_rust_delivery_reconciliations WHERE job_id=$1", &[&legacy_job]).await.unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>(0), None);
    assert!(row.get::<_, i64>(1) > 0);
    db.batch_execute("DROP TABLE IF EXISTS novu_rust_sendgrid_delivery_events,novu_rust_sendgrid_webhook_keys,novu_rust_environments CASCADE; CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('tenant-a','org-a'),('tenant-b','org-b')").await.unwrap();
    db.batch_execute(include_str!(
        "../../../api/rust-api/migrations/0004_sendgrid_webhooks.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!(
        "../../../api/rust-api/migrations/0008_sendgrid_dispatch_identity.sql"
    ))
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_delivery_events(environment_id,event_id,message_internal_id,event_type,event_timestamp,recipient_hash,dispatch_id) VALUES ('legacy','old-proof','missing','processed',1,'hash',$1)", &[&legacy_registry]).await.unwrap();
    db.batch_execute(include_str!(
        "../../../api/rust-api/migrations/0009_sendgrid_dispatch_proof.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!(
        "../../../api/rust-api/migrations/0009_sendgrid_dispatch_proof.sql"
    ))
    .await
    .unwrap();
    let verified: bool = db.query_one("SELECT dispatch_proof_verified FROM novu_rust_sendgrid_delivery_events WHERE environment_id='legacy' AND event_id='old-proof'", &[]).await.unwrap().get(0);
    assert!(!verified);
    db.batch_execute(include_str!(
        "../../../api/rust-api/migrations/0005_twilio_webhooks.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!(
        "../../../api/rust-api/migrations/0006_twilio_webhook_job_link.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!(
        "../../../api/rust-api/migrations/0007_twilio_dispatch_identity.sql"
    ))
    .await
    .unwrap();
    let email = Uuid::new_v4();
    let sms = Uuid::new_v4();
    let push = Uuid::new_v4();
    queue::enqueue(
        &db,
        email,
        "tenant-a",
        queue::sendgrid_queue::JOB_KIND,
        &json!({"secret":"must-not-appear"}),
    )
    .await
    .unwrap();
    queue::enqueue(
        &db,
        sms,
        "tenant-a",
        queue::twilio_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    queue::enqueue(&db,push,"tenant-a",queue::fcm_queue::JOB_KIND,&json!({"prepared_messages":[{},{}],"prepared_context":{"target_metadata":[{"history_index":0,"group_size":2},{"history_index":0,"group_size":2}]}})).await.unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed',last_error='unknown' WHERE id=ANY($1)",
        &[&vec![email, sms, push]],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state,http_status) VALUES ($1,'tenant-a','unknown',500)", &[&email]).await.unwrap();
    db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state) VALUES ($1,'tenant-a','unknown')", &[&sms]).await.unwrap();
    db.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,'tenant-a',0,'unknown'),($1,'tenant-a',1,'accepted')", &[&push]).await.unwrap();
    let stranded = Uuid::new_v4();
    queue::enqueue(
        &db,
        stranded,
        "tenant-a",
        queue::sendgrid_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,'tenant-a','dispatching')", &[&stranded]).await.unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '1 hour' WHERE id=$1", &[&stranded,&Uuid::new_v4()]).await.unwrap();
    let health = reconciliation::health(&db, "tenant-a").await.unwrap();
    assert_eq!(health["unknownAttempts"], 3);
    assert_eq!(health["strandedDispatches"], 0);
    db.execute(
        "UPDATE novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
        &[&stranded],
    )
    .await
    .unwrap();
    let health = reconciliation::health(&db, "tenant-a").await.unwrap();
    assert_eq!(health["strandedDispatches"], 1);
    assert!(health["oldestUnknownUpdatedAt"].is_string());
    assert!(health["oldestStrandedUpdatedAt"].is_string());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["check", "tenant-a"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        health
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["check", "tenant-b"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(output.status.success());
    let clean: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(clean["unknownAttempts"], 0);
    assert_eq!(clean["strandedDispatches"], 0);
    assert!(clean["oldestUnknownUpdatedAt"].is_null());
    assert!(reconciliation::health(&db, " ").await.is_err());
    db.execute("DELETE FROM novu_queue_jobs WHERE id=$1", &[&stranded])
        .await
        .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET last_error='push delivery outcome unknown' WHERE id=$1",
        &[&push],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ('email','tenant-a','s','s','n','w','t','subject','error',$1,'unknown')", &[&email]).await.unwrap();
    db.batch_execute("INSERT INTO novu_rust_sendgrid_delivery_events(environment_id,event_id,message_internal_id,event_type,event_timestamp,recipient_hash) VALUES ('tenant-a','e1','email','delivered',100,'hash-a'),('tenant-a','e2','email','delivered',101,'hash-b'),('tenant-a','e3','email','deferred',102,'hash-a'),('tenant-b','cross','email','delivered',103,'hash-c')").await.unwrap();
    db.execute("UPDATE novu_rust_sendgrid_delivery_events SET dispatch_id=$1 WHERE environment_id='tenant-a' AND event_id='e1'", &[&Uuid::new_v4()]).await.unwrap();
    let evidence = reconciliation::email_evidence(&db, "tenant-a", email)
        .await
        .unwrap();
    assert_eq!(evidence,json!([{"event":"deferred","eventCount":1,"recipientCount":1,"latestEventTimestamp":102},{"event":"delivered","eventCount":2,"recipientCount":2,"latestEventTimestamp":101}]).as_array().unwrap().clone());
    let dispatch_evidence = reconciliation::email_dispatch_evidence(&db, "tenant-a", email)
        .await
        .unwrap();
    assert_eq!(dispatch_evidence.len(), 2);
    assert!(
        dispatch_evidence
            .iter()
            .all(|item| item["dispatchId"].is_null())
    );
    assert!(
        reconciliation::email_dispatch_evidence(&db, "tenant-b", email)
            .await
            .is_err()
    );
    assert!(
        reconciliation::email_evidence(&db, "tenant-b", email)
            .await
            .is_err()
    );
    let sms_dispatch = Uuid::new_v4();
    db.execute("INSERT INTO novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES ($1,$2,'tenant-a')", &[&sms_dispatch,&sms]).await.unwrap();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('tenant-a','ACfixture','SMfixture','delivered',$1,$2)", &[&sms,&sms_dispatch]).await.unwrap();
    let sms_events = reconciliation::sms_evidence(&db, "tenant-a", sms)
        .await
        .unwrap();
    assert_eq!(sms_events.len(), 1);
    assert_eq!(sms_events[0]["providerMessageId"], "SMfixture");
    assert_eq!(sms_events[0]["status"], "delivered");
    assert_eq!(sms_events[0]["dispatchId"], sms_dispatch.to_string());
    assert!(
        reconciliation::sms_evidence(&db, "tenant-b", sms)
            .await
            .is_err()
    );
    assert!(
        reconciliation::sms_evidence(&db, "tenant-a", email)
            .await
            .is_err()
    );
    db.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status,sms_job_id,delivery_state) VALUES ('sms','tenant-a','s','s','n','w','t','error',$1,'unknown')", &[&sms]).await.unwrap();
    db.execute("INSERT INTO novu_rust_push_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,device_tokens,title,status,push_job_id,target_index,delivery_state) VALUES ('push','tenant-a','s','s','n','w','t',ARRAY['a','b'],'title','error',$1,0,'unknown')", &[&push]).await.unwrap();
    let listed = reconciliation::list(&db, "tenant-a").await.unwrap();
    assert_eq!(listed.len(), 3);
    assert!(
        !serde_json::to_string(&listed)
            .unwrap()
            .contains("must-not-appear")
    );
    assert!(
        reconciliation::list(&db, "tenant-b")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        reconciliation::resolve(
            &mut db, "tenant-b", email, None, "retry", "operator", "ticket 1"
        )
        .await
        .is_err()
    );
    assert!(
        reconciliation::resolve(&mut db, "tenant-a", email, None, "retry", "operator", "")
            .await
            .is_err()
    );
    // A previously authenticated event rejects retry without changing state.
    let denied = reconciliation::resolve(
        &mut db, "tenant-a", email, None, "retry", "operator", "ticket",
    )
    .await
    .unwrap_err();
    assert!(
        denied
            .to_string()
            .contains("authenticated provider callback")
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args([
            "retry",
            "tenant-a",
            &email.to_string(),
            "-",
            "operator",
            "ticket",
        ])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("authenticated provider callback"));
    let row = db.query_one("SELECT j.status,a.state,(SELECT count(*) FROM novu_rust_delivery_reconciliations WHERE job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&email]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(row.get::<_, i64>(2), 0);
    reconciliation::resolve(
        &mut db, "tenant-a", email, None, "accepted", "operator", "verified",
    )
    .await
    .unwrap();
    let email_retry = Uuid::new_v4();
    queue::enqueue(
        &db,
        email_retry,
        "tenant-a",
        queue::sendgrid_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&email_retry],
    )
    .await
    .unwrap();
    let retry_dispatch = Uuid::new_v4();
    let prior_dispatch = Uuid::new_v4();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,'tenant-a','unknown',$2)", &[&email_retry,&retry_dispatch]).await.unwrap();
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ('email-retry','tenant-a','s','s','n','w','t','subject','error',$1,'unknown')", &[&email_retry]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_delivery_events(environment_id,event_id,message_internal_id,event_type,event_timestamp,recipient_hash,dispatch_id,dispatch_proof_verified) VALUES ('tenant-a','earlier-email','email-retry','delivered',104,'hash',$1,true)", &[&prior_dispatch]).await.unwrap();
    let grouped = reconciliation::email_dispatch_evidence(&db, "tenant-a", email_retry)
        .await
        .unwrap();
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0]["dispatchId"], prior_dispatch.to_string());
    // A message ID collision from another environment does not block retry.
    db.batch_execute("INSERT INTO novu_rust_sendgrid_delivery_events(environment_id,event_id,message_internal_id,event_type,event_timestamp,recipient_hash) VALUES ('tenant-b','cross-retry','email-retry','delivered',104,'hash')").await.unwrap();
    let (mut other, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let (a, b) = tokio::join!(
        reconciliation::resolve(
            &mut db,
            "tenant-a",
            email_retry,
            None,
            "retry",
            "operator",
            "ticket 1"
        ),
        reconciliation::resolve(
            &mut other,
            "tenant-a",
            email_retry,
            None,
            "retry",
            "operator",
            "ticket 1"
        )
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let row=db.query_one("SELECT status,(SELECT count(*) FROM novu_rust_sendgrid_attempts WHERE job_id=$1),(SELECT delivery_state FROM novu_rust_email_messages WHERE email_job_id=$1) FROM novu_queue_jobs WHERE id=$1", &[&email_retry]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, i64>(1), 0);
    assert_eq!(row.get::<_, String>(2), "not_attempted");
    let retry_recorded: Option<Uuid> = db.query_one("SELECT retry_dispatch_id FROM novu_rust_delivery_reconciliations WHERE job_id=$1 AND decision='retry'", &[&email_retry]).await.unwrap().get(0);
    assert_eq!(retry_recorded, Some(retry_dispatch));
    reconciliation::resolve(
        &mut db,
        "tenant-a",
        sms,
        None,
        "accepted",
        "operator",
        "provider receipt checked",
    )
    .await
    .unwrap();
    reconciliation::resolve(
        &mut db,
        "tenant-a",
        push,
        Some(0),
        "accepted",
        "operator",
        "provider receipt checked",
    )
    .await
    .unwrap();
    for (id, table, column) in [
        (sms, "novu_rust_sms_messages", "sms_job_id"),
        (push, "novu_rust_push_messages", "push_job_id"),
    ] {
        let row=db.query_one(&format!("SELECT j.status,m.delivery_state FROM novu_queue_jobs j JOIN {table} m ON m.{column}=j.id WHERE j.id=$1"), &[&id]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
    }
    assert!(
        reconciliation::list(&db, "tenant-a")
            .await
            .unwrap()
            .is_empty()
    );
    let audits:i64=db.query_one("SELECT count(*) FROM novu_rust_delivery_reconciliations WHERE environment_id='tenant-a' AND operator_name='operator' AND database_user=current_user", &[]).await.unwrap().get(0);
    assert_eq!(audits, 4);
    let pending = Uuid::new_v4();
    queue::enqueue(&db,pending,"tenant-a",queue::fcm_queue::JOB_KIND,&json!({"prepared_messages":[{},{}],"prepared_context":{"target_metadata":[{"history_index":0,"group_size":2},{"history_index":0,"group_size":2}]}})).await.unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed',last_error='push delivery outcome unknown' WHERE id=$1",
        &[&pending],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,'tenant-a',0,'unknown'),($1,'tenant-a',1,'unknown')", &[&pending]).await.unwrap();
    db.execute("INSERT INTO novu_rust_push_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,device_tokens,title,status,push_job_id,target_index,delivery_state) VALUES ('push-retry','tenant-a','s','s','n','w','t',ARRAY['a','b'],'title','error',$1,0,'unknown')", &[&pending]).await.unwrap();
    assert!(
        reconciliation::resolve(
            &mut db,
            "tenant-a",
            pending,
            Some(9),
            "retry",
            "operator",
            "ticket 2"
        )
        .await
        .is_err()
    );
    reconciliation::resolve(
        &mut db,
        "tenant-a",
        pending,
        Some(0),
        "retry",
        "operator",
        "ticket 2",
    )
    .await
    .unwrap();
    let row=db.query_one("SELECT j.status,m.delivery_state,(SELECT count(*) FROM novu_rust_fcm_attempts a WHERE a.job_id=j.id AND a.state='unknown') FROM novu_queue_jobs j JOIN novu_rust_push_messages m ON m.push_job_id=j.id WHERE j.id=$1", &[&pending]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(row.get::<_, i64>(2), 1);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["list", "tenant-a"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let listed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 1);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["evidence", "tenant-a", &email.to_string()])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let from_cli: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(from_cli, evidence);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["email-dispatch-evidence", "tenant-a", &email.to_string()])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let from_cli: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(from_cli, dispatch_evidence);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["sms-evidence", "tenant-a", &sms.to_string()])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let from_cli: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(from_cli, sms_events);
    let sms_retry = Uuid::new_v4();
    queue::enqueue(
        &db,
        sms_retry,
        "tenant-a",
        queue::twilio_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&sms_retry],
    )
    .await
    .unwrap();
    let old_dispatch = Uuid::new_v4();
    db.execute("INSERT INTO novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES ($1,$2,'tenant-a')", &[&old_dispatch,&sms_retry]).await.unwrap();
    db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,'tenant-a','unknown',$2)", &[&sms_retry,&old_dispatch]).await.unwrap();
    // A callback for a different dispatch (or environment) cannot block a retry.
    let unrelated_dispatch = Uuid::new_v4();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('tenant-a','ACfixture','SMother','delivered',$1,$2),('tenant-b','ACfixture','SMcross','delivered',$1,$3)", &[&sms_retry,&unrelated_dispatch,&old_dispatch]).await.unwrap();
    reconciliation::resolve(
        &mut db,
        "tenant-a",
        sms_retry,
        None,
        "retry",
        "operator",
        "confirmed not accepted",
    )
    .await
    .unwrap();
    let row = db.query_one("SELECT j.status,(SELECT count(*) FROM novu_rust_twilio_attempts a WHERE a.job_id=j.id),(SELECT count(*) FROM novu_rust_delivery_reconciliations r WHERE r.job_id=j.id AND r.environment_id=j.environment_id AND r.decision='retry') FROM novu_queue_jobs j WHERE j.id=$1", &[&sms_retry]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, i64>(1), 0);
    assert_eq!(row.get::<_, i64>(2), 1);
    let recorded: Uuid = db.query_one("SELECT retry_dispatch_id FROM novu_rust_delivery_reconciliations WHERE job_id=$1 AND decision='retry'", &[&sms_retry]).await.unwrap().get(0);
    assert_eq!(recorded, old_dispatch);
    let retained: bool = db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_twilio_dispatch_ids WHERE id=$1 AND job_id=$2 AND environment_id='tenant-a')", &[&old_dispatch,&sms_retry]).await.unwrap().get(0);
    assert!(retained);
    for (legacy_event, legacy_attempt) in [(false, false), (true, false), (false, true)] {
        let blocked = Uuid::new_v4();
        let dispatch = Uuid::new_v4();
        queue::enqueue(
            &db,
            blocked,
            "tenant-a",
            queue::twilio_queue::JOB_KIND,
            &json!({}),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
            &[&blocked],
        )
        .await
        .unwrap();
        let attempt_dispatch = if legacy_attempt { None } else { Some(dispatch) };
        db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,'tenant-a','unknown',$2)", &[&blocked,&attempt_dispatch]).await.unwrap();
        let event_dispatch = if legacy_event { None } else { Some(dispatch) };
        db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('tenant-a','ACfixture',$1,'delivered',$2,$3)", &[&format!("SM{blocked}"),&blocked,&event_dispatch]).await.unwrap();
        let err = reconciliation::resolve(
            &mut db, "tenant-a", blocked, None, "retry", "operator", "ticket",
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("authenticated provider callback"));
        let row = db.query_one("SELECT j.status,a.state,(SELECT count(*) FROM novu_rust_delivery_reconciliations WHERE job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_twilio_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&blocked]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert_eq!(row.get::<_, i64>(2), 0);
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args([
                "retry",
                "tenant-a",
                &blocked.to_string(),
                "-",
                "operator",
                "ticket",
            ])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("authenticated provider callback")
        );
        // Continue the existing test without leaving extra unknown attempts.
        db.execute(
            "UPDATE novu_rust_twilio_attempts SET state='accepted' WHERE job_id=$1",
            &[&blocked],
        )
        .await
        .unwrap();
    }
    let bulk = Uuid::new_v4();
    queue::enqueue(
        &db,
        bulk,
        "tenant-a",
        queue::fcm_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&bulk],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) SELECT $1,'tenant-a',n,'unknown' FROM generate_series(0,104) n", &[&bulk]).await.unwrap();
    assert_eq!(
        reconciliation::health(&db, "tenant-a").await.unwrap()["unknownAttempts"],
        106
    );
    let first = reconciliation::list(&db, "tenant-a").await.unwrap();
    assert_eq!(first.len(), 100);
    let tail = first.last().unwrap();
    let cursor = (
        Uuid::parse_str(tail["jobId"].as_str().unwrap()).unwrap(),
        tail["targetIndex"].as_i64().map(|v| v as i32),
    );
    let second = reconciliation::list_after(&db, "tenant-a", Some(cursor))
        .await
        .unwrap();
    assert_eq!(second.len(), 6);
    let identities: std::collections::HashSet<_> = first
        .iter()
        .chain(second.iter())
        .map(|item| {
            (
                item["jobId"].as_str().unwrap().to_owned(),
                item["targetIndex"].as_i64(),
            )
        })
        .collect();
    assert_eq!(identities.len(), 106);

    // An accepted unknown can leave unsent targets, but cannot erase a
    // different failure or resume while another outcome is still unknown.
    for (suffix, error, other_state, expected_status, expected_delivery) in [
        (
            "resume",
            "push delivery outcome unknown",
            "rejected",
            "pending",
            "in_progress",
        ),
        (
            "identity",
            "subscriber identity changed",
            "rejected",
            "failed",
            "in_progress",
        ),
        (
            "unresolved",
            "push delivery outcome unknown",
            "unknown",
            "failed",
            "unknown",
        ),
    ] {
        let job = Uuid::new_v4();
        let payload = json!({"prepared_messages":[{},{},{}],"prepared_context":{"target_metadata":[{"history_index":0,"group_size":3},{"history_index":0,"group_size":3},{"history_index":0,"group_size":3}]}});
        queue::enqueue(&db, job, "tenant-a", queue::fcm_queue::JOB_KIND, &payload)
            .await
            .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status='failed',last_error=$2 WHERE id=$1",
            &[&job, &error],
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,'tenant-a',0,'unknown'),($1,'tenant-a',1,$2)", &[&job,&other_state]).await.unwrap();
        db.execute("INSERT INTO novu_rust_push_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,device_tokens,title,status,push_job_id,target_index,delivery_state) VALUES ($1,'tenant-a','s','s','n','w','t',ARRAY['a','b','c'],'title','error',$2,0,'unknown')", &[&suffix,&job]).await.unwrap();
        if suffix == "identity" {
            assert!(
                reconciliation::resolve(
                    &mut db,
                    "tenant-a",
                    job,
                    Some(0),
                    "retry",
                    "operator",
                    "checked"
                )
                .await
                .is_err()
            );
        }
        reconciliation::resolve(
            &mut db,
            "tenant-a",
            job,
            Some(0),
            "accepted",
            "operator",
            "checked receipt",
        )
        .await
        .unwrap();
        let row=db.query_one("SELECT j.status,j.last_error,m.delivery_state,(SELECT count(*) FROM novu_rust_fcm_attempts a WHERE a.job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_push_messages m ON m.push_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), expected_status);
        assert_eq!(row.get::<_, String>(2), expected_delivery);
        assert_eq!(row.get::<_, i64>(3), 2);
        if expected_status == "failed" {
            assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some(error));
        }
    }
}
