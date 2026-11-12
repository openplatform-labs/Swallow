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
    db.batch_execute("DROP TABLE IF EXISTS novu_rust_delivery_reconciliations,novu_rust_sendgrid_attempts,novu_rust_twilio_attempts,novu_rust_fcm_attempts,novu_rust_email_messages,novu_rust_sms_messages,novu_rust_push_messages,novu_queue_jobs CASCADE").await.unwrap();
    for migration in [
        queue::MIGRATION,
        queue::sendgrid_queue::MIGRATION,
        queue::twilio_queue::MIGRATION,
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
    db.batch_execute("DROP TABLE IF EXISTS novu_rust_sendgrid_delivery_events,novu_rust_sendgrid_webhook_keys,novu_rust_environments CASCADE; CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('tenant-a','org-a'),('tenant-b','org-b')").await.unwrap();
    db.batch_execute(include_str!(
        "../../../api/rust-api/migrations/0004_sendgrid_webhooks.sql"
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
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ('email','tenant-a','s','s','n','w','t','subject','error',$1,'unknown')", &[&email]).await.unwrap();
    db.batch_execute("INSERT INTO novu_rust_sendgrid_delivery_events(environment_id,event_id,message_internal_id,event_type,event_timestamp,recipient_hash) VALUES ('tenant-a','e1','email','delivered',100,'hash-a'),('tenant-a','e2','email','delivered',101,'hash-b'),('tenant-a','e3','email','deferred',102,'hash-a'),('tenant-b','cross','email','delivered',103,'hash-c')").await.unwrap();
    let evidence = reconciliation::email_evidence(&db, "tenant-a", email)
        .await
        .unwrap();
    assert_eq!(evidence,json!([{"event":"deferred","eventCount":1,"recipientCount":1,"latestEventTimestamp":102},{"event":"delivered","eventCount":2,"recipientCount":2,"latestEventTimestamp":101}]).as_array().unwrap().clone());
    assert!(
        reconciliation::email_evidence(&db, "tenant-b", email)
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
    let (mut other, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let (a, b) = tokio::join!(
        reconciliation::resolve(
            &mut db, "tenant-a", email, None, "retry", "operator", "ticket 1"
        ),
        reconciliation::resolve(
            &mut other, "tenant-a", email, None, "retry", "operator", "ticket 1"
        )
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let row=db.query_one("SELECT status,(SELECT count(*) FROM novu_rust_sendgrid_attempts WHERE job_id=$1),(SELECT delivery_state FROM novu_rust_email_messages WHERE email_job_id=$1) FROM novu_queue_jobs WHERE id=$1", &[&email]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, i64>(1), 0);
    assert_eq!(row.get::<_, String>(2), "not_attempted");
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
    assert_eq!(audits, 3);
    let pending = Uuid::new_v4();
    queue::enqueue(&db,pending,"tenant-a",queue::fcm_queue::JOB_KIND,&json!({"prepared_messages":[{},{}],"prepared_context":{"target_metadata":[{"history_index":0,"group_size":2},{"history_index":0,"group_size":2}]}})).await.unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
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
}
