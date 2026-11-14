//! Disposable PostgreSQL fixtures for conservative automatic Resend recovery.
use novu_postgres_queue as q;
use serde_json::json;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn job(db: &Client, tag: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    q::enqueue(db, id, "env", q::resend_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend outcome unknown' WHERE id=$1", &[&id]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,correlation_id) VALUES ($1,'env','unknown',$2)", &[&id,&tag]).await.unwrap();
    db.execute(
        "INSERT INTO novu_rust_resend_recipients VALUES ($1,'env','to',0,'hash')",
        &[&id],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ($1,'env','s','s','n','w','t','subject','error',$2,'unknown')", &[&id.to_string(),&id]).await.unwrap();
    id
}
async fn event(db: &Client, env: &str, tag: Option<Uuid>, id: &str, recipient: Option<&str>) {
    db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash,correlation_id) VALUES ($1,$2,$3,'email.delivered',$4,$5)", &[&env,&Uuid::new_v4().to_string(),&id,&recipient,&tag]).await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn ambiguous_evidence_is_not_accepted_and_audit_is_atomic() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org'),('other','org')").await.unwrap();
    for sql in [
        q::MIGRATION,
        q::resend_queue::MIGRATION,
        q::resend_queue::RECIPIENT_MIGRATION,
        q::in_app::EMAIL_MESSAGES_MIGRATION,
        q::in_app::EMAIL_PROVIDER_RESULT_MIGRATION,
        "ALTER TABLE novu_rust_email_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'",
        q::resend_projection::MIGRATION,
        q::resend_projection::HEARTBEAT_MIGRATION,
        include_str!("../../../api/rust-api/migrations/0010_resend_webhooks.sql"),
        include_str!("../../../api/rust-api/migrations/0013_resend_event_recipient.sql"),
        include_str!("../../../api/rust-api/migrations/0047_resend_event_correlation.sql"),
        q::reconciliation::MIGRATION,
        q::reconciliation::RETRY_DISPATCH_MIGRATION,
        "ALTER TABLE novu_rust_delivery_reconciliations ADD COLUMN retry_provider_message_id text",
        include_str!("../migrations/0134_resend_correlation.sql"),
        include_str!("../migrations/0137_resend_auto_correlation.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let mut blocked = Vec::new();
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "other", Some(tag), "cross", Some("hash")).await;
    blocked.push(id);
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "env", Some(tag), "wrong-recipient", Some("other-hash")).await;
    blocked.push(id);
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "env", Some(tag), "missing-recipient", None).await;
    blocked.push(id);
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "env", Some(tag), "multiple-a", Some("hash")).await;
    event(&db, "env", Some(tag), "multiple-b", Some("hash")).await;
    blocked.push(id);
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "env", Some(tag), "other-tag", Some("hash")).await;
    event(&db, "env", Some(Uuid::new_v4()), "other-tag", Some("hash")).await;
    blocked.push(id);
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "env", Some(tag), "untagged", Some("hash")).await;
    event(&db, "env", None, "untagged", Some("hash")).await;
    blocked.push(id);
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "env", Some(tag), "duplicate-id", Some("hash")).await;
    let other = job(&db, Uuid::new_v4()).await;
    db.execute(
        "UPDATE novu_rust_resend_attempts SET provider_message_id='duplicate-id' WHERE job_id=$1",
        &[&other],
    )
    .await
    .unwrap();
    blocked.push(id);
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "env", Some(tag), "prior-retry", Some("hash")).await;
    db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason) VALUES ($1,'env',$2,'retry','op','test')", &[&Uuid::new_v4(),&id]).await.unwrap();
    blocked.push(id);
    for id in blocked {
        assert!(
            q::resend_projection::project_with_auto_correlation(&mut db, "env", id, true)
                .await
                .unwrap()
        );
        let row=db.query_one("SELECT j.status,a.state,a.provider_message_id,(SELECT count(*) FROM novu_rust_resend_auto_correlations WHERE job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert!(row.get::<_, Option<String>>(2).is_none());
        assert_eq!(row.get::<_, i64>(3), 0);
    }
    let partial_tag = Uuid::new_v4();
    let partial = job(&db, partial_tag).await;
    db.execute(
        "INSERT INTO novu_rust_resend_recipients VALUES ($1,'env','cc',0,'other-recipient')",
        &[&partial],
    )
    .await
    .unwrap();
    event(&db, "env", Some(partial_tag), "partial-id", Some("hash")).await;
    q::resend_projection::project_with_auto_correlation(&mut db, "env", partial, true)
        .await
        .unwrap();
    let partial_row=db.query_one("SELECT j.status,j.resend_delivery_result,a.state FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&partial]).await.unwrap();
    assert_eq!(partial_row.get::<_, String>(0), "completed");
    assert_eq!(partial_row.get::<_, String>(1), "pending");
    assert_eq!(partial_row.get::<_, String>(2), "accepted");
    let tag = Uuid::new_v4();
    let id = job(&db, tag).await;
    event(&db, "env", Some(tag), "valid", Some("hash")).await;
    q::resend_projection::project(&mut db, "env", id)
        .await
        .unwrap();
    assert_eq!(
        db.query_one(
            "SELECT state FROM novu_rust_resend_attempts WHERE job_id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    db.batch_execute("CREATE FUNCTION reject_auto_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$; CREATE TRIGGER reject_auto BEFORE INSERT ON novu_rust_resend_auto_correlations FOR EACH ROW EXECUTE FUNCTION reject_auto_audit()").await.unwrap();
    assert!(
        q::resend_projection::project_with_auto_correlation(&mut db, "env", id, true)
            .await
            .is_err()
    );
    assert_eq!(
        db.query_one(
            "SELECT state FROM novu_rust_resend_attempts WHERE job_id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    db.batch_execute("DROP TRIGGER reject_auto ON novu_rust_resend_auto_correlations")
        .await
        .unwrap();
    assert!(
        q::resend_projection::project_with_auto_correlation(&mut db, "env", id, true)
            .await
            .unwrap()
    );
    let row=db.query_one("SELECT j.status,j.resend_delivery_result,a.state,a.provider_message_id,m.delivery_state,m.resend_delivery_result,(SELECT count(*) FROM novu_rust_resend_auto_correlations c WHERE c.job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, String>(1), "delivered");
    assert_eq!(row.get::<_, String>(2), "accepted");
    assert_eq!(row.get::<_, String>(3), "valid");
    assert_eq!(row.get::<_, String>(4), "accepted");
    assert_eq!(row.get::<_, String>(5), "delivered");
    assert_eq!(row.get::<_, i64>(6), 1);
    assert!(
        q::resend_projection::project_with_auto_correlation(&mut db, "env", id, true)
            .await
            .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM novu_rust_resend_auto_correlations WHERE job_id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    event(&db, "env", Some(Uuid::new_v4()), "valid", Some("hash")).await;
    q::resend_projection::project_with_auto_correlation(&mut db, "env", id, true)
        .await
        .unwrap();
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    let check = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["resend-check", "env", &id.to_string()])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_RESEND_CHECK_AUTO_CORRELATION", "true")
        .output()
        .unwrap();
    assert_eq!(check.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&check.stdout).unwrap()["evidence"]["autoCorrelationConflict"],
        true
    );
    db.execute("DELETE FROM novu_rust_resend_delivery_events WHERE environment_id='env' AND email_id='valid'", &[]).await.unwrap();
    q::resend_projection::project_with_auto_correlation(&mut db, "env", id, true)
        .await
        .unwrap();
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    let process_tag = Uuid::new_v4();
    let process_job = job(&db, process_tag).await;
    event(&db, "env", Some(process_tag), "worker-valid", Some("hash")).await;
    let mut worker = tokio::process::Command::new(env!("CARGO_BIN_EXE_resend-projection-worker"))
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
        .env("NOVU_RESEND_PROJECTION_AUTO_CORRELATE", "true")
        .env("NOVU_RESEND_PROJECTION_SCAN_INTERVAL_SECONDS", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut recovered = false;
    for _ in 0..100 {
        if db
            .query_one(
                "SELECT state='accepted' FROM novu_rust_resend_attempts WHERE job_id=$1",
                &[&process_job],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        {
            recovered = true;
            break;
        }
        assert!(
            worker.try_wait().unwrap().is_none(),
            "projection worker exited early"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    worker.kill().await.unwrap();
    assert!(recovered, "projection worker did not recover the attempt");
}
