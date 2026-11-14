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
        include_str!("../migrations/0138_resend_auto_projection_guard.sql"),
        include_str!("../migrations/0139_resend_provider_collision_invalidation.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(include_str!(
        "../migrations/0138_resend_auto_projection_guard.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!(
        "../migrations/0139_resend_provider_collision_invalidation.sql"
    ))
    .await
    .unwrap();
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
    // A provider ID used in another environment is independent; collisions
    // with a different local attempt or retry audit invalidate both records.
    let external = Uuid::new_v4();
    q::enqueue(
        &db,
        external,
        "other",
        q::resend_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'other','accepted','valid')", &[&external]).await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    let collision = Uuid::new_v4();
    q::enqueue(&db, collision, "env", q::resend_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'env','accepted','valid')", &[&collision]).await.unwrap();
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    q::resend_projection::project(&mut db, "env", id)
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
    db.execute(
        "UPDATE novu_rust_resend_attempts SET provider_message_id=NULL WHERE job_id=$1",
        &[&collision],
    )
    .await
    .unwrap();
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
        "delivered"
    );
    db.execute(
        "UPDATE novu_rust_resend_attempts SET provider_message_id='valid' WHERE job_id=$1",
        &[&collision],
    )
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
    db.execute(
        "UPDATE novu_rust_resend_attempts SET provider_message_id=NULL WHERE job_id=$1",
        &[&collision],
    )
    .await
    .unwrap();
    q::resend_projection::project_with_auto_correlation(&mut db, "env", id, true)
        .await
        .unwrap();
    let decision = Uuid::new_v4();
    db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_provider_message_id) VALUES ($1,'env',$2,'retry','operator','test','valid')", &[&decision,&collision]).await.unwrap();
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    db.execute(
        "DELETE FROM novu_rust_delivery_reconciliations WHERE id=$1",
        &[&decision],
    )
    .await
    .unwrap();
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
        "delivered"
    );
    // A callback that inserts while a legacy projector holds the job lock
    // must invalidate its result once that transaction commits.
    let (observer, observer_driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        observer_driver.await.unwrap();
    });
    let callback_url = format!(
        "{url}{}application_name=auto_guard_callback",
        if url.contains('?') { "&" } else { "?" }
    );
    let (callback_db, callback_driver) =
        tokio_postgres::connect(&callback_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        callback_driver.await.unwrap();
    });
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&id],
    )
    .await
    .unwrap();
    let callback = tokio::spawn(async move {
        event(
            &callback_db,
            "env",
            Some(tag),
            "late-other-id",
            Some("hash"),
        )
        .await;
    });
    let mut waiting = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='auto_guard_callback' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(waiting && !callback.is_finished());
    tx.execute(
        "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
        &[&id],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    callback.await.unwrap();
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    q::resend_projection::project(&mut db, "env", id)
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
    db.execute("DELETE FROM novu_rust_resend_delivery_events WHERE environment_id='env' AND email_id='late-other-id'", &[]).await.unwrap();
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
        "delivered"
    );
    event(&db, "other", Some(tag), "valid", Some("hash")).await;
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "delivered"
    );
    event(&db, "env", Some(Uuid::new_v4()), "valid", Some("hash")).await;
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    // Even a legacy projector must not restore delivered while evidence conflicts.
    q::resend_projection::project(&mut db, "env", id)
        .await
        .unwrap();
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    let role = format!("auto_guard_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT(id),UPDATE(resend_delivery_result) ON novu_queue_jobs TO {role}; GRANT SELECT(email_job_id),UPDATE(resend_delivery_result) ON novu_rust_email_messages TO {role}")).await.unwrap();
    let (limited, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    limited
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        limited
            .query("SELECT * FROM novu_rust_resend_auto_correlations", &[])
            .await
            .is_err()
    );
    limited
        .execute(
            "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap();
    limited.execute("UPDATE novu_rust_email_messages SET resend_delivery_result='delivered' WHERE email_job_id=$1", &[&id]).await.unwrap();
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
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
    db.execute("DELETE FROM novu_rust_resend_delivery_events WHERE environment_id='env' AND email_id='valid' AND correlation_id<>$1", &[&tag]).await.unwrap();
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
        "delivered"
    );
    db.execute("DELETE FROM novu_rust_resend_delivery_events WHERE environment_id='env' AND email_id='valid'", &[]).await.unwrap();
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
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

    // The auto transaction has the provider lock, then a competing attempt
    // starts. Once auto commits, the collision must invalidate its result.
    let race_tag = Uuid::new_v4();
    let race_job = job(&db, race_tag).await;
    event(&db, "env", Some(race_tag), "race-auto-first", Some("hash")).await;
    let collision_job = Uuid::new_v4();
    q::enqueue(
        &db,
        collision_job,
        "env",
        q::resend_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE FUNCTION pause_auto_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id='{}'::uuid THEN PERFORM pg_advisory_xact_lock(719931); END IF; RETURN NEW; END $$; CREATE TRIGGER pause_auto_audit BEFORE INSERT ON novu_rust_resend_auto_correlations FOR EACH ROW EXECUTE FUNCTION pause_auto_audit()",race_job)).await.unwrap();
    let (blocker, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    blocker
        .query_one("SELECT pg_advisory_lock(719931)", &[])
        .await
        .unwrap();
    let named = |name: &str| {
        format!(
            "{url}{}application_name={name}",
            if url.contains('?') { "&" } else { "?" }
        )
    };
    let (mut projector, driver) = tokio_postgres::connect(&named("auto_first_projector"), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let projection = tokio::spawn(async move {
        q::resend_projection::project_with_auto_correlation(&mut projector, "env", race_job, true)
            .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='auto_first_projector' AND wait_event='advisory')", &[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        waiting,
        "automatic recovery did not reach the audit barrier"
    );
    let (other, driver) = tokio_postgres::connect(&named("auto_first_collision"), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let colliding = tokio::spawn(async move {
        other.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'env','accepted','race-auto-first')", &[&collision_job]).await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='auto_first_collision' AND wait_event='advisory')", &[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        waiting && !colliding.is_finished(),
        "collision did not wait for automatic recovery"
    );
    blocker
        .query_one("SELECT pg_advisory_unlock(719931)", &[])
        .await
        .unwrap();
    assert!(projection.await.unwrap().unwrap());
    colliding.await.unwrap().unwrap();
    let row=db.query_one("SELECT j.resend_delivery_result,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&race_job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");

    // The conflicting attempt holds the provider lock before its commit.
    // Auto recovery must re-read after waiting and refuse the stale candidate.
    let race_tag = Uuid::new_v4();
    let race_job = job(&db, race_tag).await;
    event(
        &db,
        "env",
        Some(race_tag),
        "race-collision-first",
        Some("hash"),
    )
    .await;
    let collision_job = Uuid::new_v4();
    q::enqueue(
        &db,
        collision_job,
        "env",
        q::resend_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE FUNCTION pause_collision() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id='{}'::uuid THEN PERFORM pg_advisory_xact_lock(719932); END IF; RETURN NEW; END $$; CREATE TRIGGER zzz_pause_collision AFTER INSERT ON novu_rust_resend_attempts FOR EACH ROW EXECUTE FUNCTION pause_collision()",collision_job)).await.unwrap();
    blocker
        .query_one("SELECT pg_advisory_lock(719932)", &[])
        .await
        .unwrap();
    let (other, driver) = tokio_postgres::connect(&named("collision_first_writer"), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let colliding = tokio::spawn(async move {
        other.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'env','accepted','race-collision-first')", &[&collision_job]).await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='collision_first_writer' AND wait_event='advisory')", &[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(waiting, "collision did not reach its commit barrier");
    let (mut projector, driver) =
        tokio_postgres::connect(&named("collision_first_projector"), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let projection = tokio::spawn(async move {
        q::resend_projection::project_with_auto_correlation(&mut projector, "env", race_job, true)
            .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='collision_first_projector' AND wait_event='advisory')", &[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        waiting && !projection.is_finished(),
        "projection did not wait for the collision"
    );
    blocker
        .query_one("SELECT pg_advisory_unlock(719932)", &[])
        .await
        .unwrap();
    colliding.await.unwrap().unwrap();
    assert!(projection.await.unwrap().unwrap());
    let row=db.query_one("SELECT j.status,a.state,a.provider_message_id,(SELECT count(*) FROM novu_rust_resend_auto_correlations c WHERE c.job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&race_job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert!(row.get::<_, Option<String>>(2).is_none());
    assert_eq!(row.get::<_, i64>(3), 0);

    // A same-tag callback with a different ID must wait for an in-flight
    // automatic correlation, then invalidate its just-committed result.
    let tag = Uuid::new_v4();
    let auto_job = job(&db, tag).await;
    event(&db, "env", Some(tag), "event-after-auto", Some("hash")).await;
    db.batch_execute(&format!("CREATE FUNCTION pause_auto_event_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id='{}'::uuid THEN PERFORM pg_advisory_xact_lock(719933); END IF; RETURN NEW; END $$; CREATE TRIGGER pause_auto_event_audit BEFORE INSERT ON novu_rust_resend_auto_correlations FOR EACH ROW EXECUTE FUNCTION pause_auto_event_audit()",auto_job)).await.unwrap();
    blocker
        .query_one("SELECT pg_advisory_lock(719933)", &[])
        .await
        .unwrap();
    let (mut projector, driver) = tokio_postgres::connect(&named("event_after_projector"), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let projection = tokio::spawn(async move {
        q::resend_projection::project_with_auto_correlation(&mut projector, "env", auto_job, true)
            .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='event_after_projector' AND wait_event='advisory')",&[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(waiting);
    let (event_db, driver) = tokio_postgres::connect(&named("event_after_writer"), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let callback = tokio::spawn(async move {
        event(
            &event_db,
            "env",
            Some(tag),
            "event-after-other",
            Some("hash"),
        )
        .await;
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='event_after_writer' AND wait_event='advisory')",&[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(waiting && !callback.is_finished());
    blocker
        .query_one("SELECT pg_advisory_unlock(719933)", &[])
        .await
        .unwrap();
    assert!(projection.await.unwrap().unwrap());
    callback.await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT resend_delivery_result FROM novu_queue_jobs WHERE id=$1",
            &[&auto_job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );

    // If that callback holds the tag lock first, recovery must see both IDs
    // after the callback commits and leave the attempt unresolved.
    let tag = Uuid::new_v4();
    let auto_job = job(&db, tag).await;
    event(&db, "env", Some(tag), "event-before-auto", Some("hash")).await;
    db.batch_execute("CREATE FUNCTION pause_event_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM paused_events WHERE event_id='event-before-other') THEN PERFORM pg_advisory_xact_lock(719934); END IF; RETURN NULL; END $$; CREATE TRIGGER zzz_pause_event AFTER INSERT ON novu_rust_resend_delivery_events REFERENCING NEW TABLE AS paused_events FOR EACH STATEMENT EXECUTE FUNCTION pause_event_insert()").await.unwrap();
    blocker
        .query_one("SELECT pg_advisory_lock(719934)", &[])
        .await
        .unwrap();
    let (event_db, driver) = tokio_postgres::connect(&named("event_before_writer"), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let callback = tokio::spawn(async move {
        event_db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash,correlation_id) VALUES ('env','event-before-other','event-before-other','email.delivered','hash',$1)", &[&tag]).await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='event_before_writer' AND wait_event='advisory')",&[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(waiting);
    let (mut projector, driver) = tokio_postgres::connect(&named("event_before_projector"), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let projection = tokio::spawn(async move {
        q::resend_projection::project_with_auto_correlation(&mut projector, "env", auto_job, true)
            .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='event_before_projector' AND wait_event='advisory')",&[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(waiting && !projection.is_finished());
    blocker
        .query_one("SELECT pg_advisory_unlock(719934)", &[])
        .await
        .unwrap();
    callback.await.unwrap().unwrap();
    assert!(projection.await.unwrap().unwrap());
    let row=db.query_one("SELECT j.status,a.state,(SELECT count(*) FROM novu_rust_resend_auto_correlations c WHERE c.job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&auto_job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(row.get::<_, i64>(2), 0);
}
