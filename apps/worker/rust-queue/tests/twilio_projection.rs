//! Disposable PostgreSQL; events are synthetic, not live Twilio callbacks.
use novu_postgres_queue::{self as queue, twilio_projection as projection};
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn get(db: &tokio_postgres::Client, job: Uuid) -> tokio_postgres::Row {
    db.query_one("SELECT j.twilio_delivery_result,m.twilio_delivery_result FROM novu_queue_jobs j JOIN novu_rust_sms_messages m ON m.sms_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap()
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn projects_only_matching_current_attempt_and_recovers_on_rescan() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        queue::twilio_queue::MIGRATION,
        queue::twilio_queue::DISPATCH_ID_MIGRATION,
        queue::in_app::SMS_MESSAGES_MIGRATION,
        projection::MIGRATION,
        include_str!("../../../api/rust-api/migrations/0005_twilio_webhooks.sql"),
        include_str!("../../../api/rust-api/migrations/0006_twilio_webhook_job_link.sql"),
        include_str!("../../../api/rust-api/migrations/0007_twilio_dispatch_identity.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(projection::MIGRATION).await.unwrap();
    let job = Uuid::new_v4();
    let dispatch = Uuid::new_v4();
    queue::enqueue(&db, job, "a", queue::twilio_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,provider_message_id,dispatch_id) VALUES ($1,'a','accepted','SMmatch',$2)", &[&job,&dispatch]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status,sms_job_id) VALUES ('m','a','s','s','n','w','t','sent',$1)", &[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('a','ACtest','SMmatch','delivered',$1,$2)", &[&job,&dispatch]).await.unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    let row = get(&db, job).await;
    assert_eq!(row.get::<_, String>(0), "delivered");
    assert_eq!(row.get::<_, String>(1), "delivered");
    let duplicate = Uuid::new_v4();
    queue::enqueue(
        &db,
        duplicate,
        "a",
        queue::twilio_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'a','accepted','SMmatch')", &[&duplicate]).await.unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "conflicting");
    db.execute("DELETE FROM novu_queue_jobs WHERE id=$1", &[&duplicate])
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('a','ACtest','SMold','failed',$1,$2)", &[&job,&Uuid::new_v4()]).await.unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "delivered");
    assert!(!projection::project(&mut db, "b", job).await.unwrap());
    db.execute(
        "UPDATE novu_rust_twilio_attempts SET provider_message_id='SMother' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "conflicting");
    db.execute(
        "UPDATE novu_rust_twilio_attempts SET provider_message_id='SMmatch' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('a','ACtest','SMmatch','failed',$1,$2)", &[&job,&dispatch]).await.unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "conflicting");
    db.execute(
        "DELETE FROM novu_rust_twilio_delivery_events WHERE job_id=$1 AND status='failed'",
        &[&job],
    )
    .await
    .unwrap();
    for (status, expected) in [
        ("undelivered", "failed"),
        ("sent", "pending"),
        ("future_status", "unknown"),
        ("delivered", "delivered"),
    ] {
        db.execute("UPDATE novu_rust_twilio_delivery_events SET status=$3 WHERE job_id=$1 AND dispatch_id=$2", &[&job,&dispatch,&status]).await.unwrap();
        assert!(projection::project(&mut db, "a", job).await.unwrap());
        assert_eq!(get(&db, job).await.get::<_, String>(0), expected);
    }
    db.execute(
        "UPDATE novu_rust_twilio_attempts SET state='unknown' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert!(projection::project(&mut db, "a", job).await.unwrap());
    assert_eq!(get(&db, job).await.get::<_, String>(0), "unknown");
    db.execute(
        "UPDATE novu_rust_twilio_attempts SET state='accepted' WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let (mut other, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let lock = other.transaction().await.unwrap();
    lock.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    assert!(!projection::project(&mut db, "a", job).await.unwrap());
    lock.rollback().await.unwrap();
    let worker = std::process::Command::new(env!("CARGO_BIN_EXE_twilio-projection-worker"))
        .env("DATABASE_URL", &url)
        .env("NOVU_TWILIO_ENVIRONMENT_ID", "a")
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let _worker = Child(worker);
    let mut completed = false;
    for _ in 0..100 {
        let row=db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_twilio_projection_heartbeat WHERE environment_id='a'),twilio_delivery_result FROM novu_queue_jobs WHERE id=$1", &[&job]).await.unwrap();
        if row.get::<_, bool>(0) && row.get::<_, Option<String>>(1).as_deref() == Some("delivered")
        {
            completed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        completed,
        "worker did not complete projection and heartbeat"
    );
}
