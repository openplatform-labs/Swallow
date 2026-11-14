//! Uses a disposable PostgreSQL container and never contacts Slack.
use novu_postgres_queue::{self as queue, MIGRATION, in_app};
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    for _ in 0..100 {
        if let Ok((db, driver)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = driver.await;
            });
            return db;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("database did not become ready");
}
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(url: &str, application: &str) -> Worker {
    Worker(
        Command::new(env!("CARGO_BIN_EXE_in-app-worker"))
            .env(
                "DATABASE_URL",
                format!("{url}?application_name={application}"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_ALLOW_RECREATED_SUBSCRIBER", "true")
            .env("NOVU_IN_APP_DB_TIMEOUT_SECONDS", "2")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

#[test]
fn invalid_timeout_is_rejected_before_connecting() {
    for timeout in ["0", "301", "bad"] {
        let status = Command::new(env!("CARGO_BIN_EXE_in-app-worker"))
            .env("DATABASE_URL", "postgresql://localhost/unused")
            .env("NOVU_ALLOW_RECREATED_SUBSCRIBER", "true")
            .env("NOVU_IN_APP_DB_TIMEOUT_SECONDS", timeout)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success());
    }
}

#[tokio::test]
#[ignore = "restarts a disposable PostgreSQL container"]
async fn workers_reconnect_and_process_in_app_once() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let container = std::env::var("TEST_PG_CONTAINER").unwrap();
    let mut db = connect(&url).await;
    db.batch_execute("DROP TABLE IF EXISTS novu_rust_sendgrid_dispatch_ids, novu_rust_twilio_dispatch_ids, novu_rust_ws_events, novu_rust_subscription_preferences, novu_rust_topic_subscribers, novu_rust_topics, novu_rust_contexts, novu_rust_in_app_messages, novu_rust_subscribers, novu_queue_jobs, novu_rust_workflows CASCADE; CREATE TABLE novu_rust_workflows (environment_id text, identifier text, in_app_content text, has_steps boolean NOT NULL DEFAULT true, PRIMARY KEY (environment_id, identifier));").await.unwrap();
    for migration in [
        MIGRATION,
        in_app::MIGRATION,
        in_app::STATE_MIGRATION,
        in_app::STATE_DATES_MIGRATION,
        in_app::IDENTIFIER_MIGRATION,
        in_app::INBOX_METADATA_MIGRATION,
        in_app::INBOX_DATE_PRESENCE_MIGRATION,
        in_app::INBOX_DATA_MIGRATION,
        in_app::INBOX_DATA_ARRAY_MIGRATION,
        in_app::CONTEXT_MIGRATION,
        in_app::CONTEXT_DATA_MIGRATION,
        in_app::CONTEXT_TIMESTAMPS_MIGRATION,
        in_app::CONTEXT_CURSOR_MIGRATION,
        in_app::SUBSCRIBER_OBJECTID_MIGRATION,
        in_app::GLOBAL_PREFERENCES_MIGRATION,
        in_app::WORKFLOW_PREFERENCES_MIGRATION,
        in_app::SUBSCRIBER_WORKFLOW_PREFERENCES_MIGRATION,
        in_app::STEP_OUTCOMES_MIGRATION,
        in_app::SUBSCRIBER_ONLINE_MIGRATION,
        in_app::TOPICS_MIGRATION,
        in_app::TOPIC_DELETE_RESTRICT_MIGRATION,
        in_app::TOPIC_DATA_MIGRATION,
        in_app::TOPIC_SUBSCRIPTIONS_MIGRATION,
        in_app::TOPIC_SUBSCRIPTIONS_CURSOR_MIGRATION,
        in_app::TOPIC_SUBSCRIPTION_NAME_MIGRATION,
        in_app::TOPIC_SUBSCRIPTION_PREFERENCES_MIGRATION,
        in_app::SUBSCRIBER_SUBSCRIPTION_CURSOR_MIGRATION,
        in_app::SNOOZED_UNTIL_MIGRATION,
        in_app::DELIVERY_HISTORY_MIGRATION,
        in_app::WS_EVENTS_MIGRATION,
        in_app::WS_BEST_EFFORT_MIGRATION,
        in_app::MESSAGE_SEVERITY_MIGRATION,
        in_app::IN_APP_STEP_ACTIVE_MIGRATION,
        in_app::NOTIFICATIONS_MIGRATION,
        in_app::EMAIL_STEP_MIGRATION,
        in_app::EMAIL_MESSAGES_MIGRATION,
        in_app::EMAIL_PROVIDER_RESULT_MIGRATION,
        in_app::EMAIL_STEP_ID_MIGRATION,
        in_app::EMAIL_REVALIDATION_MIGRATION,
        in_app::SMS_STEP_MIGRATION,
        in_app::SMS_MESSAGES_MIGRATION,
        in_app::SMS_STEP_ID_MIGRATION,
        in_app::PUSH_STEP_MIGRATION,
        in_app::FCM_CREDENTIALS_MIGRATION,
        in_app::PUSH_MESSAGES_MIGRATION,
        in_app::IN_APP_SUBSCRIBER_IDENTITY_MIGRATION,
        in_app::MULTIPLE_FCM_CHANNELS_MIGRATION,
        in_app::PUSH_STEP_ID_MIGRATION,
        in_app::WORKFLOW_EMAIL_PROVIDER_MIGRATION,
        novu_postgres_queue::sendgrid_queue::MIGRATION,
        novu_postgres_queue::sendgrid_queue::DISPATCH_ID_MIGRATION,
        novu_postgres_queue::sendgrid_queue::DISPATCH_PROOF_MIGRATION,
        novu_postgres_queue::twilio_queue::MIGRATION,
        novu_postgres_queue::twilio_queue::DISPATCH_ID_MIGRATION,
        novu_postgres_queue::fcm_queue::MIGRATION,
        in_app::DELIVERY_STATE_MIGRATION,
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    let lock = db.transaction().await.unwrap();
    lock.batch_execute("LOCK TABLE novu_queue_jobs IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let observer = connect(&url).await;
    let mut first = worker(&url, "in_app_ha_1");
    let mut observed_lock = false;
    for _ in 0..100 {
        observed_lock = observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='in_app_ha_1' AND wait_event_type='Lock')", &[]).await.unwrap().get(0);
        if observed_lock {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(observed_lock, "worker did not encounter lock");
    let mut disconnected = false;
    for _ in 0..100 {
        disconnected = !observer
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='in_app_ha_1')",
                &[],
            )
            .await
            .unwrap()
            .get::<_, bool>(0);
        if disconnected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(disconnected, "worker did not abandon timed-out connection");
    lock.commit().await.unwrap();
    let mut second = worker(&url, "in_app_ha_2");
    let mut both = false;
    for _ in 0..100 {
        both = observer.query_one("SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('in_app_ha_1','in_app_ha_2')", &[]).await.unwrap().get(0);
        if both {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(both, "workers did not connect");
    assert!(
        Command::new("docker")
            .args(["restart", &container])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let db = connect(&url).await;
    let job = Uuid::new_v4();
    let payload = json!({"workflow":"test", "transactionId":"after-restart", "content":"Hello {{subscriber.firstName}}", "payload":{}, "recipients":[{"subscriberId":"alice","firstName":"Jane"}]});
    queue::enqueue(&db, job, "env-a", in_app::JOB_KIND, &payload)
        .await
        .unwrap();
    let mut recovered = false;
    for _ in 0..200 {
        let row = db.query_one("SELECT (SELECT status='completed' FROM novu_queue_jobs WHERE id=$1),(SELECT count(*)=1 FROM novu_rust_notifications WHERE job_id=$1),(SELECT count(*)=1 FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND subscriber_id='alice'),(SELECT count(*)=2 FROM pg_stat_activity WHERE application_name IN ('in_app_ha_1','in_app_ha_2'))", &[&job]).await.unwrap();
        recovered = (0..4).all(|i| row.get::<_, Option<bool>>(i) == Some(true));
        if recovered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recovered
            && first.0.try_wait().unwrap().is_none()
            && second.0.try_wait().unwrap().is_none(),
        "workers did not safely recover after restart"
    );
}
