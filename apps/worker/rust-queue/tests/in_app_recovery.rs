use novu_postgres_queue::{MIGRATION, claim, enqueue, in_app};
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database in TEST_DATABASE_URL"]
async fn worker_recovers_expired_jobs_and_isolates_invalid_jobs() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db.batch_execute("DROP TABLE IF EXISTS novu_rust_ws_events, novu_rust_subscription_preferences, novu_rust_topic_subscribers, novu_rust_topics, novu_rust_contexts, novu_rust_in_app_messages, novu_rust_subscribers, novu_queue_jobs, novu_rust_workflows CASCADE; CREATE TABLE novu_rust_workflows (environment_id text, identifier text, in_app_content text, has_steps boolean NOT NULL DEFAULT true, PRIMARY KEY (environment_id, identifier));").await.unwrap();
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
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    let good = Uuid::new_v4();
    let bad = Uuid::new_v4();
    let after_bad = Uuid::new_v4();
    let other = Uuid::new_v4();
    let payload = json!({"workflow":"test", "transactionId":"recovered", "content":"Hello {{subscriber.firstName}}", "payload":{}, "recipients":[{"subscriberId":"alice","firstName":"Jane"}]});
    enqueue(&db, good, "env-a", in_app::JOB_KIND, &payload)
        .await
        .unwrap();
    let old = claim(&db, in_app::JOB_KIND, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.id, good);
    db.execute(
        "UPDATE novu_queue_jobs SET lease_until=now()-interval '1 second' WHERE id=$1",
        &[&good],
    )
    .await
    .unwrap();
    enqueue(&db, other, "env-a", "other-kind", &json!({}))
        .await
        .unwrap();
    let other_claim = claim(&db, "other-kind", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET lease_until=now()-interval '1 second' WHERE id=$1",
        &[&other],
    )
    .await
    .unwrap();
    db.batch_execute("INSERT INTO novu_rust_subscribers (environment_id,subscriber_id,internal_id) VALUES ('env-a','blocked',novu_rust_new_objectid()); INSERT INTO novu_rust_subscriber_preferences (environment_id,subscriber_id,channels) VALUES ('env-a','blocked','{\"in_app\":false}')").await.unwrap();
    let bad_payload = json!({"workflow":"test", "transactionId":"bad", "content":"{{object}}", "payload":{"object":{"secret":"hidden"}}, "recipients":[{"subscriberId":"blocked"},{"subscriberId":"rollback"}]});
    enqueue(&db, bad, "env-a", in_app::JOB_KIND, &bad_payload)
        .await
        .unwrap();
    let next = json!({"workflow":"test", "transactionId":"after-bad", "content":"ok", "payload":{}, "recipients":[{"subscriberId":"bob"}]});
    enqueue(&db, after_bad, "env-a", in_app::JOB_KIND, &next)
        .await
        .unwrap();
    let mut worker = Worker(
        Command::new(env!("CARGO_BIN_EXE_in-app-worker"))
            .env("DATABASE_URL", &url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    for _ in 0..100 {
        if worker.0.try_wait().unwrap().is_some() {
            panic!("worker exited unexpectedly");
        }
        let states = db
            .query(
                "SELECT id,status FROM novu_queue_jobs WHERE id=ANY($1)",
                &[&vec![good, bad, after_bad]],
            )
            .await
            .unwrap();
        if states
            .iter()
            .any(|r| r.get::<_, Uuid>(0) == good && r.get::<_, String>(1) == "completed")
            && states
                .iter()
                .any(|r| r.get::<_, Uuid>(0) == bad && r.get::<_, String>(1) == "failed")
            && states
                .iter()
                .any(|r| r.get::<_, Uuid>(0) == after_bad && r.get::<_, String>(1) == "completed")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let row = db
        .query_one(
            "SELECT status,attempts FROM novu_queue_jobs WHERE id=$1",
            &[&good],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, i32>(1), 2);
    let row = db
        .query_one(
            "SELECT status,last_error FROM novu_queue_jobs WHERE id=$1",
            &[&bad],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unsupported template or input");
    let status: String = db
        .query_one(
            "SELECT status FROM novu_queue_jobs WHERE id=$1",
            &[&after_bad],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "completed");
    let row = db
        .query_one(
            "SELECT status,lease_token FROM novu_queue_jobs WHERE id=$1",
            &[&other],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "running");
    assert_eq!(row.get::<_, Uuid>(1), other_claim.lease_token);
    assert!(
        !novu_postgres_queue::complete(&db, good, old.lease_token)
            .await
            .unwrap()
    );
    let count: i64 = db.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='recovered' AND content='Hello Jane'", &[]).await.unwrap().get(0);
    assert_eq!(count, 1);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_subscribers WHERE subscriber_id='rollback'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_in_app_step_outcomes WHERE job_id=$1",
            &[&bad],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let count: i64 = db.query_one("SELECT count(*) FROM novu_rust_in_app_step_outcomes WHERE job_id=$1 AND status='completed'", &[&good]).await.unwrap().get(0);
    assert_eq!(count, 1);

    // Force overlap after each subscriber update; opposing recipient orders
    // otherwise deadlock while each worker holds its first row lock.
    worker.0.kill().unwrap();
    worker.0.wait().unwrap();
    db.batch_execute("INSERT INTO novu_rust_subscribers (environment_id,subscriber_id,internal_id) VALUES ('env-a','race-a',novu_rust_new_objectid()), ('env-a','race-b',novu_rust_new_objectid()); CREATE FUNCTION slow_race_subscriber() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.subscriber_id LIKE 'race-%' THEN PERFORM pg_sleep(0.25); END IF; RETURN NEW; END $$; CREATE TRIGGER slow_race_subscriber BEFORE UPDATE ON novu_rust_subscribers FOR EACH ROW EXECUTE FUNCTION slow_race_subscriber();").await.unwrap();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let first_payload = json!({"workflow":"test", "transactionId":"race-first", "content":"ok", "payload":{}, "recipients":["race-a","race-b"]});
    let second_payload = json!({"workflow":"test", "transactionId":"race-second", "content":"ok", "payload":{}, "recipients":["race-b","race-a"]});
    enqueue(&db, first, "env-a", in_app::JOB_KIND, &first_payload)
        .await
        .unwrap();
    enqueue(&db, second, "env-a", in_app::JOB_KIND, &second_payload)
        .await
        .unwrap();
    let (mut first_client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let (mut second_client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let (first_result, second_result) = tokio::join!(
        in_app::process_one(&mut first_client),
        in_app::process_one(&mut second_client)
    );
    assert!(first_result.unwrap());
    assert!(second_result.unwrap());
    let row = db.query_one("SELECT count(*),count(*) FILTER (WHERE status='completed') FROM novu_queue_jobs WHERE id=ANY($1)", &[&vec![first,second]]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 2);
    assert_eq!(row.get::<_, i64>(1), 2);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM novu_rust_in_app_step_outcomes WHERE job_id=ANY($1)",
            &[&vec![first, second]],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 4);

    // The reference drops WS enqueue failures; message delivery still completes.
    let event_failure = Uuid::new_v4();
    let event_payload = json!({"workflow":"test", "transactionId":"event-write-failure", "content":"ok", "payload":{}, "recipients":["event-recipient"]});
    db.batch_execute("ALTER TABLE novu_rust_ws_events ADD CONSTRAINT test_event_write CHECK (event <> 'notification_received') NOT VALID").await.unwrap();
    enqueue(
        &db,
        event_failure,
        "env-a",
        in_app::JOB_KIND,
        &event_payload,
    )
    .await
    .unwrap();
    assert!(in_app::process_one(&mut first_client).await.unwrap());
    let row = db.query_one("SELECT status,(SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='event-write-failure'),(SELECT count(*) FROM novu_rust_subscribers WHERE subscriber_id='event-recipient') FROM novu_queue_jobs WHERE id=$1", &[&event_failure]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(row.get::<_, i64>(2), 1);
    db.batch_execute("ALTER TABLE novu_rust_ws_events DROP CONSTRAINT test_event_write")
        .await
        .unwrap();
    let count: i64 = db.query_one("SELECT count(*) FROM novu_rust_ws_events e JOIN novu_rust_in_app_messages m ON m.environment_id=e.environment_id AND m.internal_id=e.data->>'messageId' WHERE m.transaction_id='event-write-failure'", &[]).await.unwrap().get(0);
    assert_eq!(count, 0);

    // The trigger delays the update past the short lease. The stale attempt
    // must roll back all writes, remain recoverable, and not stop the worker.
    let expired = Uuid::new_v4();
    let expired_payload = json!({"workflow":"test", "transactionId":"lease-during-processing", "content":"ok", "payload":{}, "recipients":[{"subscriberId":"race-a","firstName":"stale"}]});
    enqueue(&db, expired, "env-a", in_app::JOB_KIND, &expired_payload)
        .await
        .unwrap();
    assert!(
        in_app::process_one_with_lease(&mut first_client, Duration::from_millis(100))
            .await
            .unwrap()
    );
    let row = db.query_one("SELECT status,attempts,(SELECT count(*) FROM novu_rust_in_app_step_outcomes WHERE job_id=$1),(SELECT profile->>'firstName' FROM novu_rust_subscribers WHERE environment_id='env-a' AND subscriber_id='race-a') FROM novu_queue_jobs WHERE id=$1", &[&expired]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "running");
    assert_eq!(row.get::<_, i32>(1), 1);
    assert_eq!(row.get::<_, i64>(2), 0);
    assert_eq!(row.get::<_, Option<String>>(3), None);
    assert_eq!(
        novu_postgres_queue::requeue_expired(&db, in_app::JOB_KIND)
            .await
            .unwrap(),
        1
    );
    assert!(in_app::process_one(&mut first_client).await.unwrap());
    let row = db.query_one("SELECT status,attempts,(SELECT count(*) FROM novu_rust_in_app_step_outcomes WHERE job_id=$1),(SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='lease-during-processing') FROM novu_queue_jobs WHERE id=$1", &[&expired]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, i32>(1), 2);
    assert_eq!(row.get::<_, i64>(2), 1);
    assert_eq!(row.get::<_, i64>(3), 1);

    let waiting = Uuid::new_v4();
    let waiting_payload = json!({"workflow":"test", "transactionId":"lease-during-lock-wait", "content":"ok", "payload":{}, "recipients":["wait-recipient"]});
    enqueue(&db, waiting, "env-a", in_app::JOB_KIND, &waiting_payload)
        .await
        .unwrap();
    let (mut blocker, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let lock = blocker.transaction().await.unwrap();
    lock.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
        &[&"env-a", &"wait-recipient"],
    )
    .await
    .unwrap();
    let pid: i32 = second_client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let task = tokio::spawn(async move {
        let result =
            in_app::process_one_with_lease(&mut second_client, Duration::from_secs(2)).await;
        (result, second_client)
    });
    let mut expired_while_waiting = false;
    for _ in 0..400 {
        let row = db.query_opt("SELECT lease_until <= clock_timestamp(), EXISTS(SELECT 1 FROM pg_locks WHERE pid=$2 AND locktype='advisory' AND NOT granted) FROM novu_queue_jobs WHERE id=$1 AND status='running'", &[&waiting,&pid]).await.unwrap();
        if row.is_some_and(|row| row.get::<_, bool>(0) && row.get::<_, bool>(1)) {
            expired_while_waiting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(expired_while_waiting, "worker did not wait past its lease");
    // The locked running job is not requeued while its transaction is active.
    assert_eq!(
        novu_postgres_queue::requeue_expired(&db, in_app::JOB_KIND)
            .await
            .unwrap(),
        0
    );
    lock.commit().await.unwrap();
    let (result, mut second_client) = task.await.unwrap();
    assert!(result.unwrap());
    let count:i64 = db.query_one("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='lease-during-lock-wait'", &[]).await.unwrap().get(0);
    assert_eq!(count, 0);
    assert_eq!(
        novu_postgres_queue::requeue_expired(&db, in_app::JOB_KIND)
            .await
            .unwrap(),
        1
    );
    assert!(in_app::process_one(&mut second_client).await.unwrap());
    let row = db.query_one("SELECT status,attempts,(SELECT count(*) FROM novu_rust_in_app_step_outcomes WHERE job_id=$1),(SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='lease-during-lock-wait') FROM novu_queue_jobs WHERE id=$1", &[&waiting]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, i32>(1), 2);
    assert_eq!(row.get::<_, i64>(2), 1);
    assert_eq!(row.get::<_, i64>(3), 1);

    // Jobs queued before severityOverride existed retain their snapshot, while
    // new jobs without an override resolve the current workflow severity.
    db.execute("INSERT INTO novu_rust_workflows(environment_id,identifier,inbox_metadata,in_app_content) VALUES ('env-a','severity-compat','{\"severity\":\"high\",\"tags\":[\"current\"]}'::jsonb,'current content')", &[]).await.unwrap();
    for (transaction, payload, expected, expected_content, expected_tags) in [
        (
            "legacy-severity",
            json!({"severity":"low"}),
            "low",
            "ok",
            json!(["queued"]),
        ),
        (
            "current-severity",
            json!({"severity":"low","severityOverride":null}),
            "high",
            "current content",
            json!(["current"]),
        ),
        (
            "override-severity",
            json!({"severity":"low","severityOverride":"medium"}),
            "medium",
            "current content",
            json!(["current"]),
        ),
    ] {
        let mut job = json!({"workflow":"severity-compat","transactionId":transaction,"content":"ok","payload":{},"tags":["queued"],"recipients":["severity-user"]});
        job.as_object_mut()
            .unwrap()
            .extend(payload.as_object().unwrap().clone());
        enqueue(&db, Uuid::new_v4(), "env-a", in_app::JOB_KIND, &job)
            .await
            .unwrap();
        assert!(in_app::process_one(&mut second_client).await.unwrap());
        let row = db.query_one("SELECT severity,severity_present,content,tags FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND transaction_id=$1", &[&transaction]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), expected);
        assert!(row.get::<_, bool>(1));
        assert_eq!(row.get::<_, String>(2), expected_content);
        assert_eq!(row.get::<_, serde_json::Value>(3), expected_tags);
    }
    db.execute("DELETE FROM novu_rust_workflows WHERE environment_id='env-a' AND identifier='severity-compat'", &[]).await.unwrap();
    for (transaction, override_value) in [
        ("deleted-workflow", serde_json::Value::Null),
        ("deleted-override", json!("high")),
    ] {
        let job = json!({"workflow":"severity-compat","transactionId":transaction,"content":"ok","payload":{},"recipients":["severity-user"],"severity":"low","severityOverride":override_value});
        let job_id = Uuid::new_v4();
        enqueue(&db, job_id, "env-a", in_app::JOB_KIND, &job)
            .await
            .unwrap();
        assert!(in_app::process_one(&mut second_client).await.unwrap());
        let row = db.query_one("SELECT status,(SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id='env-a' AND transaction_id=$2) FROM novu_queue_jobs WHERE id=$1", &[&job_id,&transaction]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, i64>(1), 0);
    }
}
