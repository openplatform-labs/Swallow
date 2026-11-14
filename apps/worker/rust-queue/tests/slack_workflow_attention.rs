//! Tests aggregate attention, scoped acknowledgement, and alert routing without external traffic.
use novu_postgres_queue::{self as queue, alerts, reconciliation, slack_queue, slack_workflow};
use serde_json::json;
use std::process::Command;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn predispatch_failures_and_optional_missing_endpoints_are_actionable() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        slack_queue::MIGRATION,
        reconciliation::MIGRATION,
        reconciliation::OPERATOR_GRANTS_MIGRATION,
        alerts::MIGRATION,
        alerts::RESOLUTION_MIGRATION,
        reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
        slack_queue::OPERATOR_MIGRATION,
        include_str!("../migrations/0072_operator_grant_management.sql"),
        include_str!("../migrations/0073_operator_role_identity.sql"),
        slack_queue::WORKFLOW_ATTENTION_MIGRATION,
        include_str!("../migrations/0080_slack_ack_permission.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(slack_queue::WORKFLOW_ATTENTION_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(include_str!("../migrations/0080_slack_ack_permission.sql"))
        .await
        .unwrap();
    let failed = Uuid::new_v4();
    let missing = Uuid::new_v4();
    let other = Uuid::new_v4();
    let attempted = Uuid::new_v4();
    for (id, env, kind, status, error) in [
        (
            failed,
            "a",
            slack_workflow::DELIVERY_KIND,
            "failed",
            "Slack integration credentials not configured",
        ),
        (
            missing,
            "a",
            slack_workflow::FANOUT_KIND,
            "cancelled",
            "no Slack endpoints",
        ),
        (
            other,
            "b",
            slack_workflow::DELIVERY_KIND,
            "failed",
            "other error",
        ),
        (
            attempted,
            "a",
            slack_workflow::DELIVERY_KIND,
            "failed",
            "Slack outcome unknown",
        ),
    ] {
        queue::enqueue(&db, id, env, kind, &json!({"secret":"fixture-sensitive"}))
            .await
            .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status=$2,last_error=$3,attempts=1 WHERE id=$1",
            &[&id, &status, &error],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'a','unknown')",&[&attempted]).await.unwrap();
    let health = slack_queue::health(&db, "a").await.unwrap();
    assert_eq!(health["preDispatchFailures"], 1);
    assert_eq!(health["missingEndpoints"], 1);
    assert_eq!(health["unknownAttempts"], 1);
    let listed = slack_queue::list_attention(&db, "a", None).await.unwrap();
    assert_eq!(listed.len(), 3);
    for (id, category) in [
        (failed, "preDispatchFailure"),
        (missing, "missingEndpoint"),
        (attempted, "unknownAttempt"),
    ] {
        assert!(
            listed
                .iter()
                .any(|row| row["jobId"] == id.to_string() && row["category"] == category)
        );
    }
    assert!(
        !serde_json::to_string(&listed)
            .unwrap()
            .contains("fixture-sensitive")
    );
    assert_eq!(
        slack_queue::list_attention(&db, "b", None)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        slack_queue::list_attention(&db, "absent", None)
            .await
            .is_err()
    );

    assert!(
        slack_queue::acknowledge_workflow(&mut db, "a", failed, "operator", "ticket")
            .await
            .is_err()
    );
    db.execute("INSERT INTO novu_rust_operator_grant_admins(environment_id,database_principal,principal_oid) SELECT 'a',session_user,oid FROM pg_roles WHERE rolname=session_user",&[]).await.unwrap();
    let granted: bool = db.query_one("SELECT public.novu_rust_manage_operator_grant('a',session_user::text,'slack_ack','grant','fixture')",&[]).await.unwrap().get(0);
    assert!(granted);
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',session_user,'alert_send',oid FROM pg_roles WHERE rolname=session_user",&[]).await.unwrap();
    assert!(
        slack_queue::acknowledge_workflow(&mut db, "b", other, "operator", "ticket")
            .await
            .is_err()
    );
    assert!(
        slack_queue::acknowledge_workflow(&mut db, "a", attempted, "operator", "ticket")
            .await
            .is_err()
    );
    assert!(
        slack_queue::acknowledge_workflow(&mut db, "a", failed, "operator", "ticket")
            .await
            .unwrap()
    );
    assert!(
        !slack_queue::acknowledge_workflow(&mut db, "a", failed, "operator", "ticket")
            .await
            .unwrap()
    );
    assert_eq!(
        slack_queue::health(&db, "a").await.unwrap()["preDispatchFailures"],
        0
    );
    db.execute(
        "UPDATE novu_queue_jobs SET attempts=attempts+1 WHERE id=$1",
        &[&failed],
    )
    .await
    .unwrap();
    assert_eq!(
        slack_queue::health(&db, "a").await.unwrap()["preDispatchFailures"],
        1
    );
    let cli = |args: &[&str], missing_enabled: bool| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env(
                "SWALLOW_SLACK_CHECK_MISSING_ENDPOINTS",
                if missing_enabled { "true" } else { "false" },
            )
            .output()
            .unwrap()
    };
    let ack = cli(
        &[
            "slack-ack",
            "a",
            &failed.to_string(),
            "operator",
            "retry reviewed",
        ],
        false,
    );
    assert!(ack.status.success());
    db.execute(
        "DELETE FROM novu_rust_slack_attempts WHERE job_id=$1",
        &[&attempted],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='completed',last_error=NULL WHERE id=$1",
        &[&attempted],
    )
    .await
    .unwrap();
    let listed_cli = cli(&["list-slack", "a"], false);
    assert!(listed_cli.status.success());
    let listed_cli: serde_json::Value = serde_json::from_slice(&listed_cli.stdout).unwrap();
    assert_eq!(listed_cli.as_array().unwrap().len(), 1);
    assert_eq!(listed_cli[0]["jobId"], missing.to_string());
    assert_eq!(cli(&["check-slack", "a"], false).status.code(), Some(0));
    assert_eq!(cli(&["check-slack", "a"], true).status.code(), Some(2));
    db.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('a','slack','ops','rejected')",&[]).await.unwrap();
    let alert = |enabled: bool| {
        Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .args(["a", "300", "120", "60"])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "false")
            .env("SWALLOW_ALERT_SLACK_JOBS_ENABLED", "true")
            .env(
                "SWALLOW_ALERT_SLACK_MISSING_ENDPOINTS_ENABLED",
                if enabled { "true" } else { "false" },
            )
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", "ops")
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .env_remove("SWALLOW_ALERT_EMAIL_TO")
            .env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS")
            .env_remove("SWALLOW_ALERT_TWILIO_GRACE_SECONDS")
            .env_remove("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS")
            .output()
            .unwrap()
    };
    assert_eq!(alert(false).status.code(), Some(0));
    assert_eq!(alert(true).status.code(), Some(2));
    let ack = cli(
        &[
            "slack-ack",
            "a",
            &missing.to_string(),
            "operator",
            "not needed",
        ],
        true,
    );
    assert!(ack.status.success());
    assert_eq!(cli(&["check-slack", "a"], true).status.code(), Some(0));
    db.execute(
        "UPDATE novu_queue_jobs SET attempts=attempts+1 WHERE id=$1",
        &[&failed],
    )
    .await
    .unwrap();
    assert_eq!(alert(false).status.code(), Some(2));
    let race = Uuid::new_v4();
    queue::enqueue(&db, race, "a", slack_workflow::DELIVERY_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='failed',last_error='fixture failure',attempts=1 WHERE id=$1",&[&race]).await.unwrap();
    let lock = db.transaction().await.unwrap();
    lock.query_one(
        "SELECT id FROM novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&race],
    )
    .await
    .unwrap();
    let (mut actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let pid: i32 = actor
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let pending = tokio::spawn(async move {
        slack_queue::acknowledge_workflow(&mut actor, "a", race, "operator", "race check").await
    });
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let mut waiting = false;
    for _ in 0..100 {
        waiting = observer
            .query_one(
                "SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1",
                &[&pid],
            )
            .await
            .unwrap()
            .get::<_, Option<bool>>(0)
            == Some(true);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    let revocation = tokio::spawn(async move {
        observer.execute("DELETE FROM novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=session_user AND decision='slack_ack'",&[]).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!revocation.is_finished());
    lock.commit().await.unwrap();
    assert!(pending.await.unwrap().unwrap());
    assert_eq!(revocation.await.unwrap().unwrap(), 1);
    assert!(
        slack_queue::acknowledge_workflow(&mut db, "a", failed, "operator", "revoked")
            .await
            .is_err()
    );
    let records=db.query_one("SELECT count(*),bool_and(database_session_user=session_user) FROM novu_rust_slack_workflow_acknowledgements WHERE environment_id='a'",&[]).await.unwrap();
    assert_eq!(records.get::<_, i64>(0), 4);
    assert!(records.get::<_, bool>(1));
    db.execute(
        "INSERT INTO novu_rust_environments VALUES ('pagination','org')",
        &[],
    )
    .await
    .unwrap();
    for i in 1..=101u128 {
        let id = Uuid::from_u128(i);
        queue::enqueue(
            &db,
            id,
            "pagination",
            slack_workflow::DELIVERY_KIND,
            &json!({}),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status='failed',last_error='sensitive failure' WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap();
    }
    let first = slack_queue::list_attention(&db, "pagination", None)
        .await
        .unwrap();
    assert_eq!(first.len(), 100);
    let cursor = Uuid::parse_str(first[99]["jobId"].as_str().unwrap()).unwrap();
    let second = slack_queue::list_attention(&db, "pagination", Some(cursor))
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0]["jobId"], Uuid::from_u128(101).to_string());
    assert!(
        !serde_json::to_string(&first)
            .unwrap()
            .contains("sensitive failure")
    );
    let page = cli(&["list-slack", "pagination", &cursor.to_string()], false);
    assert!(page.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&page.stdout)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let text = serde_json::to_string(&slack_queue::health(&db, "a").await.unwrap()).unwrap();
    assert!(!text.contains("fixture-sensitive"));
}
