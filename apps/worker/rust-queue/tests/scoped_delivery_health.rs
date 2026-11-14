//! Scoped health checks against the full API and worker migration set.
use novu_postgres_queue::{self as queue, reconciliation};
use serde_json::json;
use std::{path::Path, process::Command};
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn scoped_checks_match_legacy_and_isolate_database_login() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    for (service, path) in [
        ("api", root.join("../../api/rust-api/migrations")),
        ("worker", root.join("migrations")),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(["apply", service, path.to_str().unwrap()])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')")
        .await
        .unwrap();
    let job = Uuid::new_v4();
    queue::enqueue(
        &db,
        job,
        "a",
        queue::resend_queue::JOB_KIND,
        &json!({"private":"hidden"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '1 hour' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,'a','unknown')",&[&job]).await.unwrap();
    let sms = Uuid::new_v4();
    queue::enqueue(&db, sms, "a", queue::twilio_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '1 hour' WHERE id=$1",
        &[&sms],
    )
    .await
    .unwrap();
    let role = format!("monitor_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'scoped-fixture-password'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_delivery_health(text),public.novu_rust_queue_health(text,integer),public.novu_rust_provider_health(text,text,integer,integer),public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer),public.novu_rust_failed_queue_count(text),public.novu_rust_failed_queue_list(text,uuid),public.novu_rust_ack_failed_queue(text,uuid,text,text) TO {role}")).await.unwrap();
    let (actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(reconciliation::scoped_health(&actor, "a").await.is_err());
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'alert_view',oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    assert_eq!(
        reconciliation::scoped_health(&actor, "a").await.unwrap(),
        reconciliation::health(&db, "a").await.unwrap()
    );
    assert_eq!(
        reconciliation::scoped_queue_health(&actor, "a", 300)
            .await
            .unwrap(),
        reconciliation::queue_health(&db, "a", 300).await.unwrap()
    );
    assert_eq!(
        reconciliation::scoped_provider_health(&actor, "a", "resend", 300, 120)
            .await
            .unwrap(),
        reconciliation::resend_health(&db, "a", 300, 120)
            .await
            .unwrap()
    );
    assert_eq!(
        reconciliation::scoped_provider_health(&actor, "a", "twilio", 300, 120)
            .await
            .unwrap(),
        reconciliation::twilio_health(&db, "a", 300, 120)
            .await
            .unwrap()
    );
    assert!(reconciliation::scoped_health(&actor, "b").await.is_err());
    assert!(
        reconciliation::scoped_queue_health(&actor, "b", 300)
            .await
            .is_err()
    );
    assert!(
        reconciliation::scoped_provider_health(&actor, "b", "resend", 300, 120)
            .await
            .is_err()
    );
    assert!(
        reconciliation::scoped_provider_health(&actor, "a", "unknown", 300, 120)
            .await
            .is_err()
    );
    assert!(
        reconciliation::scoped_queue_health(&actor, "a", 0)
            .await
            .is_err()
    );
    assert!(
        actor
            .query("SELECT payload FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',session_user,decision,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) decision WHERE rolname=session_user",&[]).await.unwrap();
    for args in [
        vec!["check", "a"],
        vec!["check-queue", "a", "300"],
        vec!["check-resend", "a", "300", "120"],
        vec!["check-twilio", "a", "300", "120"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_MONITOR_SCOPED_ACCESS", "true")
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("hidden"));
    }
    db.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('a','slack','ops','rejected')",&[]).await.unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'alert_send',oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url
        .set_password(Some("scoped-fixture-password"))
        .unwrap();
    let scoped_monitor = |environment: &str| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(["check", environment])
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_MONITOR_SCOPED_ACCESS", "true")
            .output()
            .unwrap()
    };
    assert_eq!(scoped_monitor("a").status.code(), Some(2));
    assert_eq!(scoped_monitor("b").status.code(), Some(1));
    let run_limited_alert = || {
        Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .args(["a", "300", "120", "60"])
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_CHECK_SCOPED_ACCESS", "true")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
            .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "true")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "300")
            .env("SWALLOW_ALERT_TWILIO_GRACE_SECONDS", "300")
            .env("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS", "120")
            .env("SWALLOW_ALERT_SLACK_JOBS_ENABLED", "false")
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", "ops")
            .env_remove("SWALLOW_ALERT_EMAIL_TO")
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .output()
            .unwrap()
    };
    let limited = run_limited_alert();
    assert_eq!(
        limited.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&limited.stderr)
    );

    let alert = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .args(["a", "300", "120", "60"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_CHECK_SCOPED_ACCESS", "true")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "true")
        .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "300")
        .env("SWALLOW_ALERT_TWILIO_GRACE_SECONDS", "300")
        .env("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS", "120")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "ops")
        .env_remove("SWALLOW_ALERT_EMAIL_TO")
        .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
        .output()
        .unwrap();
    assert_eq!(
        alert.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&alert.stderr)
    );
    let failed = Uuid::new_v4();
    queue::enqueue(
        &db,
        failed,
        "a",
        queue::fcm_queue::JOB_KIND,
        &json!({"secret":"fixture-private"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed',last_error='sensitive diagnostic' WHERE id=$1",
        &[&failed],
    )
    .await
    .unwrap();
    assert_eq!(
        reconciliation::failed_queue_count(&actor, "a")
            .await
            .unwrap(),
        1
    );
    assert!(
        reconciliation::failed_queue_count(&actor, "b")
            .await
            .is_err()
    );
    let listed = reconciliation::failed_queue_list(&actor, "a", None)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    let result = serde_json::to_string(&listed).unwrap();
    assert!(!result.contains("fixture-private") && !result.contains("sensitive diagnostic"));
    assert!(
        reconciliation::acknowledge_failed_queue(&actor, "a", failed, "operator", "reviewed")
            .await
            .is_err()
    );
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'queue_ack',oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let failure_cli = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert_eq!(
        failure_cli(&["check-failed-jobs", "a"]).status.code(),
        Some(2)
    );
    assert_eq!(
        failure_cli(&["list-failed-jobs", "b"]).status.code(),
        Some(1)
    );
    let failure_alert = || {
        Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .args(["a", "300", "120", "60"])
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_FAILED_JOBS_ENABLED", "true")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
            .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "false")
            .env("SWALLOW_ALERT_SLACK_JOBS_ENABLED", "false")
            .env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS")
            .env_remove("SWALLOW_ALERT_TWILIO_GRACE_SECONDS")
            .env_remove("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS")
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", "ops")
            .env_remove("SWALLOW_ALERT_EMAIL_TO")
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .output()
            .unwrap()
    };
    assert_eq!(failure_alert().status.code(), Some(2));
    assert!(
        failure_cli(&[
            "ack-failed-job",
            "a",
            &failed.to_string(),
            "operator",
            "reviewed"
        ])
        .status
        .success()
    );
    assert!(
        !reconciliation::acknowledge_failed_queue(&actor, "a", failed, "operator", "reviewed")
            .await
            .unwrap()
    );
    assert_eq!(
        failure_cli(&["check-failed-jobs", "a"]).status.code(),
        Some(0)
    );
    assert_eq!(failure_alert().status.code(), Some(0));
    assert_eq!(
        db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&failed])
            .await
            .unwrap()
            .get::<_, String>(0),
        "failed"
    );
    db.execute(
        "UPDATE novu_queue_jobs SET attempts=attempts+1 WHERE id=$1",
        &[&failed],
    )
    .await
    .unwrap();
    assert_eq!(failure_alert().status.code(), Some(2));
    db.execute("INSERT INTO novu_queue_jobs(id,environment_id,kind,payload,status) SELECT gen_random_uuid(),'a','pagination_fixture','{}'::jsonb,'failed' FROM generate_series(1,101)",&[]).await.unwrap();
    let page = reconciliation::failed_queue_list(&actor, "a", None)
        .await
        .unwrap();
    assert_eq!(page.len(), 100);
    let cursor = Uuid::parse_str(page[99]["jobId"].as_str().unwrap()).unwrap();
    assert_eq!(
        reconciliation::failed_queue_list(&actor, "a", Some(cursor))
            .await
            .unwrap()
            .len(),
        2
    );
    db.execute(
        "DELETE FROM novu_queue_jobs WHERE environment_id='a' AND kind='pagination_fixture'",
        &[],
    )
    .await
    .unwrap();
    let row=db.query_one("SELECT database_session_user FROM novu_rust_queue_failure_acknowledgements WHERE job_id=$1",&[&failed]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), role);
    db.execute("DELETE FROM novu_rust_delivery_operator_grants WHERE database_principal=$1 AND decision='alert_view'",&[&role]).await.unwrap();
    assert_eq!(scoped_monitor("a").status.code(), Some(1));
    let denied = run_limited_alert();
    assert_eq!(denied.status.code(), Some(1));
    let text = String::from_utf8_lossy(&denied.stderr);
    assert!(text.contains("health check failed"));
    db.execute(
        "DELETE FROM novu_rust_delivery_operator_grants WHERE database_principal=$1",
        &[&role],
    )
    .await
    .unwrap();
    assert!(reconciliation::scoped_health(&actor, "a").await.is_err());
    actor
        .batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    db.batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .await
        .unwrap();
}
