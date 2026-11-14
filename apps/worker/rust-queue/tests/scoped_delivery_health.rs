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
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let api_dir = root.join("../../api/rust-api/migrations");
    let initial = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
        .args(["apply", "api", api_dir.to_str().unwrap(), "--through", "17"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        initial.status.success(),
        "{}",
        String::from_utf8_lossy(&initial.stderr)
    );
    for (service, path) in [
        ("worker", root.join("migrations")),
        ("api", root.join("../../api/rust-api/migrations")),
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
    let sendgrid = Uuid::new_v4();
    queue::enqueue(
        &db,
        sendgrid,
        "a",
        queue::sendgrid_queue::JOB_KIND,
        &json!({}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '1 hour' WHERE id=$1",
        &[&sendgrid],
    )
    .await
    .unwrap();
    let dispatch = Uuid::new_v4();
    db.execute(
        "INSERT INTO novu_rust_sendgrid_dispatch_ids(id,job_id,environment_id) VALUES ($1,$2,'a')",
        &[&dispatch, &sendgrid],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,'a','accepted',$2)", &[&sendgrid,&dispatch]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_recipients(dispatch_id,environment_id,recipient_kind,recipient_index,recipient_hash,delivery_result) VALUES ($1,'a','to',0,repeat('a',64),'delivered'),($1,'a','cc',0,repeat('b',64),'pending')", &[&dispatch]).await.unwrap();
    let push = Uuid::new_v4();
    queue::enqueue(
        &db,
        push,
        "a",
        queue::fcm_queue::JOB_KIND,
        &json!({"private":"payload-secret"}),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='completed' WHERE id=$1",
        &[&push],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) SELECT $1,'a',n,'unknown' FROM generate_series(0,103) n", &[&push]).await.unwrap();
    let role = format!("monitor_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'scoped-fixture-password'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_delivery_health(text),public.novu_rust_queue_health(text,integer),public.novu_rust_provider_health(text,text,integer,integer),public.novu_rust_sendgrid_health(text,integer,integer),public.novu_rust_sendgrid_result(text,uuid),public.novu_rust_list_unknown_attempts(text,uuid,integer),public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer),public.novu_rust_failed_queue_count(text),public.novu_rust_failed_queue_list(text,uuid),public.novu_rust_ack_failed_queue(text,uuid,text,text),public.novu_rust_observe_alert_conditions(text,text,text,integer,integer) TO {role}")).await.unwrap();
    let (actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(reconciliation::scoped_health(&actor, "a").await.is_err());
    assert!(
        reconciliation::scoped_list_after(&actor, "a", None)
            .await
            .is_err()
    );
    assert!(
        reconciliation::scoped_sendgrid_result(&actor, "a", sendgrid)
            .await
            .is_err()
    );
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'alert_view',oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.batch_execute(&format!(
        "REVOKE EXECUTE ON FUNCTION public.novu_rust_sendgrid_result(text,uuid) FROM {role}"
    ))
    .await
    .unwrap();
    assert!(
        reconciliation::scoped_sendgrid_result(&actor, "a", sendgrid)
            .await
            .is_err()
    );
    db.batch_execute(&format!(
        "GRANT EXECUTE ON FUNCTION public.novu_rust_sendgrid_result(text,uuid) TO {role}"
    ))
    .await
    .unwrap();
    db.batch_execute(&format!("REVOKE EXECUTE ON FUNCTION public.novu_rust_list_unknown_attempts(text,uuid,integer) FROM {role}")).await.unwrap();
    assert!(
        reconciliation::scoped_list_after(&actor, "a", None)
            .await
            .is_err()
    );
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_list_unknown_attempts(text,uuid,integer) TO {role}")).await.unwrap();
    assert!(
        actor
            .query(
                "SELECT * FROM public.novu_rust_list_unknown_attempts('a',NULL,-2)",
                &[]
            )
            .await
            .is_err()
    );
    let first = reconciliation::scoped_list_after(&actor, "a", None)
        .await
        .unwrap();
    assert_eq!(
        first,
        reconciliation::list_after(&db, "a", None).await.unwrap()
    );
    assert_eq!(first.len(), 100);
    assert!(
        !serde_json::to_string(&first)
            .unwrap()
            .contains("payload-secret")
    );
    let last = first.last().unwrap();
    let cursor = (
        Uuid::parse_str(last["jobId"].as_str().unwrap()).unwrap(),
        last["targetIndex"].as_i64().map(|v| v as i32),
    );
    let second = reconciliation::scoped_list_after(&actor, "a", Some(cursor))
        .await
        .unwrap();
    assert_eq!(
        second,
        reconciliation::list_after(&db, "a", Some(cursor))
            .await
            .unwrap()
    );
    assert_eq!(second.len(), 5);
    assert!(
        reconciliation::scoped_list_after(&actor, "b", None)
            .await
            .is_err()
    );
    assert!(
        reconciliation::scoped_list_after(&actor, "a", Some((push, Some(-1))))
            .await
            .is_err()
    );
    assert!(
        actor
            .query("SELECT http_status FROM public.novu_rust_fcm_attempts", &[])
            .await
            .is_err()
    );
    let result = reconciliation::scoped_sendgrid_result(&actor, "a", sendgrid)
        .await
        .unwrap();
    assert_eq!(result["attemptState"], "accepted");
    assert_eq!(result["dispatchId"], dispatch.to_string());
    assert_eq!(
        result["recipients"],
        json!([{"kind":"cc","index":0,"result":"pending"},{"kind":"to","index":0,"result":"delivered"}])
    );
    assert!(!result.to_string().contains(&"a".repeat(64)));
    assert!(
        reconciliation::scoped_sendgrid_result(&actor, "b", sendgrid)
            .await
            .is_err()
    );
    assert!(
        reconciliation::scoped_sendgrid_result(&actor, "a", sms)
            .await
            .is_err()
    );
    assert!(
        actor
            .query(
                "SELECT recipient_hash FROM public.novu_rust_sendgrid_recipients",
                &[]
            )
            .await
            .is_err()
    );

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
    let sg = reconciliation::scoped_sendgrid_health(&actor, "a", 300, 120)
        .await
        .unwrap();
    assert_eq!(
        sg,
        reconciliation::sendgrid_health(&db, "a", 300, 120)
            .await
            .unwrap()
    );
    assert_eq!(sg["overdueUndelivered"], 1);
    assert_eq!(sg["projectorStale"], true);
    assert!(
        reconciliation::scoped_sendgrid_health(&actor, "b", 300, 120)
            .await
            .is_err()
    );
    assert!(
        reconciliation::scoped_sendgrid_health(&actor, "a", 0, 120)
            .await
            .is_err()
    );
    db.execute(
        "UPDATE novu_queue_jobs SET sendgrid_delivery_result='delivered' WHERE id=$1",
        &[&sendgrid],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO novu_rust_sendgrid_projection_heartbeat VALUES ('a',clock_timestamp())",
        &[],
    )
    .await
    .unwrap();
    let healthy = reconciliation::scoped_sendgrid_health(&actor, "a", 300, 120)
        .await
        .unwrap();
    assert_eq!(healthy["overdueUndelivered"], 0);
    assert_eq!(healthy["projectorStale"], false);
    db.execute(
        "UPDATE novu_queue_jobs SET sendgrid_delivery_result='unknown' WHERE id=$1",
        &[&sendgrid],
    )
    .await
    .unwrap();
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
        vec!["check-sendgrid", "a", "300", "120"],
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
    let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["sendgrid-result", "a", &sendgrid.to_string()])
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output_json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output_json["jobId"], sendgrid.to_string());
    assert_eq!(output_json["recipients"].as_array().unwrap().len(), 2);
    let list_output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["list", "a"])
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_MONITOR_SCOPED_ACCESS", "true")
        .output()
        .unwrap();
    assert!(
        list_output.status.success(),
        "{}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let list_json: serde_json::Value = serde_json::from_slice(&list_output.stdout).unwrap();
    assert_eq!(list_json.as_array().unwrap().len(), 100);
    assert!(!String::from_utf8_lossy(&list_output.stdout).contains("payload-secret"));
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
            .env("SWALLOW_ALERT_SENDGRID_GRACE_SECONDS", "300")
            .env("SWALLOW_ALERT_SENDGRID_HEARTBEAT_MAX_SECONDS", "120")
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
        &json!({"workflow_context":{"subscriber_id":"missing","subscriber_internal_id":"old-subscriber","workflow_identifier":"fixture","workflow_internal_id":null,"title_template":"hello","content_template":"hi","payload":{"secret":"fixture-private"},"context_keys":[],"actor":null}}),
    )
    .await
    .unwrap();
    assert!(
        queue::fcm_queue::process_one_with_integrations(
            &mut db,
            "a",
            &std::collections::HashMap::new()
        )
        .await
        .unwrap()
    );
    let failure=db.query_one("SELECT status,last_error,(SELECT count(*) FROM novu_rust_fcm_attempts WHERE job_id=$1) FROM novu_queue_jobs WHERE id=$1",&[&failed]).await.unwrap();
    assert_eq!(failure.get::<_, String>(0), "failed");
    assert_eq!(failure.get::<_, String>(1), "subscriber not found");
    assert_eq!(failure.get::<_, i64>(2), 0);
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
    assert!(!result.contains("fixture-private") && !result.contains("subscriber not found"));
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
    db.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,last_started_at,last_condition_mask) VALUES ('a','slack','recovery','accepted',clock_timestamp(),$1)",&[&(1i32<<18)]).await.unwrap();
    let recovery = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .args(["a", "300", "120", "60"])
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_FAILED_JOBS_ENABLED", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
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
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "recovery")
        .env_remove("SWALLOW_ALERT_EMAIL_TO")
        .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
        .output()
        .unwrap();
    assert_eq!(
        recovery.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&recovery.stderr)
    );
    assert_eq!(db.query_one("SELECT last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id='a' AND target_id='recovery'",&[]).await.unwrap().get::<_,i32>(0),0);
    assert!(
        queue::alerts::observe_conditions(&actor, "b", "slack", "recovery", 0, 1 << 18)
            .await
            .is_err()
    );
    assert_eq!(failure_alert().status.code(), Some(0));
    assert_eq!(
        db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&failed])
            .await
            .unwrap()
            .get::<_, String>(0),
        "failed"
    );
    // Repeating the same failure after a state transition must invalidate its acknowledgement.
    db.execute(
        "UPDATE novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&failed],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&failed],
    )
    .await
    .unwrap();
    assert_eq!(failure_alert().status.code(), Some(2));
    assert!(
        reconciliation::acknowledge_failed_queue(&actor, "a", failed, "operator", "recurred")
            .await
            .unwrap()
    );
    assert_eq!(failure_alert().status.code(), Some(0));
    db.execute(
        "UPDATE novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&failed],
    )
    .await
    .unwrap();
    assert!(
        queue::fcm_queue::process_one_with_integrations(
            &mut db,
            "a",
            &std::collections::HashMap::new()
        )
        .await
        .unwrap()
    );
    assert_eq!(failure_alert().status.code(), Some(2));
    let reservation =
        queue::alerts::reserve_scoped(&actor, "a", "slack", "recovery", 60, Some(1 << 18))
            .await
            .unwrap();
    let id = match reservation {
        queue::alerts::Reservation::Claimed(id) => id,
        _ => panic!("recurring incident should bypass cooldown"),
    };
    assert!(
        queue::alerts::finish_scoped(&actor, "a", "slack", "recovery", id, &Ok(()))
            .await
            .unwrap()
    );
    assert_eq!(
        queue::alerts::reserve_scoped(&actor, "a", "slack", "recovery", 60, Some(1 << 18))
            .await
            .unwrap(),
        queue::alerts::Reservation::CoolingDown
    );
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
    let row=db.query_one("SELECT database_session_user FROM novu_rust_queue_failure_acknowledgements WHERE job_id=$1 ORDER BY created_at LIMIT 1",&[&failed]).await.unwrap();
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
