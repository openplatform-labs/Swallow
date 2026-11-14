//! Uses a disposable PostgreSQL database; sends no external messages.
use novu_postgres_queue::alerts::{self, AlertError, Reservation};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn alert_reservations_fence_concurrency_and_ambiguous_recovery() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut a, da) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let (mut b, db) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = da.await;
    });
    tokio::spawn(async move {
        let _ = db.await;
    });
    a.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env-a','org'),('env-b','org')").await.unwrap();
    a.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    a.batch_execute(novu_postgres_queue::reconciliation::MIGRATION)
        .await
        .unwrap();
    a.batch_execute(novu_postgres_queue::reconciliation::OPERATOR_GRANTS_MIGRATION)
        .await
        .unwrap();
    a.batch_execute(novu_postgres_queue::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION)
        .await
        .unwrap();
    a.batch_execute(alerts::MIGRATION).await.unwrap();
    a.batch_execute(alerts::MIGRATION).await.unwrap();
    a.batch_execute(alerts::RESOLUTION_MIGRATION).await.unwrap();
    a.batch_execute(alerts::RESOLVE_FUNCTION_MIGRATION)
        .await
        .unwrap();
    a.batch_execute(alerts::RESOLVE_FUNCTION_MIGRATION)
        .await
        .unwrap();
    a.batch_execute(include_str!(
        "../migrations/0085_alert_condition_cooldown.sql"
    ))
    .await
    .unwrap();
    a.batch_execute(alerts::RESOLUTION_MIGRATION).await.unwrap();
    assert!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
            .await
            .is_err()
    );
    a.execute("INSERT INTO novu_rust_delivery_operator_grants VALUES ('env-a',session_user,'alert_send'),('env-b',session_user,'alert_send')", &[]).await.unwrap();
    let first = match alerts::reserve_with_conditions(
        &mut a,
        "env-a",
        "slack",
        "condition-fixture",
        60,
        Some(1),
    )
    .await
    .unwrap()
    {
        Reservation::Claimed(id) => id,
        _ => panic!("claim expected"),
    };
    assert!(
        alerts::finish(
            &mut a,
            "env-a",
            "slack",
            "condition-fixture",
            first,
            &Ok(())
        )
        .await
        .unwrap()
    );
    assert_eq!(
        alerts::reserve_with_conditions(&mut a, "env-a", "slack", "condition-fixture", 60, Some(1))
            .await
            .unwrap(),
        Reservation::CoolingDown
    );
    let (left, right) = tokio::join!(
        alerts::reserve_with_conditions(&mut a, "env-a", "slack", "condition-fixture", 60, Some(3)),
        alerts::reserve_with_conditions(&mut b, "env-a", "slack", "condition-fixture", 60, Some(3))
    );
    let results = [left.unwrap(), right.unwrap()];
    let id = results
        .iter()
        .find_map(|x| {
            if let Reservation::Claimed(id) = x {
                Some(*id)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        results
            .iter()
            .filter(|x| matches!(x, Reservation::Claimed(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|x| **x == Reservation::InFlight)
            .count(),
        1
    );
    assert!(
        alerts::finish(&mut a, "env-a", "slack", "condition-fixture", id, &Ok(()))
            .await
            .unwrap()
    );
    assert_eq!(
        alerts::reserve_with_conditions(&mut a, "env-a", "slack", "condition-fixture", 60, Some(2))
            .await
            .unwrap(),
        Reservation::CoolingDown
    );
    assert_eq!(
        alerts::reserve(&mut a, "env-a", "slack", "condition-fixture", 60)
            .await
            .unwrap(),
        Reservation::CoolingDown
    );
    let next = match alerts::reserve_with_conditions(
        &mut a,
        "env-a",
        "slack",
        "condition-fixture",
        60,
        Some(4),
    )
    .await
    .unwrap()
    {
        Reservation::Claimed(id) => id,
        _ => panic!("new condition claim expected"),
    };
    assert!(
        alerts::finish(
            &mut a,
            "env-a",
            "slack",
            "condition-fixture",
            next,
            &Err(AlertError::Unknown)
        )
        .await
        .unwrap()
    );
    assert_eq!(
        alerts::reserve_with_conditions(&mut a, "env-a", "slack", "condition-fixture", 60, Some(8))
            .await
            .unwrap(),
        Reservation::Blocked
    );
    a.execute("DELETE FROM novu_rust_alert_delivery_attempts WHERE environment_id='env-a' AND target_id='condition-fixture'",&[]).await.unwrap();
    a.execute("DELETE FROM novu_rust_alert_delivery_slots WHERE environment_id='env-a' AND target_id='condition-fixture'",&[]).await.unwrap();
    let (first, second) = tokio::join!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60),
        alerts::reserve(&mut b, "env-a", "slack", "ops", 60)
    );
    let results = [first.unwrap(), second.unwrap()];
    let id = results
        .iter()
        .find_map(|x| {
            if let Reservation::Claimed(id) = x {
                Some(*id)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        results
            .iter()
            .filter(|x| matches!(x, Reservation::Claimed(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|x| **x == Reservation::InFlight)
            .count(),
        1
    );
    assert!(
        alerts::finish(&mut a, "env-a", "slack", "ops", id, &Ok(()))
            .await
            .unwrap()
    );
    assert_eq!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
            .await
            .unwrap(),
        Reservation::CoolingDown
    );
    a.execute("UPDATE novu_rust_alert_delivery_slots SET last_started_at=clock_timestamp()-interval '2 minutes' WHERE environment_id='env-a' AND channel='slack' AND target_id='ops'", &[]).await.unwrap();
    let next = match alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
        .await
        .unwrap()
    {
        Reservation::Claimed(id) => id,
        _ => panic!("expected next claim"),
    };
    a.execute("UPDATE novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id='env-a' AND channel='slack' AND target_id='ops'", &[]).await.unwrap();
    assert_eq!(alerts::health(&a, "env-a").await.unwrap()["expired"], 1);
    assert!(
        !alerts::finish(&mut b, "env-a", "slack", "ops", next, &Ok(()))
            .await
            .unwrap()
    );
    assert_eq!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
            .await
            .unwrap(),
        Reservation::Blocked
    );
    let row=a.query_one("SELECT s.state,a.state FROM novu_rust_alert_delivery_slots s JOIN novu_rust_alert_delivery_attempts a ON a.id=s.attempt_id WHERE s.environment_id='env-a' AND s.channel='slack' AND s.target_id='ops'", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(alerts::health(&a, "env-a").await.unwrap()["unknown"], 1);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["alert-status", "env-a"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()[0]["state"],
        "unknown"
    );
    assert!(
        alerts::resolve(
            &mut a,
            "env-a",
            "slack",
            "ops",
            "retry",
            "operator",
            "provider checked"
        )
        .await
        .is_err()
    );
    a.execute("INSERT INTO novu_rust_delivery_operator_grants VALUES ('env-a',session_user,'alert_retry'),('env-b',session_user,'alert_retry'),('env-b',session_user,'alert_accepted')", &[]).await.unwrap();
    assert!(
        alerts::resolve(
            &mut a,
            "env-a",
            "slack",
            "ops",
            "accepted",
            "operator",
            "provider checked"
        )
        .await
        .is_err()
    );
    let resolved = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args([
            "alert-resolve",
            "env-a",
            "slack",
            "ops",
            "retry",
            "operator",
            "provider checked",
        ])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(resolved.status.success());
    assert_eq!(alerts::health(&a, "env-a").await.unwrap()["unknown"], 0);
    let audit=a.query_one("SELECT previous_state,decision,database_session_user FROM novu_rust_alert_delivery_resolutions WHERE attempt_id=$1", &[&next]).await.unwrap();
    assert_eq!(audit.get::<_, String>(0), "unknown");
    assert_eq!(audit.get::<_, String>(1), "retry");
    assert_eq!(
        audit.get::<_, String>(2),
        a.query_one("SELECT session_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0)
    );
    assert!(matches!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
            .await
            .unwrap(),
        Reservation::Claimed(_)
    ));
    let other = match alerts::reserve(&mut a, "env-b", "email", "ops", 60)
        .await
        .unwrap()
    {
        Reservation::Claimed(id) => id,
        _ => panic!("expected isolated claim"),
    };
    assert!(
        alerts::finish(
            &mut a,
            "env-b",
            "email",
            "ops",
            other,
            &Err(AlertError::Rejected(429))
        )
        .await
        .unwrap()
    );
    assert_eq!(
        alerts::reserve(&mut a, "env-b", "email", "ops", 60)
            .await
            .unwrap(),
        Reservation::Blocked
    );
    assert!(
        alerts::resolve(
            &mut a,
            "env-b",
            "email",
            "ops",
            "accepted",
            "operator",
            "provider checked"
        )
        .await
        .is_err()
    );
    alerts::resolve(
        &mut a,
        "env-b",
        "email",
        "ops",
        "retry",
        "operator",
        "provider checked",
    )
    .await
    .unwrap();
    assert!(matches!(
        alerts::reserve(&mut a, "env-b", "email", "ops", 60)
            .await
            .unwrap(),
        Reservation::Claimed(_)
    ));
    assert!(
        alerts::reserve(&mut a, "missing", "email", "ops", 60)
            .await
            .is_err()
    );
    assert!(
        alerts::reserve(&mut a, "env-a", "email", "not safe!", 60)
            .await
            .is_err()
    );
    // Exercise the actual alert command without contacting Slack: a deliberately
    // blocked slot proves unhealthy checks reached the durable dispatch path.
    for sql in [
        novu_postgres_queue::sendgrid_queue::MIGRATION,
        novu_postgres_queue::twilio_queue::MIGRATION,
        novu_postgres_queue::fcm_queue::MIGRATION,
        novu_postgres_queue::resend_queue::MIGRATION,
    ] {
        a.batch_execute(sql).await.unwrap();
    }
    a.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('env-a','slack','check','rejected')", &[]).await.unwrap();
    let job = uuid::Uuid::new_v4();
    novu_postgres_queue::enqueue(&a, job, "env-a", "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    a.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1", &[&job]).await.unwrap();
    a.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,'env-a','unknown')", &[&job]).await.unwrap();
    let run = |env: &str, queue: Option<&str>, attempts: &str, resend: &str| {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"));
        cmd.args([env, "300", "120", "60"])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .env_remove("SWALLOW_ALERT_EMAIL_TO")
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", "check")
            .env("SWALLOW_ALERT_RESEND_ENABLED", resend)
            .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", attempts);
        if let Some(value) = queue {
            cmd.env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", value);
        } else {
            cmd.env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS");
        }
        cmd.output().unwrap()
    };
    assert_eq!(
        run("env-a", Some("60"), "false", "false").status.code(),
        Some(2)
    );
    assert_eq!(run("env-a", None, "true", "false").status.code(), Some(2));
    assert_eq!(
        run("env-b", Some("60"), "true", "false").status.code(),
        Some(0)
    );
    assert_eq!(run("env-a", None, "false", "false").status.code(), Some(1));
    assert_eq!(
        run("env-a", Some("0"), "false", "false").status.code(),
        Some(1)
    );
    assert_eq!(run("env-a", None, "yes", "false").status.code(), Some(1));
    for value in ["0", "301", "invalid"] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .args(["env-a", "300", "120", "60"])
            .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", value)
            .output()
            .unwrap();
        assert!(!output.status.success());
    }
    // One blocked check must time out and still reach the independent durable
    // alert path rather than leaving a scheduled process hung indefinitely.
    let locked = b.transaction().await.unwrap();
    locked
        .batch_execute("LOCK TABLE novu_queue_jobs IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .args(["env-a", "300", "120", "60"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
        .env_remove("SWALLOW_ALERT_EMAIL_TO")
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "check")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "false")
        .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
        .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let finished = tokio::time::timeout(std::time::Duration::from_secs(8), async {
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    if finished.is_err() {
        let _ = child.kill();
    }
    let output = child.wait_with_output().unwrap();
    locked.commit().await.unwrap();
    assert!(finished.is_ok(), "alert command did not time out");
    assert_eq!(output.status.code(), Some(1));
    let log = String::from_utf8_lossy(&output.stderr);
    assert!(log.contains("queue health check failed"), "{log}");
    assert!(
        log.contains("Slack alert requires operator review"),
        "{log}"
    );
    assert_eq!(
        run("missing", Some("60"), "false", "false").status.code(),
        Some(1)
    );
    // Resend projection tables are absent: this check fails, but the queue
    // result still reaches the blocked Slack slot and the process exits 1.
    let partial = run("env-a", Some("60"), "false", "true");
    assert_eq!(partial.status.code(), Some(1));
    let log = String::from_utf8(partial.stderr).unwrap();
    assert!(log.contains("Resend health check failed"));
    assert!(log.contains("Slack alert requires operator review"));
    // A failed SendGrid check must report a command failure even when the
    // durable alert is blocked for review, allowing an external scheduler to
    // distinguish a broken check from a delivery requiring review.
    let check_sendgrid_failure = |target: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .args(["env-a", "300", "120", "60"])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .env_remove("SWALLOW_ALERT_EMAIL_TO")
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", target)
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "false")
            .env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS")
            .env("SWALLOW_ALERT_SENDGRID_GRACE_SECONDS", "300")
            .env("SWALLOW_ALERT_SENDGRID_HEARTBEAT_MAX_SECONDS", "120")
            .output()
            .unwrap()
    };
    let sendgrid_failure = check_sendgrid_failure("check");
    assert_eq!(sendgrid_failure.status.code(), Some(1));
    let log = String::from_utf8(sendgrid_failure.stderr).unwrap();
    assert!(log.contains("SendGrid health check failed"), "{log}");
    assert!(
        log.contains("Slack alert requires operator review"),
        "{log}"
    );
    a.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,last_started_at) VALUES ('env-a','slack','cooling-sendgrid','accepted',clock_timestamp())", &[]).await.unwrap();
    let cooled = check_sendgrid_failure("cooling-sendgrid");
    assert_eq!(cooled.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&cooled.stderr).contains("SendGrid health check failed"));
    assert!(String::from_utf8_lossy(&cooled.stdout).contains("CoolingDown"));
    // A failed attempts check likewise must not suppress the queue result.
    a.batch_execute("DROP TABLE novu_rust_fcm_attempts")
        .await
        .unwrap();
    let partial = run("env-a", Some("60"), "true", "false");
    assert_eq!(partial.status.code(), Some(1));
    let log = String::from_utf8(partial.stderr).unwrap();
    assert!(log.contains("delivery attempts health check failed"));
    assert!(log.contains("Slack alert requires operator review"));
    a.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('env-a','email','check','rejected')", &[]).await.unwrap();
    let both = |environment: &str, slack_url: &str, slack_id: &str, email_key: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .args([environment, "300", "120", "60"])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_ATTEMPTS_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env("SWALLOW_ALERT_SLACK_WEBHOOK", slack_url)
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", slack_id)
            .env("SWALLOW_ALERT_EMAIL_TO", "ops@example.test")
            .env("SWALLOW_ALERT_EMAIL_TARGET_ID", "check")
            .env("NOVU_RESEND_API_KEY", email_key)
            .env_remove("NOVU_RESEND_API_KEY_FILE")
            .env("NOVU_RESEND_FROM", "from@example.test")
            .output()
            .unwrap()
    };
    let output = both(
        "env-a",
        "https://bad.invalid/secret-marker",
        "check",
        "fixture-key",
    );
    assert_eq!(output.status.code(), Some(1));
    let log = String::from_utf8(output.stderr).unwrap();
    assert!(log.contains("Slack alert configuration failed"));
    assert!(log.contains("email alert requires operator review"));
    assert!(!log.contains("secret-marker") && !log.contains("fixture-key"));
    let output = both(
        "env-a",
        "https://hooks.slack.com/services/A/B/C",
        "bad id",
        "fixture-key",
    );
    assert_eq!(output.status.code(), Some(1));
    let log = String::from_utf8(output.stderr).unwrap();
    assert!(log.contains("Slack alert dispatch failed"));
    assert!(log.contains("email alert requires operator review"));
    let output = both(
        "env-a",
        "https://hooks.slack.com/services/A/B/C",
        "check",
        "",
    );
    assert_eq!(output.status.code(), Some(1));
    let log = String::from_utf8(output.stderr).unwrap();
    assert!(log.contains("email alert configuration failed"));
    assert!(log.contains("Slack alert requires operator review"));
    assert_eq!(
        both(
            "env-b",
            "https://bad.invalid/secret-marker",
            "check",
            "fixture-key"
        )
        .status
        .code(),
        Some(1)
    );
}
