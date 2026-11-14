//! Two disposable DBs; the target is stopped while the alert-state DB stays available.
use std::{path::Path, process::Command};
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires two disposable Docker PostgreSQL containers"]
async fn target_outage_is_reported_using_independent_alert_database() {
    let monitor = std::env::var("TEST_DATABASE_URL").unwrap();
    let probe = std::env::var("TEST_PROBE_URL").unwrap();
    let container = std::env::var("TEST_PROBE_CONTAINER").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (service, path) in [
        ("api", root.join("../../api/rust-api/migrations")),
        ("worker", root.join("migrations")),
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(["apply", service, path.to_str().unwrap()])
            .env("DATABASE_URL", &monitor)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let (db, driver) = tokio_postgres::connect(&monitor, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('monitor','org')")
        .await
        .unwrap();
    let role = format!("db_monitor_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'db-alert-fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer),public.novu_rust_observe_alert_conditions(text,text,text,integer,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'monitor',$1,'alert_send',oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,last_started_at,last_condition_mask) VALUES ('monitor','slack','db-primary','accepted',clock_timestamp(),$1)",&[&(1i32<<20)]).await.unwrap();
    let mut role_url = reqwest::Url::parse(&monitor).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("db-alert-fixture")).unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_send-database-alert"))
            .args(["monitor", "primary", "60"])
            .env("DATABASE_URL", role_url.as_str())
            .env("SWALLOW_DB_PROBE_URL", &probe)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_DB_PROBE_TIMEOUT_SECONDS", "2")
            .env("SWALLOW_DB_ALERT_OPERATION_TIMEOUT_SECONDS", "1")
            .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
            .env("SWALLOW_DB_ALERT_STATE_FAILURE_MODE", "send-uncoordinated")
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .env_remove("SWALLOW_ALERT_EMAIL_TO")
            .env_remove("SWALLOW_DB_ALERT_SLACK_TARGET_ID")
            .env_remove("SWALLOW_DB_ALERT_EMAIL_TARGET_ID")
            .output()
            .unwrap()
    };
    let healthy = run();
    assert_eq!(
        healthy.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    assert_eq!(db.query_one("SELECT last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id='monitor' AND target_id='db-primary'",&[]).await.unwrap().get::<_,i32>(0),0);
    db.execute("UPDATE novu_rust_alert_delivery_slots SET state='rejected' WHERE environment_id='monitor' AND target_id='db-primary'",&[]).await.unwrap();
    assert!(
        Command::new("docker")
            .args(["stop", "-t", "0", &container])
            .output()
            .unwrap()
            .status
            .success()
    );
    db.execute("UPDATE novu_rust_alert_delivery_slots SET state='accepted' WHERE environment_id='monitor' AND target_id='db-primary'",&[]).await.unwrap();
    let (mut locker, driver) = tokio_postgres::connect(&monitor, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let tx = locker.transaction().await.unwrap();
    tx.query_one("SELECT target_id FROM novu_rust_alert_delivery_slots WHERE environment_id='monitor' AND target_id='db-primary' FOR UPDATE",&[]).await.unwrap();
    let started = std::time::Instant::now();
    let locked = run();
    assert_eq!(locked.status.code(), Some(1));
    assert!(started.elapsed() < std::time::Duration::from_secs(8));
    let log = String::from_utf8_lossy(&locked.stderr);
    assert!(log.contains("observation failed or timed out"));
    assert!(log.contains("dispatch failed or timed out"));
    tx.rollback().await.unwrap();
    db.execute("UPDATE novu_rust_alert_delivery_slots SET state='rejected' WHERE environment_id='monitor' AND target_id='db-primary'",&[]).await.unwrap();
    let down = run();
    assert_eq!(
        down.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&down.stderr)
    );
    let log = String::from_utf8_lossy(&down.stderr);
    assert!(log.contains("requires operator review"));
    assert!(!log.contains("db-alert-fixture") && !log.contains(&probe));
    let monitor_down = Command::new(env!("CARGO_BIN_EXE_send-database-alert"))
        .args(["monitor", "primary", "60"])
        .env("DATABASE_URL", &probe)
        .env("SWALLOW_DB_PROBE_URL", &monitor)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_DB_PROBE_TIMEOUT_SECONDS", "2")
        .env("SWALLOW_DB_ALERT_STATE_FAILURE_MODE", "report-only")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
        .env_remove("SWALLOW_ALERT_EMAIL_TO")
        .output()
        .unwrap();
    assert_eq!(monitor_down.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&monitor_down.stderr).contains("monitor database connection"));

    assert!(
        Command::new("docker")
            .args(["start", &container])
            .output()
            .unwrap()
            .status
            .success()
    );
    for _ in 0..60 {
        if Command::new("docker")
            .args(["exec", &container, "pg_isready", "-q", "-U", "postgres"])
            .output()
            .unwrap()
            .status
            .success()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let recovered = run();
    assert_eq!(
        recovered.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    db.execute(
        "DELETE FROM novu_rust_delivery_operator_grants WHERE database_principal=$1",
        &[&role],
    )
    .await
    .unwrap();
    let denied = run();
    assert_eq!(denied.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&denied.stderr).contains("observation failed"));
    db.batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .await
        .unwrap();
}
