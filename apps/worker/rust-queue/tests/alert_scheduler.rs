//! Exercise the real scheduler with a controlled fanout executable.
#![cfg(unix)]
use std::{
    os::unix::fs::PermissionsExt,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;
#[test]
fn fixed_delay_reports_outcomes_and_stops_without_overlapping() {
    let dir = std::env::temp_dir().join(format!("swallow-scheduler-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let scheduler = dir.join("alert-scheduler");
    std::fs::copy(env!("CARGO_BIN_EXE_alert-scheduler"), &scheduler).unwrap();
    let fanout = dir.join("alert-fanout");
    std::fs::write(&fanout, "#!/bin/sh\ntest \"$1\" = delivery || exit 9\necho start >> \"$SCHEDULER_TEST_LOG\"\nsleep 2\necho end >> \"$SCHEDULER_TEST_LOG\"\nexit 2\n").unwrap();
    std::fs::set_permissions(&fanout, std::fs::Permissions::from_mode(0o700)).unwrap();
    let log = dir.join("log");
    let output = dir.join("output");
    let mut child = Command::new(&scheduler)
        .args(["1", "delivery", "env", "1", "2", "3", "config"])
        .env("SCHEDULER_TEST_LOG", &log)
        .env("SWALLOW_ALERT_SCHEDULER_FAILURE_MODE", "continue")
        .env("SWALLOW_ALERT_SCHEDULER_SHUTDOWN_GRACE_SECONDS", "10")
        .stdout(Stdio::null())
        .stderr(Stdio::from(std::fs::File::create(&output).unwrap()))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(12);
    while std::fs::read_to_string(&log)
        .unwrap_or_default()
        .matches("start")
        .count()
        < 2
    {
        assert!(Instant::now() < deadline, "scheduler did not run twice");
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(child.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(&log).unwrap(),
        "start\nend\nstart\nend\n"
    );
    assert!(
        std::fs::read_to_string(output)
            .unwrap()
            .contains("attention required")
    );
    let invalid = Command::new(&scheduler)
        .args(["0", "delivery", "env", "1", "2", "3", "config"])
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    std::fs::write(
        &fanout,
        "#!/bin/sh\ntest \"$1\" = database && test \"$3\" = probe || exit 9\necho database-routed >&2\nexit 1\n",
    )
    .unwrap();
    let database = Command::new(&scheduler)
        .args(["1", "database", "env", "probe", "60", "config"])
        .env("SWALLOW_ALERT_SCHEDULER_FAILURE_MODE", "exit")
        .output()
        .unwrap();
    assert!(!database.status.success());
    assert!(String::from_utf8_lossy(&database.stderr).contains("database-routed"));
    assert!(String::from_utf8_lossy(&database.stderr).contains("run failed"));
    std::fs::remove_file(fanout).unwrap();
    let failure = Command::new(&scheduler)
        .args(["1", "delivery", "env", "1", "2", "3", "config"])
        .env("SWALLOW_ALERT_SCHEDULER_FAILURE_MODE", "exit")
        .output()
        .unwrap();
    assert!(!failure.status.success());
    assert!(String::from_utf8_lossy(&failure.stderr).contains("run failed"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn schedules_real_fanout_with_blocked_destination() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("sched-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user", &[&env]).await.unwrap();
    db.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ($1,'slack','ops','rejected')", &[&env]).await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&db, job, &env, "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1", &[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-scheduler-db-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let webhook = dir.join("webhook");
    let binding = dir.join("binding");
    let config = dir.join("config.json");
    let output = dir.join("output");
    std::fs::write(&webhook, "https://hooks.slack.com/services/A/B/C").unwrap();
    std::fs::write(&binding, "fixture-binding-key-0123456789abcdef0123456789").unwrap();
    std::fs::write(
        &config,
        serde_json::to_vec(
            &serde_json::json!({"targets":[{"channel":"slack","id":"ops","webhook_file":webhook}]}),
        )
        .unwrap(),
    )
    .unwrap();
    let file = std::fs::File::create(&output).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_alert-scheduler"))
        .args(["1", "delivery", &env, "300", "120", "60"])
        .arg(&config)
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_BINDING_KEY_FILE", &binding)
        .env_remove("SWALLOW_ALERT_BINDING_KEY")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
        .env("SWALLOW_ALERT_SCHEDULER_SHUTDOWN_GRACE_SECONDS", "10")
        .stdout(Stdio::from(file.try_clone().unwrap()))
        .stderr(Stdio::from(file))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if std::fs::read_to_string(&output)
            .unwrap_or_default()
            .matches("alert scheduler: attention required")
            .count()
            >= 2
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "real fanout failed: {}",
            std::fs::read_to_string(&output).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(child.wait().unwrap().success());
    let text = std::fs::read_to_string(&output).unwrap();
    assert!(text.contains("slack ops: attention"));
    assert!(!text.contains("hooks.slack.com"));
    let state: String=db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='ops'", &[&env]).await.unwrap().get(0);
    assert_eq!(state, "rejected");
    std::fs::remove_dir_all(dir).unwrap();
}
