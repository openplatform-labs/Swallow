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

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn two_schedulers_do_not_retry_an_unknown_slack_attempt() {
    use std::{
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("sched-ha-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user", &[&env]).await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&db, job, &env, "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1", &[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-scheduler-ha-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let webhook = dir.join("webhook");
    let config = dir.join("config.json");
    std::fs::write(&webhook, "https://hooks.slack.com/services/A/B/C").unwrap();
    std::fs::write(
        &config,
        serde_json::to_vec(
            &serde_json::json!({"targets":[{"channel":"slack","id":"ops","webhook_file":webhook}]}),
        )
        .unwrap(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let proxy_thread = {
        let count = count.clone();
        let done = done.clone();
        thread::spawn(move || {
            while !done.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        count.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(1200));
                        drop(socket);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10))
                    }
                    Err(e) => panic!("proxy: {e}"),
                }
            }
        })
    };
    let mut children = Vec::new();
    let mut logs = Vec::new();
    for i in 0..2 {
        let log = dir.join(format!("out-{i}"));
        let file = std::fs::File::create(&log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_alert-scheduler"))
            .args(["1", "delivery", &env, "300", "120", "60"])
            .arg(&config)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "4")
            .env("SWALLOW_ALERT_SCHEDULER_SHUTDOWN_GRACE_SECONDS", "10")
            .env("SWALLOW_ALERT_SCHEDULER_FAILURE_MODE", "continue")
            .env("HTTPS_PROXY", &proxy)
            .env("https_proxy", &proxy)
            .env("NO_PROXY", "")
            .env("no_proxy", "")
            .env_remove("SWALLOW_ALERT_CHECK_SCOPED_ACCESS")
            .env_remove("SWALLOW_ALERT_SCOPED_DELIVERY")
            .env_remove("SWALLOW_ALERT_COOLDOWN_POLICY")
            .env_remove("SWALLOW_ALERT_BINDING_KEY")
            .env_remove("SWALLOW_ALERT_BINDING_KEY_FILE")
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap();
        children.push(child);
        logs.push(log);
    }
    let deadline = Instant::now() + Duration::from_secs(25);
    let success = loop {
        let state: Option<String>=db.query_opt("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='ops'",&[&env]).await.unwrap().map(|r|r.get(0));
        if state.as_deref() == Some("unknown")
            && logs.iter().all(|p| {
                std::fs::read_to_string(p)
                    .unwrap_or_default()
                    .matches("alert scheduler: attention required")
                    .count()
                    >= 2
            })
        {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    for child in &children {
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap();
    }
    for child in &mut children {
        assert!(child.wait().unwrap().success());
    }
    done.store(true, Ordering::SeqCst);
    proxy_thread.join().unwrap();
    assert!(
        success,
        "schedulers did not reach unknown state: {:?}",
        logs.iter()
            .map(|p| std::fs::read_to_string(p).unwrap_or_default())
            .collect::<Vec<_>>()
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let rows=db.query("SELECT state FROM novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND channel='slack' AND target_id='ops'",&[&env]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "unknown");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn forced_shutdown_kills_fanout_and_its_process_group() {
    let dir = std::env::temp_dir().join(format!("swallow-scheduler-kill-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let scheduler = dir.join("alert-scheduler");
    let fanout = dir.join("alert-fanout");
    let pids = dir.join("pids");
    std::fs::copy(env!("CARGO_BIN_EXE_alert-scheduler"), &scheduler).unwrap();
    std::fs::write(
        &fanout,
        "#!/bin/sh\nsleep 30 &\necho $$ $! > \"$SCHEDULER_TEST_PIDS\"\nwait\n",
    )
    .unwrap();
    std::fs::set_permissions(&fanout, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut scheduler = Command::new(scheduler)
        .args(["1", "delivery", "env", "1", "2", "3", "config"])
        .env("SCHEDULER_TEST_PIDS", &pids)
        .env("SWALLOW_ALERT_SCHEDULER_SHUTDOWN_GRACE_SECONDS", "1")
        .env("SWALLOW_ALERT_SCHEDULER_FAILURE_MODE", "continue")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !pids.exists() {
        assert!(Instant::now() < deadline, "fanout did not start");
        thread::sleep(Duration::from_millis(20));
    }
    let ids: Vec<u32> = std::fs::read_to_string(&pids)
        .unwrap()
        .split_whitespace()
        .map(|v| v.parse().unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(
        Command::new("kill")
            .args(["-TERM", &scheduler.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(!scheduler.wait().unwrap().success());
    let deadline = Instant::now() + Duration::from_secs(5);
    for id in ids {
        loop {
            let ps = Command::new("ps")
                .args(["-o", "stat=", "-p", &id.to_string()])
                .output()
                .unwrap();
            let state = String::from_utf8_lossy(&ps.stdout);
            if !ps.status.success() || state.trim().is_empty() || state.trim().starts_with('Z') {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "process {id} survived forced scheduler shutdown ({state})"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn forced_scheduler_shutdown_recovers_uncertain_send_without_resending() {
    use std::net::TcpListener;
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("sched-stop-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user",&[&env]).await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&db, job, &env, "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-scheduler-stop-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let webhook = dir.join("webhook");
    let config = dir.join("config.json");
    std::fs::write(&webhook, "https://hooks.slack.com/services/A/B/C").unwrap();
    std::fs::write(
        &config,
        serde_json::to_vec(
            &serde_json::json!({"targets":[{"channel":"slack","id":"ops","webhook_file":webhook}]}),
        )
        .unwrap(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let configure = |cmd: &mut Command| {
        cmd.env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "30")
            .env("HTTPS_PROXY", &proxy)
            .env("https_proxy", &proxy)
            .env("NO_PROXY", "")
            .env("no_proxy", "")
            .env_remove("SWALLOW_ALERT_CHECK_SCOPED_ACCESS")
            .env_remove("SWALLOW_ALERT_SCOPED_DELIVERY")
            .env_remove("SWALLOW_ALERT_COOLDOWN_POLICY")
            .env_remove("SWALLOW_ALERT_BINDING_KEY")
            .env_remove("SWALLOW_ALERT_BINDING_KEY_FILE");
    };
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_alert-scheduler"));
    cmd.args(["1", "delivery", &env, "300", "120", "60"])
        .arg(&config)
        .env("SWALLOW_ALERT_SCHEDULER_SHUTDOWN_GRACE_SECONDS", "1")
        .env("SWALLOW_ALERT_SCHEDULER_FAILURE_MODE", "continue")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure(&mut cmd);
    let mut scheduler = cmd.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let socket = loop {
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "alert did not connect");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("proxy {e}"),
        }
    };
    let state:String=db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='ops'",&[&env]).await.unwrap().get(0);
    assert_eq!(state, "dispatching");
    assert!(
        Command::new("kill")
            .args(["-TERM", &scheduler.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(!scheduler.wait().unwrap().success());
    drop(socket);
    db.execute("UPDATE novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id=$1 AND channel='slack' AND target_id='ops'",&[&env]).await.unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_alert-fanout"));
    cmd.args(["delivery", &env, "300", "120", "60"])
        .arg(&config);
    configure(&mut cmd);
    let out = cmd.output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let state:String=db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='ops'",&[&env]).await.unwrap().get(0);
    assert_eq!(state, "unknown");
    let attempts:i64=db.query_one("SELECT count(*) FROM novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND channel='slack' AND target_id='ops'",&[&env]).await.unwrap().get(0);
    assert_eq!(attempts, 1);
    assert!(
        matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock),
        "unexpected second connection"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
