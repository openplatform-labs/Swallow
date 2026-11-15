//! The actual fanout binary executes fixture child commands in an isolated directory.
#![cfg(unix)]
use std::{os::unix::fs::PermissionsExt, process::Command};
use uuid::Uuid;
#[test]
fn routes_each_destination_and_continues_after_a_failure() {
    let dir = std::env::temp_dir().join(format!("swallow-fanout-process-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fanout = dir.join("alert-fanout");
    std::fs::copy(env!("CARGO_BIN_EXE_alert-fanout"), &fanout).unwrap();
    let script = r#"#!/bin/sh
case "${SWALLOW_ALERT_SLACK_TARGET_ID:-}" in
  ops-a) test -z "${NOVU_SENDGRID_IP_POOL_NAME:-}" || exit 8; test -z "${SWALLOW_ALERT_EMAIL_TO:-}" || exit 8; printf '%s\n' 'slack ops-a' >> "$FANOUT_TEST_LOG"; exit 2;;
  ops-b) test -z "${SWALLOW_ALERT_EMAIL_TO:-}" || exit 8; printf '%s\n' 'slack ops-b' >> "$FANOUT_TEST_LOG"; exit 0;;
esac
case "${SWALLOW_ALERT_EMAIL_TARGET_ID:-}" in
  mail-a) test -z "${NOVU_SENDGRID_IP_POOL_NAME:-}" || exit 8; test -z "${SWALLOW_ALERT_SLACK_WEBHOOK:-}" || exit 8; test -z "${NOVU_RESEND_API_KEY:-}" || exit 8; test "$SWALLOW_ALERT_EMAIL_PROVIDER" = resend || exit 8; printf '%s\n' 'email mail-a' >> "$FANOUT_TEST_LOG"; exit 1;;
  mail-b) test -z "${SWALLOW_ALERT_SLACK_WEBHOOK:-}" || exit 8; test -z "${NOVU_SENDGRID_API_KEY:-}" || exit 8; test "$SWALLOW_ALERT_EMAIL_PROVIDER" = sendgrid || exit 8; test "$NOVU_SENDGRID_REGION" = eu || exit 8; test "$NOVU_SENDGRID_IP_POOL_NAME" = pool-b || exit 8; printf '%s\n' 'email mail-b' >> "$FANOUT_TEST_LOG"; exit 0;;
  mail-smtp) test "$SWALLOW_ALERT_EMAIL_PROVIDER" = smtp || exit 8; test "$SWALLOW_SMTP_HOST" = smtp.example.test || exit 8; test "$SWALLOW_SMTP_PORT" = 587 || exit 8; test "$SWALLOW_SMTP_TLS" = starttls || exit 8; test "$SWALLOW_SMTP_USERNAME" = fixture-user || exit 8; test -n "$SWALLOW_SMTP_PASSWORD_FILE" || exit 8; test -n "$SWALLOW_SMTP_CA_FILE" || exit 8; test -z "${SWALLOW_SMTP_PASSWORD:-}" || exit 8; test -z "${NOVU_RESEND_API_KEY:-}" || exit 8; printf '%s\n' 'email mail-smtp' >> "$FANOUT_TEST_LOG"; exit 0;;
  mail-c) test -z "${SWALLOW_SMTP_CA_FILE:-}" || exit 8; test -z "${SWALLOW_SMTP_PASSWORD:-}" || exit 8; test -z "${SWALLOW_SMTP_HOST:-}" || exit 8; test -z "${NOVU_SENDGRID_IP_POOL_NAME:-}" || exit 8; printf '%s\n' 'email mail-c' >> "$FANOUT_TEST_LOG"; exit 0;;
esac
exit 9
"#;
    for name in ["send-delivery-alert", "send-database-alert"] {
        let file = dir.join(name);
        std::fs::write(&file, script).unwrap();
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let config = dir.join("targets.json");
    let log = dir.join("test.log");
    let targets = serde_json::json!({"targets":[
      {"channel":"slack","id":"ops-a","webhook_file":dir.join("slack")},
      {"channel":"email","id":"mail-a","provider":"resend","to":"one@example.test","from":"sender@example.test","api_key_file":dir.join("key-a")},
      {"channel":"slack","id":"ops-b","webhook_file":dir.join("slack")},
      {"channel":"email","id":"mail-b","provider":"sendgrid","to":"two@example.test","from":"sender@example.test","api_key_file":dir.join("key-b"),"region":"eu","ip_pool_name":"pool-b"},
      {"channel":"email","id":"mail-c","provider":"sendgrid","to":"three@example.test","from":"sender@example.test","api_key_file":dir.join("key-c")},
      {"channel":"email","id":"mail-smtp","provider":"smtp","to":"four@example.test","from":"sender@example.test","smtp_host":"smtp.example.test","smtp_port":587,"smtp_tls":"starttls","smtp_username":"fixture-user","smtp_password_file":dir.join("smtp-key"),"smtp_ca_file":dir.join("smtp-ca")}
    ]});
    std::fs::write(&config, serde_json::to_vec(&targets).unwrap()).unwrap();
    for mode in ["delivery", "database"] {
        std::fs::write(&log, "").unwrap();
        let mut cmd = Command::new(&fanout);
        if mode == "delivery" {
            cmd.args(["delivery", "env", "300", "120", "60"]);
        } else {
            cmd.args(["database", "env", "db", "60"]);
        }
        let out = cmd
            .arg(&config)
            .env("FANOUT_TEST_LOG", &log)
            .env("SWALLOW_ALERT_SLACK_WEBHOOK", "inherited-slack-secret")
            .env("NOVU_RESEND_API_KEY", "inherited-email-secret")
            .env("NOVU_SENDGRID_API_KEY", "inherited-sendgrid-secret")
            .env("NOVU_SENDGRID_IP_POOL_NAME", "inherited-pool")
            .env("SWALLOW_SMTP_PASSWORD", "inherited-smtp-secret")
            .env("SWALLOW_SMTP_HOST", "inherited-host")
            .env("SWALLOW_SMTP_CA_FILE", "inherited-ca")
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1));
        assert_eq!(
            std::fs::read_to_string(&log).unwrap(),
            "slack ops-a\nemail mail-a\nslack ops-b\nemail mail-b\nemail mail-c\nemail mail-smtp\n"
        );
        let stdout = String::from_utf8(out.stdout).unwrap();
        let stderr = String::from_utf8(out.stderr).unwrap();
        assert!(stdout.contains("slack ops-a: attention"));
        assert!(stderr.contains("email mail-a: failed"));
        for secret in [
            "inherited-slack-secret",
            "inherited-email-secret",
            "inherited-sendgrid-secret",
            "inherited-smtp-secret",
            "one@example.test",
        ] {
            assert!(!stdout.contains(secret));
            assert!(!stderr.contains(secret));
        }
    }
    let duplicate = serde_json::json!({"targets":[targets["targets"][0].clone(),targets["targets"][0].clone()]});
    std::fs::write(&config, serde_json::to_vec(&duplicate).unwrap()).unwrap();
    std::fs::write(&log, "").unwrap();
    let out = Command::new(&fanout)
        .args(["delivery", "env", "300", "120", "60"])
        .arg(&config)
        .env("FANOUT_TEST_LOG", &log)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(std::fs::read_to_string(&log).unwrap(), "");
    let invalid = serde_json::json!({"targets":[{"channel":"email","id":"invalid","provider":"resend","to":"one@example.test","from":"sender@example.test","api_key_file":dir.join("key-a"),"ip_pool_name":"pool"}]});
    std::fs::write(&config, serde_json::to_vec(&invalid).unwrap()).unwrap();
    let out = Command::new(&fanout)
        .args(["delivery", "env", "300", "120", "60"])
        .arg(&config)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    for invalid in [
        serde_json::json!({"channel":"email","id":"x","provider":"smtp","to":"one@example.test","from":"sender@example.test","smtp_host":"smtp.example.test","smtp_tls":"plain"}),
        serde_json::json!({"channel":"email","id":"x","provider":"smtp","to":"one@example.test","from":"sender@example.test","smtp_host":"smtp.example.test","smtp_username":"user"}),
    ] {
        std::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({"targets":[invalid]})).unwrap(),
        )
        .unwrap();
        let result = Command::new(&fanout)
            .args(["delivery", "env", "300", "120", "60"])
            .arg(&config)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(1));
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn terminating_fanout_bounds_wait_and_stops_its_active_child() {
    use std::time::{Duration, Instant};
    let dir = std::env::temp_dir().join(format!("swallow-fanout-signal-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fanout = dir.join("alert-fanout");
    std::fs::copy(env!("CARGO_BIN_EXE_alert-fanout"), &fanout).unwrap();
    let child = dir.join("send-delivery-alert");
    std::fs::write(
        &child,
        "#!/bin/sh\nprintf '%s' $$ > \"$FANOUT_TEST_CHILD_PID.$SWALLOW_ALERT_SLACK_TARGET_ID\"\nexec sleep 15\n",
    )
    .unwrap();
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = dir.join("targets.json");
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[{"channel":"slack","id":"a","webhook_file":dir.join("secret")},{"channel":"slack","id":"b","webhook_file":dir.join("secret")}]})).unwrap()).unwrap();
    let pid_file = dir.join("child-pid");
    let mut parent = Command::new(&fanout)
        .args(["delivery", "env", "300", "120", "60"])
        .arg(&config)
        .env("SWALLOW_ALERT_SHUTDOWN_GRACE_SECONDS", "1")
        .env("SWALLOW_ALERT_FANOUT_CONCURRENCY", "2")
        .env("FANOUT_TEST_CHILD_PID", &pid_file)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let pid_files = [dir.join("child-pid.a"), dir.join("child-pid.b")];
    while !pid_files.iter().all(|p| p.exists()) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(pid_files.iter().all(|p| p.exists()));
    let pids: Vec<_> = pid_files
        .iter()
        .map(|p| std::fs::read_to_string(p).unwrap())
        .collect();
    assert!(
        Command::new("kill")
            .args(["-TERM", &parent.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = parent.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(status.code(), Some(1));
    for pid in pids {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Command::new("kill")
            .args(["-0", &pid])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
        {
            assert!(
                Instant::now() < deadline,
                "child was left running after fanout timeout"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn parallel_limit_allows_two_children_and_holds_the_third() {
    use std::time::{Duration, Instant};
    let dir = std::env::temp_dir().join(format!("swallow-fanout-parallel-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fanout = dir.join("alert-fanout");
    std::fs::copy(env!("CARGO_BIN_EXE_alert-fanout"), &fanout).unwrap();
    let child = dir.join("send-delivery-alert");
    std::fs::write(
        &child,
        r#"#!/bin/sh
printf started > "$FANOUT_TEST_DIR/$SWALLOW_ALERT_SLACK_TARGET_ID"
case "$SWALLOW_ALERT_SLACK_TARGET_ID" in
  a|b) while [ ! -f "$FANOUT_TEST_DIR/release" ]; do sleep 0.05; done;;
esac
exit 0
"#,
    )
    .unwrap();
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = dir.join("targets.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({"targets":[
            {"channel":"slack","id":"a","webhook_file":dir.join("secret")},
            {"channel":"slack","id":"b","webhook_file":dir.join("secret")},
            {"channel":"slack","id":"c","webhook_file":dir.join("secret")}
        ]}))
        .unwrap(),
    )
    .unwrap();
    for value in ["0", "9", "bad"] {
        let output = Command::new(&fanout)
            .args(["delivery", "env", "300", "120", "60"])
            .arg(&config)
            .env("FANOUT_TEST_DIR", &dir)
            .env("SWALLOW_ALERT_FANOUT_CONCURRENCY", value)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(!dir.join("a").exists());
    }
    let mut process = Command::new(&fanout)
        .args(["delivery", "env", "300", "120", "60"])
        .arg(&config)
        .env("FANOUT_TEST_DIR", &dir)
        .env("SWALLOW_ALERT_FANOUT_CONCURRENCY", "2")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !(dir.join("a").exists() && dir.join("b").exists()) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(dir.join("a").exists() && dir.join("b").exists());
    assert!(
        !dir.join("c").exists(),
        "third destination started before a slot was free"
    );
    std::fs::write(dir.join("release"), "").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(s) = process.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(25));
    };
    assert!(status.success());
    assert!(dir.join("c").exists());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn shutdown_waits_for_active_targets_and_does_not_start_pending_targets() {
    use std::time::{Duration, Instant};
    let dir = std::env::temp_dir().join(format!("swallow-fanout-stop-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fanout = dir.join("alert-fanout");
    std::fs::copy(env!("CARGO_BIN_EXE_alert-fanout"), &fanout).unwrap();
    let child = dir.join("send-delivery-alert");
    std::fs::write(&child,"#!/bin/sh\nprintf started > \"$FANOUT_TEST_DIR/$SWALLOW_ALERT_SLACK_TARGET_ID\"\nsleep 0.8\nprintf done > \"$FANOUT_TEST_DIR/$SWALLOW_ALERT_SLACK_TARGET_ID.done\"\n").unwrap();
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = dir.join("targets.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({"targets":[
            {"channel":"slack","id":"a","webhook_file":dir.join("secret")},
            {"channel":"slack","id":"b","webhook_file":dir.join("secret")},
            {"channel":"slack","id":"c","webhook_file":dir.join("secret")}
        ]}))
        .unwrap(),
    )
    .unwrap();
    let mut process = Command::new(&fanout)
        .args(["delivery", "env", "300", "120", "60"])
        .arg(&config)
        .env("FANOUT_TEST_DIR", &dir)
        .env("SWALLOW_ALERT_FANOUT_CONCURRENCY", "2")
        .env("SWALLOW_ALERT_SHUTDOWN_GRACE_SECONDS", "4")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !(dir.join("a").exists() && dir.join("b").exists()) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(dir.join("a").exists() && dir.join("b").exists());
    assert!(
        Command::new("kill")
            .args(["-TERM", &process.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(s) = process.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(status.code(), Some(1));
    assert!(dir.join("a.done").exists() && dir.join("b.done").exists());
    assert!(!dir.join("c").exists());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn target_timeout_kills_stalled_child_and_continues_other_destinations() {
    use std::time::{Duration, Instant};
    let dir = std::env::temp_dir().join(format!("swallow-fanout-timeout-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fanout = dir.join("alert-fanout");
    std::fs::copy(env!("CARGO_BIN_EXE_alert-fanout"), &fanout).unwrap();
    let script = "#!/bin/sh\nprintf '%s\\n' \"$SWALLOW_ALERT_SLACK_TARGET_ID\" >> \"$FANOUT_TEST_DIR/log\"\nif [ \"$SWALLOW_ALERT_SLACK_TARGET_ID\" = stalled ]; then printf '%s' $$ > \"$FANOUT_TEST_DIR/pid\"; exec sleep 30; fi\nexit 0\n";
    for name in ["send-delivery-alert", "send-database-alert"] {
        let child = dir.join(name);
        std::fs::write(&child, script).unwrap();
        std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let config = dir.join("targets.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({"targets":[
            {"channel":"slack","id":"stalled","webhook_file":dir.join("secret")},
            {"channel":"slack","id":"next","webhook_file":dir.join("secret")}
        ]}))
        .unwrap(),
    )
    .unwrap();
    for mode in ["delivery", "database"] {
        std::fs::write(dir.join("log"), "").unwrap();
        let mut cmd = Command::new(&fanout);
        if mode == "delivery" {
            cmd.args(["delivery", "env", "300", "120", "60"]);
        } else {
            cmd.args(["database", "env", "db", "60"]);
        }
        let output = cmd
            .arg(&config)
            .env("FANOUT_TEST_DIR", &dir)
            .env("SWALLOW_ALERT_FANOUT_TARGET_TIMEOUT_SECONDS", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(
            std::fs::read_to_string(dir.join("log")).unwrap(),
            "stalled\nnext\n"
        );
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("slack stalled: timed out; outcome unknown")
        );
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("slack next: completed")
        );
        let pid = std::fs::read_to_string(dir.join("pid")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while Command::new("kill")
            .args(["-0", &pid])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
        {
            assert!(Instant::now() < deadline, "timed out child remains running");
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    for value in ["bad", "3601", "-1"] {
        std::fs::write(dir.join("log"), "").unwrap();
        let output = Command::new(&fanout)
            .args(["delivery", "env", "300", "120", "60"])
            .arg(&config)
            .env("FANOUT_TEST_DIR", &dir)
            .env("SWALLOW_ALERT_FANOUT_TARGET_TIMEOUT_SECONDS", value)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(std::fs::read_to_string(dir.join("log")).unwrap(), "");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn configuration_file_policy_rejects_fifo_and_accepts_regular_symlinks() {
    use std::{
        io::Write,
        os::unix::fs::symlink,
        process::Stdio,
        time::{Duration, Instant},
    };
    let dir = std::env::temp_dir().join(format!("swallow-fanout-config-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fanout = dir.join("alert-fanout");
    std::fs::copy(env!("CARGO_BIN_EXE_alert-fanout"), &fanout).unwrap();
    let child = dir.join("send-delivery-alert");
    std::fs::write(
        &child,
        "#!/bin/sh\nprintf called >> \"$FANOUT_TEST_LOG\"\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(child, std::fs::Permissions::from_mode(0o700)).unwrap();
    let target = serde_json::json!({"targets":[{"channel":"slack","id":"ops","webhook_file":dir.join("fixture-key")}]});
    let bytes = serde_json::to_vec(&target).unwrap();
    let config = dir.join("targets.json");
    std::fs::write(&config, &bytes).unwrap();
    let link = dir.join("linked.json");
    symlink(&config, &link).unwrap();
    let fifo = dir.join("target-fifo");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let log = dir.join("invocations");
    let run = |path: &std::path::Path, policy: &str| {
        let mut child = Command::new(&fanout)
            .args(["delivery", "env", "300", "120", "60"])
            .arg(path)
            .env("SWALLOW_ALERT_FANOUT_CONFIG_REGULAR_ONLY", policy)
            .env("FANOUT_TEST_LOG", &log)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(result) = child.try_wait().unwrap() {
                break result;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("fanout stalled reading configuration");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    assert!(!run(&fifo, "true").success());
    assert!(!run(&dir, "true").success());
    assert!(!run(&config, "invalid").success());
    assert!(!log.exists());
    assert!(run(&link, "true").success());
    assert_eq!(std::fs::read_to_string(&log).unwrap(), "called");
    let writer_path = fifo.clone();
    let writer = std::thread::spawn(move || {
        std::fs::OpenOptions::new()
            .write(true)
            .open(writer_path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
    });
    assert!(run(&fifo, "false").success());
    writer.join().unwrap();
    assert_eq!(std::fs::read_to_string(&log).unwrap(), "calledcalled");
    std::fs::remove_dir_all(dir).unwrap();
}
