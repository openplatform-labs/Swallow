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
  ops-a) test -z "${SWALLOW_ALERT_EMAIL_TO:-}" || exit 8; printf '%s\n' 'slack ops-a' >> "$FANOUT_TEST_LOG"; exit 2;;
  ops-b) test -z "${SWALLOW_ALERT_EMAIL_TO:-}" || exit 8; printf '%s\n' 'slack ops-b' >> "$FANOUT_TEST_LOG"; exit 0;;
esac
case "${SWALLOW_ALERT_EMAIL_TARGET_ID:-}" in
  mail-a) test -z "${SWALLOW_ALERT_SLACK_WEBHOOK:-}" || exit 8; test -z "${NOVU_RESEND_API_KEY:-}" || exit 8; test "$SWALLOW_ALERT_EMAIL_PROVIDER" = resend || exit 8; printf '%s\n' 'email mail-a' >> "$FANOUT_TEST_LOG"; exit 1;;
  mail-b) test -z "${SWALLOW_ALERT_SLACK_WEBHOOK:-}" || exit 8; test -z "${NOVU_SENDGRID_API_KEY:-}" || exit 8; test "$SWALLOW_ALERT_EMAIL_PROVIDER" = sendgrid || exit 8; test "$NOVU_SENDGRID_REGION" = eu || exit 8; printf '%s\n' 'email mail-b' >> "$FANOUT_TEST_LOG"; exit 0;;
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
      {"channel":"email","id":"mail-b","provider":"sendgrid","to":"two@example.test","from":"sender@example.test","api_key_file":dir.join("key-b"),"region":"eu"}
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
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1));
        assert_eq!(
            std::fs::read_to_string(&log).unwrap(),
            "slack ops-a\nemail mail-a\nslack ops-b\nemail mail-b\n"
        );
        let stdout = String::from_utf8(out.stdout).unwrap();
        let stderr = String::from_utf8(out.stderr).unwrap();
        assert!(stdout.contains("slack ops-a: attention"));
        assert!(stderr.contains("email mail-a: failed"));
        for secret in [
            "inherited-slack-secret",
            "inherited-email-secret",
            "inherited-sendgrid-secret",
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
        "#!/bin/sh\nprintf '%s' $$ > \"$FANOUT_TEST_CHILD_PID\"\nexec sleep 15\n",
    )
    .unwrap();
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = dir.join("targets.json");
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[{"channel":"slack","id":"ops","webhook_file":dir.join("secret")}]})).unwrap()).unwrap();
    let pid_file = dir.join("child-pid");
    let mut parent = Command::new(&fanout)
        .args(["delivery", "env", "300", "120", "60"])
        .arg(&config)
        .env("SWALLOW_ALERT_SHUTDOWN_GRACE_SECONDS", "1")
        .env("FANOUT_TEST_CHILD_PID", &pid_file)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pid_file.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(pid_file.exists());
    let pid = std::fs::read_to_string(&pid_file).unwrap();
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
    std::fs::remove_dir_all(dir).unwrap();
}
