//! FIFO paths must fail at startup without waiting for a writer.
#![cfg(unix)]
use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use uuid::Uuid;

#[test]
fn reloadable_workers_reject_fifo_at_startup() {
    let dir = std::env::temp_dir().join(format!("swallow-fifo-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fifo = dir.join("credentials");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    for provider in [
        "resend",
        "sendgrid",
        "twilio",
        "slack",
        "fcm",
        "fcm-map",
        "fcm-service",
    ] {
        let (bin, environment, file, flag) = match provider {
            "resend" => (
                env!("CARGO_BIN_EXE_resend-worker"),
                "NOVU_RESEND_ENVIRONMENT_ID",
                "NOVU_RESEND_API_KEY_FILE",
                Some("NOVU_RESEND_RELOAD_API_KEY"),
            ),
            "sendgrid" => (
                env!("CARGO_BIN_EXE_sendgrid-worker"),
                "NOVU_SENDGRID_ENVIRONMENT_ID",
                "NOVU_SENDGRID_API_KEY_FILE",
                Some("NOVU_SENDGRID_RELOAD_API_KEY"),
            ),
            "twilio" => (
                env!("CARGO_BIN_EXE_twilio-worker"),
                "NOVU_TWILIO_ENVIRONMENT_ID",
                "NOVU_TWILIO_CREDENTIALS_FILE",
                None,
            ),
            "slack" => (
                env!("CARGO_BIN_EXE_slack-worker"),
                "NOVU_SLACK_ENVIRONMENT_ID",
                "NOVU_SLACK_BOT_TOKEN_FILE",
                Some("NOVU_SLACK_RELOAD_BOT_TOKEN"),
            ),
            "fcm" => (
                env!("CARGO_BIN_EXE_fcm-worker"),
                "NOVU_FCM_ENVIRONMENT_ID",
                "NOVU_FCM_ACCESS_TOKEN_FILE",
                Some("NOVU_FCM_RELOAD_ACCESS_TOKEN"),
            ),
            "fcm-map" => (
                env!("CARGO_BIN_EXE_fcm-worker"),
                "NOVU_FCM_ENVIRONMENT_ID",
                "NOVU_FCM_INTEGRATIONS_FILE",
                Some("NOVU_FCM_RELOAD_INTEGRATIONS"),
            ),
            _ => (
                env!("CARGO_BIN_EXE_fcm-worker"),
                "NOVU_FCM_ENVIRONMENT_ID",
                "NOVU_FCM_PRIVATE_KEY_FILE",
                Some("NOVU_FCM_RELOAD_SERVICE_ACCOUNT"),
            ),
        };
        let mut cmd = Command::new(bin);
        cmd.env_clear()
            .env("DATABASE_URL", "postgresql://127.0.0.1/unused")
            .env(environment, "fixture")
            .env(file, &fifo)
            .env("NOVU_RESEND_FROM", "from@example.test")
            .env("NOVU_SENDGRID_FROM", "from@example.test")
            .env("NOVU_SENDGRID_SENDER_NAME", "fixture")
            .env("NOVU_TWILIO_ACCOUNT_SID", "ACfixture")
            .env("NOVU_TWILIO_FROM", "+15550000001")
            .env("NOVU_FCM_PROJECT_ID", "fixture")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(flag) = flag {
            cmd.env(flag, "true");
        }
        if provider == "fcm-service" {
            cmd.env("NOVU_FCM_CLIENT_EMAIL", "fixture@example.test");
        }
        let mut child = cmd.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(!status.success());
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("{provider} blocked on FIFO");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn alert_commands_reject_fifo_secrets_when_regular_file_policy_is_enabled() {
    let dir = std::env::temp_dir().join(format!("swallow-alert-fifo-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fifo = dir.join("credentials");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    for (binary, args) in [
        (
            env!("CARGO_BIN_EXE_send-delivery-alert"),
            vec!["fixture", "300", "120", "60"],
        ),
        (
            env!("CARGO_BIN_EXE_send-database-alert"),
            vec!["fixture", "probe", "60"],
        ),
    ] {
        for secret in [
            "SWALLOW_ALERT_SLACK_WEBHOOK_FILE",
            "NOVU_RESEND_API_KEY_FILE",
        ] {
            let mut child = Command::new(binary)
                .args(&args)
                .env_clear()
                .env("SWALLOW_SECRET_REGULAR_FILES_ONLY", "true")
                .env(secret, &fifo)
                .env("SWALLOW_ALERT_EMAIL_TO", "recipient@example.test")
                .env("SWALLOW_ALERT_EMAIL_PROVIDER", "resend")
                .env("NOVU_RESEND_FROM", "sender@example.test")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(!status.success());
                    break;
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("alert command blocked on FIFO");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn alert_regular_file_policy_accepts_symlink_and_rejects_bad_policy_or_conflicting_sources() {
    use std::os::unix::fs::symlink;
    let dir = std::env::temp_dir().join(format!("swallow-alert-file-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(
        dir.join("secret"),
        "https://hooks.slack.com/services/A/B/C\n",
    )
    .unwrap();
    symlink("secret", dir.join("link")).unwrap();
    for (policy, conflict, expected_config_error) in [
        ("true", false, false),
        ("false", false, false),
        ("invalid", false, true),
        ("true", true, true),
    ] {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"));
        cmd.args(["fixture", "300", "120", "60"])
            .env_clear()
            .env("SWALLOW_SECRET_REGULAR_FILES_ONLY", policy)
            .env("SWALLOW_ALERT_SLACK_WEBHOOK_FILE", dir.join("link"));
        if conflict {
            cmd.env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/D/E/F",
            );
        }
        let output = cmd.output().unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(
            stderr.contains("Slack alert configuration failed"),
            expected_config_error,
            "{stderr}"
        );
        assert!(!stderr.contains("hooks.slack.com"));
        assert!(!stderr.contains(dir.to_str().unwrap()));
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn fanout_timeout_isolates_actual_alert_command_blocked_on_fifo() {
    let dir = std::env::temp_dir().join(format!("swallow-alert-fanout-fifo-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let fifo = dir.join("fifo");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let config = dir.join("targets.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({"targets":[
            {"channel":"slack","id":"blocked","webhook_file":fifo},
            {"channel":"slack","id":"missing","webhook_file":dir.join("missing")}
        ]}))
        .unwrap(),
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_alert-fanout"))
        .args(["delivery", "env", "300", "120", "60"])
        .arg(&config)
        .env_clear()
        .env("SWALLOW_ALERT_FANOUT_TARGET_TIMEOUT_SECONDS", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(8);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fanout did not continue");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("slack blocked: timed out; outcome unknown"));
    assert!(stderr.contains("slack missing: failed"));
    std::fs::remove_dir_all(dir).unwrap();
}
