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
