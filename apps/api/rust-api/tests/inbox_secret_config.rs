use std::{
    fs,
    process::{Command, Stdio},
    time::Duration,
};

fn server() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"));
    command
        .env("DATABASE_URL", "postgresql://localhost:1/unused")
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
        .env("LISTEN_ADDR", "127.0.0.1:0")
        .env_remove("INBOX_HMAC_KEYS_JSON")
        .env_remove("INBOX_HMAC_KEYS_JSON_FILE")
        .env_remove("SWALLOW_API_RESEND_WEBHOOK_BUNDLE_HEALTH_ENABLED")
        .env_remove("SWALLOW_API_STRICT_SECURITY_CONFIG")
        .env_remove("SWALLOW_API_REQUIRED_SECURITY")
        .env_remove("JWT_SECRET")
        .env_remove("JWT_SECRET_FILE")
        .env_remove("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID")
        .env_remove("NOVU_RESEND_WEBHOOK_SECRET")
        .env_remove("NOVU_RESEND_WEBHOOK_SECRET_FILE")
        .env_remove("NOVU_RESEND_WEBHOOK_SECRET_BUNDLE_FILE")
        .env_remove("NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET")
        .env_remove("NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET_FILE")
        .env_remove("NOVU_RESEND_WEBHOOK_LOCK_RETRY_JOBS")
        .env_remove("NOVU_RESEND_WEBHOOK_CAPTURE_CORRELATION")
        .env_remove("NOVU_RESEND_WEBHOOK_LOCK_CORRELATION_JOBS")
        .env_remove("NOVU_TWILIO_WEBHOOK_REQUIRE_DISPATCH_ACCOUNT")
        .env_remove("NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID")
        .env_remove("NOVU_TWILIO_WEBHOOK_ACCOUNT_SID")
        .env_remove("NOVU_TWILIO_WEBHOOK_URL")
        .env_remove("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN")
        .env_remove("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN_FILE")
        .env_remove("NOVU_TWILIO_WEBHOOK_TOKEN_BUNDLE_FILE")
        .env_remove("NOVU_TWILIO_WEBHOOK_PREVIOUS_AUTH_TOKEN")
        .env_remove("NOVU_TWILIO_WEBHOOK_PREVIOUS_AUTH_TOKEN_FILE")
        .stdout(Stdio::null());
    command
}

#[test]
fn strict_and_required_security_settings_control_startup() {
    fn check(vars: &[(&str, &str)], expected_start: bool) {
        let mut command = server();
        command.envs(vars.iter().copied()).stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        std::thread::sleep(Duration::from_millis(300));
        let exited = child.try_wait().unwrap();
        if exited.is_none() {
            child.kill().unwrap();
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(
            exited.is_none(),
            expected_start,
            "unexpected startup result: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("sensitive-test-token"));
    }
    check(&[("SWALLOW_API_STRICT_SECURITY_CONFIG", "true")], true);
    check(
        &[("SWALLOW_API_RESEND_WEBHOOK_BUNDLE_HEALTH_ENABLED", "true")],
        false,
    );
    check(
        &[(
            "SWALLOW_API_RESEND_WEBHOOK_BUNDLE_HEALTH_ENABLED",
            "invalid",
        )],
        false,
    );
    check(
        &[
            ("SWALLOW_API_STRICT_SECURITY_CONFIG", "true"),
            (
                "NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET",
                "whsec_MDEyMzQ1Njc4OWFiY2RlZg==",
            ),
        ],
        false,
    );
    let resend = [
        ("SWALLOW_API_STRICT_SECURITY_CONFIG", "true"),
        ("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env"),
        (
            "NOVU_RESEND_WEBHOOK_SECRET",
            "whsec_MDEyMzQ1Njc4OWFiY2RlZg==",
        ),
        (
            "NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET",
            "whsec_YWJjZGVmMDEyMzQ1Njc4OQ==",
        ),
    ];
    check(&resend, true);
    let mut duplicate_resend = resend;
    duplicate_resend[3].1 = resend[2].1;
    check(&duplicate_resend, false);
    let mut conflict_resend = resend.to_vec();
    conflict_resend.push(("NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET_FILE", "/missing"));
    check(&conflict_resend, false);
    check(&[("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env")], true); // legacy mode
    check(
        &[
            ("SWALLOW_API_STRICT_SECURITY_CONFIG", "true"),
            ("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env"),
        ],
        false,
    );
    check(
        &[
            ("SWALLOW_API_STRICT_SECURITY_CONFIG", "true"),
            ("JWT_SECRET", "sensitive-test-token"),
            ("JWT_SECRET_FILE", "/missing"),
        ],
        false,
    );
    check(
        &[
            ("SWALLOW_API_STRICT_SECURITY_CONFIG", "true"),
            ("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN", "sensitive-test-token"),
        ],
        false,
    );
    check(
        &[
            ("SWALLOW_API_STRICT_SECURITY_CONFIG", "true"),
            (
                "NOVU_TWILIO_WEBHOOK_PREVIOUS_AUTH_TOKEN",
                "sensitive-test-token",
            ),
        ],
        false,
    );
    let twilio = [
        ("SWALLOW_API_STRICT_SECURITY_CONFIG", "true"),
        ("NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID", "env"),
        (
            "NOVU_TWILIO_WEBHOOK_ACCOUNT_SID",
            "AC11111111111111111111111111111111",
        ),
        ("NOVU_TWILIO_WEBHOOK_URL", "https://example.test/webhook"),
        ("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN", "sensitive-test-token"),
        (
            "NOVU_TWILIO_WEBHOOK_PREVIOUS_AUTH_TOKEN",
            "older-test-token",
        ),
    ];
    check(&twilio, true);
    check(
        &[("NOVU_TWILIO_WEBHOOK_REQUIRE_DISPATCH_ACCOUNT", "true")],
        false,
    );
    check(
        &[("NOVU_TWILIO_WEBHOOK_REQUIRE_DISPATCH_ACCOUNT", "yes")],
        false,
    );
    let mut bound = twilio.to_vec();
    bound.push(("NOVU_TWILIO_WEBHOOK_REQUIRE_DISPATCH_ACCOUNT", "true"));
    check(&bound, true);
    let mut duplicate = twilio;
    duplicate[5].1 = "sensitive-test-token";
    check(&duplicate, false);
    let mut conflict = twilio.to_vec();
    conflict.push(("NOVU_TWILIO_WEBHOOK_PREVIOUS_AUTH_TOKEN_FILE", "/missing"));
    check(&conflict, false);
    check(
        &[(
            "SWALLOW_API_REQUIRED_SECURITY",
            "inbox-jwt,resend-webhook,twilio-webhook",
        )],
        false,
    );
    check(&[("SWALLOW_API_REQUIRED_SECURITY", "unknown")], false);
    check(
        &[("SWALLOW_API_REQUIRED_SECURITY", "inbox-jwt,inbox-jwt")],
        false,
    );
    check(&[("SWALLOW_API_STRICT_SECURITY_CONFIG", "yes")], false);
    check(
        &[("NOVU_RESEND_WEBHOOK_LOCK_CORRELATION_JOBS", "true")],
        false,
    );
    check(
        &[("NOVU_RESEND_WEBHOOK_LOCK_CORRELATION_JOBS", "bad")],
        false,
    );
    check(
        &[
            ("NOVU_RESEND_WEBHOOK_LOCK_CORRELATION_JOBS", "true"),
            ("NOVU_RESEND_WEBHOOK_LOCK_RETRY_JOBS", "true"),
            ("NOVU_RESEND_WEBHOOK_CAPTURE_CORRELATION", "true"),
        ],
        false,
    );
    check(
        &[
            ("NOVU_RESEND_WEBHOOK_LOCK_CORRELATION_JOBS", "true"),
            ("NOVU_RESEND_WEBHOOK_LOCK_RETRY_JOBS", "true"),
            ("NOVU_RESEND_WEBHOOK_CAPTURE_CORRELATION", "true"),
            ("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env"),
            (
                "NOVU_RESEND_WEBHOOK_SECRET",
                "whsec_MDEyMzQ1Njc4OWFiY2RlZg==",
            ),
        ],
        true,
    );
    check(
        &[
            ("SWALLOW_API_STRICT_SECURITY_CONFIG", "true"),
            (
                "SWALLOW_API_REQUIRED_SECURITY",
                "inbox-jwt,resend-webhook,twilio-webhook",
            ),
            ("JWT_SECRET", "sensitive-test-token"),
            ("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env"),
            (
                "NOVU_RESEND_WEBHOOK_SECRET",
                "whsec_MDEyMzQ1Njc4OWFiY2RlZg==",
            ),
            ("NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID", "env"),
            (
                "NOVU_TWILIO_WEBHOOK_ACCOUNT_SID",
                "AC11111111111111111111111111111111",
            ),
            ("NOVU_TWILIO_WEBHOOK_URL", "https://example.test/webhook"),
            ("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN", "sensitive-test-token"),
        ],
        true,
    );
}

#[test]
fn resend_bundle_requires_valid_exclusive_configuration() {
    let path = std::env::temp_dir().join(format!(
        "swallow-resend-bundle-config-{}",
        uuid::Uuid::new_v4()
    ));
    let check = |contents: &str, conflict: bool, expected: bool| {
        fs::write(&path, contents).unwrap();
        let mut command = server();
        command
            .env("SWALLOW_API_STRICT_SECURITY_CONFIG", "true")
            .env("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env")
            .env("NOVU_RESEND_WEBHOOK_SECRET_BUNDLE_FILE", &path)
            .env("SWALLOW_API_RESEND_WEBHOOK_BUNDLE_HEALTH_ENABLED", "true")
            .stderr(Stdio::piped());
        if conflict {
            command.env(
                "NOVU_RESEND_WEBHOOK_SECRET",
                "whsec_MDEyMzQ1Njc4OWFiY2RlZg==",
            );
        }
        let mut child = command.spawn().unwrap();
        let deadline =
            std::time::Instant::now() + Duration::from_secs(if expected { 1 } else { 5 });
        let exited = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if std::time::Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        if exited.is_none() {
            child.kill().unwrap();
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(
            exited.is_none(),
            expected,
            "unexpected startup outcome: conflict={conflict} contents_len={}",
            contents.len()
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("whsec_"));
        assert!(!stderr.contains(path.to_str().unwrap()));
    };
    check(
        r#"{"current":"whsec_MDEyMzQ1Njc4OWFiY2RlZg=="}"#,
        false,
        true,
    );
    check(
        r#"{"current":"whsec_MDEyMzQ1Njc4OWFiY2RlZg=="}"#,
        true,
        false,
    );
    check(r#"{"current":"bad"}"#, false, false);
    check(
        r#"{"current":"whsec_MDEyMzQ1Njc4OWFiY2RlZg==","extra":"x"}"#,
        false,
        false,
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn previous_resend_secret_file_is_accepted_without_exposing_path_or_secret() {
    let path = std::env::temp_dir().join(format!("swallow-resend-old-{}", uuid::Uuid::new_v4()));
    fs::write(&path, "whsec_YWJjZGVmMDEyMzQ1Njc4OQ==\n").unwrap();
    let mut child = server()
        .env("SWALLOW_API_STRICT_SECURITY_CONFIG", "true")
        .env("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", "env")
        .env(
            "NOVU_RESEND_WEBHOOK_SECRET",
            "whsec_MDEyMzQ1Njc4OWFiY2RlZg==",
        )
        .env("NOVU_RESEND_WEBHOOK_PREVIOUS_SECRET_FILE", &path)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let exited = child.try_wait().unwrap();
    if exited.is_none() {
        child.kill().unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(exited.is_none());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("whsec_"));
    assert!(!stderr.contains(path.to_str().unwrap()));
    fs::remove_file(path).unwrap();
}

#[test]
fn twilio_token_bundle_requires_valid_exclusive_configuration() {
    let path = std::env::temp_dir().join(format!(
        "swallow-twilio-bundle-config-{}",
        uuid::Uuid::new_v4()
    ));
    let check = |contents: &str, conflict: bool, expected: bool| {
        fs::write(&path, contents).unwrap();
        let mut command = server();
        command.env("SWALLOW_API_STRICT_SECURITY_CONFIG","true").env("NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID","env")
        .env("NOVU_TWILIO_WEBHOOK_ACCOUNT_SID","AC11111111111111111111111111111111").env("NOVU_TWILIO_WEBHOOK_URL","https://events.example.test/webhooks/organizations/org/environments/env/sms/twilio")
        .env("NOVU_TWILIO_WEBHOOK_TOKEN_BUNDLE_FILE",&path).stderr(Stdio::piped());
        if conflict {
            command.env("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN", "test-secret");
        }
        let mut child = command.spawn().unwrap();
        let deadline =
            std::time::Instant::now() + Duration::from_secs(if expected { 1 } else { 5 });
        let exited = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if std::time::Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        if exited.is_none() {
            child.kill().unwrap();
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(
            exited.is_none(),
            expected,
            "unexpected startup configuration outcome"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("sensitive-test-token"));
        assert!(!stderr.contains(path.to_str().unwrap()));
    };
    check(r#"{"current":"sensitive-test-token"}"#, false, true);
    check(r#"{"current":"sensitive-test-token"}"#, true, false);
    check(
        r#"{"current":"sensitive-test-token","previous":"sensitive-test-token"}"#,
        false,
        false,
    );
    check(
        r#"{"current":"sensitive-test-token","extra":"x"}"#,
        false,
        false,
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn previous_twilio_token_file_supports_rotation_without_exposing_secrets() {
    let path = std::env::temp_dir().join(format!("swallow-twilio-old-{}", uuid::Uuid::new_v4()));
    fs::write(&path, "older-fixture-token\n").unwrap();
    let mut child = server()
        .env("SWALLOW_API_STRICT_SECURITY_CONFIG", "true")
        .env("NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID", "env")
        .env(
            "NOVU_TWILIO_WEBHOOK_ACCOUNT_SID",
            "AC11111111111111111111111111111111",
        )
        .env("NOVU_TWILIO_WEBHOOK_URL", "https://example.test/webhook")
        .env("NOVU_TWILIO_WEBHOOK_AUTH_TOKEN", "current-fixture-token")
        .env("NOVU_TWILIO_WEBHOOK_PREVIOUS_AUTH_TOKEN_FILE", &path)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let exited = child.try_wait().unwrap();
    if exited.is_none() {
        child.kill().unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(exited.is_none(), "valid rotation configuration failed");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("fixture-token"));
    assert!(!stderr.contains(path.to_str().unwrap()));
    fs::remove_file(path).unwrap();
}

#[test]
fn inbox_key_sources_start_or_fail_without_exposing_secrets() {
    let dir = std::env::temp_dir().join(format!("swallow-hmac-config-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&dir).unwrap();
    let path = dir.join("keys");
    let secret = "sensitive-fixture-value";
    fs::write(&path, format!("{{\"env-a\":[\"{secret}\"]}}\n")).unwrap();
    for file in [false, true] {
        let mut command = server();
        if file {
            command.env("INBOX_HMAC_KEYS_JSON_FILE", &path);
        } else {
            command.env(
                "INBOX_HMAC_KEYS_JSON",
                format!("{{\n\"env-a\":[\"{secret}\"]\n}}"),
            );
        }
        let mut child = command.stderr(Stdio::null()).spawn().unwrap();
        std::thread::sleep(Duration::from_millis(300));
        assert!(
            child.try_wait().unwrap().is_none(),
            "valid configuration must start"
        );
        child.kill().unwrap();
        child.wait().unwrap();
    }
    for (inline, file) in [
        (
            Some(format!("{{\"env-a\":[\"{secret}\"]}}")),
            Some(path.clone()),
        ),
        (Some(format!("{{\"env-a\":\"{secret}\"}}")), None),
        (None, Some(dir.join("missing"))),
    ] {
        let mut command = server();
        if let Some(value) = inline {
            command.env("INBOX_HMAC_KEYS_JSON", value);
        }
        if let Some(value) = file {
            command.env("INBOX_HMAC_KEYS_JSON_FILE", value);
        }
        let output = command.stderr(Stdio::piped()).output().unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains(secret));
        assert!(!stderr.contains(dir.to_str().unwrap()));
    }
    fs::remove_dir_all(dir).unwrap();
}
