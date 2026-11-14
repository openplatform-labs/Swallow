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
        .stdout(Stdio::null());
    command
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
