//! Black-holed PostgreSQL protocol fixture; no external service is contacted.
use std::{
    net::TcpListener,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[test]
fn alert_command_bounds_a_database_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(4);
        let _socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < until =>
                {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(_) => return,
            }
        };
        std::thread::sleep(Duration::from_secs(3));
    });
    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .args(["env", "300", "120", "60"])
        .env(
            "DATABASE_URL",
            format!("postgresql://postgres@{address}/test"),
        )
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env_remove("NOVU_POSTGRES_CA_FILE")
        .env_remove("NOVU_POSTGRES_CLIENT_CERT_FILE")
        .env_remove("NOVU_POSTGRES_CLIENT_KEY_FILE")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
        .env_remove("SWALLOW_ALERT_EMAIL_TO")
        .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    let elapsed = started.elapsed();
    server.join().unwrap();
    assert!(!output.status.success());
    assert!(
        elapsed < Duration::from_secs(3),
        "database handshake was not bounded"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("alert database connection timed out")
    );
}
