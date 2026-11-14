//! Selected original provider option mapping with a mocked SDK, not wire or e2e.
use novu_postgres_queue::{
    resend::{self, Config},
    sendgrid::Email,
};
use serde_json::Value;
use std::process::Command;

#[test]
#[ignore = "requires Node.js with stripTypeScriptTypes"]
fn original_resend_options_match_selected_cases() {
    let output = Command::new("node")
        .args([
            "--disable-warning=ExperimentalWarning",
            "tests/reference_resend_source.cjs",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let original: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let config = Config {
        api_key: "fixture".into(),
        from: "sender@example.test".into(),
        sender_name: "Sender".into(),
    };
    let cases = [
        Email {
            to: vec!["a@example.test".into()],
            subject: "Hello".into(),
            html: "<p>Hello</p>".into(),
            reply_to: Some("reply@example.test".into()),
            ..Email::default()
        },
        Email {
            to: vec!["a@example.test".into(), "b@example.test".into()],
            cc: vec!["cc@example.test".into()],
            bcc: vec!["bcc@example.test".into()],
            from: Some("alt@example.test".into()),
            sender_name: Some("Alias".into()),
            subject: "Hi".into(),
            html: "<b>Hi</b>".into(),
            text: Some("Hi".into()),
            reply_to: Some("reply@example.test".into()),
            ..Email::default()
        },
        Email {
            to: vec!["a@example.test".into()],
            subject: "Plain".into(),
            html: "Hi".into(),
            reply_to: Some("reply@example.test".into()),
            sender_name: Some("No comma".into()),
            ..Email::default()
        },
    ];
    assert_eq!(
        original,
        cases
            .iter()
            .map(|email| resend::request_body(&config, email).unwrap())
            .collect::<Vec<_>>()
    );
}
