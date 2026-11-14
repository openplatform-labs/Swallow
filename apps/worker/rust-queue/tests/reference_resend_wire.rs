//! Built pinned Novu provider and real Resend SDK serialization with mocked fetch.
use novu_postgres_queue::{
    resend::{self, Config},
    sendgrid::{Attachment, Email},
};
use serde_json::{Value, json};
use std::{path::PathBuf, process::Command};
#[test]
#[ignore = "requires Docker and built reference source"]
fn original_resend_sdk_wire() {
    let reference = std::env::var("NOVU_REFERENCE_SOURCE").unwrap();
    let tests = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &format!("{reference}:/ref:ro"),
            "-v",
            &format!("{}:/test:ro", tests.display()),
            "-w",
            "/ref",
            "node:22.23.4-alpine3.23",
            "node",
            "/test/reference_resend_wire.cjs",
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
    let plain = Email {
        to: vec!["a@example.test".into()],
        subject: "Hello".into(),
        html: "<p>Hello</p>".into(),
        ..Email::default()
    };
    let full = Email {
        to: vec!["a@example.test".into(), "b@example.test".into()],
        cc: vec!["cc@example.test".into()],
        bcc: vec!["bcc@example.test".into()],
        from: Some("alt@example.test".into()),
        sender_name: Some("Alias".into()),
        subject: "Hi".into(),
        html: "<b>Hi</b>".into(),
        text: Some("Hi".into()),
        reply_to: Some("reply@example.test".into()),
        headers: json!({"X-Trace":"value"}).as_object().unwrap().clone(),
        ..Email::default()
    };
    assert_eq!(original[0], resend::request_body(&config, &plain).unwrap());
    assert_eq!(original[1], resend::request_body(&config, &full).unwrap());
    let attachment = Email {
        to: vec!["a@example.test".into()],
        subject: "Attachment".into(),
        html: "<img src=\"cid:logo\">".into(),
        reply_to: Some("reply@example.test".into()),
        attachments: vec![Attachment {
            file: vec![0, 1, 255],
            name: "image.png".into(),
            mime: "image/png".into(),
            cid: Some("logo".into()),
            disposition: None,
        }],
        ..Email::default()
    };
    let rust = resend::request_body(&config, &attachment).unwrap();
    assert_eq!(
        original[2]["attachments"][0]["content"],
        json!({"type":"Buffer","data":[0,1,255]})
    );
    assert_eq!(rust["attachments"][0]["content"], "AAH/");
    let mut normalized = original[2].clone();
    normalized["attachments"][0]["content"] = json!("AAH/");
    assert_eq!(normalized, rust);
}
