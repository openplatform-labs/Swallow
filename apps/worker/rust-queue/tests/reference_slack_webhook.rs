use novu_postgres_queue::slack_webhook::{Message, request_body};
use serde_json::{Value, json};
use std::process::Command;

#[test]
#[ignore = "requires pinned built Novu provider and node"]
fn webhook_body_matches_original_provider() {
    let output = Command::new("node")
        .arg("tests/reference_slack_webhook.cjs")
        .env(
            "NOVU_REFERENCE_SOURCE",
            std::env::var("NOVU_REFERENCE_SOURCE").unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let original: Value = serde_json::from_slice(&output.stdout).unwrap();
    let message = Message {
        destination: "fixture".into(),
        text: "hello".into(),
        blocks: Some(json!([{"type":"section","text":{"type":"mrkdwn","text":"hello"}}])),
        custom_data: json!({"thread_ts":"1.2","unfurl_links":false})
            .as_object()
            .cloned(),
    };
    assert_eq!(
        original,
        json!({"body":request_body(&message).unwrap(),"id":"fixture-id"})
    );
}
