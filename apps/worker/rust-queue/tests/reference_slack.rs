use novu_postgres_queue::slack::{Message, request_body};
use serde_json::{Value, json};
use std::process::Command;
#[test]
#[ignore = "requires pinned built Novu provider and node"]
fn slack_app_core_wire_matches_original() {
    let output = Command::new("node")
        .arg("tests/reference_slack.cjs")
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
    let original: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let cases = [
        Message {
            channel: "C123".into(),
            text: "Hello & 한글".into(),
            blocks: None,
        },
        Message {
            channel: "U456".into(),
            text: "line 1\nline 2".into(),
            blocks: Some(json!([{"type":"section","text":{"type":"mrkdwn","text":"*Hello*"}}])),
        },
    ];
    for (case, expected) in cases.iter().zip(original) {
        assert_eq!(
            expected,
            json!({"url":"https://slack.com/api/chat.postMessage","body":request_body(case).unwrap(),"authorization":"Bearer fixture-token","id":format!("{}:123.456",case.channel)})
        );
    }
}
