//! Execute the reference validator itself after extracting its pure functions.
//! Node.js must support --experimental-strip-types (the repository requires Node 22).
use serde_json::{Value, json};
use std::{fs, process::Command};
use uuid::Uuid;

#[test]
fn recipient_validation_matches_reference_typescript() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../src/app/events/validators/is-trigger-recipients-payload.validator.ts"
    );
    let source = fs::read_to_string(path).unwrap();
    let start = source.find("function isValidSingleRecipient(").unwrap();
    let end = source.find("@ValidatorConstraint").unwrap();
    let pure = &source[start..end];
    let start = source
        .find("  validate(value: unknown): boolean {")
        .unwrap();
    let end = source[start..].find("  defaultMessage(").unwrap() + start;
    let validate = source[start..end].replacen("  validate(", "function validate(", 1);
    let enum_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../packages/shared/src/types/events.ts"
    );
    let enum_source = fs::read_to_string(enum_path).unwrap();
    assert!(
        enum_source.contains("TOPIC = 'Topic'"),
        "reference enum changed; update the oracle"
    );
    let oracle = format!(
        "const TriggerRecipientsTypeEnum = {{ TOPIC: 'Topic' }};\n{pure}\n{validate}\nconst items = JSON.parse(process.argv[2]); console.log(JSON.stringify(items.map(validate)));\n"
    );
    let tmp = std::env::temp_dir().join(format!("novu-validator-{}.ts", Uuid::new_v4()));
    fs::write(&tmp, oracle).unwrap();
    let cases: Vec<Value> = vec![
        json!(null),
        json!(""),
        json!("alice"),
        json!([]),
        json!(["alice", ""]),
        json!({}),
        json!({"subscriberId":"alice"}),
        json!({"subscriberId":""}),
        json!({"topicKey":"news"}),
        json!({"type":"Topic","topicKey":"news"}),
        json!({"type":"Topic","subscriberId":"alice"}),
        json!({"type":"Other","subscriberId":"alice","topicKey":""}),
        json!([{"subscriberId":"alice"}, {"topicKey":"news"}]),
        json!(42),
    ];
    let output = Command::new("node")
        .arg("--experimental-strip-types")
        .arg(&tmp)
        .arg(serde_json::to_string(&cases).unwrap())
        .output()
        .expect("Node.js required for original-source differential test");
    fs::remove_file(&tmp).unwrap();
    assert!(
        output.status.success(),
        "reference validator failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reference: Vec<bool> = serde_json::from_slice(&output.stdout).unwrap();
    let actual: Vec<bool> = cases
        .iter()
        .map(|x| novu_rust_api::valid_recipients(Some(x)))
        .collect();
    assert_eq!(
        actual, reference,
        "recipient validator differs from checked-out Novu"
    );
    assert!(!novu_rust_api::valid_recipients(None));
}
