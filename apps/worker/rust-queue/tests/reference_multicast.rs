//! Differential test of the original multicast splitter for string subscriber IDs.
use std::{fs, process::Command};
use uuid::Uuid;

#[test]
fn string_subscriber_dedup_matches_reference() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../libs/application-generic/src/usecases/trigger-multicast/trigger-multicast.usecase.ts"
    );
    let source = fs::read_to_string(path).unwrap();
    let topic =
        &source[source.find("const isTopic =").unwrap()..source.find("@Injectable()").unwrap()];
    let start = source.find("export const splitByRecipientType =").unwrap();
    let end = source
        .find("const SUBSCRIBER_ID_VALIDATION_PREFIX")
        .unwrap();
    let splitter = &source[start..end];
    let oracle = format!(
        "const TriggerRecipientsTypeEnum = {{ TOPIC: 'Topic' }};\n{topic}\n{splitter}\nconst cases = JSON.parse(process.argv[2]); console.log(JSON.stringify(cases.map(c => Array.from(splitByRecipientType(c).singleSubscribers.keys()))));\n"
    );
    let path = std::env::temp_dir().join(format!("novu-split-{}.ts", Uuid::new_v4()));
    fs::write(&path, oracle).unwrap();
    let cases: Vec<Vec<String>> = vec![
        vec![],
        vec!["a".into()],
        vec!["a".into(), "b".into(), "a".into()],
        vec!["β".into(), "a".into(), "β".into(), "c".into()],
    ];
    let output = Command::new("node")
        .arg("--experimental-strip-types")
        .arg(&path)
        .arg(serde_json::to_string(&cases).unwrap())
        .output()
        .expect("Node.js is required");
    fs::remove_file(path).unwrap();
    assert!(
        output.status.success(),
        "reference splitter failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reference: Vec<Vec<String>> = serde_json::from_slice(&output.stdout).unwrap();
    let actual: Vec<Vec<&str>> = cases
        .iter()
        .map(|c| novu_postgres_queue::in_app::unique_recipient_ids(c))
        .collect();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(reference).unwrap()
    );
}

#[test]
fn subscriber_objects_match_reference_splitter() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../libs/application-generic/src/usecases/trigger-multicast/trigger-multicast.usecase.ts"
    );
    let source = fs::read_to_string(path).unwrap();
    let topic =
        &source[source.find("const isTopic =").unwrap()..source.find("@Injectable()").unwrap()];
    let start = source.find("export const splitByRecipientType =").unwrap();
    let end = source
        .find("const SUBSCRIBER_ID_VALIDATION_PREFIX")
        .unwrap();
    let validation = &source[source
        .find("export const validateSubscriberDefine =")
        .unwrap()..];
    let oracle = format!(
        "const TriggerRecipientsTypeEnum = {{ TOPIC: 'Topic' }}; class BadRequestException extends Error {{}}\n{topic}\n{}\n{validation}\nconst cases=JSON.parse(process.argv[2]); console.log(JSON.stringify(cases.map(c=>Array.from(splitByRecipientType(c).singleSubscribers.values()).map(({{subscriberId,firstName,lastName,phone,avatar,data}})=>({{subscriberId,firstName,lastName,phone,avatar,data}})))));",
        &source[start..end]
    );
    let tmp = std::env::temp_dir().join(format!("novu-profile-{}.ts", Uuid::new_v4()));
    fs::write(&tmp, oracle).unwrap();
    let cases = serde_json::json!([
      ["a", {"subscriberId":"b", "firstName":"Jane", "lastName":"Doe"}],
      [{"subscriberId":"a", "firstName":"first"}, {"subscriberId":"b"}, {"type":"Subscriber", "subscriberId":"a", "lastName":null}],
      [{"subscriberId":"a", "firstName":"Jane"}, "a"],
      [{"subscriberId":"a", "phone":"+12025550123", "avatar":"https://example.test/avatar", "data":{"plan":"gold"}}, {"subscriberId":"b", "phone":null, "data":null}]
    ]);
    let output = Command::new("node")
        .arg("--experimental-strip-types")
        .arg(&tmp)
        .arg(cases.to_string())
        .output()
        .expect("node required");
    fs::remove_file(tmp).unwrap();
    assert!(
        output.status.success(),
        "reference failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let actual: Vec<_> = cases
        .as_array()
        .unwrap()
        .iter()
        .map(|case| novu_rust_contracts::recipients::normalize(case).unwrap())
        .collect();
    assert_eq!(serde_json::to_value(actual).unwrap(), expected);
}
