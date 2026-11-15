//! Compare selected transport body cases with the checked-out Novu deep merge.
use novu_postgres_queue::discord::request_body;
use serde_json::{Value, json};

#[test]
#[ignore = "requires Node with native TypeScript stripping and the Novu source checkout"]
fn original_body_merge_cases() {
    let cases = json!([
        {"content":"trigger", "custom":{"content":"custom", "embeds":[{"title":"a"}]}, "provider":{"embeds":[{"title":"b"}]}, "passthrough":{"embeds":[{"title":"c"}]}},
        {"content":"trigger", "custom":{"nested":{"n":1,"values":[1]}}, "provider":{"nested":{"n":2,"values":[2]}}, "passthrough":{"nested":{"values":[3]}}},
        {"content":"trigger", "custom":{"a":{"x":1},"b":[1],"c":"value"}, "provider":{"a":[2],"b":{"x":3},"c":{"d":4}}, "passthrough":{"content":null}},
        {"content":"trigger", "custom":{"content":[1]}, "provider":{"content":[2]}, "passthrough":{"content":[3]}}
    ]);
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let script = r#"import {deepMerge} from './packages/providers/src/utils/deepmerge.utils.ts';
const cases = JSON.parse(process.argv[1]);
console.log(JSON.stringify(cases.map(c => deepMerge([{body:{content:c.content,...c.custom},headers:{},query:{}},{body:c.provider,headers:{},query:{}},{body:c.passthrough,headers:{},query:{}}]).body)));"#;
    let output = std::process::Command::new("node")
        .current_dir(repo)
        .args(["--input-type=module", "-e", script, &cases.to_string()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reference: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    for (case, expected) in cases.as_array().unwrap().iter().zip(reference) {
        let body = request_body(
            case["content"].as_str().unwrap(),
            case["custom"].as_object().unwrap(),
            case["provider"].as_object().unwrap(),
            case["passthrough"].as_object().unwrap(),
        )
        .unwrap();
        assert_eq!(body, expected);
    }
}
