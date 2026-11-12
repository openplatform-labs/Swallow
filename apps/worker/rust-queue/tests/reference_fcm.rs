use novu_postgres_queue::fcm::{Push, plan};
use serde_json::{Value, json};
use std::process::Command;

#[test]
#[ignore = "requires built pinned reference provider and node"]
fn original_fcm_message_plans() {
    let output = Command::new("node")
        .arg("tests/reference_fcm.cjs")
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
        (
            vec!["t1", "t2"],
            json!({"other":"ignored","__nvMessageId":"m1"}),
            json!({}),
            json!({}),
        ),
        (
            vec!["t1"],
            json!({"a":1,"flag":true,"nested":{"x":2}}),
            json!({"type":"data","data":{"extra":"value","title":"overwritten"}}),
            json!({}),
        ),
        (
            vec!["t1"],
            json!({}),
            json!({"badge":3,"webPush":{"headers":{"Urgency":"high"}}}),
            json!({"notification":{"title":"Provider"},"apns":{"headers":{"apns-priority":"10"}}}),
        ),
        (
            vec!["t1"],
            json!({}),
            json!({"badge":3}),
            json!({"token":"one","topic":"orders"}),
        ),
        (
            vec!["t1"],
            json!({}),
            json!({}),
            json!({"tokens":["a","",null,"b"],"_passthrough":{"body":{"topic":"broadcast","notification":{"body":"override"}}}}),
        ),
        (
            vec!["t1"],
            json!({}),
            json!({}),
            json!({"token":"","tokens":["x","y"],"_passthrough":{"body":{"topic":""}}}),
        ),
        (
            vec!["t1"],
            json!({}),
            json!({}),
            json!({"condition":"'a' in topics","data":{"key":"v"},"android":{"notification":{"vibrateTimingsMillis":[1,2]}},"_passthrough":{"body":{"android":{"notification":{"vibrateTimingsMillis":[3]}}}}}),
        ),
    ];
    for (index, (targets, payload, overrides, provider_data)) in cases.into_iter().enumerate() {
        let push = Push {
            title: "Hi".into(),
            content: "Hello".into(),
            targets: targets.into_iter().map(str::to_owned).collect(),
            payload: payload.as_object().unwrap().clone(),
            overrides: overrides.as_object().unwrap().clone(),
            provider_data: provider_data.as_object().unwrap().clone(),
            ..Push::default()
        };
        let result = plan(&push);
        assert_eq!(
            original[index],
            json!({"multicast":result.multicast,"message":result.message}),
            "case {index}"
        );
    }
}
