use novu_rust_contracts::template::render_in_app_with_actor;
use serde_json::{Value, json};
use std::process::Command;

#[test]
#[ignore = "requires built original Novu application-generic and node"]
fn original_custom_html_email_compiler() {
    let output = Command::new("node")
        .arg("tests/reference_email_template.cjs")
        .env(
            "NOVU_REFERENCE_SOURCE",
            std::env::var("NOVU_REFERENCE_SOURCE").unwrap(),
        )
        .env("NEW_RELIC_ENABLED", "false")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let cases = [
        (
            " Hello {{subscriber.firstName}} ",
            "<p>{{name}}</p>",
            json!({"name":"<Alice>"}),
            json!({"subscriberId":"s","firstName":"Jane"}),
            json!({}),
            None,
        ),
        (
            "{{actor.firstName}}: {{subscriber.subscriberId}}",
            " {{context.tenant.data.plan}} & {{name}} ",
            json!({"name":"A&B"}),
            json!({"subscriberId":"s"}),
            json!({"tenant":{"data":{"plan":"gold"}}}),
            Some(json!({"firstName":"Sam"})),
        ),
        (
            "{{missing}}",
            "{{subscriber.email}} {{branding.color}}",
            json!({}),
            json!({"email":"a@example.test"}),
            json!({}),
            None,
        ),
    ];
    let actual: Vec<Value> = cases.iter().map(|(subject,html,payload,subscriber,context,actor)| json!({"subject":render_in_app_with_actor(subject,payload,subscriber,context,actor.as_ref()).unwrap().trim(),"html":render_in_app_with_actor(html,payload,subscriber,context,actor.as_ref()).unwrap()})).collect();
    assert_eq!(actual, expected);
}
