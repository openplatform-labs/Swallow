//! Differential comparison against the reference revision's Handlebars dependency.
use serde_json::{Value, json};
use std::process::Command;

#[test]
#[ignore = "requires NODE_PATH to include handlebars 4.7.10 from the reference lockfile"]
fn supported_interpolation_matches_reference_handlebars() {
    let fixtures = json!([
        {"template":"Hello {{name}}!", "data":{"name":"<Alice & Bob>"}},
        {"template":"{{name}}", "data":{"name":"a&<>\"'`="}},
        {"template":"{{subscriber.subscriberId}} / {{missing}} / {{flag}}", "data":{"subscriber":{"subscriberId":"s-1"}, "flag":false}},
        {"template":"{{subscriber.phone}}|{{subscriber.avatar}}|{{subscriber.data.plan}}", "data":{"subscriber":{"phone":"+12025550123","avatar":"https://example.test/?a=1&b=2","data":{"plan":"gold"}}}},
        {"template":"{{subscriber.phone}}|{{subscriber.data.plan}}", "data":{"subscriber":{"phone":null,"data":null}}},
        {"template":"{{ branding.color }}", "data":{"branding":{"color":"#f47373"}}},
        {"template":"{{absent.value}}", "data":{"absent":null}},
        {"template":"text with no variables", "data":{}},
        {"template":"literal &#x27; and {{name}}", "data":{"name":"&#x27;"}},
        {"template":"{{context.tenant.id}}|{{context.tenant.data.plan}}|{{context.tenant.data.nested.name}}", "data":{"context":{"tenant":{"id":"acme","data":{"plan":"gold & <safe>","nested":{"name":"Team"}}}}}},
        {"template":"{{context.tenant.id}}", "data":{"context":{}}},
        {"template":"{{n}}/{{z}}/{{limit}}", "data":{"n":-42,"z":0,"limit":9007199254740991i64}}
    ]);
    let script = r#"const h=require('handlebars'); if (require('handlebars/package.json').version !== '4.7.10') process.exit(2); const x=JSON.parse(process.argv[1]); console.log(JSON.stringify(x.map(v=>h.compile(v.template)(v.data,{}).replace(/&#x27;/g, "'"))));"#;
    let result = Command::new("node")
        .arg("-e")
        .arg(script)
        .arg(fixtures.to_string())
        .output()
        .expect("node is required");
    assert!(
        result.status.success(),
        "reference render failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let reference: Vec<String> = serde_json::from_slice(&result.stdout).unwrap();
    let actual: Vec<String> = fixtures
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            novu_rust_contracts::template::render(v["template"].as_str().unwrap(), &v["data"])
                .unwrap()
        })
        .collect();
    assert_eq!(actual, reference);
    assert!(novu_rust_contracts::template::render("{{value}}", &json!({"value": 1.25})).is_err());
    let _: Value = fixtures;
}
