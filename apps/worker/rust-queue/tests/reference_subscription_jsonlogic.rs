//! Differential tests of subscription condition decisions against json-logic-js.
use serde_json::{Value, json};
use std::process::Command;

#[test]
#[ignore = "requires NOVU_REFERENCE_SOURCE to point to the built Novu checkout"]
fn subscription_conditions_match_reference() {
    let root = std::env::var("NOVU_REFERENCE_SOURCE").unwrap();
    let variables = json!({"payload":{"tier":"gold","amount":5,"flag":true,"list":[1,2]},"subscriber":{"subscriberId":"a","data":{"plan":"premium"}},"actor":{"firstName":"Taylor"},"context":{"tenant":{"id":"one","data":{"active":true}}}});
    let conditions = vec![
        json!(true),
        json!(false),
        json!(1),
        json!("text"),
        json!({"===":[{"var":"payload.tier"},"gold"]}),
        json!({"===":[{"var":"subscriber.data.plan"},"premium"]}),
        json!({"===":[{"var":"actor.firstName"},"Taylor"]}),
        json!({"===":[{"var":"context.tenant.id"},"one"]}),
        json!({"and":[{">=":[{"var":"payload.amount"},5]},{"var":"payload.flag"}]}),
        json!({"or":[false,{"===":[1,1]}]}),
        json!({"!":[{"var":"payload.flag"}]}),
        json!({"in":[2,{"var":"payload.list"}]}),
        json!({"missing":["payload.absent"]}),
        json!({"if":[{"var":"payload.flag"},true,false]}),
        json!({"===":[{"var":"payload.absent"},null]}),
        json!({"nonexistent":[1]}),
    ];
    let js = r#"const logic=require(process.argv[1]+'/apps/worker/node_modules/json-logic-js');const cases=JSON.parse(process.argv[2]),data=JSON.parse(process.argv[3]);console.log(JSON.stringify(cases.map(x=>{try{const r=logic.apply(x,data);return typeof r==='boolean'?r:false}catch(_){return false}})));"#;
    let output = Command::new("node")
        .args([
            "-e",
            js,
            &root,
            &serde_json::to_string(&conditions).unwrap(),
            &variables.to_string(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Vec<bool> = serde_json::from_slice(&output.stdout).unwrap();
    for (condition, expected) in conditions.iter().zip(expected) {
        let preference = json!({"all":{"enabled":false,"condition":condition}});
        assert_eq!(
            novu_postgres_queue::in_app::topic_preference_passes(Some(&preference), &variables),
            expected,
            "{condition}"
        );
    }
    assert!(novu_postgres_queue::in_app::topic_preference_passes(
        None,
        &Value::Null
    ));
    assert!(!novu_postgres_queue::in_app::topic_preference_passes(
        Some(&json!({"all":{"enabled":false}})),
        &Value::Null
    ));
}
