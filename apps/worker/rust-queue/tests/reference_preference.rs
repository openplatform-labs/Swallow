//! Differential comparison of bounded preference merges against original Novu.
use serde_json::{Value, json};
use std::process::Command;
#[test]
#[ignore = "requires NOVU_REFERENCE_GENERIC to point to the built reference application-generic package"]
fn global_and_readonly_match_reference_merge() {
    let base = std::env::var("NOVU_REFERENCE_GENERIC").unwrap();
    let mut cases = Vec::new();
    for read_only in [false, true] {
        for all in [false, true] {
            for channel in [None, Some(false), Some(true)] {
                for subscriber in [None, Some(false), Some(true)] {
                    for workflow_subscriber in [None, Some(false), Some(true)] {
                        cases.push(json!({"readOnly":read_only,"all":all,"channel":channel,"subscriber":subscriber,"workflowSubscriber":workflow_subscriber}));
                    }
                }
            }
        }
    }
    let js = r#"const base=process.argv[1],a=JSON.parse(process.argv[2]);const {MergePreferences}=require(base+'/build/main/usecases/merge-preferences/merge-preferences.usecase.js');const {buildWorkflowPreferences}=require(base+'/node_modules/@novu/shared');const out=a.map(x=>{const p={all:{enabled:x.all,readOnly:x.readOnly},channels:{}};if(x.channel!==null)p.channels.in_app={enabled:x.channel};const c={workflowResourcePreference:{type:'WORKFLOW_RESOURCE',preferences:p}};if(x.subscriber!==null)c.subscriberGlobalPreference={type:'SUBSCRIBER_GLOBAL',preferences:{channels:{in_app:{enabled:x.subscriber}}}};if(x.workflowSubscriber!==null)c.subscriberWorkflowPreference={type:'SUBSCRIBER_WORKFLOW',preferences:{channels:{in_app:{enabled:x.workflowSubscriber}}}};const m=MergePreferences.execute(c);return buildWorkflowPreferences(m.preferences).channels.in_app.enabled});process.stdout.write(JSON.stringify(out));"#;
    let output = Command::new("node")
        .args(["-e", js, &base, &serde_json::to_string(&cases).unwrap()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Vec<bool> = serde_json::from_slice(&output.stdout).unwrap();
    for (case, expected) in cases.iter().zip(expected) {
        let boolean = |key: &str| case.get(key).and_then(Value::as_bool);
        let actual = novu_rust_contracts::preference::in_app_enabled(
            boolean("readOnly"),
            boolean("all"),
            boolean("channel"),
            boolean("subscriber"),
            boolean("workflowSubscriber"),
        );
        assert_eq!(actual, expected, "{case}");
    }
}
