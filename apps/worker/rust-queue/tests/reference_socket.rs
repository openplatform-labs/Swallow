//! Execute the actual checked-out gateway's pure context matcher as an oracle.
use std::{fs, process::Command};
use uuid::Uuid;

#[test]
fn context_routing_matches_original_gateway() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../ws/src/socket/ws.gateway.ts"
    );
    let source = fs::read_to_string(path).unwrap();
    let start = source.find("  private isExactMatch(").unwrap();
    let end = source[start..]
        .find("  async sendUnreadCountToAllConnections")
        .unwrap()
        + start;
    let function =
        source[start..end].replacen("  private isExactMatch(", "function isExactMatch(", 1);
    let oracle = format!(
        "{function}\nconst c=JSON.parse(process.argv[2]); console.log(JSON.stringify(c.map(([a,b]) => isExactMatch(a,b))));\n"
    );
    let tmp = std::env::temp_dir().join(format!("novu-context-{}.ts", Uuid::new_v4()));
    fs::write(&tmp, oracle).unwrap();
    let cases: Vec<(Vec<String>, Vec<String>)> = vec![
        (vec![], vec![]),
        (vec![], vec!["a".into()]),
        (vec!["a".into(), "b".into()], vec!["b".into(), "a".into()]),
        (vec!["a".into()], vec!["b".into()]),
        (vec!["a".into(), "a".into()], vec!["a".into(), "b".into()]),
    ];
    let output = Command::new("node")
        .arg("--experimental-strip-types")
        .arg(&tmp)
        .arg(serde_json::to_string(&cases).unwrap())
        .output()
        .expect("node required");
    fs::remove_file(tmp).unwrap();
    assert!(
        output.status.success(),
        "reference failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reference: Vec<bool> = serde_json::from_slice(&output.stdout).unwrap();
    let actual: Vec<bool> = cases
        .iter()
        .map(|(a, b)| novu_rust_contracts::socket::context_matches(a, b))
        .collect();
    assert_eq!(actual, reference);
}
