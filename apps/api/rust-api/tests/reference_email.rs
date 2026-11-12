//! Differential check against the reference lockfile's validator.js.
use std::process::Command;

#[test]
#[ignore = "requires NODE_PATH to contain validator 13.15.26"]
fn ascii_email_subset_matches_validator_js() {
    let locals = [
        "alice",
        "A+B",
        "a.b",
        "a..b",
        ".a",
        "a.",
        "a b",
        "a_b",
        "a!#$%&'*+-/=?^_`{|}~",
        "",
        "a@b",
    ];
    let domains = [
        "example.com",
        "example.test",
        "EXAMPLE.COM",
        "localhost",
        "bad..test",
        "-bad.test",
        "bad-.test",
        "example.123",
        "example.x",
        "example.xn--test",
        "exa_mple.test",
        "",
        "a.b.c",
    ];
    let mut cases = Vec::new();
    for local in locals {
        for domain in domains {
            cases.push(format!("{local}@{domain}"));
        }
    }
    cases.extend([
        format!("{}@example.test", "a".repeat(64)),
        format!("{}@example.test", "a".repeat(65)),
        format!("a@{}.test", "a".repeat(63)),
        format!("a@{}.test", "a".repeat(64)),
    ]);
    let output = Command::new("node").args(["-e", "const v=require('validator'); if(require('validator/package.json').version!=='13.15.26')process.exit(2); console.log(JSON.stringify(JSON.parse(process.argv[1]).map(x=>v.isEmail(x))));", &serde_json::to_string(&cases).unwrap()]).output().expect("node required");
    assert!(
        output.status.success(),
        "validator.js failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Vec<bool> = serde_json::from_slice(&output.stdout).unwrap();
    for (email, result) in cases.iter().zip(expected) {
        assert_eq!(
            novu_rust_contracts::email::validate(email),
            Some(result),
            "{email:?}"
        );
    }
    assert_eq!(
        novu_rust_contracts::email::validate("\"name\"@example.test"),
        None
    );
    assert_eq!(
        novu_rust_contracts::email::validate("한@example.test"),
        None
    );
}
