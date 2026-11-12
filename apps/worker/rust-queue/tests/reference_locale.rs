//! Compare locale grammar with Novu's validator.js dependency.
use std::process::Command;
#[test]
#[ignore = "requires NOVU_VALIDATOR_JS to point to validator's lib/isLocale.js"]
fn locale_matches_validator_js() {
    let path = std::env::var("NOVU_VALIDATOR_JS").unwrap();
    let mut inputs: Vec<String> = [
        "en-US",
        "ko-KR",
        "zh-Hant-TW",
        "en_US",
        "x-private",
        "en-US-u-ca-gregory",
        "i-klingon",
        "en-GB-oed",
        "invalid_locale",
        "a",
        "en--US",
        "EN-us",
        "und",
        "zh-cmn-Hans-CN",
        "x",
        "en-a-abc-x-foo",
        "en-u-a",
        "en-1234",
        "en-12[4",
        "en-ABCD-123",
        "en-1--2",
        "en-1---",
        "en-1--A",
        "en-1-_-",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let tags = [
        "en", "abc", "abcdef", "a", "ABCD", "US", "419", "1234", "foo", "abcde", "x", "u", "ca",
        "gregory", "A1", "1[23",
    ];
    for a in tags {
        for b in tags {
            for c in ["", "x-foo", "u-ca", "1234"] {
                inputs.push(format!(
                    "{a}-{b}{}",
                    if c.is_empty() {
                        String::new()
                    } else {
                        format!("-{c}")
                    }
                ));
            }
        }
    }
    let output=Command::new("node").args(["-e","const f=require(process.argv[1]);process.stdout.write(JSON.stringify(JSON.parse(process.argv[2]).map(f)));", &path,&serde_json::to_string(&inputs).unwrap()]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Vec<bool> = serde_json::from_slice(&output.stdout).unwrap();
    for (value, expected) in inputs.iter().zip(expected) {
        assert_eq!(
            novu_rust_contracts::locale::valid(value),
            expected,
            "{value}"
        );
    }
}
