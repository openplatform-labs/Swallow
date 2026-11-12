//! Compares timezone validation with the Novu reference Node/ICU runtime.
use std::process::Command;
#[test]
#[ignore = "requires Docker and node:22.23.4-alpine3.23"]
fn timezones_match_reference_node() {
    let names = std::fs::read_to_string(format!(
        "{}/../../api/rust-contracts/src/timezones-node22.txt",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let mut inputs: Vec<String> = names
        .lines()
        .flat_map(|s| [s.to_owned(), s.to_ascii_uppercase()])
        .collect();
    inputs.extend(
        [
            "UTC",
            "Asia/Seoul",
            "US/Eastern",
            "PST",
            "SystemV/EST5EDT",
            "+00",
            "+0530",
            "−05:00",
            "+23:59",
            "-2359",
            "+24",
            "+12:60",
            "+12:3",
            "GMT+05:00",
            "Factory",
            "Z",
            "invalid",
            "",
            "한글",
            " +05",
        ]
        .iter()
        .map(|s| s.to_string()),
    );
    for hour in 0..=25 {
        for minute in [0, 1, 30, 59, 60] {
            inputs.push(format!("+{hour:02}:{minute:02}"));
            inputs.push(format!("-{hour:02}{minute:02}"));
        }
    }
    let script = "const a=JSON.parse(process.argv[1]);process.stdout.write(JSON.stringify(a.map(z=>{try{Intl.DateTimeFormat('en',{timeZone:z});return true}catch{return false}})))";
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "node:22.23.4-alpine3.23",
            "node",
            "-e",
            script,
            &serde_json::to_string(&inputs).unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Vec<bool> = serde_json::from_slice(&output.stdout).unwrap();
    for (value, expected) in inputs.iter().zip(expected) {
        assert_eq!(
            novu_rust_contracts::timezone::valid(value),
            expected,
            "{value}"
        );
    }
}
