//! Configuration safety is checked before a database connection or binding.
use std::process::{Command, Output};

fn run(policy: &str, strict: &str, checks: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"));
    command
        .env_clear()
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", policy)
        .env("SWALLOW_ALERT_REQUIRE_DISTINCT_SHARED_CHECKS", strict)
        .args(["env", "300", "120", "60"]);
    for (name, value) in checks {
        command.env(name, value);
    }
    command.output().unwrap()
}
#[test]
fn strict_new_condition_rejects_each_overlapping_pair_before_connection() {
    let checks = [
        ("SWALLOW_ALERT_FAILED_JOBS_ENABLED", "true"),
        ("SWALLOW_ALERT_RESEND_AUTO_GRACE_SECONDS", "300"),
        ("SWALLOW_ALERT_SLACK_COOLDOWN_GRACE_SECONDS", "300"),
    ];
    for i in 0..checks.len() {
        for j in i + 1..checks.len() {
            let result = run("new-condition", "true", &[checks[i], checks[j]]);
            assert!(!result.status.success());
            assert!(
                String::from_utf8_lossy(&result.stderr)
                    .contains("separate invocations and target IDs")
            );
        }
    }
    let single = run("new-condition", "true", &[checks[0]]);
    assert!(!String::from_utf8_lossy(&single.stderr).contains("separate invocations"));
    for (policy, strict) in [("new-condition", "false"), ("destination", "true")] {
        let result = run(policy, strict, &[checks[0], checks[1]]);
        assert!(!String::from_utf8_lossy(&result.stderr).contains("separate invocations"));
    }
    assert!(!run("new-condition", "yes", &[]).status.success());
}
