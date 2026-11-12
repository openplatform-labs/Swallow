use novu_postgres_queue::twilio::{Config, Region, Sms, Twilio, request_body};
use serde_json::{Value, json};
use std::process::Command;

#[test]
#[ignore = "requires built pinned reference provider and node"]
fn original_twilio_provider_body() {
    let output = Command::new("node")
        .arg("tests/reference_twilio.cjs")
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
    let account = "AC00000000000000000000000000000001";
    let config = Config {
        account_sid: account.into(),
        auth_token: "fixture".into(),
        api_key_sid: None,
        api_key_secret: None,
        from: "+15550000001".into(),
        region: Region::Us,
    };
    for (index, (content, from)) in [
        ("Hello & 한글", None),
        ("line 1\nline 2", Some("+15550000003")),
        ("Hello", None),
    ]
    .into_iter()
    .enumerate()
    {
        let sms = Sms {
            to: "+15550000002".into(),
            content: content.into(),
            from: from.map(str::to_owned),
            workflow_context: None,
            provider_data: Default::default(),
        };
        let provider = Twilio::new(Config {
            account_sid: account.into(),
            auth_token: "fixture".into(),
            api_key_sid: None,
            api_key_secret: None,
            from: config.from.clone(),
            region: if index == 2 { Region::Eu } else { Region::Us },
        })
        .unwrap();
        assert_eq!(
            original[index],
            json!({"uri":provider.endpoint(), "data":request_body(&config, &sms).unwrap(),
                "username": if index == 2 { "SK00000000000000000000000000000002" } else { account },
                "password":"fixture", "receipt":{"id":"SM123", "date":"2026-10-28T10:00:00.000Z"}})
        );
    }
}
