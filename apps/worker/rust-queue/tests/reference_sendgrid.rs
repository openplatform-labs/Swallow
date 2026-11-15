use novu_postgres_queue::sendgrid::{Alternative, Attachment, Config, Email, Region, request_body};
use serde_json::{Value, json};
use std::process::Command;

#[test]
#[ignore = "requires built pinned reference provider and node"]
fn original_sendgrid_wire_body() {
    let source = std::env::var("NOVU_REFERENCE_SOURCE").unwrap();
    let output = Command::new("node")
        .arg("tests/reference_sendgrid.cjs")
        .env("NOVU_REFERENCE_SOURCE", source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let original: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let config = Config {
        api_key: "SG.fixture".into(),
        from: "sender@example.test".into(),
        sender_name: "Sender".into(),
        region: Region::Global,
    };
    let plain = Email {
        to: vec!["a@example.test".into()],
        subject: "Hi".into(),
        html: "<p>Hi</p>".into(),
        ..Email::default()
    };
    let empty = Email {
        to: vec!["a@example.test".into()],
        subject: "Hi".into(),
        html: "<p>Hi</p>".into(),
        message_id: Some("".into()),
        transaction_id: Some("".into()),
        workflow_identifier: Some("".into()),
        subscriber_id: Some("".into()),
        ..Email::default()
    };
    let basic = Email {
        to: vec!["a@example.test".into()],
        subject: "Hi".into(),
        html: "<p>Hi</p>".into(),
        message_id: Some("message123".into()),
        transaction_id: Some("txn1".into()),
        workflow_identifier: Some("greeting".into()),
        subscriber_id: Some("sub1".into()),
        ..Email::default()
    };
    let full = Email {
        to: vec!["a@example.test".into(), "b@example.test".into()],
        cc: vec!["cc@example.test".into()],
        bcc: vec!["bcc@example.test".into()],
        from: Some("alt@example.test".into()),
        sender_name: Some("Alias".into()),
        reply_to: Some("reply@example.test".into()),
        subject: "Hi".into(),
        html: "<p>Hi</p>".into(),
        headers: json!({"X-Test":"yes"}).as_object().unwrap().clone(),
        custom_data: json!({"source":"example"}).as_object().unwrap().clone(),
        message_id: Some("m1".into()),
        transaction_id: Some("txn1".into()),
        workflow_identifier: Some("greeting".into()),
        subscriber_id: Some("sub1".into()),
        ..Email::default()
    };
    let dynamic = Email {
        to: vec!["a@example.test".into()],
        subject: "Hi".into(),
        html: "<p>Hi</p>".into(),
        custom_data:
            json!({"templateId":"d-123","dynamicTemplateData":{"name":"Lee"},"source":"example"})
                .as_object()
                .unwrap()
                .clone(),
        ..Email::default()
    };
    let alternative = Email {
        to: vec!["a@example.test".into()],
        subject: "Hi".into(),
        html: "<p>Hi</p>".into(),
        text: Some("Plain".into()),
        alternatives: vec![Alternative {
            content_type: "text/calendar".into(),
            content: "Meeting".into(),
        }],
        ..Email::default()
    };
    let multi_alternatives = Email {
        to: vec!["a@example.test".into()], subject: "Hi".into(), html: "<p>Hi</p>".into(),
        alternatives: vec![Alternative { content_type: "text/calendar; method=REQUEST".into(), content: "BEGIN:VCALENDAR".into() }, Alternative { content_type: "text/x-amp-html".into(), content: "<html amp4email></html>".into() }],
        ..Email::default()
    };
    let empty_alternative = Email {
        to: vec!["a@example.test".into()], subject: "Hi".into(), html: "<p>Hi</p>".into(), text: Some("".into()),
        alternatives: vec![Alternative { content_type: "text/calendar".into(), content: "".into() }],
        ..Email::default()
    };
    let attachment = Email {
        to: vec!["a@example.test".into()],
        subject: "Hi".into(),
        html: "<p>Hi</p>".into(),
        attachments: vec![
            Attachment {
                file: b"hello".to_vec(),
                name: "a.txt".into(),
                mime: "text/plain".into(),
                cid: None,
                disposition: None,
            },
            Attachment {
                file: vec![0, 1, 255],
                name: "img.png".into(),
                mime: "image/png".into(),
                cid: Some("img".into()),
                disposition: None,
            },
            Attachment {
                file: b"hi".to_vec(),
                name: "x.txt".into(),
                mime: "text/plain".into(),
                cid: Some("x".into()),
                disposition: Some("attachment".into()),
            },
        ],
        ..Email::default()
    };
    let tracking = |value: Value| Email {
        to: vec!["a@example.test".into()], subject: "Hi".into(), html: "<p>Hi</p>".into(),
        tracking_settings: value.as_object().unwrap().clone(), ..Email::default()
    };
    assert_eq!(
        vec![
            request_body(&config, &plain).unwrap(),
            request_body(&config, &empty).unwrap(),
            request_body(&config, &basic).unwrap(),
            request_body(&config, &full).unwrap(),
            request_body(&config, &dynamic).unwrap(),
            request_body(&config, &alternative).unwrap(),
            request_body(&config, &multi_alternatives).unwrap(),
            request_body(&config, &empty_alternative).unwrap(),
            request_body(&config, &attachment).unwrap(),
            request_body(&config, &tracking(json!({"clickTracking":{"enable":true,"enableText":false},"openTracking":{"enable":true,"substitutionTag":"%open%"}}))).unwrap(),
            request_body(&config, &tracking(json!({"subscriptionTracking":{"enable":true,"text":"unsubscribe","html":"<p>unsubscribe</p>","substitutionTag":"%unsub%"},"ganalytics":{"enable":true,"utmSource":"source","utmMedium":"email","utmTerm":"term","utmContent":"content","utmCampaign":"campaign"}}))).unwrap(),
            request_body(&config, &tracking(json!({"clickTracking":{"enable":false,"enableText":true},"openTracking":{"enable":true},"subscriptionTracking":{"enable":true,"text":"bye"}}))).unwrap()
        ],
        original
    );
}

#[test]
#[ignore = "requires original source, esbuild, and isolated SendGrid dependencies"]
fn original_sendgrid_ip_pool_precedence() {
    use novu_postgres_queue::sendgrid::request_body_with_default_pool;
    let output = Command::new("node")
        .arg("tests/reference_sendgrid_ip_pool.cjs")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let original: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let config = Config {
        api_key: "SG.fixture".into(),
        from: "sender@example.test".into(),
        sender_name: "Sender".into(),
        region: Region::Global,
    };
    let mut email = Email {
        to: vec!["a@example.test".into()],
        subject: "Hi".into(),
        html: "<p>Hi</p>".into(),
        ..Email::default()
    };
    let mut actual = Vec::new();
    let pool = |email: &Email, default| {
        request_body_with_default_pool(&config, email, default)
            .unwrap()
            .get("ip_pool_name")
            .cloned()
            .unwrap_or(Value::Null)
    };
    actual.push(pool(&email, None));
    actual.push(pool(&email, Some("default-pool")));
    email.ip_pool_name = Some("override-pool".into());
    actual.push(pool(&email, Some("default-pool")));
    email.ip_pool_name = Some(String::new());
    actual.push(pool(&email, Some("default-pool")));
    email.ip_pool_name = Some("provider-pool".into());
    email.ip_pool_explicit_provider = true;
    actual.push(pool(&email, Some("default-pool")));
    email.ip_pool_name = Some(String::new());
    actual.push(pool(&email, Some("default-pool")));
    assert_eq!(actual, original);
}
