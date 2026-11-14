use novu_postgres_queue::fcm::{Plan, wire_messages};
use serde_json::{Value, json};
use std::process::Command;

#[test]
#[ignore = "requires built pinned firebase-admin and node"]
fn original_firebase_http_conversion() {
    let output = Command::new("node")
        .arg("tests/reference_fcm_wire.cjs")
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
    let plans = [
        Plan {
            multicast: false,
            message: json!({"token":"device-a","notification":{"title":"Hi","body":"Hello","imageUrl":"https://example.test/a.png"},"data":{"key":"value"}}),
        },
        Plan {
            multicast: false,
            message: json!({"topic":"orders","data":{"title":"Hi","message":"Hello"}}),
        },
        Plan {
            multicast: false,
            message: json!({"condition":"'a' in topics","notification":{"title":"Hi","body":"Hello"},"data":{}}),
        },
        Plan {
            multicast: true,
            message: json!({"tokens":["device-b"],"notification":{"title":"Hi","body":"Hello"},"data":{}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"device-a","android":{"priority":"high","ttl":1501,"collapseKey":"orders","restrictedPackageName":"com.example","directBootOk":true,"data":{"kind":"update"}}}),
        },
        Plan {
            multicast: false,
            message: json!({"topic":"news","webpush":{"headers":{"Urgency":"high"},"data":{"key":"value"},"notification":{"title":"News","icon":"https://example.test/icon.png"},"fcmOptions":{"link":"https://example.test/"}},"fcmOptions":{"analyticsLabel":"campaign"}}),
        },
        Plan {
            multicast: true,
            message: json!({"tokens":["device-b"],"android":{"ttl":0,"priority":"normal"},"webpush":{"headers":{"TTL":"60"}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"ios-a","apns":{"headers":{"apns-priority":"10"},"payload":{"aps":{"alert":{"title":"Hi","locKey":"KEY","locArgs":["one"],"titleLocKey":"TITLE","titleLocArgs":["two"],"launchImage":"splash"},"badge":2,"sound":{"name":"alert.aiff","critical":true,"volume":0.5},"contentAvailable":true,"mutableContent":false,"threadId":"thread"},"custom":{"order":5}},"fcmOptions":{"imageUrl":"https://example.test/image.png","analyticsLabel":"campaign"}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"ios-b","apns":{"payload":{"aps":{"alert":"Hi","sound":"default","contentAvailable":false,"mutableContent":true}},"headers":{"apns-push-type":"alert"}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"android-a","android":{"notification":{"title":"Hello","body":"World","icon":"bell","sound":"default","color":"#AAbbCC","tag":"tag","clickAction":"OPEN","channelId":"orders","bodyLocKey":"BODY","bodyLocArgs":["one"],"titleLocKey":"TITLE","titleLocArgs":["two"],"imageUrl":"https://example.test/img.png","localOnly":true,"priority":"high","visibility":"public","proxy":"allow","vibrateTimingsMillis":[0,1501],"defaultVibrateTimings":true,"defaultSound":false,"defaultLightSettings":true,"notificationCount":3}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"led-a","android":{"notification":{"lightSettings":{"color":"#FF0080","lightOnDurationMillis":1001,"lightOffDurationMillis":0}}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"led-b","android":{"notification":{"lightSettings":{"color":"#11223344","lightOnDurationMillis":0,"lightOffDurationMillis":2500}}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"sticky-a","android":{"notification":{"sticky":true}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"sticky-b","android":{"notification":{"sticky":false}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"event-a","android":{"notification":{"eventTime":"2026-11-14T12:34:56.123Z"}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"event-b","android":{"notification":{"eventTime":"2026-11-14T21:34:56.999987+09:00"}}}),
        },
        Plan {
            multicast: false,
            message: json!({"token":"event-c","android":{"notification":{"eventTime":"1969-12-31T23:59:59Z"}}}),
        },
    ];
    for (index, plan) in plans.iter().enumerate() {
        assert_eq!(wire_messages(plan).unwrap(), vec![original[index].clone()]);
    }
}
