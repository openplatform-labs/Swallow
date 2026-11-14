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
    ];
    for (index, plan) in plans.iter().enumerate() {
        assert_eq!(wire_messages(plan).unwrap(), vec![original[index].clone()]);
    }
}
