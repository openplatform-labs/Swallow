//! Message payload normalization matching the observed Mongoose storage behavior.
use serde_json::Value;

/// Remove email attachments and recursively omit empty object-valued properties.
/// Empty objects in arrays remain as array elements.
pub fn in_app_payload(payload: &Value) -> Value {
    fn minimize(value: &mut Value) {
        match value {
            Value::Object(map) => {
                for child in map.values_mut() {
                    minimize(child);
                }
                map.retain(|_, child| !child.as_object().is_some_and(|object| object.is_empty()));
            }
            Value::Array(items) => {
                for item in items {
                    minimize(item);
                }
            }
            _ => {}
        }
    }
    let mut message_payload = payload.clone();
    if let Some(object) = message_payload.as_object_mut() {
        object.remove("attachments");
    }
    minimize(&mut message_payload);
    message_payload
}

#[cfg(test)]
mod tests {
    use super::in_app_payload;
    use serde_json::json;

    #[test]
    fn strips_attachments_and_minimizes_objects_without_dropping_array_items() {
        let payload = json!({
            "attachments": [{"file": "secret"}],
            "nested": {"empty": {}, "kept": 3},
            "items": [{}, {"empty": {}, "kept": true}],
        });
        assert_eq!(
            in_app_payload(&payload),
            json!({
                "nested": {"kept": 3},
                "items": [{}, {"kept": true}],
            })
        );
        assert!(payload.get("attachments").is_some());
    }
}
