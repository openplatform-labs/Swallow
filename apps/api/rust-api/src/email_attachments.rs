//! Bounded trigger attachments. Queue contents are sensitive and require retention controls.
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

const MAX_TOTAL: usize = 512 * 1024;

/// Returns normalized private attachments and removes bytes from the public payload.
pub(super) fn extract(payload: &mut Value) -> Result<Vec<Value>, ()> {
    let Some(object) = payload.as_object_mut() else {
        return Ok(Vec::new());
    };
    let Some(value) = object.get_mut("attachments") else {
        return Ok(Vec::new());
    };
    let items = value.as_array_mut().ok_or(())?;
    if items.len() > 10 {
        return Err(());
    }
    let mut total = 0usize;
    let mut normalized = Vec::with_capacity(items.len());
    for item in items {
        let fields = item.as_object_mut().ok_or(())?;
        if fields.keys().any(|key| {
            !["file", "name", "mime", "cid", "disposition", "channels"].contains(&key.as_str())
        }) {
            return Err(());
        }
        let name = fields
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 255 && !s.contains(['\r', '\n']))
            .ok_or(())?
            .to_owned();
        let mime = fields
            .get("mime")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 255 && !s.contains(['\r', '\n']))
            .ok_or(())?
            .to_owned();
        let optional = |key: &str| -> Result<Option<String>, ()> {
            match fields.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(v))
                    if !v.is_empty() && v.len() <= 255 && !v.contains(['\r', '\n']) =>
                {
                    Ok(Some(v.clone()))
                }
                _ => Err(()),
            }
        };
        let cid = optional("cid")?;
        let disposition = optional("disposition")?;
        if disposition
            .as_deref()
            .is_some_and(|s| !["inline", "attachment"].contains(&s))
        {
            return Err(());
        }
        if fields
            .get("channels")
            .is_some_and(|v| v.as_array().is_none_or(|a| a.len() != 1 || a[0] != "email"))
        {
            return Err(());
        }
        let file = fields.get("file").ok_or(())?;
        let bytes = if let Some(encoded) = file.as_str() {
            if encoded.len() > MAX_TOTAL * 2 {
                return Err(());
            }
            STANDARD.decode(encoded).map_err(|_| ())?
        } else {
            let buffer = file.as_object().ok_or(())?;
            if buffer.len() != 2 || buffer.get("type").and_then(Value::as_str) != Some("Buffer") {
                return Err(());
            }
            let values = buffer.get("data").and_then(Value::as_array).ok_or(())?;
            if values.len() > MAX_TOTAL {
                return Err(());
            }
            values
                .iter()
                .map(|v| v.as_u64().filter(|v| *v <= 255).map(|v| v as u8).ok_or(()))
                .collect::<Result<Vec<_>, _>>()?
        };
        total = total.checked_add(bytes.len()).ok_or(())?;
        if total > MAX_TOTAL {
            return Err(());
        }
        fields.remove("file");
        normalized.push(
            json!({"file":bytes,"name":name,"mime":mime,"cid":cid,"disposition":disposition}),
        );
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_both_file_encodings_and_removes_bytes_from_payload() {
        let mut payload = json!({"attachments":[{"name":"a.txt","mime":"text/plain","file":"aGVsbG8="},{"name":"b.bin","mime":"application/octet-stream","file":{"type":"Buffer","data":[0,255]},"channels":["email"]}]});
        let result = extract(&mut payload).unwrap();
        assert_eq!(result[0]["file"], json!([104, 101, 108, 108, 111]));
        assert_eq!(result[1]["file"], json!([0, 255]));
        assert!(
            payload["attachments"]
                .as_array()
                .unwrap()
                .iter()
                .all(|v| v.get("file").is_none())
        );
    }
    #[test]
    fn rejects_unbounded_or_unsupported_input() {
        for file in [
            json!(null),
            json!({"type":"Buffer","data":[256]}),
            json!("?"),
            json!("a".repeat(MAX_TOTAL * 2 + 1)),
        ] {
            assert!(
                extract(&mut json!({"attachments":[{"name":"a","mime":"text/plain","file":file}]}))
                    .is_err()
            );
        }
        assert!(extract(&mut json!({"attachments":[{"name":"a","mime":"text/plain","file":"YQ==","channels":["sms"]}]})).is_err());
    }
    #[test]
    fn enforces_total_decoded_size_and_count() {
        let large = STANDARD.encode(vec![7u8; MAX_TOTAL]);
        let mut within = json!({"attachments":[{"name":"a","mime":"text/plain","file":large}]});
        assert_eq!(
            extract(&mut within).unwrap()[0]["file"]
                .as_array()
                .unwrap()
                .len(),
            MAX_TOTAL
        );
        let mut over = json!({"attachments":[{"name":"a","mime":"text/plain","file":large},{"name":"b","mime":"text/plain","file":"YQ=="}]});
        assert!(extract(&mut over).is_err());
        let mut many =
            json!({"attachments": vec![json!({"name":"a","mime":"text/plain","file":""}); 11]});
        assert!(extract(&mut many).is_err());
    }
}
