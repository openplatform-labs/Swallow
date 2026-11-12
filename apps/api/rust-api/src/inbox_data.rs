//! Normalize Novu's Inbox data filter to AND-ed paths and OR groups.
use serde_json::{Value, json};

pub fn normalize(input: &Value) -> Option<Value> {
    fn scalar(value: &Value) -> bool {
        value.is_null()
            || value.is_boolean()
            || value.is_number()
            || value
                .as_str()
                .is_some_and(|s| s.encode_utf16().count() <= 256)
    }
    fn or_group(value: &Value, empty: bool) -> Option<Vec<Value>> {
        let values = value.as_array()?;
        if values.len() > 100 || (!empty && values.is_empty()) || !values.iter().all(scalar) {
            return None;
        }
        Some(values.clone())
    }
    fn groups(value: &Value) -> Option<Vec<Vec<Value>>> {
        if value.is_array() {
            let group = or_group(value, true)?;
            return Some(if group.is_empty() {
                vec![]
            } else {
                vec![group]
            });
        }
        if let Some(object) = value.as_object() {
            if object.len() != 1 {
                return None;
            }
            if let Some(or) = object.get("or") {
                let group = or_group(or, true)?;
                return Some(if group.is_empty() {
                    vec![]
                } else {
                    vec![group]
                });
            }
            let and = object.get("and")?.as_array()?;
            if and.len() > 30 {
                return None;
            }
            let mut result = Vec::new();
            for group in and {
                let group = group.as_object()?;
                if group.len() != 1 {
                    return None;
                }
                result.push(or_group(group.get("or")?, false)?);
            }
            if result.iter().map(Vec::len).sum::<usize>() > 200 {
                return None;
            }
            return Some(result);
        }
        if scalar(value) {
            Some(vec![vec![value.clone()]])
        } else {
            None
        }
    }
    fn flatten(
        object: &serde_json::Map<String, Value>,
        prefix: &[String],
        depth: usize,
        out: &mut Vec<Value>,
    ) -> Option<()> {
        if depth > 2 {
            return None;
        }
        for (key, value) in object {
            if key.is_empty()
                || key.starts_with(['$', '.'])
                || ["__proto__", "constructor", "prototype"].contains(&key.as_str())
            {
                return None;
            }
            let mut path = prefix.to_vec();
            path.extend(key.split('.').map(str::to_owned));
            if let Some(nested) = value
                .as_object()
                .filter(|o| !o.contains_key("or") && !o.contains_key("and"))
            {
                flatten(nested, &path, depth + 1, out)?;
            } else {
                let groups = groups(value)?;
                if !groups.is_empty() {
                    out.push(json!({"path":path,"groups":groups}));
                }
            }
        }
        Some(())
    }
    if input.is_null() {
        return Some(json!([]));
    }
    let mut entries = Vec::new();
    flatten(input.as_object()?, &[], 1, &mut entries)?;
    Some(json!(entries))
}
