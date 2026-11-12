//! Supported subset of Novu's individual subscriber fan-out.
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Separate supported topic recipients from direct subscribers. The reference
/// unions exclusions across all topics but keeps explicitly addressed users.
pub fn split(input: &Value) -> Result<(Vec<Value>, Vec<String>, Vec<String>), &'static str> {
    let values: Vec<&Value> = match input {
        Value::Array(values) if !values.is_empty() => values.iter().collect(),
        Value::Array(_) => return Err("empty recipients"),
        value => vec![value],
    };
    let mut direct = Vec::new();
    let mut topics = Vec::new();
    let mut exclusions = Vec::new();
    for value in values {
        if value.get("type").and_then(Value::as_str) == Some("Topic") {
            let object = value.as_object().ok_or("invalid topic")?;
            if object
                .keys()
                .any(|key| !["type", "topicKey", "exclude"].contains(&key.as_str()))
            {
                return Err("unsupported topic field");
            }
            let key = object
                .get("topicKey")
                .and_then(Value::as_str)
                .filter(|key| !key.is_empty())
                .ok_or("invalid topic")?;
            if !topics.iter().any(|existing| existing == key) {
                topics.push(key.to_owned());
            }
            if let Some(value) = object.get("exclude").filter(|value| !value.is_null()) {
                let list = value
                    .as_array()
                    .filter(|list| list.len() <= 100)
                    .ok_or("invalid exclusions")?;
                for item in list {
                    let id = item.as_str().ok_or("invalid exclusion")?;
                    if !exclusions.iter().any(|existing| existing == id) {
                        exclusions.push(id.to_owned());
                    }
                }
            }
        } else {
            direct.push(value.clone());
        }
    }
    let direct = if direct.is_empty() {
        Vec::new()
    } else {
        normalize(&Value::Array(direct))?
    };
    Ok((direct, topics, exclusions))
}

/// Convert individual string/object recipients to profiles and deduplicate by
/// subscriberId, preserving first key position and last supplied recipient.
/// Topic recipients and channel credentials are not implemented.
pub fn normalize(input: &Value) -> Result<Vec<Value>, &'static str> {
    let list: Vec<&Value> = match input {
        Value::Array(values) if !values.is_empty() => values.iter().collect(),
        Value::Array(_) => return Err("empty recipients"),
        other => vec![other],
    };
    let mut index = HashMap::<String, usize>::new();
    let mut result = Vec::new();
    for item in list {
        let mut profile = Map::new();
        let id = match item {
            Value::String(id) => id.as_str(),
            Value::Object(object) => {
                if object.keys().any(|key| {
                    ![
                        "subscriberId",
                        "type",
                        "firstName",
                        "lastName",
                        "email",
                        "phone",
                        "avatar",
                        "locale",
                        "timezone",
                        "data",
                    ]
                    .contains(&key.as_str())
                }) || object
                    .get("type")
                    .is_some_and(|v| v.as_str() != Some("Subscriber"))
                {
                    return Err("unsupported recipient field");
                }
                for field in [
                    "firstName",
                    "lastName",
                    "email",
                    "phone",
                    "avatar",
                    "locale",
                    "timezone",
                    "data",
                ] {
                    if let Some(value) = object.get(field) {
                        if !value.is_null()
                            && if field == "data" {
                                !value.is_object()
                            } else {
                                !value.is_string()
                            }
                        {
                            return Err("unsupported recipient field");
                        }
                        if field == "email"
                            && !value.is_null()
                            && value.as_str().and_then(crate::email::validate) != Some(true)
                        {
                            return Err("invalid or unsupported email");
                        }
                        if field == "locale"
                            && !value.is_null()
                            && !value.as_str().is_some_and(crate::locale::valid)
                        {
                            return Err("invalid locale");
                        }
                        if field == "timezone"
                            && !value.is_null()
                            && !value.as_str().is_some_and(crate::timezone::valid)
                        {
                            return Err("invalid timezone");
                        }
                        profile.insert(field.into(), value.clone());
                    }
                }
                object
                    .get("subscriberId")
                    .and_then(Value::as_str)
                    .ok_or("invalid subscriber id")?
            }
            _ => return Err("unsupported recipient"),
        };
        // Trimming has downstream Novu-specific effects and is not implemented yet.
        if id.is_empty() || id.trim() != id {
            return Err("invalid or unsupported subscriber id");
        }
        profile.insert("subscriberId".into(), Value::String(id.to_owned()));
        let profile = Value::Object(profile);
        if let Some(position) = index.get(id) {
            result[*position] = profile;
        } else {
            index.insert(id.to_owned(), result.len());
            result.push(profile);
        }
    }
    Ok(result)
}
