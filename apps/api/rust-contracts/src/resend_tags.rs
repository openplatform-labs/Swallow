//! Bounded user tags; the internal correlation name is reserved.
use serde_json::Value;
use std::collections::HashSet;

pub fn valid(value:&Value)->bool {
    let Some(items)=value.as_array() else {return false;};
    if items.len()>31 {return false;}
    let valid_text=|v:&str| !v.is_empty() && v.len()<=256 && v.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'||b==b'-');
    let mut names=HashSet::new();
    items.iter().all(|item| {
        let Some(item)=item.as_object() else {return false;};
        let (Some(name),Some(value))=(item.get("name").and_then(Value::as_str),item.get("value").and_then(Value::as_str)) else {return false;};
        item.len()==2 && valid_text(name) && valid_text(value) && !name.eq_ignore_ascii_case("swallow_dispatch") && names.insert(name.to_ascii_lowercase())
    })
}

#[cfg(test)]
mod tests {
    use super::valid;
    use serde_json::json;
    #[test]
    fn bounds_and_reserved_names() {
        assert!(valid(&json!([])));assert!(valid(&json!([{ "name":"campaign", "value":"summer-1" }])));
        assert!(!valid(&json!([{ "name":"Swallow_Dispatch", "value":"x" }])));
        assert!(!valid(&json!([{ "name":"a", "value":"x" },{ "name":"A", "value":"y" }])));
        assert!(!valid(&json!([{ "name":"a", "value":"x y" }])));
        assert!(!valid(&json!([{ "name":"a", "value":"x", "extra":true }])));
        assert!(!valid(&json!((0..32).map(|i|json!({"name":format!("tag{i}"),"value":"v"})).collect::<Vec<_>>())));
    }
}
