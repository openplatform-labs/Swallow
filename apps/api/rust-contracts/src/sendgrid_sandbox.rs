//! Supported subset of SendGrid mailSettings, in the Novu SDK's camelCase.
use serde_json::Value;

pub fn enabled(value: &Value) -> Option<bool> {
    let settings = value.as_object()?;
    if settings.len() != 1 { return None; }
    let sandbox = settings.get("sandboxMode")?.as_object()?;
    if sandbox.len() != 1 { return None; }
    sandbox.get("enable")?.as_bool()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn only_explicit_boolean_sandbox_is_supported() {
        assert_eq!(enabled(&json!({"sandboxMode":{"enable":true}})), Some(true));
        assert_eq!(enabled(&json!({"sandboxMode":{"enable":false}})), Some(false));
        for input in [json!({}), json!({"sandboxMode":{}}), json!({"sandboxMode":{"enable":"true"}}), json!({"sandboxMode":{"enable":true,"extra":1}}), json!({"sandboxMode":{"enable":true},"footer":{}})] {
            assert_eq!(enabled(&input), None);
        }
    }
}
