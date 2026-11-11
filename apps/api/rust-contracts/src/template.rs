//! Deliberately limited Handlebars-compatible variable interpolation.
//! Helpers, block syntax, arrays and numeric interpolation are not implemented.
use serde_json::Value;

fn valid_segment(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !matches!(s, "__proto__" | "prototype" | "constructor" | "this")
}

fn escape(value: &str, output: &mut String) {
    for c in value.chars() {
        match c {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            // Novu replaces Handlebars' &#x27; with an apostrophe after rendering.
            '\'' => output.push('\''),
            '`' => output.push_str("&#x60;"),
            '=' => output.push_str("&#x3D;"),
            _ => output.push(c),
        }
    }
}

/// Reject unsupported expressions rather than silently rendering them incorrectly.
pub fn render(template: &str, context: &Value) -> Result<String, &'static str> {
    let mut output = String::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        rest = &rest[start + 2..];
        let end = rest.find("}}").ok_or("unclosed expression")?;
        let path = rest[..end].trim();
        if path.is_empty()
            || !path.split('.').all(valid_segment)
            || matches!(
                path,
                "if" | "unless"
                    | "each"
                    | "with"
                    | "lookup"
                    | "log"
                    | "json"
                    | "equals"
                    | "titlecase"
                    | "uppercase"
                    | "lowercase"
                    | "pluralize"
                    | "dateFormat"
                    | "unique"
                    | "groupBy"
                    | "sortBy"
                    | "numberFormat"
                    | "i18n"
                    | "gt"
                    | "gte"
                    | "lt"
                    | "lte"
                    | "eq"
                    | "ne"
            )
        {
            return Err("unsupported expression");
        }
        let mut value = Some(context);
        for part in path.split('.') {
            value = match value {
                Some(Value::Object(object)) => object.get(part),
                Some(Value::Null) | None => None,
                _ => return Err("unsupported nested value"),
            };
        }
        match value {
            Some(Value::String(s)) => escape(s, &mut output),
            Some(Value::Bool(b)) => output.push_str(if *b { "true" } else { "false" }),
            Some(Value::Null) | None => (),
            _ => return Err("unsupported value"),
        }
        rest = &rest[end + 2..];
    }
    output.push_str(rest);
    Ok(output.replace("&#x27;", "'"))
}

/// The prototype supports subscriber IDs only and the default branding color.
pub fn render_in_app(
    template: &str,
    payload: &Value,
    subscriber_id: &str,
) -> Result<String, &'static str> {
    // Subscriber profiles and organization branding are not loaded by this prototype.
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        rest = &rest[start + 2..];
        let end = rest.find("}}").ok_or("unclosed expression")?;
        let path = rest[..end].trim();
        if (path.starts_with("subscriber.") && path != "subscriber.subscriberId")
            || (path.starts_with("branding.") && path != "branding.color")
        {
            return Err("unsupported subscriber or branding field");
        }
        rest = &rest[end + 2..];
    }
    let mut context = payload.as_object().ok_or("invalid payload")?.clone();
    context.insert(
        "subscriber".into(),
        serde_json::json!({"subscriberId": subscriber_id}),
    );
    context.insert("branding".into(), serde_json::json!({"color": "#f47373"}));
    render(template, &Value::Object(context))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn escapes_and_rejects_unsupported_syntax() {
        assert_eq!(
            render(
                "Hi {{name}} {{subscriber.subscriberId}}",
                &json!({"name":"a&<\"'=``", "subscriber":{"subscriberId":"s"}})
            )
            .unwrap(),
            "Hi a&amp;&lt;&quot;'&#x3D;&#x60;&#x60; s"
        );
        for template in [
            "{{{name}}}",
            "{{#if name}}yes{{/if}}",
            "{{upper name}}",
            "{{name",
            "{{__proto__.x}}",
        ] {
            assert!(render(template, &json!({})).is_err());
        }
        assert!(render("{{n}}", &json!({"n":1})).is_err());
        assert!(render("{{if}}", &json!({})).is_err());
        assert!(render_in_app("{{subscriber.firstName}}", &json!({}), "alice").is_err());
        assert!(render_in_app("{{branding.logo}}", &json!({}), "alice").is_err());
    }
}
