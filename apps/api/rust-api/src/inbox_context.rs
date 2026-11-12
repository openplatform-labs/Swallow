//! Context identity and the reference's OLPC canonical JSON HMAC encoding.
use serde_json::{Value, json};

pub struct ParsedContext {
    pub keys: Vec<String>,
    pub entries: Value,
    pub canonical: Option<String>,
}

pub fn parse(value: &Value) -> Option<ParsedContext> {
    let object = value.as_object()?;
    if object.is_empty() || object.len() > 5 {
        return None;
    }
    let valid = |value: &str| {
        !value.is_empty()
            && value.len() <= 100
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    };
    let mut entries = Vec::new();
    let mut keys = Vec::new();
    for (kind, value) in object {
        if !valid(kind) {
            return None;
        }
        let (id, data) = match value {
            Value::String(id) => (id.as_str(), None),
            Value::Object(fields) => {
                let id = fields.get("id")?.as_str()?;
                let data = fields.get("data");
                if data.is_some_and(|d| !d.is_object() && !d.is_array()) {
                    return None;
                }
                (id, data)
            }
            _ => return None,
        };
        if !valid(id) {
            return None;
        }
        let key = format!("{kind}:{id}");
        keys.push(key.clone());
        entries.push(json!({"key":key,"data":data}));
    }
    keys.sort();
    Some(ParsedContext {
        keys,
        entries: Value::Array(entries),
        canonical: canonicalize(value),
    })
}

// @tufjs/canonical-json escapes only quotes and backslashes and sorts object
// keys in JavaScript's UTF-16 order. Floats cannot be encoded by the reference.
// JavaScript treats all JSON numbers as IEEE-754 binary64 values.
fn canonicalize(value: &Value) -> Option<String> {
    fn quoted(value: &str) -> String {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    }
    match value {
        Value::Null => Some("null".into()),
        Value::Bool(v) => Some(v.to_string()),
        Value::String(v) => Some(quoted(v)),
        Value::Number(v) => {
            let number = v.as_f64()?;
            if !number.is_finite() || number.fract() != 0.0 {
                return None;
            }
            Some(js_integer(number))
        }
        Value::Array(values) => Some(format!(
            "[{}]",
            values
                .iter()
                .map(canonicalize)
                .collect::<Option<Vec<_>>>()?
                .join(",")
        )),
        Value::Object(values) => {
            let mut keys: Vec<_> = values.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            let fields = keys
                .into_iter()
                .map(|key| Some(format!("{}:{}", quoted(key), canonicalize(&values[key])?)))
                .collect::<Option<Vec<_>>>()?;
            Some(format!("{{{}}}", fields.join(",")))
        }
    }
}

/// Convert an integer-valued binary64 to JSON.stringify's decimal notation.
fn js_integer(value: f64) -> String {
    if value == 0.0 {
        return "0".into();
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let mut buffer = ryu::Buffer::new();
    let text = buffer.format_finite(value.abs());
    let (mantissa, exponent) = match text.split_once('e') {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().expect("ryu exponent")),
        None => (text, 0),
    };
    let decimal = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    let point = decimal + exponent;
    if point > 21 {
        let first = &digits[..1];
        let remainder = &digits[1..];
        if remainder.is_empty() {
            format!("{sign}{first}e+{}", point - 1)
        } else {
            format!("{sign}{first}.{remainder}e+{}", point - 1)
        }
    } else if point as usize >= digits.len() {
        format!(
            "{sign}{digits}{}",
            "0".repeat(point as usize - digits.len())
        )
    } else {
        format!(
            "{sign}{}.{}",
            &digits[..point as usize],
            &digits[point as usize..]
        )
    }
}

#[cfg(test)]
mod tests {
    use super::parse;
    use serde_json::json;
    #[test]
    fn canonicalizes_and_sorts_context_keys() {
        let parsed = parse(&json!({"tenant":{"id":"acme","data":{"quote":"a\"b","newline":"a\nb","count":1}}, "app":"jira"})).unwrap();
        assert_eq!(parsed.keys, ["app:jira", "tenant:acme"]);
        assert_eq!(
            parsed.canonical.unwrap(),
            "{\"app\":\"jira\",\"tenant\":{\"data\":{\"count\":1,\"newline\":\"a\nb\",\"quote\":\"a\\\"b\"},\"id\":\"acme\"}}"
        );
    }
    #[test]
    fn rejects_invalid_contexts_and_cannot_sign_floats() {
        for value in [
            json!({}),
            json!({"tenant":{"id":""}}),
            json!({"tenant":"a:b"}),
            json!({"tenant":{"id":"x","data":null}}),
            json!({"a":"1","b":"2","c":"3","d":"4","e":"5","f":"6"}),
        ] {
            assert!(parse(&value).is_none());
        }
        assert!(
            parse(&json!({"tenant":{"id":"x","data":{"x":1.5}}}))
                .unwrap()
                .canonical
                .is_none()
        );
    }

    #[test]
    fn formats_large_integers_as_javascript() {
        for (value, expected) in [
            (9007199254740992.0, "9007199254740992"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (1.0000000000000001e18, "1000000000000000100"),
            (-0.0, "0"),
        ] {
            assert_eq!(super::js_integer(value), expected);
        }
    }

    #[test]
    #[ignore = "requires NOVU_CANONICAL_JSON to point to the reference package"]
    fn integer_formats_match_reference() {
        let package = std::env::var("NOVU_CANONICAL_JSON").unwrap();
        let mut state = 0x874c_aa32_e56d_2831u64;
        let mut numbers = vec![
            1e20,
            1e21,
            1.0000000000000001e18,
            f64::MAX,
            9007199254740992.0,
            -0.0,
        ];
        for _ in 0..5000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let x = f64::from_bits(state);
            if x.is_finite() && x.fract() == 0.0 {
                numbers.push(x);
            }
        }
        let input = serde_json::to_string(&numbers).unwrap();
        let output = std::process::Command::new("node").args(["-e", "const c=require(process.argv[1]); const a=JSON.parse(process.argv[2]); process.stdout.write(JSON.stringify(a.map(n=>c.canonicalize(n))));", &package, &input]).output().unwrap();
        assert!(output.status.success());
        let expected: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
        let actual: Vec<String> = numbers.into_iter().map(super::js_integer).collect();
        for (actual, expected) in actual.iter().zip(expected.iter()) {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    #[ignore = "requires NOVU_CANONICAL_JSON to point to the reference package"]
    fn matches_reference_canonical_json() {
        let package = std::env::var("NOVU_CANONICAL_JSON").unwrap();
        for context in [
            json!({"tenant":{"id":"acme","data":{"quote":"\"\\","line":"a\nb","nested":[true,null,-12]}}}),
            json!({"z":"z", "a":{"id":"x", "data":{"\u{e000}":1,"\u{10000}":2}}}),
            json!({"tenant":{"id":"x","data":{"number":1.0}}}),
        ] {
            let output = std::process::Command::new("node")
                .args(["-e", "process.stdout.write(require(process.argv[1]).canonicalize(JSON.parse(process.argv[2])))", &package, &context.to_string()])
                .output().unwrap();
            assert!(output.status.success());
            assert_eq!(
                parse(&context).unwrap().canonical.unwrap().as_bytes(),
                output.stdout
            );
        }
    }
}
