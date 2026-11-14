//! Experimental FCM provider message planning and limited workflow/queue delivery.
use reqwest::{Client, header};
use serde_json::{Map, Value, json};
use std::time::Duration;

#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Push {
    pub title: String,
    pub content: String,
    pub targets: Vec<String>,
    pub payload: Map<String, Value>,
    pub overrides: Map<String, Value>,
    pub provider_data: Map<String, Value>,
    pub workflow_context: Option<WorkflowContext>,
    pub prepared_messages: Option<Vec<Value>>,
    pub prepared_context: Option<Value>,
    pub workflow_guard: Option<Value>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowContext {
    pub subscriber_id: String,
    #[serde(default)]
    pub subscriber_internal_id: Option<String>,
    #[serde(default)]
    pub notification_internal_id: Option<String>,
    #[serde(default)]
    pub transaction_id: Option<String>,
    pub workflow_identifier: String,
    pub workflow_internal_id: Option<String>,
    pub title_template: String,
    pub content_template: String,
    pub payload: Value,
    pub context_keys: Vec<String>,
    pub actor: Option<Value>,
    #[serde(default)]
    pub actor_internal_id: Option<String>,
    #[serde(default)]
    pub overrides: Map<String, Value>,
    #[serde(default)]
    pub provider_data: Map<String, Value>,
}

#[derive(Debug, PartialEq)]
pub struct Plan {
    pub multicast: bool,
    pub message: Value,
}

const ROUTING: [&str; 4] = ["token", "topic", "condition", "tokens"];

fn claimed(layer: &Map<String, Value>) -> Option<(&str, Value)> {
    for key in ROUTING {
        let Some(value) = layer.get(key) else {
            continue;
        };
        match value {
            Value::String(s) if !s.is_empty() => return Some((key, value.clone())),
            Value::Array(items) => {
                let values: Vec<Value> = items
                    .iter()
                    .filter(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                    .cloned()
                    .collect();
                if !values.is_empty() {
                    return Some((key, Value::Array(values)));
                }
            }
            _ => {}
        }
    }
    None
}

pub(crate) fn routing(data: &Map<String, Value>) -> Option<(&str, Value)> {
    data.get("_passthrough")
        .and_then(|v| v.get("body"))
        .and_then(Value::as_object)
        .and_then(claimed)
        .or_else(|| claimed(data))
}

// BaseProvider deepmerge concatenates arrays and recursively merges objects.
fn merge(target: &mut Value, source: &Value) {
    match (target, source) {
        (Value::Object(target), Value::Object(source)) => {
            for (key, value) in source {
                if matches!(key.as_str(), "__proto__" | "constructor" | "prototype") {
                    continue;
                }
                if let Some(old) = target.get_mut(key) {
                    merge(old, value);
                } else {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
        (Value::Array(target), Value::Array(source)) => target.extend(source.clone()),
        (target, source) => *target = source.clone(),
    }
}

/// Construct the message passed to firebase-admin `send` or `sendEachForMulticast`.
pub fn plan(push: &Push) -> Plan {
    let passthrough = push
        .provider_data
        .get("_passthrough")
        .and_then(|v| v.get("body"))
        .and_then(Value::as_object);
    let routing = routing(&push.provider_data);
    let mut message = Map::new();
    let multicast = match routing {
        Some((key @ ("token" | "topic" | "condition"), Value::String(value))) => {
            message.insert(key.into(), value.into());
            false
        }
        Some(("tokens", Value::Array(values))) => {
            message.insert("tokens".into(), Value::Array(values));
            true
        }
        _ => {
            message.insert("tokens".into(), json!(push.targets));
            true
        }
    };
    let mut clean = Map::new();
    for (key, value) in &push.payload {
        clean.insert(
            key.clone(),
            Value::String(match value {
                Value::String(value) => value.clone(),
                _ => value.to_string(),
            }),
        );
    }
    let data_override = push.overrides.get("data").and_then(Value::as_object);
    let data_only = push.overrides.get("type").and_then(Value::as_str) == Some("data");
    let mut data = if data_only {
        clean.clone()
    } else {
        clean
            .get("__nvMessageId")
            .filter(|v| v.as_str().is_some_and(|s| !s.is_empty()))
            .map(|v| Map::from_iter([("__nvMessageId".into(), v.clone())]))
            .unwrap_or_default()
    };
    if let Some(fields) = data_override {
        data.extend(fields.clone());
    }
    if data_only {
        data.insert("title".into(), push.title.clone().into());
        data.insert("body".into(), push.content.clone().into());
        data.insert("message".into(), push.content.clone().into());
    } else {
        let mut notification = Map::from_iter([
            ("title".into(), push.title.clone().into()),
            ("body".into(), push.content.clone().into()),
        ]);
        if multicast {
            for (key, value) in &push.overrides {
                if ![
                    "deviceTokens",
                    "type",
                    "android",
                    "apns",
                    "fcmOptions",
                    "webPush",
                    "data",
                ]
                .contains(&key.as_str())
                {
                    notification.insert(key.clone(), value.clone());
                }
            }
        }
        message.insert("notification".into(), Value::Object(notification));
    }
    message.insert("data".into(), Value::Object(data));
    for (key, output) in [
        ("android", "android"),
        ("apns", "apns"),
        ("fcmOptions", "fcmOptions"),
        ("webPush", "webpush"),
    ] {
        if let Some(value) = push.overrides.get(key) {
            message.insert(output.into(), value.clone());
        }
    }
    let mut result = Value::Object(message);
    let mut provider = push.provider_data.clone();
    provider.remove("_passthrough");
    for key in ROUTING {
        provider.remove(key);
    }
    merge(&mut result, &Value::Object(provider));
    if let Some(body) = passthrough {
        let mut body = body.clone();
        for key in ROUTING {
            body.remove(key);
        }
        merge(&mut result, &Value::Object(body));
    }
    Plan {
        multicast,
        message: result,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    InvalidConfiguration,
    UnsupportedMessage,
    Transport,
    Provider(u16),
    InvalidDeviceToken(u16),
    InvalidResponse,
    OAuth(crate::google_oauth::AuthError),
}
impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfiguration => f.write_str("invalid FCM configuration"),
            Self::UnsupportedMessage => f.write_str("unsupported FCM message"),
            Self::Transport => f.write_str("FCM transport failed"),
            Self::Provider(code) => write!(f, "FCM returned HTTP {code}"),
            Self::InvalidDeviceToken(code) => {
                write!(f, "FCM returned HTTP {code}: invalid device token")
            }
            Self::InvalidResponse => f.write_str("invalid FCM response"),
            Self::OAuth(error) => write!(f, "FCM authentication failed: {error}"),
        }
    }
}
impl std::error::Error for SendError {}

// Mirrors the pinned firebase-admin error conversion followed by Novu's FCM
// isTokenInvalid check. Unknown JSON errors and non-JSON responses include the
// raw response in the SDK's generated message; known errors do not.
fn invalid_device_token_response(body: &[u8]) -> bool {
    const INVALID: &str = "Requested entity was not found";
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return String::from_utf8_lossy(body).contains(INVALID);
    };
    let error = value.get("error");
    if error
        .and_then(|v| v.get("message"))
        .and_then(Value::as_str)
        .is_some_and(|v| v.contains(INVALID))
    {
        return true;
    }
    let code = error.and_then(|error| {
        error.as_str().or_else(|| {
            if let Some(detail) =
                error
                    .get("details")
                    .and_then(Value::as_array)
                    .and_then(|details| {
                        details.iter().find(|item| {
                            item.get("@type").and_then(Value::as_str)
                                == Some("type.googleapis.com/google.firebase.fcm.v1.FcmError")
                        })
                    })
            {
                detail.get("errorCode").and_then(Value::as_str)
            } else {
                error
                    .get("status")
                    .or_else(|| error.get("message"))
                    .and_then(Value::as_str)
            }
        })
    });
    let known = code.is_some_and(|code| {
        matches!(
            code,
            "InvalidParameters"
                | "MismatchSenderId"
                | "Unavailable"
                | "InternalServerError"
                | "InvalidRegistration"
                | "NotRegistered"
                | "InvalidPackageName"
                | "MessageTooBig"
                | "InvalidDataKey"
                | "InvalidTtl"
                | "DeviceMessageRateExceeded"
                | "TopicsMessageRateExceeded"
                | "InvalidApnsCredential"
                | "NOT_FOUND"
                | "PERMISSION_DENIED"
                | "RESOURCE_EXHAUSTED"
                | "UNAUTHENTICATED"
                | "APNS_AUTH_ERROR"
                | "INTERNAL"
                | "INVALID_ARGUMENT"
                | "QUOTA_EXCEEDED"
                | "SENDER_ID_MISMATCH"
                | "THIRD_PARTY_AUTH_ERROR"
                | "UNAVAILABLE"
                | "UNREGISTERED"
        )
    });
    !known && value.to_string().contains(INVALID)
}

// Apply the pinned Firebase Admin SDK's APNs payload transformations.
fn apns_config(value: &Value) -> Result<Value, SendError> {
    let mut config = value
        .as_object()
        .cloned()
        .ok_or(SendError::UnsupportedMessage)?;
    if config
        .keys()
        .any(|key| !["headers", "payload", "fcmOptions"].contains(&key.as_str()))
    {
        return Err(SendError::UnsupportedMessage);
    }
    if let Some(headers) = config.get("headers")
        && !headers
            .as_object()
            .is_some_and(|m| m.values().all(Value::is_string))
    {
        return Err(SendError::UnsupportedMessage);
    }
    if let Some(payload) = config.get_mut("payload") {
        let payload = payload
            .as_object_mut()
            .ok_or(SendError::UnsupportedMessage)?;
        if let Some(aps) = payload.get_mut("aps") {
            let aps = aps.as_object_mut().ok_or(SendError::UnsupportedMessage)?;
            if let Some(alert) = aps.get_mut("alert") {
                if let Some(fields) = alert.as_object_mut() {
                    for (args, key) in [
                        ("locArgs", "locKey"),
                        ("titleLocArgs", "titleLocKey"),
                        ("subtitleLocArgs", "subtitleLocKey"),
                    ] {
                        if fields
                            .get(args)
                            .and_then(Value::as_array)
                            .is_some_and(|v| !v.is_empty())
                            && fields
                                .get(key)
                                .and_then(Value::as_str)
                                .is_none_or(|s| s.is_empty())
                        {
                            return Err(SendError::UnsupportedMessage);
                        }
                    }
                    for (from, to) in [
                        ("locKey", "loc-key"),
                        ("locArgs", "loc-args"),
                        ("titleLocKey", "title-loc-key"),
                        ("titleLocArgs", "title-loc-args"),
                        ("subtitleLocKey", "subtitle-loc-key"),
                        ("subtitleLocArgs", "subtitle-loc-args"),
                        ("actionLocKey", "action-loc-key"),
                        ("launchImage", "launch-image"),
                    ] {
                        if let Some(v) = fields.remove(from) {
                            fields.insert(to.to_owned(), v);
                        }
                    }
                } else if !alert.is_string() {
                    return Err(SendError::UnsupportedMessage);
                }
            }
            if let Some(sound) = aps.get_mut("sound") {
                if let Some(fields) = sound.as_object_mut() {
                    if fields
                        .get("name")
                        .and_then(Value::as_str)
                        .is_none_or(|v| v.is_empty())
                        || fields
                            .get("volume")
                            .is_some_and(|v| !v.as_f64().is_some_and(|n| (0.0..=1.0).contains(&n)))
                    {
                        return Err(SendError::UnsupportedMessage);
                    }
                    if let Some(critical) = fields.remove("critical")
                        && (critical == json!(true) || critical == json!(1))
                    {
                        fields.insert("critical".into(), json!(1));
                    }
                } else if sound.as_str().is_none_or(|s| s.is_empty()) {
                    return Err(SendError::UnsupportedMessage);
                }
            }
            for (from, to) in [
                ("contentAvailable", "content-available"),
                ("mutableContent", "mutable-content"),
                ("threadId", "thread-id"),
            ] {
                if aps.contains_key(from) && aps.contains_key(to) {
                    return Err(SendError::UnsupportedMessage);
                }
                if let Some(v) = aps.remove(from) {
                    aps.insert(to.to_owned(), v);
                }
            }
            for key in ["content-available", "mutable-content"] {
                if let Some(v) = aps.remove(key)
                    && (v == json!(true) || v == json!(1))
                {
                    aps.insert(key.to_owned(), json!(1));
                }
            }
        }
    }
    if let Some(options) = config.get_mut("fcmOptions") {
        let options = options
            .as_object_mut()
            .ok_or(SendError::UnsupportedMessage)?;
        if options
            .get("analyticsLabel")
            .is_some_and(|v| !v.is_string())
        {
            return Err(SendError::UnsupportedMessage);
        }
        if let Some(image) = options.remove("imageUrl") {
            if options.contains_key("image")
                || !image
                    .as_str()
                    .and_then(|v| reqwest::Url::parse(v).ok())
                    .is_some_and(|u| {
                        ["http", "https"].contains(&u.scheme()) && u.host_str().is_some()
                    })
            {
                return Err(SendError::UnsupportedMessage);
            }
            options.insert("image".into(), image);
        }
    }
    Ok(Value::Object(config))
}

fn android_duration(value: &Value) -> Result<Value, SendError> {
    let millis = value
        .as_u64()
        .filter(|n| *n <= 9_007_199_254_740_991)
        .ok_or(SendError::UnsupportedMessage)?;
    let seconds = millis / 1000;
    let nanos = (millis % 1000) * 1_000_000;
    Ok(Value::String(if nanos == 0 {
        format!("{seconds}s")
    } else {
        format!("{seconds}.{nanos:09}s")
    }))
}

fn android_light_settings(value: &Value) -> Result<Value, SendError> {
    let fields = value.as_object().ok_or(SendError::UnsupportedMessage)?;
    if fields.keys().any(|key| {
        !["color", "lightOnDurationMillis", "lightOffDurationMillis"].contains(&key.as_str())
    }) {
        return Err(SendError::UnsupportedMessage);
    }
    let color = fields
        .get("color")
        .and_then(Value::as_str)
        .ok_or(SendError::UnsupportedMessage)?;
    let bytes = color.as_bytes();
    if ![7, 9].contains(&bytes.len())
        || bytes[0] != b'#'
        || !bytes[1..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(SendError::UnsupportedMessage);
    }
    let color_byte = |i: usize| -> Result<Value, SendError> {
        let digits =
            std::str::from_utf8(&bytes[i..i + 2]).map_err(|_| SendError::UnsupportedMessage)?;
        let component =
            u8::from_str_radix(digits, 16).map_err(|_| SendError::UnsupportedMessage)?;
        Ok(match component {
            0 => json!(0),
            255 => json!(1),
            other => json!(f64::from(other) / 255.0),
        })
    };
    let alpha = if bytes.len() == 9 {
        color_byte(7)?
    } else {
        json!(1)
    };
    let on = android_duration(
        fields
            .get("lightOnDurationMillis")
            .ok_or(SendError::UnsupportedMessage)?,
    )?;
    let off = android_duration(
        fields
            .get("lightOffDurationMillis")
            .ok_or(SendError::UnsupportedMessage)?,
    )?;
    Ok(
        json!({"color":{"red":color_byte(1)?,"green":color_byte(3)?,"blue":color_byte(5)?,"alpha":alpha},"light_on_duration":on,"light_off_duration":off}),
    )
}

fn android_notification(value: &Value) -> Result<Value, SendError> {
    let fields = value.as_object().ok_or(SendError::UnsupportedMessage)?;
    for (args, key) in [
        ("bodyLocArgs", "bodyLocKey"),
        ("titleLocArgs", "titleLocKey"),
    ] {
        if fields
            .get(args)
            .and_then(Value::as_array)
            .is_some_and(|v| !v.is_empty())
            && fields
                .get(key)
                .and_then(Value::as_str)
                .is_none_or(|s| s.is_empty())
        {
            return Err(SendError::UnsupportedMessage);
        }
    }
    let mut result = Map::new();
    for (key, value) in fields {
        let (wire, converted) = match key.as_str() {
            "title" | "body" | "icon" | "sound" | "tag" | "ticker" => (
                key.as_str(),
                value
                    .as_str()
                    .map(|_| value.clone())
                    .ok_or(SendError::UnsupportedMessage)?,
            ),
            "color" => {
                let color = value.as_str().ok_or(SendError::UnsupportedMessage)?;
                if color.len() != 7
                    || !color.starts_with('#')
                    || !color.as_bytes()[1..].iter().all(u8::is_ascii_hexdigit)
                {
                    return Err(SendError::UnsupportedMessage);
                }
                ("color", value.clone())
            }
            "clickAction" | "bodyLocKey" | "titleLocKey" | "channelId" => (
                match key.as_str() {
                    "clickAction" => "click_action",
                    "bodyLocKey" => "body_loc_key",
                    "titleLocKey" => "title_loc_key",
                    _ => "channel_id",
                },
                value
                    .as_str()
                    .map(|_| value.clone())
                    .ok_or(SendError::UnsupportedMessage)?,
            ),
            "bodyLocArgs" | "titleLocArgs" => (
                if key == "bodyLocArgs" {
                    "body_loc_args"
                } else {
                    "title_loc_args"
                },
                value
                    .as_array()
                    .filter(|v| v.iter().all(Value::is_string))
                    .map(|_| value.clone())
                    .ok_or(SendError::UnsupportedMessage)?,
            ),
            "imageUrl" => {
                if !value
                    .as_str()
                    .and_then(|v| reqwest::Url::parse(v).ok())
                    .is_some_and(|u| {
                        ["http", "https"].contains(&u.scheme()) && u.host_str().is_some()
                    })
                {
                    return Err(SendError::UnsupportedMessage);
                }
                ("image", value.clone())
            }
            "sticky"
            | "localOnly"
            | "defaultVibrateTimings"
            | "defaultSound"
            | "defaultLightSettings" => (
                match key.as_str() {
                    "sticky" => "sticky",
                    "localOnly" => "local_only",
                    "defaultVibrateTimings" => "default_vibrate_timings",
                    "defaultSound" => "default_sound",
                    _ => "default_light_settings",
                },
                value
                    .as_bool()
                    .map(|_| value.clone())
                    .ok_or(SendError::UnsupportedMessage)?,
            ),
            "notificationCount" => (
                "notification_count",
                value
                    .as_u64()
                    .filter(|n| *n <= i32::MAX as u64)
                    .map(|_| value.clone())
                    .ok_or(SendError::UnsupportedMessage)?,
            ),
            "priority" => {
                let v = value
                    .as_str()
                    .filter(|v| ["min", "low", "default", "high", "max"].contains(v))
                    .ok_or(SendError::UnsupportedMessage)?;
                (
                    "notification_priority",
                    Value::String(format!("PRIORITY_{}", v.to_uppercase())),
                )
            }
            "visibility" => {
                let v = value
                    .as_str()
                    .filter(|v| ["private", "public", "secret"].contains(v))
                    .ok_or(SendError::UnsupportedMessage)?;
                ("visibility", Value::String(v.to_uppercase()))
            }
            "proxy" => {
                let v = value
                    .as_str()
                    .filter(|v| ["allow", "deny", "if_priority_lowered"].contains(v))
                    .ok_or(SendError::UnsupportedMessage)?;
                ("proxy", Value::String(v.to_uppercase()))
            }
            "lightSettings" => ("light_settings", android_light_settings(value)?),
            "vibrateTimingsMillis" => {
                let values = value
                    .as_array()
                    .filter(|v| !v.is_empty())
                    .ok_or(SendError::UnsupportedMessage)?;
                (
                    "vibrate_timings",
                    Value::Array(
                        values
                            .iter()
                            .map(android_duration)
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                )
            }
            _ => return Err(SendError::UnsupportedMessage),
        };
        result.insert(wire.to_owned(), converted);
    }
    Ok(Value::Object(result))
}

// Preserve the selected Firebase Admin SDK fields and their HTTP conversion.
fn platform_config(name: &str, value: &Value) -> Result<Value, SendError> {
    let source = value.as_object().ok_or(SendError::UnsupportedMessage)?;
    let mut output = Map::new();
    for (key, value) in source {
        let wire = match (name, key.as_str()) {
            ("android", "collapseKey") => "collapse_key",
            ("android", "restrictedPackageName") => "restricted_package_name",
            ("android", "directBootOk") => "direct_boot_ok",
            ("android", "priority" | "ttl" | "data" | "notification") => key,
            ("webpush", "headers" | "data" | "notification" | "fcmOptions") => key,
            ("fcmOptions", "analyticsLabel") => key,
            _ => return Err(SendError::UnsupportedMessage),
        };
        let converted = match (name, key.as_str()) {
            ("android", "ttl") => android_duration(value)?,
            ("android", "notification") => android_notification(value)?,
            ("android", "directBootOk") if value.is_boolean() => value.clone(),
            ("android", "priority") if matches!(value.as_str(), Some("normal" | "high")) => {
                value.clone()
            }
            ("android", "collapseKey" | "restrictedPackageName")
            | ("fcmOptions", "analyticsLabel")
                if value.is_string() =>
            {
                value.clone()
            }
            ("android" | "webpush", "data") | ("webpush", "headers")
                if value
                    .as_object()
                    .is_some_and(|m| m.values().all(Value::is_string)) =>
            {
                value.clone()
            }
            ("webpush", "notification" | "fcmOptions") if value.is_object() => value.clone(),
            _ => return Err(SendError::UnsupportedMessage),
        };
        output.insert(wire.to_owned(), converted);
    }
    Ok(Value::Object(output))
}

/// Convert the supported Firebase message subset into HTTP v1 single-send bodies.
/// Unsupported platform-specific options are rejected rather than dropped.
pub fn wire_messages(plan: &Plan) -> Result<Vec<Value>, SendError> {
    let object = plan
        .message
        .as_object()
        .ok_or(SendError::UnsupportedMessage)?;
    if object.keys().any(|key| {
        ![
            "token",
            "tokens",
            "topic",
            "condition",
            "notification",
            "data",
            "android",
            "apns",
            "webpush",
            "fcmOptions",
        ]
        .contains(&key.as_str())
    }) {
        return Err(SendError::UnsupportedMessage);
    }
    let mut common = Map::new();
    if let Some(data) = object.get("data") {
        let data = data
            .as_object()
            .filter(|v| v.values().all(Value::is_string))
            .ok_or(SendError::UnsupportedMessage)?;
        common.insert("data".into(), Value::Object(data.clone()));
    }
    if let Some(notification) = object.get("notification") {
        let fields = notification
            .as_object()
            .ok_or(SendError::UnsupportedMessage)?;
        let mut result = Map::new();
        for (key, value) in fields {
            let wire = match key.as_str() {
                "title" => "title",
                "body" => "body",
                "imageUrl" => "image",
                _ => return Err(SendError::UnsupportedMessage),
            };
            if !value.is_string() {
                return Err(SendError::UnsupportedMessage);
            }
            result.insert(wire.into(), value.clone());
        }
        common.insert("notification".into(), Value::Object(result));
    }
    if let Some(value) = object.get("apns") {
        common.insert("apns".into(), apns_config(value)?);
    }
    for name in ["android", "webpush", "fcmOptions"] {
        if let Some(value) = object.get(name) {
            common.insert(name.to_owned(), platform_config(name, value)?);
        }
    }
    let routes: Vec<(&str, String)> = if plan.multicast {
        if object
            .keys()
            .any(|k| ["token", "topic", "condition"].contains(&k.as_str()))
        {
            return Err(SendError::UnsupportedMessage);
        }
        let tokens = object
            .get("tokens")
            .and_then(Value::as_array)
            .filter(|v| !v.is_empty() && v.len() <= 500)
            .ok_or(SendError::UnsupportedMessage)?;
        tokens
            .iter()
            .map(|v| {
                v.as_str()
                    .filter(|s| !s.is_empty())
                    .map(|s| ("token", s.to_owned()))
                    .ok_or(SendError::UnsupportedMessage)
            })
            .collect::<Result<_, _>>()?
    } else {
        let routes: Vec<_> = ["token", "topic", "condition"]
            .into_iter()
            .filter_map(|key| object.get(key).map(|v| (key, v)))
            .collect();
        if routes.len() != 1 || object.contains_key("tokens") {
            return Err(SendError::UnsupportedMessage);
        }
        let (key, value) = routes[0];
        vec![(
            key,
            value
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or(SendError::UnsupportedMessage)?
                .to_owned(),
        )]
    };
    Ok(routes
        .into_iter()
        .map(|(key, value)| {
            let mut message = common.clone();
            message.insert(key.into(), value.into());
            json!({"message":message})
        })
        .collect())
}

enum Auth {
    Bearer(String),
    ServiceAccount(Box<crate::google_oauth::ServiceAccount>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidTokenPolicy {
    Reference,
    Unregistered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenCleanupGuard {
    Reference,
    Version,
}
impl TokenCleanupGuard {
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_FCM_TOKEN_CLEANUP_GUARD").as_deref() {
            Ok("reference") | Err(std::env::VarError::NotPresent) => Ok(Self::Reference),
            Ok("version") => Ok(Self::Version),
            _ => Err("invalid FCM token cleanup guard"),
        }
    }
}
impl InvalidTokenPolicy {
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_FCM_INVALID_TOKEN_POLICY").as_deref() {
            Ok("reference") | Err(std::env::VarError::NotPresent) => Ok(Self::Reference),
            Ok("unregistered") => Ok(Self::Unregistered),
            _ => Err("invalid FCM invalid token policy"),
        }
    }
}

fn unregistered_device_token_response(status: u16, body: &[u8]) -> bool {
    if status != 404 {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    value
        .pointer("/error/details")
        .and_then(Value::as_array)
        .is_some_and(|details| {
            details.iter().any(|item| {
                item.get("@type").and_then(Value::as_str)
                    == Some("type.googleapis.com/google.firebase.fcm.v1.FcmError")
                    && item.get("errorCode").and_then(Value::as_str) == Some("UNREGISTERED")
            })
        })
}

pub struct Fcm {
    http: Client,
    endpoint: String,
    auth: Auth,
    project_id: String,
    invalid_token_policy: InvalidTokenPolicy,
    token_cleanup_guard: TokenCleanupGuard,
}
impl Fcm {
    pub fn with_invalid_token_policy(mut self, policy: InvalidTokenPolicy) -> Self {
        self.invalid_token_policy = policy;
        self
    }
    pub fn with_token_cleanup_guard(mut self, guard: TokenCleanupGuard) -> Self {
        self.token_cleanup_guard = guard;
        self
    }
    pub fn token_cleanup_guard(&self) -> TokenCleanupGuard {
        self.token_cleanup_guard
    }
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    pub fn new(project_id: &str, access_token: String) -> Result<Self, SendError> {
        Self::with_origin(project_id, access_token, "https://fcm.googleapis.com")
    }
    pub fn with_service_account(
        project_id: &str,
        email: String,
        private_key_pem: &str,
    ) -> Result<Self, SendError> {
        let account = crate::google_oauth::ServiceAccount::new(email, private_key_pem)
            .map_err(SendError::OAuth)?;
        let mut provider = Self::with_origin(
            project_id,
            "configured".into(),
            "https://fcm.googleapis.com",
        )?;
        provider.auth = Auth::ServiceAccount(Box::new(account));
        Ok(provider)
    }
    pub(crate) fn with_origin(
        project_id: &str,
        access_token: String,
        origin: &str,
    ) -> Result<Self, SendError> {
        if project_id.is_empty()
            || !project_id
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || v == b'-')
            || access_token.is_empty()
            || header::HeaderValue::from_str(&format!("Bearer {access_token}")).is_err()
        {
            return Err(SendError::InvalidConfiguration);
        }
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| SendError::InvalidConfiguration)?;
        Ok(Self {
            http,
            endpoint: format!("{origin}/v1/projects/{project_id}/messages:send"),
            auth: Auth::Bearer(access_token),
            project_id: project_id.to_owned(),
            invalid_token_policy: InvalidTokenPolicy::Reference,
            token_cleanup_guard: TokenCleanupGuard::Reference,
        })
    }
    /// One request per target, each attempted once. Failures may be ambiguous.
    pub async fn send(&self, plan: &Plan) -> Result<Vec<Result<String, SendError>>, SendError> {
        let messages = wire_messages(plan)?;
        let mut outcomes = Vec::with_capacity(messages.len());
        for message in messages {
            outcomes.push(self.send_wire(&message).await);
        }
        Ok(outcomes)
    }

    /// The caller must first validate the body through `wire_messages`.
    pub(crate) async fn send_wire(&self, message: &Value) -> Result<String, SendError> {
        let token = match &self.auth {
            Auth::Bearer(token) => token.clone(),
            Auth::ServiceAccount(account) => {
                account.access_token().await.map_err(SendError::OAuth)?
            }
        };
        let mut response = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&token)
            .json(message)
            .send()
            .await
            .map_err(|_| SendError::Transport)?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            // The source Firebase SDK exposes error.message to the provider.
            // Bound the body and keep only the classification, never the raw
            // provider response, which can contain sensitive data.
            let mut bytes = Vec::new();
            loop {
                let chunk = match response.chunk().await {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => break,
                    Err(_) => return Err(SendError::Provider(status)),
                };
                if bytes.len() + chunk.len() > 16 * 1024 {
                    return Err(SendError::Provider(status));
                }
                bytes.extend_from_slice(&chunk);
            }
            let invalid = match self.invalid_token_policy {
                InvalidTokenPolicy::Reference => invalid_device_token_response(&bytes),
                InvalidTokenPolicy::Unregistered => {
                    unregistered_device_token_response(status, &bytes)
                }
            };
            if invalid {
                return Err(SendError::InvalidDeviceToken(status));
            }
            return Err(SendError::Provider(status));
        }
        let result = response.json::<Value>().await.ok().and_then(|v| {
            v.get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        });
        result.ok_or(SendError::InvalidResponse)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    #[test]
    fn structured_unregistered_is_narrowly_classified() {
        let valid = br#"{"error":{"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}"#;
        assert!(unregistered_device_token_response(404, valid));
        assert!(!unregistered_device_token_response(500, valid));
        for body in [
            r#"Requested entity was not found."#,
            r#"{"error":{"message":"Requested entity was not found."}}"#,
            r#"{"error":{"details":[{"@type":"other","errorCode":"UNREGISTERED"}]}}"#,
            r#"{"error":{"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"INVALID_ARGUMENT"}]}}"#,
            r#"{"error":{"status":"UNREGISTERED"}}"#,
        ] {
            assert!(
                !unregistered_device_token_response(404, body.as_bytes()),
                "{body}"
            );
        }
    }
    #[test]
    #[ignore = "requires the built pinned Firebase SDK and Node.js"]
    fn original_firebase_error_classification_matches() {
        let output = std::process::Command::new("node")
            .arg("tests/reference_fcm_errors.cjs")
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
        let cases: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
        for case in cases {
            let body = case["body"].as_str().unwrap();
            assert_eq!(
                invalid_device_token_response(body.as_bytes()),
                case["invalid"].as_bool().unwrap(),
                "{body}"
            );
        }
    }
    #[tokio::test]
    async fn sends_multicast_separately_and_records_each_http_outcome() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for (index, status) in [200, 429].into_iter().enumerate() {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut tmp = [0; 4096];
                loop {
                    let n = socket.read(&mut tmp).await.unwrap();
                    assert!(n > 0);
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&buf[..pos]);
                        let len = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        if buf.len() >= pos + 4 + len {
                            break;
                        }
                    }
                }
                let raw = String::from_utf8(buf).unwrap();
                assert!(
                    raw.starts_with("POST /v1/projects/demo-project/messages:send HTTP/1.1\r\n")
                );
                assert!(raw.contains("authorization: Bearer fixture-token\r\n"));
                let body: Value =
                    serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
                assert_eq!(
                    body,
                    json!({"message":{"token":format!("token{index}"),"notification":{"title":"Hi","body":"Hello"},"data":{}}})
                );
                let response = r#"{"name":"projects/demo-project/messages/123"}"#;
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
            }
        });
        let fcm = Fcm::with_origin("demo-project", "fixture-token".into(), &origin).unwrap();
        let result = fcm
            .send(&plan(&Push {
                title: "Hi".into(),
                content: "Hello".into(),
                targets: vec!["token0".into(), "token1".into()],
                ..Push::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![
                Ok("projects/demo-project/messages/123".into()),
                Err(SendError::Provider(429))
            ]
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn service_account_token_is_used_for_fcm_and_reused() {
        let output = std::process::Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        let pem = String::from_utf8(output.stdout).unwrap();
        let oauth = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let oauth_url = format!("http://{}/token", oauth.local_addr().unwrap());
        let oauth_server = tokio::spawn(async move {
            let (mut socket, _) = oauth.accept().await.unwrap();
            let mut bytes = [0; 4096];
            assert!(socket.read(&mut bytes).await.unwrap() > 0);
            let body =
                r#"{"access_token":"oauth-fixture","expires_in":3600,"token_type":"Bearer"}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = [0; 4096];
                let n = socket.read(&mut bytes).await.unwrap();
                let request = String::from_utf8_lossy(&bytes[..n]);
                assert!(request.contains("authorization: Bearer oauth-fixture\r\n"));
                let body = r#"{"name":"projects/demo/messages/1"}"#;
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        let mut provider = Fcm::with_origin("demo-project", "placeholder".into(), &origin).unwrap();
        let account = crate::google_oauth::ServiceAccount::with_endpoint(
            "fixture@example.test".into(),
            &pem,
            &oauth_url,
        )
        .unwrap();
        provider.auth = Auth::ServiceAccount(Box::new(account));
        let push = Push {
            title: "Hi".into(),
            content: "Hello".into(),
            targets: vec!["one".into(), "two".into()],
            ..Push::default()
        };
        let outcomes = provider.send(&plan(&push)).await.unwrap();
        assert_eq!(
            outcomes,
            vec![
                Ok("projects/demo/messages/1".into()),
                Ok("projects/demo/messages/1".into())
            ]
        );
        oauth_server.await.unwrap();
        server.await.unwrap();
    }
    #[test]
    fn platform_config_is_bounded_and_shared_across_multicast_targets() {
        let plan = Plan {
            multicast: true,
            message: json!({"tokens":["a","b"],"android":{"ttl":1501,"priority":"high"},"apns":{"payload":{"aps":{"mutableContent":true},"custom":"value"}},"webpush":{"headers":{"TTL":"60"}},"fcmOptions":{"analyticsLabel":"campaign"}}),
        };
        let messages = wire_messages(&plan).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["message"]["android"]["ttl"], "1.501000000s");
        assert_eq!(messages[1]["message"]["token"], "b");
        assert_eq!(
            messages[0]["message"]["apns"]["payload"]["aps"]["mutable-content"],
            1
        );
        assert_eq!(
            messages[0]["message"]["apns"],
            messages[1]["message"]["apns"]
        );
        assert_eq!(
            messages[0]["message"]["webpush"],
            messages[1]["message"]["webpush"]
        );
        for invalid in [
            json!({"android":{"ttl":-1}}),
            json!({"android":{"notification":{"bodyLocArgs":["x"]}}}),
            json!({"android":{"notification":{"color":"#zzzzzz"}}}),
            json!({"android":{"notification":{"imageUrl":"bad url"}}}),
            json!({"android":{"notification":{"vibrateTimingsMillis":[-1]}}}),
            json!({"android":{"notification":{"notificationCount":-1}}}),
            json!({"android":{"notification":{"sticky":"true"}}}),
            json!({"android":{"notification":{"lightSettings":{}}}}),
            json!({"android":{"notification":{"lightSettings":{"color":"#abcxyz","lightOnDurationMillis":1,"lightOffDurationMillis":2}}}}),
            json!({"android":{"notification":{"lightSettings":{"color":"#123456","lightOnDurationMillis":-1,"lightOffDurationMillis":2}}}}),
            json!({"android":{"notification":{"lightSettings":{"color":"#123456","lightOnDurationMillis":1,"lightOffDurationMillis":1.5}}}}),
            json!({"android":{"ttl":1.5}}),
            json!({"android":{"data":{"x":1}}}),
            json!({"android":{"directBootOk":"yes"}}),
            json!({"webpush":{"headers":{"TTL":1}}}),
            json!({"webpush":{"unknown":1}}),
            json!({"fcmOptions":{"analyticsLabel":2}}),
            json!({"apns":{"headers":{"bad":3}}}),
            json!({"apns":{"payload":{"aps":{"alert":{"locArgs":["x"]}}}}}),
            json!({"apns":{"payload":{"aps":{"sound":{"name":"default","volume":1.1}}}}}),
            json!({"apns":{"payload":{"aps":{"contentAvailable":true,"content-available":1}}}}),
            json!({"apns":{"fcmOptions":{"imageUrl":"not-a-url"}}}),
            json!({"apns":{"fcmOptions":{"imageUrl":"https://example.test/image","image":"duplicate"}}}),
        ] {
            let mut message = invalid;
            message["token"] = json!("a");
            assert_eq!(
                wire_messages(&Plan {
                    multicast: false,
                    message
                }),
                Err(SendError::UnsupportedMessage)
            );
        }
    }

    #[test]
    fn rejects_unsupported_and_empty_destinations() {
        let push = Push {
            title: "Hi".into(),
            content: "Hello".into(),
            targets: vec!["a".into()],
            provider_data: json!({"android":{"notification":{"lightSettings":{}}}})
                .as_object()
                .unwrap()
                .clone(),
            ..Push::default()
        };
        assert_eq!(
            wire_messages(&plan(&push)),
            Err(SendError::UnsupportedMessage)
        );
        assert_eq!(
            wire_messages(&plan(&Push::default())),
            Err(SendError::UnsupportedMessage)
        );
    }
}
