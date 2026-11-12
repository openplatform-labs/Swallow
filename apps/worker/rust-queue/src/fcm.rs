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
    let routing = passthrough
        .and_then(claimed)
        .or_else(|| claimed(&push.provider_data));
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
            Self::InvalidResponse => f.write_str("invalid FCM response"),
            Self::OAuth(error) => write!(f, "FCM authentication failed: {error}"),
        }
    }
}
impl std::error::Error for SendError {}

/// Convert the bounded Firebase message subset into HTTP v1 single-send bodies.
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

pub struct Fcm {
    http: Client,
    endpoint: String,
    auth: Auth,
}
impl Fcm {
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
        let response = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&token)
            .json(message)
            .send()
            .await
            .map_err(|_| SendError::Transport)?;
        if !response.status().is_success() {
            return Err(SendError::Provider(response.status().as_u16()));
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
    fn rejects_unsupported_and_empty_destinations() {
        let push = Push {
            title: "Hi".into(),
            content: "Hello".into(),
            targets: vec!["a".into()],
            provider_data: json!({"android":{"priority":"high"}})
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
