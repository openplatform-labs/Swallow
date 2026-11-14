//! Slack chat.postMessage adapter. Credentials never appear in diagnostics.
use reqwest::{Client, Url, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{fmt, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub channel: String,
    pub text: String,
    #[serde(default)]
    pub blocks: Option<Value>,
    #[serde(default, rename = "customData")]
    pub custom_data: Option<serde_json::Map<String, Value>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    Invalid,
    Rejected(Option<u16>),
    Unknown,
}
impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid Slack message",
            Self::Rejected(_) => "Slack rejected request",
            Self::Unknown => "Slack outcome unknown",
        })
    }
}
impl std::error::Error for SendError {}

pub fn request_body(message: &Message) -> Result<Value, SendError> {
    let mut body = json!({"channel":message.channel,"text":message.text});
    if let Some(blocks) = &message.blocks {
        body["blocks"] = blocks.clone();
    }
    if let Some(custom_data) = &message.custom_data {
        body.as_object_mut()
            .ok_or(SendError::Invalid)?
            .extend(custom_data.clone());
    }
    let channel = body
        .get("channel")
        .and_then(Value::as_str)
        .ok_or(SendError::Invalid)?;
    let text = body
        .get("text")
        .and_then(Value::as_str)
        .ok_or(SendError::Invalid)?;
    if channel.is_empty()
        || channel.len() > 255
        || channel.contains(['\r', '\n', '\0'])
        || text.len() > 40_000
        || text.contains('\0')
        || body.get("blocks").is_some_and(|v| !v.is_array())
    {
        return Err(SendError::Invalid);
    }
    if serde_json::to_vec(&body)
        .map_err(|_| SendError::Invalid)?
        .len()
        > 128 * 1024
    {
        return Err(SendError::Invalid);
    }
    Ok(body)
}

pub struct Slack {
    client: Client,
    token: String,
    endpoint: Url,
}
impl Slack {
    pub fn new(token: String) -> Result<Self, SendError> {
        Self::with_endpoint(
            token,
            Url::parse("https://slack.com/api/chat.postMessage").map_err(|_| SendError::Invalid)?,
        )
    }
    fn with_endpoint(token: String, endpoint: Url) -> Result<Self, SendError> {
        if token.is_empty() || header::HeaderValue::from_str(&format!("Bearer {token}")).is_err() {
            return Err(SendError::Invalid);
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| SendError::Invalid)?;
        Ok(Self {
            client,
            token,
            endpoint,
        })
    }
    #[cfg(test)]
    pub(crate) fn test_endpoint(token: String, endpoint: &str) -> Result<Self, SendError> {
        Self::with_endpoint(token, Url::parse(endpoint).map_err(|_| SendError::Invalid)?)
    }
    pub async fn send(&self, message: &Message) -> Result<String, SendError> {
        let body = request_body(message)?;
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|_| SendError::Unknown)?;
        let status = response.status().as_u16();
        if (400..500).contains(&status) && ![408, 409].contains(&status) {
            return Err(SendError::Rejected(Some(status)));
        }
        if !(200..300).contains(&status) {
            return Err(SendError::Unknown);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| SendError::Unknown)? {
            if bytes.len() + chunk.len() > 65_536 {
                return Err(SendError::Unknown);
            }
            bytes.extend_from_slice(&chunk);
        }
        let data: Value = serde_json::from_slice(&bytes).map_err(|_| SendError::Unknown)?;
        match data.get("ok").and_then(Value::as_bool) {
            Some(true) => {
                let channel = data
                    .get("channel")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty() && v.len() <= 255)
                    .ok_or(SendError::Unknown)?;
                let ts = data
                    .get("ts")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty() && v.len() <= 255)
                    .ok_or(SendError::Unknown)?;
                Ok(format!("{channel}:{ts}"))
            }
            Some(false)
                if matches!(
                    data.get("error").and_then(Value::as_str),
                    Some(
                        "invalid_auth"
                            | "not_authed"
                            | "channel_not_found"
                            | "not_in_channel"
                            | "rate_limited"
                            | "ratelimited"
                    )
                ) =>
            {
                Err(SendError::Rejected(None))
            }
            _ => Err(SendError::Unknown),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    async fn mock(status: &str, body: &str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let status = status.to_owned();
        let body = body.to_owned();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(p) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&bytes[..p]);
                    let len: usize = h
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|x| x.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= p + 4 + len {
                        break;
                    }
                }
            }
            stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            String::from_utf8(bytes).unwrap()
        });
        (url, task)
    }
    #[tokio::test]
    async fn verifies_slack_response_and_secret_redaction() {
        let message = Message {
            channel: "C123".into(),
            text: "hi".into(),
            blocks: None,
            custom_data: None,
        };
        for (status, body, expected) in [
            (
                "200 OK",
                r#"{"ok":true,"channel":"C123","ts":"1.2"}"#,
                Ok("C123:1.2".to_owned()),
            ),
            (
                "200 OK",
                r#"{"ok":false,"error":"channel_not_found"}"#,
                Err(SendError::Rejected(None)),
            ),
            (
                "200 OK",
                r#"{"ok":false,"error":"fatal_error"}"#,
                Err(SendError::Unknown),
            ),
            ("200 OK", r#"{"ok":true}"#, Err(SendError::Unknown)),
            (
                "429 Too Many Requests",
                "limit",
                Err(SendError::Rejected(Some(429))),
            ),
            ("409 Conflict", "ambiguous", Err(SendError::Unknown)),
            (
                "500 Internal Server Error",
                "failed",
                Err(SendError::Unknown),
            ),
        ] {
            let (url, task) = mock(status, body).await;
            let slack = Slack::test_endpoint("fixture-token".into(), &url).unwrap();
            assert_eq!(slack.send(&message).await, expected);
            let req = task.await.unwrap();
            assert!(
                req.to_ascii_lowercase()
                    .contains("authorization: bearer fixture-token")
            );
        }
        assert!(!SendError::Unknown.to_string().contains("fixture-token"));
        assert!(
            request_body(&Message {
                channel: "C\n123".into(),
                ..message
            })
            .is_err()
        );
    }

    #[test]
    fn validates_effective_custom_data_after_overrides() {
        let mut message = Message {
            channel: "C123".into(),
            text: "original".into(),
            blocks: None,
            custom_data: json!({"channel":"C999","text":"custom","thread_ts":"1.2"})
                .as_object()
                .cloned(),
        };
        assert_eq!(
            request_body(&message).unwrap(),
            json!({"channel":"C999","text":"custom","thread_ts":"1.2"})
        );
        for invalid in [
            json!({"channel":"C\n999"}),
            json!({"channel":null}),
            json!({"text":false}),
            json!({"blocks":{}}),
        ] {
            message.custom_data = invalid.as_object().cloned();
            assert_eq!(request_body(&message), Err(SendError::Invalid));
        }
    }
}
