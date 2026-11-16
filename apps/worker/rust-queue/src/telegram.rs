//! Telegram Bot API transport. Bot tokens must not appear in logs or queue payloads.
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    Invalid,
    Rejected(u16),
    Unknown,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Accepted {
    pub id: String,
    pub timestamp: String,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub destination: String,
    #[serde(rename = "chatId")]
    pub chat_id: String,
    pub text: String,
    #[serde(default)]
    pub passthrough: Map<String, Value>,
}
/// Passthrough values are already in Telegram's snake_case; they override base fields.
pub fn request_body(message: &Message) -> Result<Value, SendError> {
    if !crate::slack_webhook::valid_destination(&message.destination)
        || message.chat_id.is_empty()
        || message.chat_id.len() > 256
        || message.chat_id.contains(['\r', '\n', '\0'])
        || message.text.is_empty()
        || message.passthrough.len() > 100
    {
        return Err(SendError::Invalid);
    }
    let mut body = Map::from_iter([
        ("chat_id".into(), json!(message.chat_id)),
        ("text".into(), json!(message.text)),
    ]);
    body.extend(message.passthrough.clone());
    let result = Value::Object(body);
    if serde_json::to_vec(&result)
        .map_err(|_| SendError::Invalid)?
        .len()
        > 128 * 1024
    {
        return Err(SendError::Invalid);
    }
    Ok(result)
}
pub struct Telegram {
    client: Client,
    endpoint: Url,
}
impl Telegram {
    pub fn new(token: &str) -> Result<Self, SendError> {
        let Some((id, secret)) = token.split_once(':') else {
            return Err(SendError::Invalid);
        };
        if id.is_empty()
            || !id.bytes().all(|b| b.is_ascii_digit())
            || secret.is_empty()
            || secret.len() > 256
            || !secret
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        {
            return Err(SendError::Invalid);
        }
        let endpoint = Url::parse(&format!("https://api.telegram.org/bot{token}/sendMessage"))
            .map_err(|_| SendError::Invalid)?;
        Self::with_endpoint(endpoint)
    }
    fn with_endpoint(endpoint: Url) -> Result<Self, SendError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| SendError::Invalid)?;
        Ok(Self { client, endpoint })
    }
    #[cfg(any(test, feature = "test-mocks"))]
    pub fn test_endpoint(url: &str) -> Result<Self, SendError> {
        Self::with_endpoint(Url::parse(url).map_err(|_| SendError::Invalid)?)
    }
    pub async fn send(&self, body: &Value) -> Result<Accepted, SendError> {
        if !body.is_object()
            || serde_json::to_vec(body)
                .map_err(|_| SendError::Invalid)?
                .len()
                > 128 * 1024
        {
            return Err(SendError::Invalid);
        }
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .json(body)
            .send()
            .await
            .map_err(|_| SendError::Unknown)?;
        let status = response.status().as_u16();
        if (400..500).contains(&status) && ![408, 409].contains(&status) {
            return Err(SendError::Rejected(status));
        }
        if !response.status().is_success() {
            return Err(SendError::Unknown);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| SendError::Unknown)? {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(SendError::Unknown);
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| SendError::Unknown)?;
        if value.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(SendError::Unknown);
        }
        let result = value
            .get("result")
            .and_then(Value::as_object)
            .ok_or(SendError::Unknown)?;
        let id = result
            .get("message_id")
            .and_then(Value::as_i64)
            .filter(|n| *n > 0)
            .ok_or(SendError::Unknown)?;
        let timestamp = result
            .get("date")
            .and_then(Value::as_i64)
            .and_then(|n| chrono::DateTime::from_timestamp(n, 0))
            .ok_or(SendError::Unknown)?
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        Ok(Accepted {
            id: id.to_string(),
            timestamp,
        })
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
    fn validates_request_and_token_without_disclosing_it() {
        let msg = Message {
            destination: "ops".into(),
            chat_id: "123".into(),
            text: "Hello".into(),
            passthrough: Map::from_iter([("parse_mode".into(), json!("HTML"))]),
        };
        assert_eq!(
            request_body(&msg).unwrap(),
            json!({"chat_id":"123","text":"Hello","parse_mode":"HTML"})
        );
        assert!(Telegram::new("123:abc_DEF-1").is_ok());
        for bad in ["x:abc", "123:bad/segment", "123:bad?query", "123:"] {
            assert!(Telegram::new(bad).is_err());
        }
    }
    #[tokio::test]
    async fn classifies_responses_and_bounds_body() {
        for (status, body, expected) in [
            (
                "200 OK",
                r#"{"ok":true,"result":{"message_id":42,"date":100}}"#,
                Ok(Accepted {
                    id: "42".into(),
                    timestamp: "1970-01-01T00:01:40.000Z".into(),
                }),
            ),
            ("200 OK", r#"{"ok":true}"#, Err(SendError::Unknown)),
            ("400 Bad Request", "bad", Err(SendError::Rejected(400))),
            ("408 Request Timeout", "bad", Err(SendError::Unknown)),
            ("409 Conflict", "bad", Err(SendError::Unknown)),
            ("500 Server Error", "bad", Err(SendError::Unknown)),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/sendMessage", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut s, _) = listener.accept().await.unwrap();
                let mut buf = [0; 4096];
                s.read(&mut buf).await.unwrap();
                s.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            });
            assert_eq!(
                Telegram::test_endpoint(&url)
                    .unwrap()
                    .send(&json!({"chat_id":"123","text":"Hello"}))
                    .await,
                expected
            );
            server.await.unwrap();
        }
    }
}
