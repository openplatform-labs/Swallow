//! Incoming webhook adapter; the endpoint URL is a credential.
use crate::slack::SendError;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub destination: String,
    pub text: String,
    #[serde(default)]
    pub blocks: Option<Value>,
    #[serde(default, rename = "customData")]
    pub custom_data: Option<serde_json::Map<String, Value>>,
}
pub fn valid_destination(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
pub fn request_body(message: &Message) -> Result<Value, SendError> {
    if !valid_destination(&message.destination) {
        return Err(SendError::Invalid);
    }
    let mut body = json!({"text":message.text});
    if let Some(blocks) = &message.blocks {
        body["blocks"] = blocks.clone();
    }
    if let Some(data) = &message.custom_data {
        body.as_object_mut()
            .ok_or(SendError::Invalid)?
            .extend(data.clone());
    }
    let text = body
        .get("text")
        .and_then(Value::as_str)
        .ok_or(SendError::Invalid)?;
    if text.len() > 40_000
        || text.contains('\0')
        || body.get("blocks").is_some_and(|v| !v.is_array())
        || serde_json::to_vec(&body)
            .map_err(|_| SendError::Invalid)?
            .len()
            > 128 * 1024
    {
        return Err(SendError::Invalid);
    }
    Ok(body)
}
pub struct Webhook {
    client: Client,
    endpoint: Url,
}
impl Webhook {
    pub fn new(url: &str) -> Result<Self, SendError> {
        let endpoint = Url::parse(url).map_err(|_| SendError::Invalid)?;
        let segments: Vec<_> = endpoint
            .path_segments()
            .ok_or(SendError::Invalid)?
            .collect();
        if endpoint.scheme() != "https"
            || !matches!(
                endpoint.host_str(),
                Some("hooks.slack.com" | "hooks.slack-gov.com")
            )
            || endpoint.port().is_some_and(|p| p != 443)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || segments.len() != 4
            || segments[0] != "services"
            || segments[1..].iter().any(|v| {
                v.is_empty() || v.len() > 128 || !v.bytes().all(|b| b.is_ascii_alphanumeric())
            })
        {
            return Err(SendError::Invalid);
        }
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
    #[cfg(test)]
    pub(crate) fn test_endpoint(url: &str) -> Result<Self, SendError> {
        Self::with_endpoint(Url::parse(url).map_err(|_| SendError::Invalid)?)
    }
    pub async fn send(&self, message: &Message) -> Result<String, SendError> {
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .json(&request_body(message)?)
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
        let id = response
            .headers()
            .get("x-slack-req-id")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty() && s.len() <= 255 && !s.chars().any(char::is_control))
            .map(str::to_owned);
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| SendError::Unknown)? {
            if body.len() + chunk.len() > 256 {
                return Err(SendError::Unknown);
            }
            body.extend_from_slice(&chunk);
        }
        if body != b"ok" {
            return Err(SendError::Unknown);
        }
        Ok(id.unwrap_or_else(|| format!("webhook-{}", uuid::Uuid::new_v4())))
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
    async fn webhook_wire_response_and_url_validation() {
        for url in [
            "http://hooks.slack.com/services/A/B/C",
            "https://hooks.slack.com.evil.test/services/A/B/C",
            "https://hooks.slack.com/services/A/B/C?x=y",
            "https://user@hooks.slack.com/services/A/B/C",
            "https://hooks.slack.com/services/A/B/C/extra",
        ] {
            assert!(Webhook::new(url).is_err());
        }
        assert!(Webhook::new("https://hooks.slack.com/services/A/B/C").is_ok());
        for (status, body, expected) in [
            (200, "ok", true),
            (200, "unexpected", false),
            (302, "ok", false),
            (408, "error", false),
            (429, "error", false),
            (500, "error", false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0; 4096];
                loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..pos]);
                        let len: usize = headers
                            .lines()
                            .find_map(|v| {
                                v.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|x| x.parse().ok())
                            })
                            .unwrap();
                        if bytes.len() >= pos + 4 + len {
                            break;
                        }
                    }
                }
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nX-Slack-Req-Id: fixture-id\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                String::from_utf8(bytes).unwrap()
            });
            let message = Message {
                destination: "ops".into(),
                text: "hello".into(),
                blocks: None,
                custom_data: Some(json!({"thread_ts":"1.2"}).as_object().unwrap().clone()),
            };
            let result = Webhook::test_endpoint(&url).unwrap().send(&message).await;
            assert_eq!(result.is_ok(), expected);
            if status == 408 || status == 302 || status == 500 || (status == 200 && body != "ok") {
                assert_eq!(result, Err(SendError::Unknown));
            }
            if status == 429 {
                assert_eq!(result, Err(SendError::Rejected(Some(429))));
            }
            let req = server.await.unwrap();
            assert!(!req.to_ascii_lowercase().contains("authorization:"));
            assert_eq!(
                serde_json::from_str::<Value>(req.split_once("\r\n\r\n").unwrap().1).unwrap(),
                json!({"text":"hello","thread_ts":"1.2"})
            );
        }
    }
}
