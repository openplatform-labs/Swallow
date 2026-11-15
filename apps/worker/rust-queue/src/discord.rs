//! Discord webhook transport. The endpoint URL contains a credential.
use crate::slack::SendError;
use reqwest::{Client, Url};
use serde_json::{Map, Value, json};
use std::time::Duration;

// Discord reports fractional seconds. Round up so that a stored whole-second
// guidance never understates a valid provider value.
fn fractional_seconds(value: f64) -> Option<u32> {
    if !value.is_finite() || !(0.0..=86_400.0).contains(&value) {
        return None;
    }
    Some(value.ceil() as u32)
}
fn discord_retry_header(headers: &reqwest::header::HeaderMap) -> Option<u32> {
    if let Some(value) = crate::slack::retry_after_seconds(headers) {
        return Some(value);
    }
    let mut values = headers.get_all(reqwest::header::RETRY_AFTER).iter();
    let value = match (values.next(), values.next()) {
        (Some(v), None) => v.to_str().ok()?,
        _ => return None,
    };
    if value.is_empty()
        || !value.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        || !value.contains('.')
    {
        return None;
    }
    fractional_seconds(value.parse().ok()?)
}
#[derive(serde::Deserialize)]
struct DiscordRateLimitBody {
    retry_after: Option<f64>,
}
fn discord_retry_body(bytes: &[u8]) -> Option<u32> {
    let body: DiscordRateLimitBody = serde_json::from_slice(bytes).ok()?;
    fractional_seconds(body.retry_after?)
}
async fn rate_limit_body(response: &mut reqwest::Response) -> Option<u32> {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if bytes.len().checked_add(chunk.len())? > 64 * 1024 {
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        discord_retry_body(&bytes)
    })
    .await
    .ok()
    .flatten()
}

/// Provider body fields override trigger data, as in Novu's provider transform.
/// This transport accepts already-transformed provider fields; casing conversion is
/// the caller's responsibility.
pub fn request_body(
    content: &str,
    custom: &Map<String, Value>,
    provider: &Map<String, Value>,
    passthrough: &Map<String, Value>,
) -> Result<Value, SendError> {
    fn merge(target: &mut Map<String, Value>, source: &Map<String, Value>) {
        for (key, value) in source {
            match (target.get_mut(key), value) {
                (Some(Value::Object(existing)), Value::Object(incoming)) => {
                    merge(existing, incoming)
                }
                (Some(Value::Array(existing)), Value::Array(incoming)) => {
                    existing.extend(incoming.clone())
                }
                _ => {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
    }
    let mut body = Map::new();
    body.insert("content".into(), json!(content));
    // Novu spreads customData into the trigger body before the deep merge.
    body.extend(custom.clone());
    merge(&mut body, provider);
    merge(&mut body, passthrough);
    let body = Value::Object(body);
    if serde_json::to_vec(&body)
        .map_err(|_| SendError::Invalid)?
        .len()
        > 128 * 1024
    {
        return Err(SendError::Invalid);
    }
    Ok(body)
}

#[derive(Debug, PartialEq, Eq)]
pub struct Accepted {
    pub id: String,
    pub timestamp: String,
}

pub struct Webhook {
    client: Client,
    endpoint: Url,
}
impl Webhook {
    /// Restrict credential-bearing destinations to Discord's official HTTPS hosts.
    pub fn new(url: &str) -> Result<Self, SendError> {
        let endpoint = Url::parse(url).map_err(|_| SendError::Invalid)?;
        let segments: Vec<_> = endpoint
            .path_segments()
            .ok_or(SendError::Invalid)?
            .collect();
        if endpoint.scheme() != "https"
            || !matches!(endpoint.host_str(), Some("discord.com" | "discordapp.com"))
            || endpoint.port().is_some_and(|p| p != 443)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
            || segments.len() != 4
            || segments[0] != "api"
            || segments[1] != "webhooks"
            || segments[2].is_empty()
            || !segments[2].bytes().all(|b| b.is_ascii_digit())
            || segments[3].is_empty()
            || segments[3].len() > 256
            || !segments[3]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        {
            return Err(SendError::Invalid);
        }
        Self::with_endpoint(endpoint)
    }
    fn with_endpoint(mut endpoint: Url) -> Result<Self, SendError> {
        let query: Vec<(String, String)> = endpoint
            .query_pairs()
            .filter(|(k, _)| k != "wait")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        endpoint.set_query(None);
        {
            let mut pairs = endpoint.query_pairs_mut();
            for (k, v) in query {
                pairs.append_pair(&k, &v);
            }
            pairs.append_pair("wait", "true");
        }
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
        if status == 429 {
            let header = discord_retry_header(response.headers());
            let body = rate_limit_body(&mut response).await;
            return Err(SendError::RateLimited(match (header, body) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            }));
        }
        if (400..500).contains(&status) && ![408, 409].contains(&status) {
            return Err(SendError::Rejected(Some(status)));
        }
        if !(200..300).contains(&status) {
            return Err(SendError::Unknown);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| SendError::Unknown)? {
            if bytes.len() + chunk.len() > 64 * 1024 {
                return Err(SendError::Unknown);
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| SendError::Unknown)?;
        let field = |name| {
            value
                .get(name)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty() && s.len() <= 255 && !s.chars().any(char::is_control))
                .map(str::to_owned)
                .ok_or(SendError::Unknown)
        };
        Ok(Accepted {
            id: field("id")?,
            timestamp: field("timestamp")?,
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
    fn official_webhook_urls_and_merge_precedence() {
        for invalid in [
            "http://discord.com/api/webhooks/123/token",
            "https://discord.com.evil.test/api/webhooks/123/token",
            "https://user@discord.com/api/webhooks/123/token",
            "https://discord.com/api/webhooks/abc/token",
            "https://discord.com/api/webhooks/123/token/extra",
            "https://discord.com/api/webhooks/123/token#secret",
        ] {
            assert!(Webhook::new(invalid).is_err(), "{invalid}");
        }
        assert!(
            Webhook::new("https://discord.com/api/webhooks/123/a-b_C?thread_id=42&wait=false")
                .is_ok()
        );
        let map = |v: Value| v.as_object().unwrap().clone();
        let body = request_body(
            "trigger",
            &map(json!({"content":"custom", "allowed_mentions":{"parse":[]}})),
            &map(json!({"content":"provider", "allowed_mentions":{"users":["1"]}})),
            &map(json!({"content":"passthrough"})),
        )
        .unwrap();
        assert_eq!(
            body,
            json!({"content":"passthrough", "allowed_mentions":{"parse":[],"users":["1"]}})
        );
        let merged = request_body(
            "text",
            &map(json!({"embeds":[{"title":"a"}]})),
            &map(json!({"embeds":[{"title":"b"}]})),
            &map(json!({"embeds":[{"title":"c"}]})),
        )
        .unwrap();
        assert_eq!(
            merged["embeds"],
            json!([{"title":"a"},{"title":"b"},{"title":"c"}])
        );
        assert!(request_body(&"x".repeat(131073), &Map::new(), &Map::new(), &Map::new()).is_err());
    }

    #[test]
    fn rate_limit_guidance_rejects_invalid_and_rounds_fractional_values() {
        assert_eq!(discord_retry_body(br#"{"retry_after":1.25}"#), Some(2));
        assert_eq!(discord_retry_body(br#"{"retry_after":0}"#), Some(0));
        for value in [
            br#"{"retry_after":-1}"#.as_slice(),
            br#"{"retry_after":86400.1}"#,
            br#"{"retry_after":"5"}"#,
            br#"{"retry_after":1,"retry_after":2}"#,
            b"not json",
        ] {
            assert_eq!(discord_retry_body(value), None);
        }
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "1.25".parse().unwrap());
        assert_eq!(discord_retry_header(&headers), Some(2));
        headers.append(reqwest::header::RETRY_AFTER, "3".parse().unwrap());
        assert_eq!(discord_retry_header(&headers), None);
    }
    #[tokio::test]
    async fn rate_limit_response_uses_larger_valid_body_or_header_without_retry() {
        for (header, body, expected) in [
            ("Retry-After: 1.2\r\n", r#"{"retry_after":3.25}"#, Some(4)),
            ("Retry-After: 7\r\n", r#"{"retry_after":2.5}"#, Some(7)),
            ("", r#"{"retry_after":0.25}"#, Some(1)),
            ("", r#"{"retry_after":-1}"#, None),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/webhook", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await.unwrap();
                socket.write_all(format!("HTTP/1.1 429 Too Many Requests\r\n{header}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            });
            assert_eq!(
                Webhook::test_endpoint(&url)
                    .unwrap()
                    .send(&json!({"content":"test"}))
                    .await,
                Err(SendError::RateLimited(expected))
            );
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn stalled_rate_limit_body_keeps_header_guidance() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/webhook", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await.unwrap();
            socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 7\r\nContent-Length: 100\r\n\r\n").await.unwrap();
            tokio::time::sleep(Duration::from_millis(1500)).await;
        });
        assert_eq!(
            Webhook::test_endpoint(&url)
                .unwrap()
                .send(&json!({"content":"test"}))
                .await,
            Err(SendError::RateLimited(Some(7)))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sends_novu_webhook_contract_and_classifies_uncertain_results() {
        for (status, response_body, expected) in [
            (
                200,
                r#"{"id":"123","timestamp":"2026-01-02T00:00:00Z"}"#,
                Ok(Accepted {
                    id: "123".into(),
                    timestamp: "2026-01-02T00:00:00Z".into(),
                }),
            ),
            (204, "", Err(SendError::Unknown)),
            (200, "{}", Err(SendError::Unknown)),
            (400, "bad", Err(SendError::Rejected(Some(400)))),
            (408, "timeout", Err(SendError::Unknown)),
            (429, "rate", Err(SendError::RateLimited(Some(7)))),
            (500, "error", Err(SendError::Unknown)),
            (302, "redirect", Err(SendError::Unknown)),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!(
                "http://{}/api/webhooks/123/token?thread_id=42&wait=false",
                listener.local_addr().unwrap()
            );
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&buffer[..n]);
                    if let Some(pos) = data.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..pos]);
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse().ok())
                            })
                            .unwrap();
                        if data.len() >= pos + 4 + length {
                            break;
                        }
                    }
                }
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nRetry-After: 7\r\nConnection: close\r\n\r\n{response_body}", response_body.len()).as_bytes()).await.unwrap();
                String::from_utf8(data).unwrap()
            });
            let result = Webhook::test_endpoint(&url)
                .unwrap()
                .send(&json!({"content":"hello"}))
                .await;
            assert_eq!(result, expected);
            let request = server.await.unwrap();
            assert!(
                request.starts_with(
                    "POST /api/webhooks/123/token?thread_id=42&wait=true HTTP/1.1\r\n"
                ),
                "{request}"
            );
            assert!(request.ends_with(r#"{"content":"hello"}"#));
        }
    }
}
