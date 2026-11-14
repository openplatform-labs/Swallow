//! Bounded Resend API adapter used by the experimental email worker.
use crate::sendgrid::Email;
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{Client, header};
use serde_json::{Value, json};
use std::{fmt, time::Duration};

pub struct Config {
    pub api_key: String,
    pub from: String,
    pub sender_name: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    InvalidConfiguration,
    InvalidMessage,
    Transport,
    InvalidResponse,
    Provider(u16),
}
impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => f.write_str("invalid Resend configuration"),
            Self::InvalidMessage => f.write_str("invalid Resend message"),
            Self::Transport => f.write_str("Resend transport failed"),
            Self::InvalidResponse => f.write_str("invalid Resend success response"),
            Self::Provider(code) => write!(f, "Resend returned HTTP {code}"),
        }
    }
}
impl std::error::Error for SendError {}

pub fn request_body(config: &Config, email: &Email) -> Result<Value, SendError> {
    if email.to.is_empty()
        || email.to.len() > 50
        || email.to.iter().any(String::is_empty)
        || email.cc.iter().chain(&email.bcc).any(String::is_empty)
        || email.attachments.len() > 100
        || !email.alternatives.is_empty()
        || !email.custom_data.is_empty()
        || email.provider_from_without_name
    {
        return Err(SendError::InvalidMessage);
    }
    let from = email
        .from
        .as_deref()
        .filter(|v| !v.is_empty())
        .unwrap_or(&config.from);
    if from.is_empty() {
        return Err(SendError::InvalidConfiguration);
    }
    let name = email
        .sender_name
        .as_deref()
        .filter(|v| !v.is_empty())
        .unwrap_or(&config.sender_name);
    if from.contains(['\r', '\n']) || name.contains(['\r', '\n', '<', '>']) {
        return Err(SendError::InvalidMessage);
    }
    let from = if name.is_empty() {
        from.to_owned()
    } else {
        format!("{name} <{from}>")
    };
    let mut body = json!({"from":from,"to":email.to,"subject":email.subject,"html":email.html,"reply_to":email.reply_to});
    if !email.cc.is_empty() || email.workflow_wire_defaults {
        body["cc"] = json!(email.cc);
    }
    if !email.bcc.is_empty() || email.workflow_wire_defaults {
        body["bcc"] = json!(email.bcc);
    }
    if let Some(text) = &email.text {
        body["text"] = json!(text);
    }
    if email.headers.len() > 100
        || email.headers.iter().any(|(key, value)| {
            header::HeaderName::from_bytes(key.as_bytes()).is_err()
                || value
                    .as_str()
                    .is_none_or(|v| v.len() > 4096 || header::HeaderValue::from_str(v).is_err())
        })
    {
        return Err(SendError::InvalidMessage);
    }
    if !email.headers.is_empty() || email.workflow_wire_defaults {
        body["headers"] = json!(email.headers);
    }
    // Use an intentionally conservative limit below Resend's encoded 40 MB
    // limit. Reject unknown disposition rather than silently dropping it.
    let mut size = 0usize;
    let mut attachments = Vec::with_capacity(email.attachments.len());
    for file in &email.attachments {
        size = size
            .checked_add(file.file.len())
            .ok_or(SendError::InvalidMessage)?;
        if size > 20 * 1024 * 1024
            || file.name.is_empty()
            || file.name.len() > 255
            || file.name.contains(['\r', '\n'])
            || file.disposition.is_some()
            || file
                .cid
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 255 || v.contains(['\r', '\n']))
        {
            return Err(SendError::InvalidMessage);
        }
        let mut item = json!({"filename":file.name,"content":STANDARD.encode(&file.file)});
        if let Some(cid) = &file.cid {
            item["content_id"] = json!(cid);
        }
        attachments.push(item);
    }
    if !attachments.is_empty() {
        body["attachments"] = json!(attachments);
    }
    Ok(body)
}

pub struct Resend {
    http: Client,
    endpoint: String,
    config: Config,
}
impl Resend {
    pub fn new(config: Config) -> Result<Self, SendError> {
        Self::with_endpoint(config, "https://api.resend.com/emails")
    }
    #[cfg(any(test, feature = "test-mocks"))]
    pub fn test_endpoint(config: Config, endpoint: &str) -> Result<Self, SendError> {
        Self::with_endpoint(config, endpoint)
    }
    fn with_endpoint(config: Config, endpoint: &str) -> Result<Self, SendError> {
        if config.api_key.is_empty()
            || header::HeaderValue::from_str(&format!("Bearer {}", config.api_key)).is_err()
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
            endpoint: endpoint.to_owned(),
            config,
        })
    }
    /// Sends one request. A transport error or invalid success body is ambiguous.
    pub async fn send(
        &self,
        email: &Email,
        idempotency_key: Option<&str>,
    ) -> Result<String, SendError> {
        self.send_with_correlation(email, idempotency_key, None)
            .await
    }
    pub async fn send_with_correlation(
        &self,
        email: &Email,
        idempotency_key: Option<&str>,
        correlation: Option<uuid::Uuid>,
    ) -> Result<String, SendError> {
        let mut body = request_body(&self.config, email)?;
        if let Some(correlation) = correlation {
            body["tags"] = json!([{"name":"swallow_dispatch","value":correlation.to_string()}]);
        }
        if idempotency_key.is_some_and(|key| {
            key.is_empty() || key.len() > 256 || !key.bytes().all(|v| (0x21..=0x7e).contains(&v))
        }) {
            return Err(SendError::InvalidMessage);
        }
        let mut request = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&self.config.api_key)
            .json(&body);
        if let Some(key) = idempotency_key {
            request = request.header("Idempotency-Key", key);
        }
        let mut response = request.send().await.map_err(|_| SendError::Transport)?;
        if !response.status().is_success() {
            return Err(SendError::Provider(response.status().as_u16()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| SendError::Transport)? {
            if bytes.len() + chunk.len() > 16_384 {
                return Err(SendError::InvalidResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        let body: Value = serde_json::from_slice(&bytes).map_err(|_| SendError::InvalidResponse)?;
        let id = body
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && id.len() <= 1024)
            .ok_or(SendError::InvalidResponse)?;
        Ok(id.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn config() -> Config {
        Config {
            api_key: "re_test".into(),
            from: "sender@example.test".into(),
            sender_name: "Example".into(),
        }
    }
    fn message() -> Email {
        Email {
            to: vec!["to@example.test".into()],
            subject: "Hello".into(),
            html: "<b>Hi</b>".into(),
            ..Default::default()
        }
    }
    #[test]
    fn bounded_message_mapping() {
        let mut email = message();
        email.cc = vec!["cc@example.test".into()];
        email.bcc = vec!["bcc@example.test".into()];
        email.reply_to = Some("reply@example.test".into());
        email.text = Some("Hi".into());
        assert_eq!(
            request_body(&config(), &email).unwrap(),
            json!({"from":"Example <sender@example.test>","to":["to@example.test"],"subject":"Hello","html":"<b>Hi</b>","cc":["cc@example.test"],"bcc":["bcc@example.test"],"reply_to":"reply@example.test","text":"Hi"})
        );
        email.custom_data.insert("unsupported".into(), json!(true));
        assert_eq!(
            request_body(&config(), &email),
            Err(SendError::InvalidMessage)
        );
        email.custom_data.clear();
        email.to = vec!["x".into(); 51];
        assert_eq!(
            request_body(&config(), &email),
            Err(SendError::InvalidMessage)
        );
    }
    #[test]
    fn attachments_and_headers_are_encoded() {
        let mut email = message();
        email.headers.insert("X-Trace".into(), json!("value"));
        email.attachments.push(crate::sendgrid::Attachment {
            file: vec![0, 1, 255],
            name: "image.png".into(),
            mime: "image/png".into(),
            cid: Some("logo".into()),
            disposition: None,
        });
        let body = request_body(&config(), &email).unwrap();
        assert_eq!(body["headers"], json!({"X-Trace":"value"}));
        assert_eq!(
            body["attachments"],
            json!([{"filename":"image.png","content":"AAH/","content_id":"logo"}])
        );
        email.headers.insert("Bad\nHeader".into(), json!("value"));
        assert_eq!(
            request_body(&config(), &email),
            Err(SendError::InvalidMessage)
        );
        email.headers.clear();
        email.attachments[0].disposition = Some("attachment".into());
        assert_eq!(
            request_body(&config(), &email),
            Err(SendError::InvalidMessage)
        );
    }
    #[tokio::test]
    async fn requests_and_responses_are_bounded() {
        for (status, body, expected) in [
            (200, "{\"id\":\"provider-id\"}", Ok("provider-id")),
            (200, "{}", Err(SendError::InvalidResponse)),
            (429, "{}", Err(SendError::Provider(429))),
            (500, "{}", Err(SendError::Provider(500))),
            (302, "{}", Err(SendError::Provider(302))),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
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
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse().ok())
                            })
                            .unwrap();
                        if bytes.len() >= pos + 4 + len {
                            break;
                        }
                    }
                }
                let req = String::from_utf8(bytes).unwrap();
                assert!(req.starts_with("POST /emails HTTP/1.1\r\n"));
                assert!(
                    req.to_ascii_lowercase()
                        .contains("authorization: bearer re_test")
                );
                assert!(
                    req.to_ascii_lowercase()
                        .contains("idempotency-key: job/123")
                );
                let body_json: Value =
                    serde_json::from_str(req.split_once("\r\n\r\n").unwrap().1).unwrap();
                assert_eq!(body_json, request_body(&config(), &message()).unwrap());
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nLocation: http://127.0.0.1:1/\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            });
            let provider = Resend::with_endpoint(config(), &endpoint).unwrap();
            assert_eq!(
                provider.send(&message(), Some("job/123")).await.as_deref(),
                expected.as_ref().map(|v| *v)
            );
            server.await.unwrap();
        }
        let provider = Resend::new(config()).unwrap();
        assert_eq!(
            provider.send(&message(), Some("\ninvalid")).await,
            Err(SendError::InvalidMessage)
        );
    }

    #[tokio::test]
    #[ignore = "sends real email and requires explicit Resend credentials and recipient"]
    async fn live_send() {
        let config = Config {
            api_key: std::env::var("NOVU_RESEND_API_KEY").expect("NOVU_RESEND_API_KEY is required"),
            from: std::env::var("NOVU_RESEND_FROM").expect("NOVU_RESEND_FROM is required"),
            sender_name: String::new(),
        };
        let recipient =
            std::env::var("NOVU_RESEND_TEST_TO").expect("NOVU_RESEND_TEST_TO is required");
        assert!(!recipient.trim().is_empty());
        let email = Email {
            to: vec![recipient],
            subject: "Swallow Resend test".into(),
            html: "<p>Swallow Resend test message.</p>".into(),
            ..Default::default()
        };
        let provider = Resend::new(config).unwrap();
        let idempotency_key = format!("swallow-live-test/{}", uuid::Uuid::new_v4());
        let id = provider
            .send(&email, Some(&idempotency_key))
            .await
            .expect("Resend API request failed");
        assert!(!id.is_empty());
    }
}
