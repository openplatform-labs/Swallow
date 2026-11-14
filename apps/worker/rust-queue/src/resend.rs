//! Bounded Resend API adapter. Queue and workflow integration are separate.
use crate::sendgrid::Email;
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
        || !email.alternatives.is_empty()
        || !email.attachments.is_empty()
        || !email.headers.is_empty()
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
    let mut body = json!({"from":from,"to":email.to,"subject":email.subject,"html":email.html});
    if !email.cc.is_empty() {
        body["cc"] = json!(email.cc);
    }
    if !email.bcc.is_empty() {
        body["bcc"] = json!(email.bcc);
    }
    if let Some(text) = &email.text {
        body["text"] = json!(text);
    }
    if let Some(reply) = &email.reply_to {
        body["reply_to"] = json!(reply);
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
        let body = request_body(&self.config, email)?;
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
}
