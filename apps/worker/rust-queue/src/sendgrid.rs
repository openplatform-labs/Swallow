//! Bounded SendGrid email adapter. Workflow and integration management are not wired yet.
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{Client, header};
use serde_json::{Map, Value, json};
use std::{fmt, time::Duration};

/// Settings for one integration. This value must not be logged.
pub struct Config {
    pub api_key: String,
    pub from: String,
    pub sender_name: String,
    pub region: Region,
}
#[derive(Clone, Copy)]
pub enum Region {
    Global,
    Eu,
}

/// Supported subset of Novu's email provider options.
#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Email {
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub from: Option<String>,
    pub sender_name: Option<String>,
    pub provider_from_without_name: bool,
    pub reply_to: Option<String>,
    pub subject: String,
    pub html: String,
    pub text: Option<String>,
    pub alternatives: Vec<Alternative>,
    pub attachments: Vec<Attachment>,
    pub headers: Map<String, Value>,
    pub custom_data: Map<String, Value>,
    pub message_id: Option<String>,
    pub transaction_id: Option<String>,
    pub workflow_identifier: Option<String>,
    pub subscriber_id: Option<String>,
    pub workflow_context: Option<WorkflowContext>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowContext {
    pub subscriber_internal_id: String,
    #[serde(default)]
    pub check_workflow: bool,
    #[serde(default)]
    pub workflow_internal_id: Option<String>,
    #[serde(default)]
    pub notification_internal_id: Option<String>,
    pub subject_template: String,
    pub html_template: String,
    pub payload: Value,
    pub context: Value,
    #[serde(default)]
    pub context_keys: Option<Vec<String>>,
    pub actor: Option<Value>,
    pub overrides: Map<String, Value>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Alternative {
    pub content_type: String,
    pub content: String,
}

/// The file is raw bytes; base64 encoding happens only in the provider body.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attachment {
    pub file: Vec<u8>,
    pub name: String,
    pub mime: String,
    pub cid: Option<String>,
    pub disposition: Option<String>,
}
#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    InvalidConfiguration,
    InvalidMessage,
    Transport,
    Provider(u16),
}
impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => f.write_str("invalid SendGrid configuration"),
            Self::InvalidMessage => f.write_str("invalid SendGrid message"),
            Self::Transport => f.write_str("SendGrid transport failed"),
            Self::Provider(status) => write!(f, "SendGrid returned HTTP {status}"),
        }
    }
}
impl std::error::Error for SendError {}
pub struct Receipt {
    pub id: Option<String>,
    pub date: Option<String>,
}

/// Construct the supported SendGrid v3 wire body without credentials.
pub fn request_body(config: &Config, message: &Email) -> Result<Value, SendError> {
    if message.to.is_empty() || message.to.iter().any(String::is_empty) {
        return Err(SendError::InvalidMessage);
    }
    let address = message
        .from
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(&config.from);
    if address.is_empty() {
        return Err(SendError::InvalidConfiguration);
    }
    let name = message
        .sender_name
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(&config.sender_name);
    let addresses = |items: &[String]| -> Vec<Value> {
        items.iter().map(|email| json!({"email":email})).collect()
    };
    let mut personalization = json!({"to":addresses(&message.to)});
    if let Some(data) = message
        .custom_data
        .get("dynamicTemplateData")
        .filter(|v| !v.is_null())
    {
        if !data.is_object() {
            return Err(SendError::InvalidMessage);
        }
        personalization["dynamic_template_data"] = data.clone();
    }
    if !message.cc.is_empty() {
        personalization["cc"] = json!(addresses(&message.cc));
    }
    if !message.bcc.is_empty() {
        personalization["bcc"] = json!(addresses(&message.bcc));
    }
    let mut custom = Map::new();
    for (key, value) in [
        ("id", message.message_id.as_ref()),
        ("novuTransactionId", message.transaction_id.as_ref()),
        ("novuMessageId", message.message_id.as_ref()),
        (
            "novuWorkflowIdentifier",
            message.workflow_identifier.as_ref(),
        ),
        ("novuSubscriberId", message.subscriber_id.as_ref()),
    ] {
        if let Some(value) = value {
            custom.insert(key.to_owned(), json!(value));
        }
    }
    custom.extend(
        message
            .custom_data
            .iter()
            .filter(|(key, _)| *key != "templateId" && *key != "dynamicTemplateData")
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    let mut body = json!({"from":{"email":address,"name":name},"subject":message.subject,"personalizations":[personalization],"content":[{"value":message.html,"type":"text/html"}]});
    if message.provider_from_without_name {
        body["from"] = json!({"email":address});
    }
    if let Some(template) = message
        .custom_data
        .get("templateId")
        .filter(|v| !v.is_null())
    {
        let template = template.as_str().ok_or(SendError::InvalidMessage)?;
        if !template.is_empty() {
            body["template_id"] = json!(template);
        }
    }
    if !message.alternatives.is_empty() {
        let mut content = Vec::new();
        if let Some(text) = message.text.as_ref().filter(|v| !v.is_empty()) {
            content.push(json!({"type":"text/plain","value":text}));
        }
        content.push(json!({"type":"text/html","value":message.html}));
        content.extend(
            message
                .alternatives
                .iter()
                .map(|alt| json!({"type":alt.content_type,"value":alt.content})),
        );
        body["content"] = json!(content);
    }
    if !message.attachments.is_empty() {
        let attachments: Vec<Value> = message.attachments.iter().map(|attachment| {
            let mut item = json!({"content":STANDARD.encode(&attachment.file),"filename":attachment.name,"type":attachment.mime});
            if let Some(cid) = attachment.cid.as_ref().filter(|v| !v.is_empty()) { item["content_id"] = json!(cid); }
            if let Some(disposition) = attachment.disposition.as_ref().filter(|v| !v.is_empty()).map(String::as_str).or_else(|| attachment.cid.as_ref().filter(|v| !v.is_empty()).map(|_| "inline")) { item["disposition"] = json!(disposition); }
            item
        }).collect();
        body["attachments"] = json!(attachments);
    }
    if !custom.is_empty() {
        body["custom_args"] = json!(custom);
    }
    if let Some(workflow) = &message.workflow_identifier {
        body["categories"] = if workflow.is_empty() {
            json!([])
        } else {
            json!([workflow])
        };
    }
    if let Some(reply) = &message.reply_to {
        body["reply_to"] = json!({"email":reply});
    }
    if !message.headers.is_empty() {
        body["headers"] = json!(message.headers);
    }
    Ok(body)
}

pub struct Sendgrid {
    http: Client,
    endpoint: String,
    config: Config,
}
impl Sendgrid {
    pub fn new(config: Config) -> Result<Self, SendError> {
        let endpoint = match config.region {
            Region::Global => "https://api.sendgrid.com/v3/mail/send",
            Region::Eu => "https://api.eu.sendgrid.com/v3/mail/send",
        };
        Self::with_endpoint(config, endpoint)
    }
    #[cfg(test)]
    pub(crate) fn test_endpoint(config: Config, endpoint: &str) -> Result<Self, SendError> {
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
    /// Send once; ambiguous network failures are never automatically retried.
    pub async fn send(&self, message: &Email) -> Result<Receipt, SendError> {
        let body = request_body(&self.config, message)?;
        let response = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&self.config.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|_| SendError::Transport)?;
        let status = response.status();
        if !status.is_success() {
            return Err(SendError::Provider(status.as_u16()));
        }
        let id = message
            .message_id
            .clone()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                response
                    .headers()
                    .get("x-message-id")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
            });
        let date = response
            .headers()
            .get(header::DATE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        Ok(Receipt { id, date })
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
            api_key: "SG.test".into(),
            from: "sender@example.test".into(),
            sender_name: "Sender".into(),
            region: Region::Global,
        }
    }
    fn message() -> Email {
        Email {
            to: vec!["a@example.test".into()],
            subject: "Hello".into(),
            html: "<p>Hello</p>".into(),
            ..Email::default()
        }
    }
    #[test]
    fn rejects_invalid_dynamic_templates() {
        let mut email = message();
        email.custom_data.insert("templateId".into(), json!(123));
        assert_eq!(
            request_body(&config(), &email),
            Err(SendError::InvalidMessage)
        );
    }
    #[tokio::test]
    async fn posts_http_and_does_not_follow_redirects() {
        for (status, expected) in [
            (202, Ok(())),
            (429, Err(SendError::Provider(429))),
            (302, Err(SendError::Provider(302))),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v3/mail/send", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut tmp = [0; 4096];
                loop {
                    let n = socket.read(&mut tmp).await.unwrap();
                    assert!(n > 0);
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&buf[..pos]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        if buf.len() >= pos + 4 + length {
                            break;
                        }
                    }
                }
                let text = String::from_utf8(buf).unwrap();
                assert!(text.starts_with("POST /v3/mail/send HTTP/1.1\r\n"));
                assert!(
                    text.to_ascii_lowercase()
                        .contains("authorization: bearer sg.test")
                );
                let (_, body) = text.split_once("\r\n\r\n").unwrap();
                let body: Value = serde_json::from_str(body).unwrap();
                assert_eq!(body, request_body(&config(), &message()).unwrap());
                assert!(!body.to_string().contains("SG.test"));
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nX-Message-Id: provider-id\r\nLocation: http://127.0.0.1:1/\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            });
            let provider = Sendgrid::with_endpoint(config(), &endpoint).unwrap();
            let result = provider.send(&message()).await;
            match expected {
                Ok(()) => assert_eq!(result.unwrap().id.as_deref(), Some("provider-id")),
                Err(error) => assert_eq!(result.err(), Some(error)),
            }
            server.await.unwrap();
        }
    }
}
