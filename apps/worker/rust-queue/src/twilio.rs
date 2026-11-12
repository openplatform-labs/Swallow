//! Experimental Twilio SMS provider connected to a limited workflow and queue.
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use std::{fmt, time::Duration};

/// Credentials must not be logged or stored in queue payloads.
pub struct Config {
    pub account_sid: String,
    pub auth_token: String,
    pub api_key_sid: Option<String>,
    pub api_key_secret: Option<String>,
    pub from: String,
    pub region: Region,
}

#[derive(Clone, Copy)]
pub enum Region {
    Us,
    Eu,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sms {
    pub to: String,
    pub content: String,
    pub from: Option<String>,
    #[serde(default)]
    pub provider_data: serde_json::Map<String, serde_json::Value>,
    pub workflow_context: Option<WorkflowContext>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowContext {
    pub subscriber_id: String,
    pub subscriber_internal_id: String,
    pub workflow_identifier: String,
    pub workflow_internal_id: Option<String>,
    #[serde(default)]
    pub notification_internal_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub transaction_id: Option<String>,
    pub content_template: String,
    pub payload: serde_json::Value,
    pub context: serde_json::Value,
    pub context_keys: Vec<String>,
    pub actor: Option<serde_json::Value>,
    #[serde(default)]
    pub actor_internal_id: Option<String>,
    pub overrides: Option<serde_json::Value>,
}

#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct RequestBody<'a> {
    #[serde(rename = "To")]
    pub to: &'a str,
    #[serde(rename = "From")]
    pub from: &'a str,
    #[serde(rename = "Body")]
    pub body: &'a str,
    #[serde(rename = "StatusCallback", skip_serializing_if = "Option::is_none")]
    pub status_callback: Option<&'a str>,
    #[serde(rename = "ApplicationSid", skip_serializing_if = "Option::is_none")]
    pub application_sid: Option<&'a str>,
    #[serde(
        rename = "MessagingServiceSid",
        skip_serializing_if = "Option::is_none"
    )]
    pub messaging_service_sid: Option<&'a str>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub fn request_body<'a>(config: &'a Config, sms: &'a Sms) -> Result<RequestBody<'a>, SendError> {
    let from = sms
        .from
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(&config.from);
    if sms.to.is_empty() || from.is_empty() {
        return Err(SendError::InvalidMessage);
    }
    let top = &sms.provider_data;
    let pass = top
        .get("_passthrough")
        .and_then(|v| v.get("body"))
        .and_then(serde_json::Value::as_object);
    let field = |key: &str| {
        pass.and_then(|v| v.get(key))
            .and_then(serde_json::Value::as_str)
            .or_else(|| top.get(key).and_then(serde_json::Value::as_str))
    };
    let to = pass
        .and_then(|v| v.get("to"))
        .and_then(serde_json::Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            top.get("to")
                .and_then(serde_json::Value::as_str)
                .filter(|v| !v.trim().is_empty())
        })
        .unwrap_or(&sms.to);
    let mut extra = serde_json::Map::new();
    for (key, wire, boolean) in [
        ("maxPrice", "MaxPrice", false),
        ("provideFeedback", "ProvideFeedback", true),
        ("attempt", "Attempt", false),
        ("validityPeriod", "ValidityPeriod", false),
        ("forceDelivery", "ForceDelivery", true),
        ("contentRetention", "ContentRetention", false),
        ("addressRetention", "AddressRetention", false),
        ("smartEncoded", "SmartEncoded", true),
        ("persistentAction", "PersistentAction", false),
        ("shortenUrls", "ShortenUrls", true),
        ("scheduleType", "ScheduleType", false),
        ("sendAt", "SendAt", false),
        ("sendAsMms", "SendAsMms", true),
        ("contentVariables", "ContentVariables", false),
        ("riskCheck", "RiskCheck", false),
        ("mediaUrl", "MediaUrl", false),
        ("contentSid", "ContentSid", false),
    ] {
        if let Some(value) = pass.and_then(|v| v.get(key)).or_else(|| top.get(key)) {
            let value = if boolean && value.is_boolean() {
                serde_json::Value::String(value.to_string())
            } else {
                value.clone()
            };
            extra.insert(wire.into(), value);
        }
    }
    Ok(RequestBody {
        to,
        from: field("from").unwrap_or(from),
        body: field("body").unwrap_or(&sms.content),
        status_callback: field("statusCallback"),
        application_sid: field("applicationSid"),
        messaging_service_sid: field("messagingServiceSid"),
        extra,
    })
}

/// Encode arrays using repeated keys, as Twilio's Node SDK does.
pub fn form_body(body: &RequestBody<'_>) -> Result<String, SendError> {
    let object = serde_json::to_value(body).map_err(|_| SendError::InvalidMessage)?;
    let mut fields = Vec::<(String, String)>::new();
    for (key, value) in object.as_object().ok_or(SendError::InvalidMessage)? {
        let values: Vec<&serde_json::Value> = match value {
            serde_json::Value::Array(items) => items.iter().collect(),
            _ => vec![value],
        };
        for item in values {
            let value = match item {
                serde_json::Value::String(value) => value.clone(),
                serde_json::Value::Number(_) | serde_json::Value::Bool(_) => item.to_string(),
                _ => return Err(SendError::InvalidMessage),
            };
            fields.push((key.clone(), value));
        }
    }
    serde_urlencoded::to_string(fields).map_err(|_| SendError::InvalidMessage)
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    InvalidConfiguration,
    InvalidMessage,
    Transport,
    Provider(u16),
    InvalidResponse,
}
impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => f.write_str("invalid Twilio configuration"),
            Self::InvalidMessage => f.write_str("invalid Twilio message"),
            Self::Transport => f.write_str("Twilio transport failed"),
            Self::Provider(status) => write!(f, "Twilio returned HTTP {status}"),
            Self::InvalidResponse => f.write_str("invalid Twilio response"),
        }
    }
}
impl std::error::Error for SendError {}

#[derive(Debug, PartialEq, Eq)]
pub struct Receipt {
    pub id: String,
    pub date: String,
}

#[derive(Deserialize)]
struct Response {
    sid: String,
    date_created: String,
}

pub struct Twilio {
    http: Client,
    endpoint: String,
    config: Config,
    status_callback_base: Option<String>,
}
impl Twilio {
    pub fn new(config: Config) -> Result<Self, SendError> {
        let origin = match config.region {
            Region::Us => "https://api.twilio.com",
            Region::Eu => "https://api.dublin.ie1.twilio.com",
        };
        Self::with_origin(config, origin)
    }

    /// Enable job-specific callbacks. This URL must match the API's configured public URL.
    pub fn with_status_callback(
        mut self,
        base: &str,
        environment: &str,
    ) -> Result<Self, SendError> {
        let url = reqwest::Url::parse(base).map_err(|_| SendError::InvalidConfiguration)?;
        let suffix = format!("/environments/{environment}/sms/twilio");
        let prefix = url
            .path()
            .strip_suffix(&suffix)
            .ok_or(SendError::InvalidConfiguration)?;
        if url.scheme() != "https"
            || url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.as_str() != base
            || environment.is_empty()
            || !environment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || !prefix.starts_with("/webhooks/organizations/")
            || prefix
                .trim_start_matches("/webhooks/organizations/")
                .is_empty()
            || prefix
                .trim_start_matches("/webhooks/organizations/")
                .contains('/')
        {
            return Err(SendError::InvalidConfiguration);
        }
        self.status_callback_base = Some(base.to_owned());
        Ok(self)
    }

    pub(crate) fn status_callback(&self, job: uuid::Uuid, dispatch: uuid::Uuid) -> Option<String> {
        self.status_callback_base
            .as_ref()
            .map(|base| format!("{base}/jobs/{job}/attempts/{dispatch}"))
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[cfg(test)]
    pub(crate) fn test_origin(config: Config, origin: &str) -> Result<Self, SendError> {
        Self::with_origin(config, origin)
    }

    fn with_origin(config: Config, origin: &str) -> Result<Self, SendError> {
        if config.account_sid.is_empty()
            || !config
                .account_sid
                .bytes()
                .all(|b| b.is_ascii_alphanumeric())
        {
            return Err(SendError::InvalidConfiguration);
        }
        let (username, password) = match config.api_key_sid.as_deref().filter(|v| !v.is_empty()) {
            Some(key) => (key, config.api_key_secret.as_deref().unwrap_or("")),
            None => (config.account_sid.as_str(), config.auth_token.as_str()),
        };
        if username.is_empty() || password.is_empty() || username.contains(':') {
            return Err(SendError::InvalidConfiguration);
        }
        let endpoint = format!(
            "{origin}/2010-04-01/Accounts/{}/Messages.json",
            config.account_sid
        );
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| SendError::InvalidConfiguration)?;
        Ok(Self {
            http,
            endpoint,
            config,
            status_callback_base: None,
        })
    }

    /// Submits once. A transport or malformed response does not prove non-delivery.
    pub async fn send(&self, sms: &Sms) -> Result<Receipt, SendError> {
        let body = form_body(&request_body(&self.config, sms)?)?;
        let (username, password) =
            match self.config.api_key_sid.as_deref().filter(|v| !v.is_empty()) {
                Some(key) => (key, self.config.api_key_secret.as_deref().unwrap_or("")),
                None => (
                    self.config.account_sid.as_str(),
                    self.config.auth_token.as_str(),
                ),
            };
        let response = self
            .http
            .post(&self.endpoint)
            .basic_auth(username, Some(password))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .map_err(|_| SendError::Transport)?;
        let status = response.status();
        if !status.is_success() {
            return Err(SendError::Provider(status.as_u16()));
        }
        let response: Response = response
            .json()
            .await
            .map_err(|_| SendError::InvalidResponse)?;
        let date = DateTime::parse_from_rfc2822(&response.date_created)
            .map_err(|_| SendError::InvalidResponse)?
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        Ok(Receipt {
            id: response.sid,
            date,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn config(api_key: bool) -> Config {
        Config {
            account_sid: "AC00000000000000000000000000000001".into(),
            auth_token: "fixture-token".into(),
            api_key_sid: api_key.then(|| "SKfixture".into()),
            api_key_secret: api_key.then(|| "fixture-secret".into()),
            from: "+15550000001".into(),
            region: Region::Us,
        }
    }

    #[tokio::test]
    async fn submits_form_with_both_auth_modes_and_rejects_errors() {
        for (api_key, status) in [(false, 201), (true, 201), (false, 429), (false, 302)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
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
                let request = String::from_utf8(buf).unwrap();
                assert!(request.starts_with("POST /2010-04-01/Accounts/AC00000000000000000000000000000001/Messages.json HTTP/1.1\r\n"));
                let credentials = if api_key {
                    "SKfixture:fixture-secret"
                } else {
                    "AC00000000000000000000000000000001:fixture-token"
                };
                assert!(request.contains(&format!(
                    "authorization: Basic {}\r\n",
                    STANDARD.encode(credentials)
                )));
                assert!(request.contains("content-type: application/x-www-form-urlencoded\r\n"));
                let (_, body) = request.split_once("\r\n\r\n").unwrap();
                let form: std::collections::HashMap<String, String> =
                    serde_urlencoded::from_str(body).unwrap();
                assert_eq!(form.get("To").unwrap(), "+15550000002");
                assert_eq!(form.get("From").unwrap(), "+15550000001");
                assert_eq!(form.get("Body").unwrap(), "Hello & 한글");
                let body = r#"{"sid":"SM123","date_created":"Wed, 28 Oct 2026 10:00:00 +0000"}"#;
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nLocation: http://127.0.0.1:1/\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
            let provider = Twilio::with_origin(config(api_key), &origin).unwrap();
            let result = provider
                .send(&Sms {
                    to: "+15550000002".into(),
                    content: "Hello & 한글".into(),
                    from: None,
                    workflow_context: None,
                    provider_data: Default::default(),
                })
                .await;
            if status == 201 {
                assert_eq!(
                    result.unwrap(),
                    Receipt {
                        id: "SM123".into(),
                        date: "2026-10-28T10:00:00.000Z".into()
                    }
                );
            } else {
                assert_eq!(result.err(), Some(SendError::Provider(status)));
            }
            server.await.unwrap();
        }
    }

    #[test]
    fn validates_configuration_and_uses_message_from_override() {
        let mut config = config(false);
        let sms = Sms {
            to: "+15550000002".into(),
            content: "Hi".into(),
            from: Some("+15550000003".into()),
            workflow_context: None,
            provider_data: Default::default(),
        };
        assert_eq!(request_body(&config, &sms).unwrap().from, "+15550000003");
        config.account_sid = "../bad".into();
        assert!(matches!(
            Twilio::new(config),
            Err(SendError::InvalidConfiguration)
        ));
        let mut config = config_with_missing_key_secret();
        assert!(matches!(
            Twilio::new(config_with_missing_key_secret()),
            Err(SendError::InvalidConfiguration)
        ));
        config.api_key_sid = None;
        assert!(Twilio::new(config).is_ok());
    }

    #[test]
    fn provider_arrays_use_repeated_form_keys_and_booleans_are_strings() {
        let sms = Sms {
            to: "+15550000002".into(),
            content: "Hello".into(),
            provider_data: serde_json::json!({"mediaUrl":["https://example.test/a","https://example.test/b"],"provideFeedback":true,"attempt":2}).as_object().unwrap().clone(),
            ..Sms::default()
        };
        let config = config(false);
        let body = request_body(&config, &sms).unwrap();
        let fields: Vec<(String, String)> =
            serde_urlencoded::from_str(&form_body(&body).unwrap()).unwrap();
        let media: Vec<_> = fields
            .iter()
            .filter(|(key, _)| key == "MediaUrl")
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(media, ["https://example.test/a", "https://example.test/b"]);
        assert!(fields.contains(&("ProvideFeedback".into(), "true".into())));
        assert!(fields.contains(&("Attempt".into(), "2".into())));
    }

    fn config_with_missing_key_secret() -> Config {
        let mut config = config(true);
        config.api_key_secret = None;
        config
    }
}
