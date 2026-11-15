//! SMTP alert transport. SMTP acceptance is not recipient delivery.
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor, message::header::ContentType,
    transport::smtp::authentication::Credentials,
};
use std::time::Duration;

pub struct SmtpAlert {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: String,
    host: String,
    port: String,
    tls: String,
    username: String,
    password: String,
    ca_fingerprint: Option<String>,
    ca_pem: Option<String>,
}
impl SmtpAlert {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::from_env_with_password_file(None)
    }
    pub fn from_env_with_password_file(
        path: Option<&std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let host = std::env::var("SWALLOW_SMTP_HOST")?;
        let from = std::env::var("SWALLOW_SMTP_FROM")?;
        let tls = std::env::var("SWALLOW_SMTP_TLS").unwrap_or_else(|_| "starttls".into());
        let port = match std::env::var("SWALLOW_SMTP_PORT") {
            Ok(v) => v
                .parse::<u16>()
                .ok()
                .filter(|v| *v > 0)
                .ok_or("invalid SMTP port")?,
            Err(std::env::VarError::NotPresent) => {
                if tls == "tls" {
                    465
                } else {
                    587
                }
            }
            Err(_) => return Err("invalid SMTP port".into()),
        };
        let username = match std::env::var("SWALLOW_SMTP_USERNAME") {
            Ok(v) => Some(v),
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => return Err("invalid SMTP username".into()),
        };
        let password = if let Some(path) = path {
            if std::env::var_os("SWALLOW_SMTP_PASSWORD").is_some() {
                return Err("conflicting SMTP password sources".into());
            }
            Some(novu_rust_contracts::secret::read_regular_file(path)?)
        } else {
            novu_rust_contracts::secret::from_env(
                "SWALLOW_SMTP_PASSWORD",
                "SWALLOW_SMTP_PASSWORD_FILE",
            )?
        };
        let ca_pem = match std::env::var_os("SWALLOW_SMTP_CA_FILE") {
            Some(path) => Some(novu_rust_contracts::secret::read_regular_multiline_file(
                std::path::Path::new(&path),
                65_536,
            )?),
            None => None,
        };
        Self::new(host, port, tls, from, username, password, ca_pem)
    }
    fn new(
        host: String,
        port: u16,
        tls: String,
        from: String,
        username: Option<String>,
        password: Option<String>,
        ca_pem: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if host.is_empty()
            || host.len() > 253
            || host
                .bytes()
                .any(|b| !(b.is_ascii_alphanumeric() || b == b'.' || b == b'-'))
            || from.len() > 320
            || from.contains(['\r', '\n', '\0', '<', '>'])
            || from.parse::<lettre::Address>().is_err()
        {
            return Err("invalid SMTP configuration".into());
        }
        if username
            .as_ref()
            .is_some_and(|u| u.is_empty() || u.len() > 1024 || u.contains(['\r', '\n', '\0']))
            || password
                .as_ref()
                .is_some_and(|p| p.is_empty() || p.len() > 4096 || p.contains('\0'))
            || username.is_some() != password.is_some()
        {
            return Err("invalid SMTP credentials".into());
        }
        let mut builder = match tls.as_str() {
            "tls" => AsyncSmtpTransport::<Tokio1Executor>::relay(&host)?,
            "starttls" => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&host)?,
            _ => return Err("invalid SMTP TLS mode".into()),
        }
        .port(port)
        .timeout(Some(Duration::from_secs(30)));
        let ca_fingerprint = if let Some(pem) = ca_pem.as_deref() {
            use lettre::transport::smtp::client::{Certificate, Tls, TlsParameters};
            if !pem.contains("-----BEGIN CERTIFICATE-----")
                || !pem.contains("-----END CERTIFICATE-----")
            {
                return Err("invalid SMTP CA".into());
            }
            let cert = Certificate::from_pem(pem.as_bytes()).map_err(|_| "invalid SMTP CA")?;
            let params = TlsParameters::builder(host.clone())
                .add_root_certificate(cert)
                .build()
                .map_err(|_| "invalid SMTP CA")?;
            builder = builder.tls(if tls == "tls" {
                Tls::Wrapper(params)
            } else {
                Tls::Required(params)
            });
            Some(format!(
                "ca-sha256:{}",
                ring::digest::digest(&ring::digest::SHA256, pem.as_bytes())
                    .as_ref()
                    .iter()
                    .map(|v| format!("{v:02x}"))
                    .collect::<String>()
            ))
        } else {
            None
        };
        if let (Some(user), Some(pass)) = (&username, &password) {
            builder = builder.credentials(Credentials::new(user.clone(), pass.clone()));
        }
        Ok(Self {
            transport: builder.build(),
            from,
            host,
            port: port.to_string(),
            tls,
            username: username.unwrap_or_default(),
            password: password.unwrap_or_default(),
            ca_fingerprint,
            ca_pem,
        })
    }
    /// Rebuild the transport only after the replacement secret validates.
    pub fn reload_password_file(
        &mut self,
        path: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let password = novu_rust_contracts::secret::read_regular_file(path)?;
        if password == self.password {
            return Ok(());
        }
        let replacement = Self::new(
            self.host.clone(),
            self.port.parse()?,
            self.tls.clone(),
            self.from.clone(),
            Some(self.username.clone()),
            Some(password),
            self.ca_pem.clone(),
        )?;
        *self = replacement;
        Ok(())
    }
    pub fn binding_parts(&self) -> Vec<&str> {
        let mut values = vec![
            self.host.as_str(),
            self.port.as_str(),
            self.tls.as_str(),
            self.from.as_str(),
            self.username.as_str(),
            self.password.as_str(),
        ];
        if let Some(ca) = &self.ca_fingerprint {
            values.push(ca);
        }
        values
    }
    pub async fn send(&self, recipient: &str, text: String) -> Result<(), ()> {
        if recipient.len() > 320 || recipient.contains(['\r', '\n', '\0', '<', '>']) {
            return Err(());
        }
        let from = self.from.parse().map_err(|_| ())?;
        let to = recipient.parse().map_err(|_| ())?;
        let email = Message::builder()
            .from(from)
            .to(to)
            .subject("Swallow delivery alert")
            .header(ContentType::TEXT_PLAIN)
            .body(text)
            .map_err(|_| ())?;
        self.transport.send(email).await.map(|_| ()).map_err(|_| ())
    }
    /// Build the supported workflow subset before reserving a send.
    pub fn prepare_workflow(&self, email: &crate::sendgrid::Email) -> Result<Message, ()> {
        use lettre::message::{Attachment, Mailbox, MultiPart, SinglePart};
        fn address(value: &str) -> Result<lettre::Address, ()> {
            if value.len() > 320 || value.contains(['\r', '\n', '\0', '<', '>']) {
                return Err(());
            }
            value.parse().map_err(|_| ())
        }
        if email.to.is_empty()
            || email
                .to
                .len()
                .saturating_add(email.cc.len())
                .saturating_add(email.bcc.len())
                > 1000
            || email.subject.contains(['\r', '\n', '\0'])
            || email.subject.len() > 998
            || email.html.len() > 1_048_576
            || email.text.as_ref().is_some_and(|v| v.len() > 1_048_576)
            || !email.alternatives.is_empty()
            || !email.custom_data.is_empty()
            || email.ip_pool_name.is_some()
        {
            return Err(());
        }
        let from = address(
            email
                .from
                .as_deref()
                .filter(|v| !v.is_empty())
                .unwrap_or(&self.from),
        )?;
        let name = email
            .sender_name
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned();
        if name
            .as_ref()
            .is_some_and(|s| s.len() > 256 || s.contains(['\r', '\n', '\0']))
        {
            return Err(());
        }
        let mut builder = Message::builder()
            .from(Mailbox::new(name, from))
            .subject(email.subject.as_str());
        // Keep transport, addressing and MIME fields controlled by the builder.
        // Reject case-insensitive duplicates instead of silently overwriting one.
        let mut header_names = std::collections::HashSet::new();
        if email.headers.len() > 100 {
            return Err(());
        }
        for (name, value) in &email.headers {
            let lower = name.to_ascii_lowercase();
            let reserved = matches!(
                lower.as_str(),
                "from"
                    | "sender"
                    | "reply-to"
                    | "to"
                    | "cc"
                    | "bcc"
                    | "subject"
                    | "date"
                    | "message-id"
                    | "mime-version"
                    | "received"
                    | "return-path"
                    | "delivered-to"
                    | "authentication-results"
                    | "dkim-signature"
            ) || lower.starts_with("content-")
                || lower.starts_with("resent-");
            let Some(value) = value.as_str() else {
                return Err(());
            };
            if reserved
                || name.len() > 76
                || name.is_empty()
                || !name.bytes().all(|c| (33..=126).contains(&c) && c != b':')
                || !header_names.insert(lower)
                || value.len() > 4096
                || value.chars().any(char::is_control)
            {
                return Err(());
            }
            let name = lettre::message::header::HeaderName::new_from_ascii(name.clone())
                .map_err(|_| ())?;
            builder = builder.raw_header(lettre::message::header::HeaderValue::new(
                name,
                value.to_owned(),
            ));
        }
        for value in &email.to {
            builder = builder.to(Mailbox::new(None, address(value)?));
        }
        for value in &email.cc {
            builder = builder.cc(Mailbox::new(None, address(value)?));
        }
        for value in &email.bcc {
            builder = builder.bcc(Mailbox::new(None, address(value)?));
        }
        if let Some(reply) = email.reply_to.as_ref().filter(|s| !s.is_empty()) {
            builder = builder.reply_to(Mailbox::new(None, address(reply)?));
        }
        // Validate before allocating MIME parts or reserving an external send.
        let mut total = 0usize;
        let mut inline = Vec::new();
        let mut attached = Vec::new();
        let mut content_ids = std::collections::HashSet::new();
        if email.attachments.len() > 100 {
            return Err(());
        }
        for file in &email.attachments {
            total = total.checked_add(file.file.len()).ok_or(())?;
            if total > 20 * 1024 * 1024
                || file.name.is_empty()
                || file.name.len() > 255
                || file.name.chars().any(char::is_control)
                || file.mime.len() > 255
            {
                return Err(());
            }
            let mime = ContentType::parse(&file.mime).map_err(|_| ())?;
            let cid = file.cid.as_deref().filter(|v| !v.is_empty());
            if file.cid.is_some() && cid.is_none() {
                return Err(());
            }
            if cid.is_some_and(|id| !content_ids.insert(id)) {
                return Err(());
            }
            if cid.is_some_and(|v| {
                v.len() > 255
                    || !v
                        .bytes()
                        .all(|c| c.is_ascii_graphic() && !matches!(c, 34 | 60 | 62 | 92))
            }) {
                return Err(());
            }
            match (file.disposition.as_deref().filter(|v| !v.is_empty()), cid) {
                (None | Some("inline"), Some(id)) => inline.push(
                    Attachment::new_inline_with_name(id.to_owned(), file.name.clone())
                        .body(file.file.clone(), mime),
                ),
                (None | Some("attachment"), None) => {
                    attached.push(Attachment::new(file.name.clone()).body(file.file.clone(), mime))
                }
                _ => return Err(()),
            }
        }
        if email.attachments.is_empty() {
            return match &email.text {
                Some(text) => builder
                    .multipart(
                        MultiPart::alternative()
                            .singlepart(SinglePart::plain(text.clone()))
                            .singlepart(SinglePart::html(email.html.clone())),
                    )
                    .map_err(|_| ()),
                None => builder
                    .header(ContentType::TEXT_HTML)
                    .body(email.html.clone())
                    .map_err(|_| ()),
            };
        }
        let mut related = MultiPart::related().singlepart(SinglePart::html(email.html.clone()));
        for part in inline {
            related = related.singlepart(part);
        }
        let mut mixed = if let Some(text) = &email.text {
            MultiPart::mixed().multipart(
                MultiPart::alternative()
                    .singlepart(SinglePart::plain(text.clone()))
                    .multipart(related),
            )
        } else {
            MultiPart::mixed().multipart(related)
        };
        for part in attached {
            mixed = mixed.singlepart(part);
        }
        builder.multipart(mixed).map_err(|_| ())
    }
    pub async fn deliver_workflow(&self, message: Message) -> Result<(), ()> {
        self.transport
            .send(message)
            .await
            .map(|_| ())
            .map_err(|_| ())
    }
    #[cfg(test)]
    fn test_transport(host: String, port: u16) -> Self {
        Self {
            transport: AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host)
                .port(port)
                .timeout(Some(Duration::from_secs(2)))
                .build(),
            from: "sender@example.test".into(),
            host,
            port: port.to_string(),
            tls: "test-plaintext".into(),
            username: String::new(),
            password: String::new(),
            ca_fingerprint: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    #[tokio::test]
    async fn smtp_protocol_acceptance_and_lost_reply_are_distinct() {
        for lose_reply in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let (reader, mut writer) = socket.into_split();
                let mut reader = BufReader::new(reader);
                let mut line = String::new();
                let mut content = String::new();
                let mut data = false;
                writer.write_all(b"220 localhost ESMTP\r\n").await.unwrap();
                loop {
                    line.clear();
                    if reader.read_line(&mut line).await.unwrap() == 0 {
                        break;
                    }
                    if data {
                        if line == ".\r\n" {
                            if !lose_reply {
                                writer.write_all(b"250 queued\r\n").await.unwrap();
                            }
                            break;
                        }
                        content.push_str(&line);
                        continue;
                    }
                    let upper = line.to_ascii_uppercase();
                    if upper.starts_with("EHLO") {
                        writer
                            .write_all(b"250-localhost\r\n250 8BITMIME\r\n")
                            .await
                            .unwrap();
                    } else if upper.starts_with("MAIL FROM") || upper.starts_with("RCPT TO") {
                        writer.write_all(b"250 OK\r\n").await.unwrap();
                    } else if upper.starts_with("DATA") {
                        data = true;
                        writer.write_all(b"354 Continue\r\n").await.unwrap();
                    } else {
                        panic!("unexpected command {line:?}");
                    }
                }
                content
            });
            let transport = SmtpAlert::test_transport("127.0.0.1".into(), port);
            let result = transport
                .send("ops@example.test", "fixture alert".into())
                .await;
            assert_eq!(result.is_ok(), !lose_reply);
            let content = server.await.unwrap();
            assert!(content.contains("fixture alert"));
            assert!(content.contains("Swallow delivery alert"));
        }
    }
    #[tokio::test]
    async fn starttls_downgrade_rejects_without_auth_or_message() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            writer.write_all(b"220 localhost ESMTP\r\n").await.unwrap();
            reader.read_line(&mut line).await.unwrap();
            assert!(line.to_ascii_uppercase().starts_with("EHLO"));
            writer
                .write_all(b"250-localhost\r\n250 AUTH PLAIN\r\n")
                .await
                .unwrap();
            line.clear();
            let n = reader.read_line(&mut line).await.unwrap();
            if n > 0 {
                assert!(!line.to_ascii_uppercase().starts_with("AUTH"));
                assert!(!line.to_ascii_uppercase().starts_with("MAIL"));
            }
        });
        let smtp = SmtpAlert::new(
            "127.0.0.1".into(),
            port,
            "starttls".into(),
            "sender@example.test".into(),
            Some("user".into()),
            Some("secret".into()),
            None,
        )
        .unwrap();
        assert!(smtp.send("ops@example.test", "alert".into()).await.is_err());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn authenticated_starttls_accepts_a_message_after_tls_validation() {
        use std::{
            io::{BufRead, BufReader},
            process::{Command, Stdio},
        };
        let dir = std::env::temp_dir().join(format!("swallow-smtp-tls-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        let output = dir.join("message");
        let result = Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-keyout",
            ])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(result.success());
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/smtp_starttls_fixture.py");
        let mut server = Command::new("python3")
            .arg(fixture)
            .arg(&cert)
            .arg(&key)
            .arg(&output)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(server.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let port: u16 = line.trim().parse().unwrap();
        let smtp = SmtpAlert::new(
            "localhost".into(),
            port,
            "starttls".into(),
            "sender@example.test".into(),
            Some("fixture-user".into()),
            Some("fixture-password".into()),
            Some(std::fs::read_to_string(&cert).unwrap()),
        )
        .unwrap();
        assert_eq!(smtp.binding_parts().len(), 7);
        let no_ca = SmtpAlert::new(
            "localhost".into(),
            port,
            "starttls".into(),
            "sender@example.test".into(),
            Some("fixture-user".into()),
            Some("fixture-password".into()),
            None,
        )
        .unwrap();
        assert_eq!(no_ca.binding_parts().len(), 6);
        assert_eq!(&smtp.binding_parts()[..6], &no_ca.binding_parts()[..]);
        assert!(
            smtp.send("ops@example.test", "TLS fixture alert".into())
                .await
                .is_ok()
        );
        assert!(server.wait().unwrap().success());
        let message = std::fs::read_to_string(&output).unwrap();
        assert!(message.contains("TLS fixture alert"));
        assert!(!message.contains("fixture-password"));
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/smtp_starttls_fixture.py");
        let mut wrong_host_server = Command::new("python3")
            .arg(fixture)
            .arg(&cert)
            .arg(&key)
            .arg(&output)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(wrong_host_server.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let port: u16 = line.trim().parse().unwrap();
        let wrong_host = SmtpAlert::new(
            "127.0.0.1".into(),
            port,
            "starttls".into(),
            "sender@example.test".into(),
            Some("fixture-user".into()),
            Some("fixture-password".into()),
            Some(std::fs::read_to_string(&cert).unwrap()),
        )
        .unwrap();
        assert!(
            wrong_host
                .send("ops@example.test", "should not send".into())
                .await
                .is_err()
        );
        assert!(!wrong_host_server.wait().unwrap().success());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn workflow_messages_keep_bcc_out_of_headers_and_reject_unsupported_fields() {
        let provider = SmtpAlert::test_transport("127.0.0.1".into(), 25);
        let mut email = crate::sendgrid::Email {
            to: vec!["visible@example.test".into()],
            cc: vec!["copy@example.test".into()],
            bcc: vec!["hidden@example.test".into()],
            subject: "fixture".into(),
            html: "<p>Hello</p>".into(),
            text: Some("Hello".into()),
            ..Default::default()
        };
        let msg = provider.prepare_workflow(&email).unwrap();
        let encoded = String::from_utf8(msg.formatted()).unwrap();
        assert!(!encoded.to_lowercase().contains("bcc:"));
        assert!(!encoded.contains("hidden@example.test"));
        assert_eq!(msg.envelope().to().len(), 3);
        assert!(encoded.contains("multipart/alternative"));
        email.headers.insert("X-Test".into(), "value".into());
        let msg = provider.prepare_workflow(&email).unwrap();
        assert!(
            String::from_utf8(msg.formatted())
                .unwrap()
                .contains("X-Test: value")
        );
        email.headers.insert("x-test".into(), "duplicate".into());
        assert!(provider.prepare_workflow(&email).is_err());
        email.headers.remove("x-test");
        for name in [
            "Bcc",
            "Content-Type",
            "Resent-To",
            "Return-Path",
            "Bad\r\nHeader",
        ] {
            email.headers.insert(name.into(), "invalid".into());
            assert!(provider.prepare_workflow(&email).is_err());
            email.headers.remove(name);
        }
        email
            .headers
            .insert("X-Test".into(), "bad\r\nBcc: victim@example.test".into());
        assert!(provider.prepare_workflow(&email).is_err());
        email
            .headers
            .insert("X-Test".into(), serde_json::json!(123));
        assert!(provider.prepare_workflow(&email).is_err());
    }
    #[test]
    fn workflow_attachments_validate_and_build_mime() {
        let provider = SmtpAlert::test_transport("127.0.0.1".into(), 25);
        let mut email = crate::sendgrid::Email {
            to: vec!["to@example.test".into()],
            subject: "test".into(),
            html: "<img src=cid:logo>".into(),
            text: Some("plain".into()),
            ..Default::default()
        };
        email.attachments.push(crate::sendgrid::Attachment {
            file: b"hello".to_vec(),
            name: "report.txt".into(),
            mime: "text/plain".into(),
            cid: None,
            disposition: None,
        });
        email.attachments.push(crate::sendgrid::Attachment {
            file: vec![0, 1, 255],
            name: "logo.png".into(),
            mime: "image/png".into(),
            cid: Some("logo".into()),
            disposition: Some("inline".into()),
        });
        let wire =
            String::from_utf8(provider.prepare_workflow(&email).unwrap().formatted()).unwrap();
        for expected in [
            "multipart/mixed",
            "multipart/alternative",
            "multipart/related",
            "Content-ID: <logo>",
            "report.txt",
            "AAH/",
        ] {
            assert!(wire.contains(expected), "missing {expected}");
        }
        email.attachments[1].cid = Some("bad\r\nvalue".into());
        assert!(provider.prepare_workflow(&email).is_err());
        email.attachments[1].cid = Some("logo".into());
        email.attachments[1].disposition = Some("attachment".into());
        assert!(provider.prepare_workflow(&email).is_err());
        email.attachments[1].disposition = Some("inline".into());
        email.attachments[1].mime = "invalid".into();
        assert!(provider.prepare_workflow(&email).is_err());
        email.attachments[1].mime = "image/png".into();
        email.attachments[1].name = "bad\nname".into();
        assert!(provider.prepare_workflow(&email).is_err());
        email.attachments[1].name = "logo.png".into();
        email.attachments.push(crate::sendgrid::Attachment {
            file: vec![],
            name: "duplicate.png".into(),
            mime: "image/png".into(),
            cid: Some("logo".into()),
            disposition: None,
        });
        assert!(provider.prepare_workflow(&email).is_err());
        email.attachments.pop();
        email.attachments[0].file = vec![0; 20 * 1024 * 1024 + 1];
        assert!(provider.prepare_workflow(&email).is_err());
        email.attachments[0].file.clear();
        for i in 0..99 {
            email.attachments.push(crate::sendgrid::Attachment {
                file: vec![],
                name: format!("{i}.txt"),
                mime: "text/plain".into(),
                cid: None,
                disposition: None,
            });
        }
        assert!(provider.prepare_workflow(&email).is_err());
    }
    #[test]
    fn validates_tls_credentials_and_sender() {
        assert!(
            SmtpAlert::new(
                "example.test".into(),
                587,
                "starttls".into(),
                "sender@example.test".into(),
                None,
                None,
                Some("-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n".into())
            )
            .is_err()
        );
        assert!(
            SmtpAlert::new(
                "example.test".into(),
                587,
                "starttls".into(),
                "sender@example.test".into(),
                None,
                None,
                Some("not a certificate".into())
            )
            .is_err()
        );
        for mode in ["plain", "opportunistic", ""] {
            assert!(
                SmtpAlert::new(
                    "example.test".into(),
                    587,
                    mode.into(),
                    "sender@example.test".into(),
                    None,
                    None,
                    None
                )
                .is_err()
            );
        }
        assert!(
            SmtpAlert::new(
                "example.test".into(),
                587,
                "starttls".into(),
                "sender@example.test".into(),
                Some("user".into()),
                None,
                None
            )
            .is_err()
        );
        assert!(
            SmtpAlert::new(
                "example.test".into(),
                587,
                "starttls".into(),
                "sender@example.test\r\nBcc:x@example.test".into(),
                None,
                None,
                None
            )
            .is_err()
        );
    }
}
