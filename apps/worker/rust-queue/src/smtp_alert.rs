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
}
impl SmtpAlert {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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
        let password = novu_rust_contracts::secret::from_env(
            "SWALLOW_SMTP_PASSWORD",
            "SWALLOW_SMTP_PASSWORD_FILE",
        )?;
        Self::new(host, port, tls, from, username, password)
    }
    fn new(
        host: String,
        port: u16,
        tls: String,
        from: String,
        username: Option<String>,
        password: Option<String>,
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
        })
    }
    pub fn binding_parts(&self) -> [&str; 6] {
        [
            &self.host,
            &self.port,
            &self.tls,
            &self.from,
            &self.username,
            &self.password,
        ]
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
        )
        .unwrap();
        assert!(smtp.send("ops@example.test", "alert".into()).await.is_err());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn authenticated_starttls_accepts_a_message_after_tls_validation() {
        use lettre::transport::smtp::client::{Certificate, Tls, TlsParameters};
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
        let certificate = Certificate::from_pem(&std::fs::read(&cert).unwrap()).unwrap();
        let tls = TlsParameters::builder("localhost".into())
            .add_root_certificate(certificate)
            .build()
            .unwrap();
        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay("localhost")
            .unwrap()
            .tls(Tls::Required(tls))
            .port(port)
            .credentials(Credentials::new(
                "fixture-user".into(),
                "fixture-password".into(),
            ))
            .timeout(Some(Duration::from_secs(5)))
            .build();
        let smtp = SmtpAlert {
            transport,
            from: "sender@example.test".into(),
            host: "localhost".into(),
            port: port.to_string(),
            tls: "starttls".into(),
            username: "fixture-user".into(),
            password: "fixture-password".into(),
        };
        assert!(
            smtp.send("ops@example.test", "TLS fixture alert".into())
                .await
                .is_ok()
        );
        assert!(server.wait().unwrap().success());
        let message = std::fs::read_to_string(&output).unwrap();
        assert!(message.contains("TLS fixture alert"));
        assert!(!message.contains("fixture-password"));
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn validates_tls_credentials_and_sender() {
        for mode in ["plain", "opportunistic", ""] {
            assert!(
                SmtpAlert::new(
                    "example.test".into(),
                    587,
                    mode.into(),
                    "sender@example.test".into(),
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
                None
            )
            .is_err()
        );
    }
}
