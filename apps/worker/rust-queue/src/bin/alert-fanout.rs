//! Run one existing alert command per explicitly configured destination.
use serde::Deserialize;
use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    targets: Vec<Target>,
}
#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct DeliveryChecks {
    resend: Option<bool>,
    attempts: Option<bool>,
    slack_jobs: Option<bool>,
    smtp: Option<bool>,
    discord: Option<bool>,
    failed_jobs: Option<bool>,
    slack_missing_endpoints: Option<bool>,
    email_policy_cancelled: Option<bool>,
    sms_policy_cancelled: Option<bool>,
    push_policy_cancelled: Option<bool>,
    slack_policy_cancelled: Option<bool>,
    sendgrid_webhook_keys: Option<bool>,
    fcm_cooldown_source: Option<bool>,
    fcm_cooldown_distinct_source: Option<bool>,
    fcm_unreviewed: Option<bool>,
}
impl DeliveryChecks {
    fn apply(&self, cmd: &mut Command) {
        for (key, value) in [
            ("SWALLOW_ALERT_RESEND_ENABLED", self.resend),
            ("SWALLOW_ALERT_ATTEMPTS_ENABLED", self.attempts),
            ("SWALLOW_ALERT_SLACK_JOBS_ENABLED", self.slack_jobs),
            ("SWALLOW_ALERT_SMTP_ENABLED", self.smtp),
            ("SWALLOW_ALERT_DISCORD_ENABLED", self.discord),
            ("SWALLOW_ALERT_FAILED_JOBS_ENABLED", self.failed_jobs),
            ("SWALLOW_ALERT_SLACK_MISSING_ENDPOINTS_ENABLED", self.slack_missing_endpoints),
            ("SWALLOW_ALERT_EMAIL_POLICY_CANCELLED_ENABLED", self.email_policy_cancelled),
            ("SWALLOW_ALERT_SMS_POLICY_CANCELLED_ENABLED", self.sms_policy_cancelled),
            ("SWALLOW_ALERT_PUSH_POLICY_CANCELLED_ENABLED", self.push_policy_cancelled),
            ("SWALLOW_ALERT_SLACK_POLICY_CANCELLED_ENABLED", self.slack_policy_cancelled),
            ("SWALLOW_ALERT_SENDGRID_WEBHOOK_KEYS_ENABLED", self.sendgrid_webhook_keys),
            ("SWALLOW_ALERT_FCM_COOLDOWN_SOURCE_ENABLED", self.fcm_cooldown_source),
            ("SWALLOW_ALERT_FCM_COOLDOWN_DISTINCT_SOURCE", self.fcm_cooldown_distinct_source),
            ("SWALLOW_ALERT_FCM_UNREVIEWED_ENABLED", self.fcm_unreviewed),
        ] {
            if let Some(enabled) = value {
                cmd.env(key, if enabled { "true" } else { "false" });
            }
        }
    }
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Threshold { Value(u32), Disabled(Disabled) }
#[derive(Deserialize)]
enum Disabled { #[serde(rename = "disabled")] Disabled }
#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct DeliveryThresholds {
    discord_cooldown_grace_seconds: Option<Threshold>,
    discord_retry_grace_seconds: Option<Threshold>,
    smtp_credential_max_age_seconds: Option<Threshold>,
    smtp_credential_min_healthy: Option<Threshold>,
    smtp_ca_max_age_seconds: Option<Threshold>,
    smtp_ca_min_healthy: Option<Threshold>,
    fcm_credential_max_age_seconds: Option<Threshold>,
    fcm_credential_min_healthy: Option<Threshold>,
    fcm_unrecorded_claim_min_gap: Option<Threshold>,
    fcm_unrecorded_claim_grace_seconds: Option<Threshold>,
    fcm_cooldown_grace_seconds: Option<Threshold>,
    fcm_partial_window_seconds: Option<Threshold>,
    twilio_delayed_grace_seconds: Option<Threshold>,
    resend_cooldown_grace_seconds: Option<Threshold>,
    slack_cooldown_grace_seconds: Option<Threshold>,
    resend_auto_grace_seconds: Option<Threshold>,
    twilio_grace_seconds: Option<Threshold>,
    twilio_heartbeat_max_seconds: Option<Threshold>,
    sendgrid_grace_seconds: Option<Threshold>,
    sendgrid_heartbeat_max_seconds: Option<Threshold>,
    queue_max_wait_seconds: Option<Threshold>,
}
impl DeliveryThresholds {
    fn values(&self) -> [(&'static str, &Option<Threshold>, u32, u32); 21] {
        [
            ("SWALLOW_ALERT_DISCORD_COOLDOWN_GRACE_SECONDS", &self.discord_cooldown_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_DISCORD_RETRY_GRACE_SECONDS", &self.discord_retry_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_SMTP_CREDENTIAL_MAX_AGE_SECONDS", &self.smtp_credential_max_age_seconds, 1, 86400),
            ("SWALLOW_ALERT_SMTP_CREDENTIAL_MIN_HEALTHY", &self.smtp_credential_min_healthy, 1, 1000),
            ("SWALLOW_ALERT_SMTP_CA_MAX_AGE_SECONDS", &self.smtp_ca_max_age_seconds, 1, 86400),
            ("SWALLOW_ALERT_SMTP_CA_MIN_HEALTHY", &self.smtp_ca_min_healthy, 1, 1000),
            ("SWALLOW_ALERT_FCM_CREDENTIAL_MAX_AGE_SECONDS", &self.fcm_credential_max_age_seconds, 1, 86400),
            ("SWALLOW_ALERT_FCM_CREDENTIAL_MIN_HEALTHY", &self.fcm_credential_min_healthy, 1, 1000),
            ("SWALLOW_ALERT_FCM_UNRECORDED_CLAIM_MIN_GAP", &self.fcm_unrecorded_claim_min_gap, 2, 100000),
            ("SWALLOW_ALERT_FCM_UNRECORDED_CLAIM_GRACE_SECONDS", &self.fcm_unrecorded_claim_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_FCM_COOLDOWN_GRACE_SECONDS", &self.fcm_cooldown_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_FCM_PARTIAL_WINDOW_SECONDS", &self.fcm_partial_window_seconds, 1, 604800),
            ("SWALLOW_ALERT_TWILIO_DELAYED_GRACE_SECONDS", &self.twilio_delayed_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_RESEND_COOLDOWN_GRACE_SECONDS", &self.resend_cooldown_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_SLACK_COOLDOWN_GRACE_SECONDS", &self.slack_cooldown_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_RESEND_AUTO_GRACE_SECONDS", &self.resend_auto_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_TWILIO_GRACE_SECONDS", &self.twilio_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS", &self.twilio_heartbeat_max_seconds, 1, 86400),
            ("SWALLOW_ALERT_SENDGRID_GRACE_SECONDS", &self.sendgrid_grace_seconds, 1, 86400),
            ("SWALLOW_ALERT_SENDGRID_HEARTBEAT_MAX_SECONDS", &self.sendgrid_heartbeat_max_seconds, 1, 86400),
            ("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", &self.queue_max_wait_seconds, 1, 86400),
        ]
    }
    fn valid(&self) -> bool {
        self.values().iter().all(|(_, value, min, max)| match value {
            Some(Threshold::Value(value)) => (*min..=*max).contains(value),
            _ => true,
        })
    }
    fn apply(&self, cmd: &mut Command) {
        for (key, value, _, _) in self.values() {
            match value {
                Some(Threshold::Value(value)) => { cmd.env(key, value.to_string()); }
                Some(Threshold::Disabled(_)) => { cmd.env_remove(key); }
                None => {}
            }
        }
    }
}
#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct AlertTiming {
    resend_grace_seconds: Option<u32>,
    resend_heartbeat_max_seconds: Option<u32>,
    cooldown_seconds: Option<u32>,
}
impl AlertTiming {
    fn valid(&self) -> bool {
        self.resend_grace_seconds.is_none_or(|v| (1..=86400).contains(&v))
            && self.resend_heartbeat_max_seconds.is_none_or(|v| (1..=86400).contains(&v))
            && self.cooldown_seconds.is_none_or(|v| (60..=86400).contains(&v))
    }
}
#[derive(Deserialize)]
#[serde(tag = "channel", rename_all = "lowercase", deny_unknown_fields)]
enum Target {
    Slack {
        id: String,
        webhook_file: String,
        delivery_checks: Option<DeliveryChecks>,
        delivery_thresholds: Option<DeliveryThresholds>,
        alert_timing: Option<AlertTiming>,
    },
    Email {
        id: String,
        to: String,
        provider: Provider,
        from: String,
        api_key_file: Option<String>,
        smtp_host: Option<String>,
        smtp_port: Option<u16>,
        smtp_tls: Option<String>,
        smtp_username: Option<String>,
        smtp_password_file: Option<String>,
        smtp_ca_file: Option<String>,
        sender_name: Option<String>,
        region: Option<String>,
        ip_pool_name: Option<String>,
        delivery_checks: Option<DeliveryChecks>,
        delivery_thresholds: Option<DeliveryThresholds>,
        alert_timing: Option<AlertTiming>,
    },
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Provider {
    Resend,
    Sendgrid,
    Smtp,
}
impl Target {
    fn key(&self) -> (&str, &str) {
        match self {
            Self::Slack { id, .. } => ("slack", id),
            Self::Email { id, .. } => ("email", id),
        }
    }
    fn valid(&self) -> bool {
        let (_, id) = self.key();
        let valid_id = !id.is_empty()
            && id.len() <= 64
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
        fn path(value: &str) -> bool {
            !value.is_empty()
                && value.len() <= 4096
                && Path::new(value).is_absolute()
                && !value.contains('\0')
        }
        fn email(value: &str) -> bool {
            !value.is_empty()
                && value.len() <= 320
                && value.contains('@')
                && !value.contains(['\r', '\n', '\0', '<', '>'])
        }
        let thresholds = match self {
            Self::Slack { delivery_thresholds, .. } | Self::Email { delivery_thresholds, .. } => delivery_thresholds,
        };
        let timing = match self {
            Self::Slack { alert_timing, .. } | Self::Email { alert_timing, .. } => alert_timing,
        };
        valid_id
            && thresholds.as_ref().is_none_or(DeliveryThresholds::valid)
            && timing.as_ref().is_none_or(AlertTiming::valid)
            && match self {
                Self::Slack { webhook_file, .. } => path(webhook_file),
                Self::Email {
                    to,
                    from,
                    api_key_file,
                    sender_name,
                    region,
                    provider,
                    ip_pool_name,
                    smtp_host,
                    smtp_port,
                    smtp_tls,
                    smtp_username,
                    smtp_password_file,
                    smtp_ca_file,
                    ..
                } => {
                    let no_smtp = smtp_host.is_none()
                        && smtp_port.is_none()
                        && smtp_tls.is_none()
                        && smtp_username.is_none()
                        && smtp_password_file.is_none()
                        && smtp_ca_file.is_none();
                    email(to)
                        && email(from)
                        && sender_name
                            .as_ref()
                            .is_none_or(|v| v.len() <= 256 && !v.contains(['\r', '\n', '\0']))
                        && match provider {
                            Provider::Resend => {
                                no_smtp
                                    && api_key_file.as_ref().is_some_and(|v| path(v))
                                    && region.is_none()
                                    && ip_pool_name.is_none()
                            }
                            Provider::Sendgrid => {
                                no_smtp
                                    && api_key_file.as_ref().is_some_and(|v| path(v))
                                    && region.as_ref().is_none_or(|v| v == "global" || v == "eu")
                                    && ip_pool_name.as_ref().is_none_or(|v| {
                                        v.len() <= 256 && !v.contains(['\r', '\n', '\0'])
                                    })
                            }
                            Provider::Smtp => {
                                api_key_file.is_none()
                                    && sender_name.is_none()
                                    && region.is_none()
                                    && ip_pool_name.is_none()
                                    && smtp_host.as_ref().is_some_and(|v| {
                                        !v.is_empty()
                                            && v.len() <= 253
                                            && v.bytes().all(|b| {
                                                b.is_ascii_alphanumeric() || b == b'.' || b == b'-'
                                            })
                                    })
                                    && smtp_port.is_none_or(|v| v > 0)
                                    && smtp_tls
                                        .as_ref()
                                        .is_none_or(|v| v == "tls" || v == "starttls")
                                    && smtp_username.as_ref().is_none_or(|v| {
                                        !v.is_empty()
                                            && v.len() <= 1024
                                            && !v.contains(['\r', '\n', '\0'])
                                    })
                                    && smtp_username.is_some() == smtp_password_file.is_some()
                                    && smtp_password_file.as_ref().is_none_or(|v| path(v))
                                    && smtp_ca_file.as_ref().is_none_or(|v| path(v))
                            }
                        }
                }
            }
    }
    fn command_args(&self, default: &[String], delivery: bool) -> Vec<String> {
        let mut args = default.to_vec();
        let timing = match self {
            Self::Slack { alert_timing, .. } | Self::Email { alert_timing, .. } => alert_timing,
        };
        if let Some(timing) = timing {
            if delivery {
                if let Some(value) = timing.resend_grace_seconds { args[1] = value.to_string(); }
                if let Some(value) = timing.resend_heartbeat_max_seconds { args[2] = value.to_string(); }
            }
            if let Some(value) = timing.cooldown_seconds {
                let last = args.len() - 1;
                args[last] = value.to_string();
            }
        }
        args
    }
    fn apply(&self, cmd: &mut Command) {
        for key in [
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "SWALLOW_ALERT_SLACK_WEBHOOK_FILE",
            "SWALLOW_ALERT_EMAIL_TO",
            "SWALLOW_ALERT_EMAIL_PROVIDER",
            "SWALLOW_ALERT_SLACK_TARGET_ID",
            "SWALLOW_ALERT_EMAIL_TARGET_ID",
            "SWALLOW_DB_ALERT_SLACK_TARGET_ID",
            "SWALLOW_DB_ALERT_EMAIL_TARGET_ID",
            "NOVU_RESEND_API_KEY",
            "NOVU_RESEND_API_KEY_FILE",
            "NOVU_RESEND_FROM",
            "NOVU_RESEND_SENDER_NAME",
            "NOVU_SENDGRID_API_KEY",
            "NOVU_SENDGRID_API_KEY_FILE",
            "NOVU_SENDGRID_FROM",
            "NOVU_SENDGRID_SENDER_NAME",
            "NOVU_SENDGRID_REGION",
            "NOVU_SENDGRID_IP_POOL_NAME",
            "SWALLOW_SMTP_HOST",
            "SWALLOW_SMTP_PORT",
            "SWALLOW_SMTP_TLS",
            "SWALLOW_SMTP_FROM",
            "SWALLOW_SMTP_USERNAME",
            "SWALLOW_SMTP_PASSWORD",
            "SWALLOW_SMTP_PASSWORD_FILE",
            "SWALLOW_SMTP_CA_FILE",
        ] {
            cmd.env_remove(key);
        }
        match self {
            Self::Slack { id, webhook_file, .. } => {
                cmd.env("SWALLOW_ALERT_SLACK_TARGET_ID", id)
                    .env("SWALLOW_DB_ALERT_SLACK_TARGET_ID", id)
                    .env("SWALLOW_ALERT_SLACK_WEBHOOK_FILE", webhook_file);
            }
            Self::Email {
                id,
                to,
                provider,
                from,
                api_key_file,
                smtp_host,
                smtp_port,
                smtp_tls,
                smtp_username,
                smtp_password_file,
                smtp_ca_file,
                sender_name,
                region,
                ip_pool_name,
                ..
            } => {
                cmd.env("SWALLOW_ALERT_EMAIL_TARGET_ID", id)
                    .env("SWALLOW_DB_ALERT_EMAIL_TARGET_ID", id)
                    .env("SWALLOW_ALERT_EMAIL_TO", to);
                match provider {
                    Provider::Resend => {
                        cmd.env("SWALLOW_ALERT_EMAIL_PROVIDER", "resend")
                            .env("NOVU_RESEND_FROM", from)
                            .env("NOVU_RESEND_API_KEY_FILE", api_key_file.as_deref().unwrap());
                        if let Some(name) = sender_name {
                            cmd.env("NOVU_RESEND_SENDER_NAME", name);
                        }
                    }
                    Provider::Sendgrid => {
                        cmd.env("SWALLOW_ALERT_EMAIL_PROVIDER", "sendgrid")
                            .env("NOVU_SENDGRID_FROM", from)
                            .env(
                                "NOVU_SENDGRID_API_KEY_FILE",
                                api_key_file.as_deref().unwrap(),
                            )
                            .env(
                                "NOVU_SENDGRID_SENDER_NAME",
                                sender_name.as_deref().unwrap_or(""),
                            );
                        if let Some(region) = region {
                            cmd.env("NOVU_SENDGRID_REGION", region);
                        }
                        if let Some(pool) = ip_pool_name {
                            cmd.env("NOVU_SENDGRID_IP_POOL_NAME", pool);
                        }
                    }
                    Provider::Smtp => {
                        cmd.env("SWALLOW_ALERT_EMAIL_PROVIDER", "smtp")
                            .env("SWALLOW_SMTP_FROM", from)
                            .env("SWALLOW_SMTP_HOST", smtp_host.as_deref().unwrap());
                        if let Some(port) = smtp_port {
                            cmd.env("SWALLOW_SMTP_PORT", port.to_string());
                        }
                        if let Some(tls) = smtp_tls {
                            cmd.env("SWALLOW_SMTP_TLS", tls);
                        }
                        if let Some(user) = smtp_username {
                            cmd.env("SWALLOW_SMTP_USERNAME", user);
                        }
                        if let Some(password) = smtp_password_file {
                            cmd.env("SWALLOW_SMTP_PASSWORD_FILE", password);
                        }
                        if let Some(ca) = smtp_ca_file {
                            cmd.env("SWALLOW_SMTP_CA_FILE", ca);
                        }
                    }
                }
            }
        }
        let checks = match self {
            Self::Slack { delivery_checks, .. } | Self::Email { delivery_checks, .. } => delivery_checks,
        };
        if let Some(checks) = checks { checks.apply(cmd); }
        let thresholds = match self {
            Self::Slack { delivery_thresholds, .. } | Self::Email { delivery_thresholds, .. } => delivery_thresholds,
        };
        if let Some(thresholds) = thresholds { thresholds.apply(cmd); }
    }
}
fn read_config(path: &str) -> Result<Config> {
    let regular_only = match std::env::var("SWALLOW_ALERT_FANOUT_CONFIG_REGULAR_ONLY").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid alert configuration file policy".into()),
    };
    let file = if regular_only {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NONBLOCK);
        }
        let file = options
            .open(path)
            .map_err(|_| "alert configuration unavailable")?;
        if !file
            .metadata()
            .map_err(|_| "alert configuration unavailable")?
            .is_file()
        {
            return Err("invalid alert configuration file type".into());
        }
        file
    } else {
        File::open(path).map_err(|_| "alert configuration unavailable")?
    };
    let mut raw = Vec::new();
    file.take(65_537)
        .read_to_end(&mut raw)
        .map_err(|_| "alert configuration unavailable")?;
    if raw.len() > 65_536 {
        return Err("alert configuration too large".into());
    }
    let config: Config = serde_json::from_slice(&raw).map_err(|_| "invalid alert configuration")?;
    if config.targets.is_empty() || config.targets.len() > 32 {
        return Err("alert target count must be 1-32".into());
    }
    let mut ids = HashSet::new();
    for target in &config.targets {
        if !target.valid() || !ids.insert(target.key()) {
            return Err("invalid or duplicate alert target".into());
        }
    }
    Ok(config)
}
fn child_path(name: &str) -> Result<PathBuf> {
    let mut path = std::env::current_exe()?;
    path.set_file_name(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    Ok(path)
}
#[tokio::main]
async fn main() -> Result<()> {
    novu_postgres_queue::shutdown::run_until_signal_with_setting(
        "SWALLOW_ALERT_SHUTDOWN_GRACE_SECONDS",
        run,
    )
    .await
}
async fn run(stop: novu_postgres_queue::shutdown::Stop) -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (name, command_args, file) = match args.as_slice() {
        [mode,env,grace,heartbeat,cooldown,file] if mode=="delivery" => ("send-delivery-alert",vec![env,grace,heartbeat,cooldown],file),
        [mode,env,label,cooldown,file] if mode=="database" => ("send-database-alert",vec![env,label,cooldown],file),
        _ => return Err("usage: alert-fanout delivery ENV GRACE HEARTBEAT COOLDOWN CONFIG | database ENV LABEL COOLDOWN CONFIG".into()),
    };
    let concurrency: usize = match std::env::var("SWALLOW_ALERT_FANOUT_CONCURRENCY") {
        Ok(value) => value.parse().map_err(|_| "invalid fanout concurrency")?,
        Err(std::env::VarError::NotPresent) => 1,
        Err(_) => return Err("invalid fanout concurrency".into()),
    };
    if !(1..=8).contains(&concurrency) {
        return Err("invalid fanout concurrency".into());
    }
    let target_timeout = match std::env::var("SWALLOW_ALERT_FANOUT_TARGET_TIMEOUT_SECONDS") {
        Err(std::env::VarError::NotPresent) => None,
        Ok(value) if value == "0" => None,
        Ok(value) => Some(Duration::from_secs(
            value
                .parse::<u64>()
                .ok()
                .filter(|value| (1..=3600).contains(value))
                .ok_or("invalid fanout target timeout")?,
        )),
        Err(_) => return Err("invalid fanout target timeout".into()),
    };
    let config = read_config(file)?;
    let binary = child_path(name)?;
    let command_args: Vec<String> = command_args.into_iter().cloned().collect();
    let mut remaining = VecDeque::from(config.targets);
    let mut active = tokio::task::JoinSet::new();
    let mut failed = false;
    let mut review = false;
    loop {
        while active.len() < concurrency && !stop.requested() {
            let Some(target) = remaining.pop_front() else {
                break;
            };
            let mut command = Command::new(&binary);
            command
                .args(target.command_args(&command_args, name == "send-delivery-alert"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            target.apply(&mut command);
            let (channel, id) = target.key();
            let (channel, id) = (channel.to_owned(), id.to_owned());
            active.spawn(async move {
                let (status, timed_out) = match command.spawn() {
                    Ok(mut child) => {
                        if let Some(limit) = target_timeout {
                            match tokio::time::timeout(limit, child.wait()).await {
                                Ok(result) => (result.ok().and_then(|status| status.code()), false),
                                Err(_) => {
                                    // The outcome may already have been committed or sent.
                                    // Bound cleanup as well; kill_on_drop remains enabled.
                                    let _ =
                                        tokio::time::timeout(Duration::from_secs(2), child.kill())
                                            .await;
                                    (None, true)
                                }
                            }
                        } else {
                            (
                                child.wait().await.ok().and_then(|status| status.code()),
                                false,
                            )
                        }
                    }
                    Err(_) => (None, false),
                };
                (channel, id, status, timed_out)
            });
        }
        if active.is_empty() {
            break;
        }
        match active.join_next().await {
            Some(Ok((channel, id, Some(0), false))) => println!("{channel} {id}: completed"),
            Some(Ok((channel, id, Some(2), false))) => {
                review = true;
                println!("{channel} {id}: attention");
            }
            Some(Ok((channel, id, _, true))) => {
                failed = true;
                eprintln!("{channel} {id}: timed out; outcome unknown");
            }
            Some(Ok((channel, id, _, false))) => {
                failed = true;
                eprintln!("{channel} {id}: failed");
            }
            Some(Err(_)) => {
                failed = true;
                eprintln!("alert target task failed");
            }
            None => unreachable!(),
        }
    }
    if !remaining.is_empty() {
        return Err("alert fanout interrupted before all destinations were attempted".into());
    }
    if failed {
        std::process::exit(1);
    }
    if review {
        std::process::exit(2);
    }
    Ok(())
}
