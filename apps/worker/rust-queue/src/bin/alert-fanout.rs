//! Run one existing alert command per explicitly configured destination.
use serde::Deserialize;
use std::{
    collections::HashSet,
    error::Error,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::Command;
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    targets: Vec<Target>,
}
#[derive(Deserialize)]
#[serde(tag = "channel", rename_all = "lowercase", deny_unknown_fields)]
enum Target {
    Slack {
        id: String,
        webhook_file: String,
    },
    Email {
        id: String,
        to: String,
        provider: Provider,
        from: String,
        api_key_file: String,
        sender_name: Option<String>,
        region: Option<String>,
    },
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Provider {
    Resend,
    Sendgrid,
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
                && !value.contains(['\r', '\n', '<', '>'])
        }
        valid_id
            && match self {
                Self::Slack { webhook_file, .. } => path(webhook_file),
                Self::Email {
                    to,
                    from,
                    api_key_file,
                    sender_name,
                    region,
                    provider,
                    ..
                } => {
                    email(to)
                        && email(from)
                        && path(api_key_file)
                        && sender_name
                            .as_ref()
                            .is_none_or(|v| v.len() <= 256 && !v.contains(['\r', '\n', '\0']))
                        && match provider {
                            Provider::Resend => region.is_none(),
                            Provider::Sendgrid => {
                                region.as_ref().is_none_or(|v| v == "global" || v == "eu")
                            }
                        }
                }
            }
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
        ] {
            cmd.env_remove(key);
        }
        match self {
            Self::Slack { id, webhook_file } => {
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
                sender_name,
                region,
            } => {
                cmd.env("SWALLOW_ALERT_EMAIL_TARGET_ID", id)
                    .env("SWALLOW_DB_ALERT_EMAIL_TARGET_ID", id)
                    .env("SWALLOW_ALERT_EMAIL_TO", to);
                match provider {
                    Provider::Resend => {
                        cmd.env("SWALLOW_ALERT_EMAIL_PROVIDER", "resend")
                            .env("NOVU_RESEND_FROM", from)
                            .env("NOVU_RESEND_API_KEY_FILE", api_key_file);
                        if let Some(name) = sender_name {
                            cmd.env("NOVU_RESEND_SENDER_NAME", name);
                        }
                    }
                    Provider::Sendgrid => {
                        cmd.env("SWALLOW_ALERT_EMAIL_PROVIDER", "sendgrid")
                            .env("NOVU_SENDGRID_FROM", from)
                            .env("NOVU_SENDGRID_API_KEY_FILE", api_key_file)
                            .env(
                                "NOVU_SENDGRID_SENDER_NAME",
                                sender_name.as_deref().unwrap_or(""),
                            );
                        if let Some(region) = region {
                            cmd.env("NOVU_SENDGRID_REGION", region);
                        }
                    }
                }
            }
        }
    }
}
fn read_config(path: &str) -> Result<Config> {
    let mut raw = Vec::new();
    File::open(path)
        .map_err(|_| "alert configuration unavailable")?
        .take(65_537)
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
    let config = read_config(file)?;
    let binary = child_path(name)?;
    let mut failed = false;
    let mut review = false;
    for target in config.targets {
        if stop.requested() {
            return Err("alert fanout interrupted before all destinations were attempted".into());
        }
        let mut command = Command::new(&binary);
        command
            .args(&command_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        target.apply(&mut command);
        let (channel, id) = target.key();
        match command.status().await {
            Ok(status) if status.code() == Some(0) => println!("{channel} {id}: completed"),
            Ok(status) if status.code() == Some(2) => {
                review = true;
                println!("{channel} {id}: attention");
            }
            _ => {
                failed = true;
                eprintln!("{channel} {id}: failed");
            }
        }
    }
    if failed {
        std::process::exit(1);
    }
    if review {
        std::process::exit(2);
    }
    Ok(())
}
