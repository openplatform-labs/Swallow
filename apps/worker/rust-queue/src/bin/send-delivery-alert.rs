//! One scheduled pass. Explicitly configured destinations only.
use novu_postgres_queue::{
    alerts::{self, Alert, Dispatch, SlackWebhook},
    reconciliation,
    resend::{Config, Resend},
};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        return Err(
            "usage: send-delivery-alert ENV GRACE_SECONDS HEARTBEAT_MAX_SECONDS COOLDOWN_SECONDS"
                .into(),
        );
    }
    let grace: i32 = args[1].parse()?;
    let heartbeat: i32 = args[2].parse()?;
    let cooldown: i32 = args[3].parse()?;
    if !(60..=86_400).contains(&cooldown) {
        return Err("invalid cooldown (60-86400 seconds)".into());
    }
    let slack = novu_rust_contracts::secret::from_env(
        "SWALLOW_ALERT_SLACK_WEBHOOK",
        "SWALLOW_ALERT_SLACK_WEBHOOK_FILE",
    )?
    .map(|s| SlackWebhook::new(&s))
    .transpose()
    .map_err(|_| "invalid Slack configuration")?;
    let email_to = std::env::var("SWALLOW_ALERT_EMAIL_TO").ok();
    let email = if let Some(to) = email_to {
        let provider = Resend::new(Config {
            api_key: novu_rust_contracts::secret::from_env(
                "NOVU_RESEND_API_KEY",
                "NOVU_RESEND_API_KEY_FILE",
            )?
            .ok_or("Resend API key is required")?,
            from: std::env::var("NOVU_RESEND_FROM")?,
            sender_name: std::env::var("NOVU_RESEND_SENDER_NAME").unwrap_or_default(),
        })
        .map_err(|_| "invalid email configuration")?;
        Some((to, provider))
    } else {
        None
    };
    if slack.is_none() && email.is_none() {
        return Err("no alert destination configured".into());
    }
    let slack_id =
        std::env::var("SWALLOW_ALERT_SLACK_TARGET_ID").unwrap_or_else(|_| "default".into());
    let email_id =
        std::env::var("SWALLOW_ALERT_EMAIL_TARGET_ID").unwrap_or_else(|_| "default".into());
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let resend_enabled = env_bool("SWALLOW_ALERT_RESEND_ENABLED", true)?;
    let attempts_enabled = env_bool("SWALLOW_ALERT_ATTEMPTS_ENABLED", false)?;
    let queue_wait = match std::env::var("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS") {
        Ok(value) => Some(value.parse::<i32>()?),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err("invalid queue wait threshold".into()),
    };
    if !resend_enabled && !attempts_enabled && queue_wait.is_none() {
        return Err("no alert checks configured".into());
    }
    let (overdue, stale) = if resend_enabled {
        let health = reconciliation::resend_health(&db, &args[0], grace, heartbeat).await?;
        (
            health["overdueUndelivered"]
                .as_u64()
                .ok_or("invalid health result")?,
            health["projectorStale"]
                .as_bool()
                .ok_or("invalid health result")?,
        )
    } else {
        (0, false)
    };
    let queue = if let Some(wait) = queue_wait {
        let health = reconciliation::queue_health(&db, &args[0], wait).await?;
        Some((
            health["overdueJobs"]
                .as_u64()
                .ok_or("invalid queue health")?,
            health["expiredLeases"]
                .as_u64()
                .ok_or("invalid queue health")?,
        ))
    } else {
        None
    };
    let attempts = if attempts_enabled {
        let health = reconciliation::health(&db, &args[0]).await?;
        Some((
            health["unknownAttempts"]
                .as_u64()
                .ok_or("invalid attempt health")?,
            health["strandedDispatches"]
                .as_u64()
                .ok_or("invalid attempt health")?,
        ))
    } else {
        None
    };
    let unhealthy = (resend_enabled && (overdue > 0 || stale))
        || queue.is_some_and(|(overdue, expired)| overdue > 0 || expired > 0)
        || attempts.is_some_and(|(unknown, stranded)| unknown > 0 || stranded > 0);
    let alert = Alert {
        environment: args[0].clone(),
        overdue_undelivered: overdue,
        projector_stale: stale,
        resend_enabled,
        queue,
        attempts,
    };
    alert.text().map_err(|_| "invalid alert environment")?;
    if !unhealthy {
        println!("{{\"status\":\"healthy\"}}");
        return Ok(());
    }
    let mut failed = false;
    if let Some(provider) = slack {
        let result =
            alerts::dispatch_slack(&mut db, &slack_id, cooldown, &alert, &provider).await?;
        if result == Dispatch::NeedsReview {
            failed = true;
            eprintln!("Slack alert requires operator review");
        } else {
            println!("Slack alert: {result:?}");
        }
    }
    if let Some((to, provider)) = email {
        let result =
            alerts::dispatch_email(&mut db, &email_id, cooldown, &alert, &provider, &to).await?;
        if result == Dispatch::NeedsReview {
            failed = true;
            eprintln!("email alert requires operator review");
        } else {
            println!("email alert: {result:?}");
        }
    }
    if failed {
        std::process::exit(2);
    }
    Ok(())
}

fn env_bool(name: &str, default: bool) -> Result<bool, Box<dyn Error + Send + Sync>> {
    match std::env::var(name) {
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Err(std::env::VarError::NotPresent) => Ok(default),
        _ => Err(format!("{name} must be true or false").into()),
    }
}
