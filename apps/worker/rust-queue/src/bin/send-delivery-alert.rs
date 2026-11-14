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
    // A broken destination must not suppress a second configured destination.
    // Never print provider configuration or the underlying error: it may contain secrets.
    let mut configuration_error = false;
    let slack = match slack_config() {
        Ok(value) => value,
        Err(_) => {
            eprintln!("Slack alert configuration failed");
            configuration_error = true;
            None
        }
    };
    let email = match email_config() {
        Ok(value) => value,
        Err(_) => {
            eprintln!("email alert configuration failed");
            configuration_error = true;
            None
        }
    };
    if slack.is_none() && email.is_none() {
        return Err("no usable alert destination configured".into());
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
    if (resend_enabled && (!(1..=86_400).contains(&grace) || !(1..=86_400).contains(&heartbeat)))
        || queue_wait.is_some_and(|v| !(1..=86_400).contains(&v))
    {
        return Err("invalid alert check threshold (1-86400 seconds)".into());
    }
    let mut failed_checks = [false; 3];
    let resend = if resend_enabled {
        let result = reconciliation::resend_health(&db, &args[0], grace, heartbeat).await;
        let values = result.ok().and_then(|health| {
            Some((
                health["overdueUndelivered"].as_u64()?,
                health["projectorStale"].as_bool()?,
            ))
        });
        if values.is_none() {
            failed_checks[0] = true;
            eprintln!("Resend health check failed");
        }
        values
    } else {
        None
    };
    let queue = if let Some(wait) = queue_wait {
        let result = reconciliation::queue_health(&db, &args[0], wait).await;
        let values = result.ok().and_then(|health| {
            Some((
                health["overdueJobs"].as_u64()?,
                health["expiredLeases"].as_u64()?,
            ))
        });
        if values.is_none() {
            failed_checks[1] = true;
            eprintln!("queue health check failed");
        }
        values
    } else {
        None
    };
    let attempts = if attempts_enabled {
        let result = reconciliation::health(&db, &args[0]).await;
        let values = result.ok().and_then(|health| {
            Some((
                health["unknownAttempts"].as_u64()?,
                health["strandedDispatches"].as_u64()?,
            ))
        });
        if values.is_none() {
            failed_checks[2] = true;
            eprintln!("delivery attempts health check failed");
        }
        values
    } else {
        None
    };
    let unhealthy = failed_checks.iter().any(|failed| *failed)
        || resend.is_some_and(|(overdue, stale)| overdue > 0 || stale)
        || queue.is_some_and(|(overdue, expired)| overdue > 0 || expired > 0)
        || attempts.is_some_and(|(unknown, stranded)| unknown > 0 || stranded > 0);
    let (overdue, stale) = resend.unwrap_or((0, false));
    let alert = Alert {
        environment: args[0].clone(),
        overdue_undelivered: overdue,
        projector_stale: stale,
        resend_enabled: resend.is_some(),
        queue,
        attempts,
        failed_checks,
    };
    alert.text().map_err(|_| "invalid alert environment")?;
    if !unhealthy {
        if configuration_error {
            std::process::exit(1);
        }
        println!("{{\"status\":\"healthy\"}}");
        return Ok(());
    }
    let mut error = configuration_error || failed_checks.iter().any(|failed| *failed);
    let mut needs_review = false;
    if let Some(provider) = slack {
        match alerts::dispatch_slack(&mut db, &slack_id, cooldown, &alert, &provider).await {
            Ok(Dispatch::NeedsReview) => {
                needs_review = true;
                eprintln!("Slack alert requires operator review");
            }
            Ok(result) => println!("Slack alert: {result:?}"),
            Err(_) => {
                error = true;
                eprintln!("Slack alert dispatch failed");
            }
        }
    }
    if let Some((to, provider)) = email {
        match alerts::dispatch_email(&mut db, &email_id, cooldown, &alert, &provider, &to).await {
            Ok(Dispatch::NeedsReview) => {
                needs_review = true;
                eprintln!("email alert requires operator review");
            }
            Ok(result) => println!("email alert: {result:?}"),
            Err(_) => {
                error = true;
                eprintln!("email alert dispatch failed");
            }
        }
    }
    if error {
        std::process::exit(1);
    }
    if needs_review {
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

fn slack_config() -> Result<Option<SlackWebhook>, Box<dyn Error + Send + Sync>> {
    Ok(novu_rust_contracts::secret::from_env(
        "SWALLOW_ALERT_SLACK_WEBHOOK",
        "SWALLOW_ALERT_SLACK_WEBHOOK_FILE",
    )?
    .map(|s| SlackWebhook::new(&s).map_err(|_| "invalid Slack configuration"))
    .transpose()?)
}

fn email_config() -> Result<Option<(String, Resend)>, Box<dyn Error + Send + Sync>> {
    let to = match std::env::var("SWALLOW_ALERT_EMAIL_TO") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(_) => return Err("invalid email recipient".into()),
    };
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
    Ok(Some((to, provider)))
}
