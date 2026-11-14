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
    let health = reconciliation::resend_health(&db, &args[0], grace, heartbeat).await?;
    let overdue = health["overdueUndelivered"]
        .as_u64()
        .ok_or("invalid health result")?;
    let stale = health["projectorStale"]
        .as_bool()
        .ok_or("invalid health result")?;
    let alert = Alert {
        environment: args[0].clone(),
        overdue_undelivered: overdue,
        projector_stale: stale,
    };
    alert.text().map_err(|_| "invalid alert environment")?;
    if overdue == 0 && !stale {
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
