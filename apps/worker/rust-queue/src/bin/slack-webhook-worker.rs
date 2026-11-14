//! Standalone Slack webhook worker; use a process supervisor and external monitoring.
use base64::{Engine, engine::general_purpose::STANDARD};
use novu_postgres_queue::{
    slack_webhook::{Webhook, valid_destination},
    slack_webhook_queue,
};
use std::{error::Error, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_SLACK_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("Slack environment is required".into());
    }
    let destination = std::env::var("NOVU_SLACK_WEBHOOK_DESTINATION_ID")?;
    if !valid_destination(&destination) {
        return Err("invalid Slack webhook destination ID".into());
    }
    let url = novu_rust_contracts::secret::from_env(
        "NOVU_SLACK_WEBHOOK_URL",
        "NOVU_SLACK_WEBHOOK_URL_FILE",
    )?
    .ok_or("Slack webhook URL required")?;
    let provider = Webhook::new(&url)?;
    let endpoint_hash =
        STANDARD.encode(ring::digest::digest(&ring::digest::SHA256, url.as_bytes()).as_ref());
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_SLACK_WEBHOOK_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid Slack database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid Slack database timeout".into()),
    };
    let timeout = Duration::from_secs(timeout);
    loop {
        let connection = tokio::time::timeout(timeout, config.connect(tls.clone())).await;
        if let Ok(Ok((mut db, driver))) = connection {
            let task = tokio::spawn(driver);
            let statement_timeout = format!("{}ms", timeout.as_millis());
            let configured = matches!(
                tokio::time::timeout(
                    timeout,
                    db.query_one(
                        "SELECT set_config('statement_timeout',$1,false)",
                        &[&statement_timeout]
                    )
                )
                .await,
                Ok(Ok(_))
            );
            if configured {
                match tokio::time::timeout(timeout, db.query_opt("INSERT INTO novu_rust_slack_webhook_destinations(environment_id,destination_id,endpoint_hash) VALUES ($1,$2,$3) ON CONFLICT (environment_id,destination_id) DO UPDATE SET endpoint_hash=EXCLUDED.endpoint_hash WHERE novu_rust_slack_webhook_destinations.endpoint_hash=EXCLUDED.endpoint_hash RETURNING 1", &[&environment,&destination,&endpoint_hash])).await {
                    Ok(Ok(Some(_))) => {},
                    Ok(Ok(None)) => { task.abort(); return Err("Slack webhook destination already uses a different endpoint".into()); },
                    _ => { task.abort(); let _=task.await; eprintln!("Slack webhook destination registration failed; reconnecting"); tokio::time::sleep(Duration::from_secs(2)).await; continue; }
                }
            }
            let mut recovery = tokio::time::Instant::now();
            if configured {
                loop {
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                slack_webhook_queue::recover_expired(
                                    &mut db,
                                    &environment,
                                    &destination
                                )
                            )
                            .await,
                            Ok(Ok(_))
                        ) {
                            break;
                        }
                        recovery = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                    match tokio::time::timeout(
                        timeout,
                        slack_webhook_queue::process_one(
                            &mut db,
                            &environment,
                            &destination,
                            &provider,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {}
                        Ok(Ok(false)) => tokio::time::sleep(Duration::from_secs(1)).await,
                        _ => break,
                    }
                }
            }
            // Dropping the client alone can leave the driver running while a
            // cancelled transaction waits on the server. Abort it before retry.
            task.abort();
            let _ = task.await;
        }
        eprintln!("Slack worker database operation stopped; reconnecting");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
