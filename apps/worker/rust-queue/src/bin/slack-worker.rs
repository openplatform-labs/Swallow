//! Standalone Slack app worker; use a process supervisor and external monitoring.
use novu_postgres_queue::{slack::Slack, slack_queue};
use std::{error::Error, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_SLACK_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("Slack environment is required".into());
    }
    let token =
        novu_rust_contracts::secret::from_env("NOVU_SLACK_BOT_TOKEN", "NOVU_SLACK_BOT_TOKEN_FILE")?
            .ok_or("Slack token is required")?;
    let provider = Slack::new(token)?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_SLACK_DB_TIMEOUT_SECONDS") {
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
            let mut recovery = tokio::time::Instant::now();
            if configured {
                loop {
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                slack_queue::recover_expired(&mut db, &environment)
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
                        slack_queue::process_one(&mut db, &environment, &provider),
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
