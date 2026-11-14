//! Experimental environment-scoped SendGrid worker, not a full Novu worker.
use novu_postgres_queue::{
    sendgrid::{Config, Region, Sendgrid},
    sendgrid_queue,
};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_SENDGRID_ENVIRONMENT_ID")?;
    if environment.is_empty() {
        return Err("NOVU_SENDGRID_ENVIRONMENT_ID must not be empty".into());
    }
    let region = match std::env::var("NOVU_SENDGRID_REGION").as_deref() {
        Ok("eu") => Region::Eu,
        Ok("global") | Err(std::env::VarError::NotPresent) => Region::Global,
        _ => return Err("invalid NOVU_SENDGRID_REGION".into()),
    };
    let mut provider = Sendgrid::new(Config {
        api_key: novu_rust_contracts::secret::from_env(
            "NOVU_SENDGRID_API_KEY",
            "NOVU_SENDGRID_API_KEY_FILE",
        )?
        .ok_or("SendGrid API key is required")?,
        from: std::env::var("NOVU_SENDGRID_FROM")?,
        sender_name: std::env::var("NOVU_SENDGRID_SENDER_NAME")?,
        region,
    })?;
    match std::env::var("NOVU_SENDGRID_DISPATCH_TRACKING").as_deref() {
        Ok("true") => provider = provider.with_dispatch_tracking(),
        Ok("false") | Err(std::env::VarError::NotPresent) => {}
        _ => return Err("invalid NOVU_SENDGRID_DISPATCH_TRACKING".into()),
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_SENDGRID_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid SendGrid database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid SendGrid database timeout".into()),
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
                                sendgrid_queue::recover_expired(&mut db, &environment)
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
                        sendgrid_queue::process_one(&mut db, &environment, &provider),
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
        eprintln!("SendGrid worker database operation stopped; reconnecting");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
