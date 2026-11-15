//! Environment-scoped SMTP worker. Delivery after a recorded attempt is never retried automatically.
use novu_postgres_queue::{email_step_policy, smtp_alert::SmtpAlert, smtp_queue};
use std::{error::Error, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}
async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_SMTP_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let provider = SmtpAlert::from_env().map_err(|_| "invalid SMTP configuration")?;
    let policy = email_step_policy::Policy::from_env()?;
    let seconds = match std::env::var("NOVU_SMTP_DB_TIMEOUT_SECONDS") {
        Ok(v) => v
            .parse::<u64>()
            .ok()
            .filter(|v| (1..=300).contains(v))
            .ok_or("invalid SMTP database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid SMTP database timeout".into()),
    };
    let timeout = Duration::from_secs(seconds);
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    loop {
        if stop.requested() {
            return Ok(());
        }
        if let Ok(Ok((mut db, driver))) =
            tokio::time::timeout(timeout, config.connect(tls.clone())).await
        {
            let task = tokio::spawn(driver);
            let statement_timeout = format!("{}ms", seconds * 1000);
            let ready = matches!(
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
            if ready {
                match tokio::time::timeout(
                    timeout,
                    email_step_policy::register(&db, &environment, policy),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => {
                        task.abort();
                        return Err("email workers have different workflow policies".into());
                    }
                    _ => {
                        task.abort();
                        eprintln!("SMTP policy registration failed; reconnecting");
                        stop.wait(Duration::from_secs(2)).await;
                        continue;
                    }
                }
                let mut recovery = tokio::time::Instant::now();
                loop {
                    if stop.requested() {
                        break;
                    }
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                smtp_queue::recover_expired(&mut db, &environment)
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
                        smtp_queue::process_one(&mut db, &environment, &provider, policy),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {}
                        Ok(Ok(false)) => stop.wait(Duration::from_secs(1)).await,
                        _ => break,
                    }
                }
            }
            task.abort();
            let _ = task.await;
        }
        if stop.requested() {
            return Ok(());
        }
        eprintln!("SMTP worker unavailable; reconnecting");
        stop.wait(Duration::from_secs(2)).await;
    }
}
