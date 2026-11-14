//! Experimental Resend worker; use only with isolated fixtures.
use novu_postgres_queue::{
    resend::{Config, Resend},
    resend_queue,
};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}

async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let step_policy = novu_postgres_queue::email_step_policy::Policy::from_env()?;
    let environment = std::env::var("NOVU_RESEND_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let provider = Resend::new(Config {
        api_key: novu_rust_contracts::secret::from_env(
            "NOVU_RESEND_API_KEY",
            "NOVU_RESEND_API_KEY_FILE",
        )?
        .ok_or("Resend API key is required")?,
        from: std::env::var("NOVU_RESEND_FROM")?,
        sender_name: std::env::var("NOVU_RESEND_SENDER_NAME").unwrap_or_default(),
    })?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_RESEND_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid Resend database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid Resend database timeout".into()),
    };
    let retry_guard = match std::env::var("NOVU_RESEND_RETRY_EVIDENCE_GUARD").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Resend retry evidence guard setting".into()),
    };
    let correlation = match std::env::var("NOVU_RESEND_CORRELATION_TAG").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Resend correlation tag setting".into()),
    };
    let correlation_guard = match std::env::var("NOVU_RESEND_RETRY_CORRELATION_GUARD").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Resend retry correlation guard setting".into()),
    };
    let timeout = Duration::from_secs(timeout);
    loop {
        if stop.requested() {
            return Ok(());
        }
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
                match tokio::time::timeout(
                    timeout,
                    novu_postgres_queue::email_step_policy::register(
                        &db,
                        &environment,
                        step_policy,
                    ),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => {
                        task.abort();
                        return Err("email workers have different workflow step policies".into());
                    }
                    _ => {
                        task.abort();
                        let _ = task.await;
                        eprintln!("email workflow policy registration failed; reconnecting");
                        stop.wait(Duration::from_secs(2)).await;
                        continue;
                    }
                }
                loop {
                    if stop.requested() {
                        break;
                    }
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                resend_queue::recover_expired(&mut db, &environment)
                            )
                            .await,
                            Ok(Ok(_))
                        ) {
                            break;
                        }
                        recovery = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                    if stop.requested() {
                        break;
                    }
                    match tokio::time::timeout(
                        timeout,
                        resend_queue::process_one_with_correlation_guard(
                            &mut db,
                            &environment,
                            &provider,
                            retry_guard,
                            step_policy,
                            correlation,
                            correlation_guard,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {}
                        Ok(Ok(false)) => stop.wait(resend_queue::POLL_INTERVAL).await,
                        _ => break,
                    }
                }
            }
            // Dropping the client alone can leave the driver running while a
            // cancelled transaction waits on the server. Abort it before retry.
            task.abort();
            let _ = task.await;
        }
        if stop.requested() {
            return Ok(());
        }
        eprintln!("Resend worker database operation stopped; reconnecting");
        stop.wait(Duration::from_secs(2)).await;
    }
}
