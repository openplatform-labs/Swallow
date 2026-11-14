//! Experimental, environment-scoped Twilio consumer; not a full Novu worker.
use novu_postgres_queue::{
    twilio::{Config, Region, Twilio},
    twilio_queue,
};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}

async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_TWILIO_ENVIRONMENT_ID")?;
    if environment.is_empty() {
        return Err("NOVU_TWILIO_ENVIRONMENT_ID must not be empty".into());
    }
    let step_policy = novu_postgres_queue::sms_step_policy::Policy::from_env()?;
    let region = match std::env::var("NOVU_TWILIO_REGION").as_deref() {
        Ok("us") | Err(std::env::VarError::NotPresent) => Region::Us,
        Ok("eu") => Region::Eu,
        _ => return Err("invalid NOVU_TWILIO_REGION".into()),
    };
    let mut provider = Twilio::new(Config {
        account_sid: std::env::var("NOVU_TWILIO_ACCOUNT_SID")?,
        auth_token: novu_rust_contracts::secret::from_env(
            "NOVU_TWILIO_AUTH_TOKEN",
            "NOVU_TWILIO_AUTH_TOKEN_FILE",
        )?
        .unwrap_or_default(),
        api_key_sid: std::env::var("NOVU_TWILIO_API_KEY_SID").ok(),
        api_key_secret: novu_rust_contracts::secret::from_env(
            "NOVU_TWILIO_API_KEY_SECRET",
            "NOVU_TWILIO_API_KEY_SECRET_FILE",
        )?,
        from: std::env::var("NOVU_TWILIO_FROM")?,
        region,
    })?;
    if let Ok(base) = std::env::var("NOVU_TWILIO_STATUS_CALLBACK_BASE_URL") {
        provider = provider.with_status_callback(&base, &environment)?;
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_TWILIO_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid Twilio database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid Twilio database timeout".into()),
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
                    novu_postgres_queue::sms_step_policy::register(&db, &environment, step_policy),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => {
                        task.abort();
                        return Err("SMS workers have different workflow step policies".into());
                    }
                    _ => {
                        task.abort();
                        let _ = task.await;
                        eprintln!("SMS workflow policy registration failed; reconnecting");
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
                                twilio_queue::recover_expired(&mut db, &environment)
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
                        twilio_queue::process_one_with_step_policy(
                            &mut db,
                            &environment,
                            &provider,
                            step_policy,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {}
                        Ok(Ok(false)) => stop.wait(Duration::from_secs(1)).await,
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
        eprintln!("Twilio worker database operation stopped; reconnecting");
        stop.wait(Duration::from_secs(2)).await;
    }
}
