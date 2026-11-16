//! Standalone Telegram worker; supervise and monitor independently.
use base64::{Engine, engine::general_purpose::STANDARD};
use novu_postgres_queue::{slack_webhook::valid_destination, telegram::Telegram, telegram_queue};
use std::{error::Error, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}
async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let scoped = match std::env::var("NOVU_TELEGRAM_SCOPED_DB").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Telegram scoped database setting".into()),
    };
    let environment = std::env::var("NOVU_TELEGRAM_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("Telegram environment required".into());
    }
    let destination = std::env::var("NOVU_TELEGRAM_DESTINATION_ID")?;
    if !valid_destination(&destination) {
        return Err("invalid Telegram destination".into());
    }
    let token = novu_rust_contracts::secret::from_env(
        "NOVU_TELEGRAM_BOT_TOKEN",
        "NOVU_TELEGRAM_BOT_TOKEN_FILE",
    )?
    .ok_or("Telegram bot token required")?;
    let provider = Telegram::new(&token).map_err(|_| "invalid Telegram bot configuration")?;
    #[cfg(feature = "test-mocks")]
    let provider = match std::env::var("SWALLOW_TEST_TELEGRAM_ENDPOINT") {
        Ok(v) => {
            let u = reqwest::Url::parse(&v)?;
            if u.scheme() != "http"
                || u.host_str() != Some("127.0.0.1")
                || u.port().is_none()
                || u.path() != "/sendMessage"
                || u.query().is_some()
                || u.fragment().is_some()
                || !u.username().is_empty()
                || u.password().is_some()
            {
                return Err("invalid Telegram fixture".into());
            }
            Telegram::test_endpoint(&v).map_err(|_| "invalid Telegram fixture")?
        }
        Err(std::env::VarError::NotPresent) => provider,
        Err(_) => return Err("invalid Telegram fixture".into()),
    };
    let hash =
        STANDARD.encode(ring::digest::digest(&ring::digest::SHA256, token.as_bytes()).as_ref());
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = Duration::from_secs(30);
    while !stop.requested() {
        if let Ok(Ok((mut db, driver))) =
            tokio::time::timeout(timeout, config.connect(tls.clone())).await
        {
            let task = tokio::spawn(driver);
            let registered = if scoped {
                tokio::time::timeout(
                    timeout,
                    db.query_opt(
                        "SELECT 1 WHERE public.novu_rust_telegram_worker_register($1,$2,$3)",
                        &[&environment, &destination, &hash],
                    ),
                )
                .await
            } else {
                tokio::time::timeout(timeout,db.query_opt("INSERT INTO public.novu_rust_telegram_destinations(environment_id,destination_id,token_hash) VALUES($1,$2,$3) ON CONFLICT(environment_id,destination_id) DO UPDATE SET token_hash=EXCLUDED.token_hash WHERE public.novu_rust_telegram_destinations.token_hash=EXCLUDED.token_hash RETURNING 1",&[&environment,&destination,&hash])).await
            };
            match registered {
                Ok(Ok(Some(_))) => {}
                Ok(Ok(None)) => {
                    task.abort();
                    return Err("Telegram destination uses another bot".into());
                }
                _ => {
                    task.abort();
                    let _ = task.await;
                    stop.wait(Duration::from_secs(2)).await;
                    continue;
                }
            }
            let mut recovery = tokio::time::Instant::now();
            while !stop.requested() {
                if tokio::time::Instant::now() >= recovery {
                    if !matches!(
                        tokio::time::timeout(
                            timeout,
                            telegram_queue::recover_expired_with_scope(
                                &mut db,
                                &environment,
                                &destination,
                                scoped
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
                    telegram_queue::process_one_with_scope(
                        &mut db,
                        &environment,
                        &destination,
                        &provider,
                        scoped,
                    ),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => stop.wait(Duration::from_secs(1)).await,
                    _ => break,
                }
            }
            task.abort();
            let _ = task.await;
        }
        if !stop.requested() {
            eprintln!("Telegram worker database operation stopped; reconnecting");
            stop.wait(Duration::from_secs(2)).await;
        }
    }
    Ok(())
}
