//! Standalone Discord webhook worker; use a process supervisor and external monitoring.
use base64::{Engine, engine::general_purpose::STANDARD};
use novu_postgres_queue::{discord::Webhook, discord_queue, slack_webhook::valid_destination};
use std::{error::Error, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}

async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_DISCORD_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("Discord environment is required".into());
    }
    let destination = std::env::var("NOVU_DISCORD_WEBHOOK_DESTINATION_ID")?;
    if !valid_destination(&destination) {
        return Err("invalid Discord webhook destination ID".into());
    }
    let retry_mode = match std::env::var("NOVU_DISCORD_429_RETRY_MODE") {
        Ok(v) => v,
        Err(std::env::VarError::NotPresent) => "off".to_owned(),
        Err(_) => return Err("invalid Discord 429 retry mode".into()),
    };
    if !["off", "known", "fallback"].contains(&retry_mode.as_str()) {
        return Err("invalid Discord 429 retry mode".into());
    }
    let retry_max = match std::env::var("NOVU_DISCORD_429_MAX_RETRIES") {
        Ok(v) => v
            .parse::<i32>()
            .ok()
            .filter(|v| (0..=10).contains(v))
            .ok_or("invalid Discord 429 retry limit")?,
        Err(std::env::VarError::NotPresent) => 3,
        Err(_) => return Err("invalid Discord 429 retry limit".into()),
    };
    let retry_fallback = match std::env::var("NOVU_DISCORD_429_FALLBACK_SECONDS") {
        Ok(v) => v
            .parse::<i32>()
            .ok()
            .filter(|v| (1..=86400).contains(v))
            .ok_or("invalid Discord 429 fallback")?,
        Err(std::env::VarError::NotPresent) => 60,
        Err(_) => return Err("invalid Discord 429 fallback".into()),
    };
    let url = novu_rust_contracts::secret::from_env(
        "NOVU_DISCORD_WEBHOOK_URL",
        "NOVU_DISCORD_WEBHOOK_URL_FILE",
    )?
    .ok_or("Discord webhook URL required")?;
    let provider = Webhook::new(&url)?;
    let endpoint_hash =
        STANDARD.encode(ring::digest::digest(&ring::digest::SHA256, url.as_bytes()).as_ref());
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_DISCORD_WEBHOOK_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid Discord database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid Discord database timeout".into()),
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
            if configured {
                match tokio::time::timeout(timeout, db.query_opt("INSERT INTO novu_rust_discord_destinations(environment_id,destination_id,endpoint_hash) VALUES ($1,$2,$3) ON CONFLICT (environment_id,destination_id) DO UPDATE SET endpoint_hash=EXCLUDED.endpoint_hash WHERE novu_rust_discord_destinations.endpoint_hash=EXCLUDED.endpoint_hash RETURNING 1", &[&environment,&destination,&endpoint_hash])).await {
                    Ok(Ok(Some(_))) => {},
                    Ok(Ok(None)) => { task.abort(); return Err("Discord webhook destination already uses a different endpoint".into()); },
                    _ => { task.abort(); let _=task.await; eprintln!("Discord webhook destination registration failed; reconnecting"); stop.wait(Duration::from_secs(2)).await; continue; }
                }
            }
            if configured {
                match tokio::time::timeout(timeout, db.query_opt("INSERT INTO public.novu_rust_discord_retry_policy(environment_id,destination_id,mode,max_retries,fallback_seconds) VALUES($1,$2,$3,$4,$5) ON CONFLICT(environment_id,destination_id) DO UPDATE SET mode=EXCLUDED.mode WHERE (novu_rust_discord_retry_policy.mode,novu_rust_discord_retry_policy.max_retries,novu_rust_discord_retry_policy.fallback_seconds)=(EXCLUDED.mode,EXCLUDED.max_retries,EXCLUDED.fallback_seconds) RETURNING 1", &[&environment,&destination,&retry_mode,&retry_max,&retry_fallback])).await {
                    Ok(Ok(Some(_))) => {},
                    Ok(Ok(None)) => {task.abort();return Err("Discord 429 policy conflicts with registered policy".into());},
                    _ => {task.abort();let _=task.await;eprintln!("Discord 429 policy registration failed; reconnecting");stop.wait(Duration::from_secs(2)).await;continue;}
                }
            }
            let mut recovery = tokio::time::Instant::now();
            if configured {
                loop {
                    if stop.requested() {
                        break;
                    }
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                discord_queue::recover_expired(&mut db, &environment, &destination)
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
                        discord_queue::process_one(&mut db, &environment, &destination, &provider),
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
        eprintln!("Discord worker database operation stopped; reconnecting");
        stop.wait(Duration::from_secs(2)).await;
    }
}
