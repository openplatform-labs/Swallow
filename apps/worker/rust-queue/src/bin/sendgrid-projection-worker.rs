//! Poll signed webhook evidence; supervise the process and its database connection.
use novu_postgres_queue::sendgrid_projection;
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}

async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_SENDGRID_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let database_url = std::env::var("DATABASE_URL")?;
    let timeout_seconds: u64 = match std::env::var("NOVU_SENDGRID_PROJECTION_DB_TIMEOUT_SECONDS") {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid projection database timeout".into()),
    };
    if !(1..=300).contains(&timeout_seconds) {
        return Err("projection database timeout must be 1-300 seconds".into());
    }
    let require_full_scan =
        match std::env::var("NOVU_SENDGRID_PROJECTION_REQUIRE_FULL_SCAN").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("invalid full scan policy".into()),
        };
    let scan_seconds: u64 = match std::env::var("NOVU_SENDGRID_PROJECTION_SCAN_INTERVAL_SECONDS") {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid scan interval".into()),
    };
    if !(1..=300).contains(&scan_seconds) {
        return Err("scan interval must be 1-300 seconds".into());
    }
    let timeout = Duration::from_secs(timeout_seconds);
    let (config, tls) = novu_rust_contracts::postgres::configure(&database_url)?;
    loop {
        if stop.requested() {
            return Ok(());
        }
        let (mut db, connection) =
            match tokio::time::timeout(timeout, config.connect(tls.clone())).await {
                Ok(Ok(connection)) => connection,
                _ => {
                    eprintln!("projection database unavailable; retrying");
                    stop.wait(Duration::from_secs(2)).await;
                    continue;
                }
            };
        let mut driver = tokio::spawn(connection);
        let statement_timeout = format!("{}ms", timeout_seconds * 1000);
        if !matches!(
            tokio::time::timeout(
                timeout,
                db.query_one(
                    "SELECT set_config('statement_timeout',$1,false)",
                    &[&statement_timeout]
                )
            )
            .await,
            Ok(Ok(_))
        ) {
            driver.abort();
            eprintln!("projection database unavailable; retrying");
            stop.wait(Duration::from_secs(2)).await;
            continue;
        }
        match tokio::time::timeout(
            timeout,
            db.query_opt(
                "SELECT 1 FROM novu_rust_environments WHERE id=$1",
                &[&environment],
            ),
        )
        .await
        {
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => {
                driver.abort();
                return Err("environment not found".into());
            }
            _ => {
                driver.abort();
                eprintln!("projection database unavailable; retrying");
                stop.wait(Duration::from_secs(2)).await;
                continue;
            }
        }
        loop {
            if stop.requested() {
                break;
            }
            let scan = async {
                let mut after = None;
                let mut skipped = false;
                loop {
                    if stop.requested() {
                        return Ok(false);
                    }
                    let jobs = tokio::time::timeout(
                        timeout,
                        sendgrid_projection::page(&db, &environment, after),
                    )
                    .await??;
                    if stop.requested() {
                        return Ok(false);
                    }
                    if jobs.is_empty() {
                        break;
                    }
                    after = jobs.last().copied();
                    for job in jobs {
                        if stop.requested() {
                            return Ok(false);
                        }
                        let processed = tokio::time::timeout(
                            timeout,
                            sendgrid_projection::project(&mut db, &environment, job),
                        )
                        .await??;
                        skipped |= !processed;
                    }
                }
                if stop.requested() {
                    return Ok(false);
                }
                if !require_full_scan || !skipped {
                    tokio::time::timeout(timeout, db.execute("INSERT INTO novu_rust_sendgrid_projection_heartbeat(environment_id,last_completed_at) VALUES ($1,clock_timestamp()) ON CONFLICT(environment_id) DO UPDATE SET last_completed_at=EXCLUDED.last_completed_at", &[&environment])).await??;
                } else {
                    eprintln!("projection scan skipped jobs; heartbeat not advanced");
                }
                Ok::<bool, Box<dyn Error + Send + Sync>>(true)
            };
            let failed = tokio::select! {
                _ = &mut driver => true,
                result = scan => match result { Ok(true) => false, Ok(false) => break, Err(_) => true },
            };
            if failed {
                break;
            }
            tokio::select! {
                _ = &mut driver => break,
                _ = stop.wait(Duration::from_secs(scan_seconds)) => {},
            }
        }
        driver.abort();
        if stop.requested() {
            return Ok(());
        }
        eprintln!("projection scan stopped; reconnecting");
        stop.wait(Duration::from_secs(2)).await;
    }
}
