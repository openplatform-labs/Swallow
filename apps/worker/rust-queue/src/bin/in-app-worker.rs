//! Experimental static in-app worker; not a Novu-compatible production worker.
use novu_postgres_queue::{in_app, requeue_expired};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}

async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let slack_snapshot = match std::env::var("NOVU_SLACK_SNAPSHOT_ENDPOINTS").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("NOVU_SLACK_SNAPSHOT_ENDPOINTS must be true or false".into()),
    };
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_IN_APP_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid in-app database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid in-app database timeout".into()),
    };
    let timeout = Duration::from_secs(timeout);
    loop {
        if stop.requested() {
            return Ok(());
        }
        if let Ok(Ok((mut db, driver))) =
            tokio::time::timeout(timeout, config.connect(tls.clone())).await
        {
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
                    if stop.requested() {
                        break;
                    }
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(timeout, requeue_expired(&db, in_app::JOB_KIND))
                                .await,
                            Ok(Ok(_))
                        ) {
                            break;
                        }
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                requeue_expired(&db, in_app::ATTACHMENT_JOB_KIND)
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
                    match tokio::time::timeout(timeout, in_app::process_one_unsnooze(&db)).await {
                        Ok(Ok(true)) => continue,
                        Ok(Ok(false)) => {}
                        _ => break,
                    }
                    if stop.requested() {
                        break;
                    }
                    match tokio::time::timeout(
                        timeout,
                        in_app::process_one_with_slack_snapshot(
                            &mut db,
                            Duration::from_secs(60),
                            slack_snapshot,
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
            task.abort();
            let _ = task.await;
        }
        if stop.requested() {
            return Ok(());
        }
        eprintln!("in-app worker database operation stopped; reconnecting");
        stop.wait(Duration::from_secs(2)).await;
    }
}
