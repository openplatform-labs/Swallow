//! Poll signed webhook evidence; supervise the process and its database connection.
use novu_postgres_queue::resend_projection;
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_RESEND_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let database_url = std::env::var("DATABASE_URL")?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&database_url)?;
    loop {
        let (mut db, connection) = match config.connect(tls.clone()).await {
            Ok(connection) => connection,
            Err(_) => {
                eprintln!("projection database unavailable; retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let mut driver = tokio::spawn(connection);
        match db
            .query_opt(
                "SELECT 1 FROM novu_rust_environments WHERE id=$1",
                &[&environment],
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                driver.abort();
                return Err("environment not found".into());
            }
            Err(_) => {
                driver.abort();
                eprintln!("projection database unavailable; retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        }
        loop {
            let scan = async {
                let mut after = None;
                loop {
                    let jobs = resend_projection::page(&db, &environment, after).await?;
                    if jobs.is_empty() {
                        break;
                    }
                    after = jobs.last().copied();
                    for job in jobs {
                        resend_projection::project(&mut db, &environment, job).await?;
                    }
                }
                db.execute("INSERT INTO novu_rust_resend_projection_heartbeat(environment_id,last_completed_at) VALUES ($1,clock_timestamp()) ON CONFLICT(environment_id) DO UPDATE SET last_completed_at=EXCLUDED.last_completed_at", &[&environment]).await?;
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            };
            let failed = tokio::select! {
                _ = &mut driver => true,
                result = scan => result.is_err(),
            };
            if failed {
                break;
            }
            tokio::select! {
                _ = &mut driver => break,
                _ = tokio::time::sleep(Duration::from_secs(30)) => {},
            }
        }
        driver.abort();
        eprintln!("projection scan stopped; reconnecting");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
