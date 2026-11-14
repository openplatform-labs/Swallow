//! Poll signed webhook evidence; supervise the process and its database connection.
use novu_postgres_queue::resend_projection;
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_RESEND_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("projection database connection failed");
        }
    });
    if db
        .query_opt(
            "SELECT 1 FROM novu_rust_environments WHERE id=$1",
            &[&environment],
        )
        .await?
        .is_none()
    {
        return Err("environment not found".into());
    }
    loop {
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
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
