//! Experimental Resend worker; use only with isolated fixtures.
use novu_postgres_queue::{
    resend::{Config, Resend},
    resend_queue,
};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_RESEND_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let provider = Resend::new(Config {
        api_key: std::env::var("NOVU_RESEND_API_KEY")?,
        from: std::env::var("NOVU_RESEND_FROM")?,
        sender_name: std::env::var("NOVU_RESEND_SENDER_NAME").unwrap_or_default(),
    })?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("Resend worker database connection failed");
        }
    });
    let mut next_recovery = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_recovery {
            resend_queue::recover_expired(&mut db, &environment).await?;
            next_recovery = tokio::time::Instant::now() + Duration::from_secs(5);
        }
        if !resend_queue::process_one(&mut db, &environment, &provider).await? {
            tokio::time::sleep(resend_queue::POLL_INTERVAL).await;
        }
    }
}
