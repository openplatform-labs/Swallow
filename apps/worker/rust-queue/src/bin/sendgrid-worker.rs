//! Experimental environment-scoped SendGrid worker, not a full Novu worker.
use novu_postgres_queue::{
    sendgrid::{Config, Region, Sendgrid},
    sendgrid_queue,
};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_SENDGRID_ENVIRONMENT_ID")?;
    if environment.is_empty() {
        return Err("NOVU_SENDGRID_ENVIRONMENT_ID must not be empty".into());
    }
    let region = match std::env::var("NOVU_SENDGRID_REGION").as_deref() {
        Ok("eu") => Region::Eu,
        Ok("global") | Err(std::env::VarError::NotPresent) => Region::Global,
        _ => return Err("invalid NOVU_SENDGRID_REGION".into()),
    };
    let provider = Sendgrid::new(Config {
        api_key: std::env::var("NOVU_SENDGRID_API_KEY")?,
        from: std::env::var("NOVU_SENDGRID_FROM")?,
        sender_name: std::env::var("NOVU_SENDGRID_SENDER_NAME")?,
        region,
    })?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("email worker database connection failed");
        }
    });
    let mut next_recovery = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_recovery {
            sendgrid_queue::recover_expired(&mut db, &environment).await?;
            next_recovery = tokio::time::Instant::now() + Duration::from_secs(5);
        }
        if !sendgrid_queue::process_one(&mut db, &environment, &provider).await? {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
