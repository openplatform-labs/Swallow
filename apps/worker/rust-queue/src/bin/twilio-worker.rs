//! Experimental, environment-scoped Twilio consumer; not a full Novu worker.
use novu_postgres_queue::{
    twilio::{Config, Region, Twilio},
    twilio_queue,
};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_TWILIO_ENVIRONMENT_ID")?;
    if environment.is_empty() {
        return Err("NOVU_TWILIO_ENVIRONMENT_ID must not be empty".into());
    }
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
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("sms worker database connection failed");
        }
    });
    let mut next_recovery = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_recovery {
            twilio_queue::recover_expired(&mut db, &environment).await?;
            next_recovery = tokio::time::Instant::now() + Duration::from_secs(5);
        }
        if !twilio_queue::process_one(&mut db, &environment, &provider).await? {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
