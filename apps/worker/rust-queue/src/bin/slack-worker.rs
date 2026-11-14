//! Standalone Slack app worker; use a process supervisor.
use novu_postgres_queue::{slack::Slack, slack_queue};
use std::{error::Error, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_SLACK_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("Slack environment is required".into());
    }
    let token =
        novu_rust_contracts::secret::from_env("NOVU_SLACK_BOT_TOKEN", "NOVU_SLACK_BOT_TOKEN_FILE")?
            .ok_or("Slack token is required")?;
    let provider = Slack::new(token)?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, driver) = config.connect(tls).await?;
    tokio::spawn(async move {
        if driver.await.is_err() {
            eprintln!("Slack database connection failed");
        }
    });
    let mut recovery = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= recovery {
            slack_queue::recover_expired(&mut db, &environment).await?;
            recovery = tokio::time::Instant::now() + Duration::from_secs(5);
        }
        if !slack_queue::process_one(&mut db, &environment, &provider).await? {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
