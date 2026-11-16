//! Submit one standalone Telegram job from stdin. No credentials are read here.
use std::{error::Error, io::Read};
use uuid::Uuid;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 1 || args[0].trim().is_empty() {
        return Err("usage: telegram-enqueue ENV < message.json".into());
    }
    let mut bytes = Vec::new();
    std::io::stdin().take(131_073).read_to_end(&mut bytes)?;
    if bytes.len() > 131_072 {
        return Err("invalid Telegram job".into());
    }
    let message: novu_postgres_queue::telegram::Message =
        serde_json::from_slice(&bytes).map_err(|_| "invalid Telegram job")?;
    novu_postgres_queue::telegram::request_body(&message).map_err(|_| "invalid Telegram job")?;
    let payload = serde_json::to_value(message).map_err(|_| "invalid Telegram job")?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (db, driver) = config.connect(tls).await?;
    tokio::spawn(driver);
    let id = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        id,
        &args[0],
        novu_postgres_queue::telegram_queue::JOB_KIND,
        &payload,
    )
    .await?;
    println!("{}", serde_json::json!({"jobId":id}));
    Ok(())
}
