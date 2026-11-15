//! Validate one SendGrid request without creating a queue job or delivering mail.
use novu_postgres_queue::sendgrid::{Config, Email, Region, Sendgrid};
use std::io::Read;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("SendGrid sandbox validation failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args_os().len() != 1 {
        return Err("read a single Email JSON object from standard input".into());
    }
    let mut input = Vec::new();
    std::io::stdin()
        .lock()
        .take(1_048_577)
        .read_to_end(&mut input)?;
    if input.len() > 1_048_576 {
        return Err("input exceeds 1 MiB".into());
    }
    let email: Email = serde_json::from_slice(&input).map_err(|_| "invalid Email JSON")?;
    let region = match std::env::var("NOVU_SENDGRID_REGION").as_deref() {
        Ok("eu") => Region::Eu,
        Ok("global") | Err(std::env::VarError::NotPresent) => Region::Global,
        _ => return Err("invalid NOVU_SENDGRID_REGION".into()),
    };
    let api_key = novu_rust_contracts::secret::from_env(
        "NOVU_SENDGRID_API_KEY",
        "NOVU_SENDGRID_API_KEY_FILE",
    )?
    .ok_or("SendGrid API key is required")?;
    let mut provider = Sendgrid::new(Config {
        api_key,
        from: std::env::var("NOVU_SENDGRID_FROM")?,
        sender_name: std::env::var("NOVU_SENDGRID_SENDER_NAME")?,
        region,
    })?;
    match std::env::var("NOVU_SENDGRID_IP_POOL_NAME") {
        Ok(pool) => provider = provider.with_default_ip_pool(pool),
        Err(std::env::VarError::NotPresent) => {}
        Err(_) => return Err("invalid SendGrid IP pool setting".into()),
    }
    provider.validate_sandbox(&email).await?;
    println!("{{\"result\":\"validated\",\"delivered\":false}}");
    Ok(())
}
