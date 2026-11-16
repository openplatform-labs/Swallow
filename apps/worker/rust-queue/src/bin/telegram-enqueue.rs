//! Submit one standalone Telegram job from stdin. No credentials are read here.
use std::{error::Error, io::Read};
use uuid::Uuid;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let scoped = match std::env::var("NOVU_TELEGRAM_ENQUEUE_SCOPED").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Telegram enqueue mode".into()),
    };
    if (scoped && args.len() != 2)
        || (!scoped && args.len() != 1)
        || args.first().is_none_or(|v| v.trim().is_empty())
    {
        return Err("usage: telegram-enqueue ENV [REQUEST_UUID] < message.json".into());
    }
    let request = if scoped {
        Some(Uuid::parse_str(&args[1]).map_err(|_| "invalid Telegram request ID")?)
    } else {
        None
    };
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
    if let Some(request) = request {
        let row = db
            .query_one(
                "SELECT public.novu_rust_telegram_enqueue($1,$2,$3)",
                &[&args[0], &request, &payload],
            )
            .await?;
        println!("{}", row.get::<_, serde_json::Value>(0));
    } else {
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
    }
    Ok(())
}
