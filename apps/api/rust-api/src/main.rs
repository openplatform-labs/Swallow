use std::{error::Error, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = std::env::var("DATABASE_URL")?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&url)?;
    let (db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("database connection failed: {error}");
        }
    });
    let address = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    axum::serve(listener, novu_rust_api::router(Arc::new(db))).await?;
    Ok(())
}
