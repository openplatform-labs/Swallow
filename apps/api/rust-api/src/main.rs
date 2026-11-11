use std::{error::Error, sync::Arc};
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let (db, connection) = tokio_postgres::connect(&url, NoTls).await?;
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
