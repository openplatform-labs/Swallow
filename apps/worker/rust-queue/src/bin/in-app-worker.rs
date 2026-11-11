//! Experimental static in-app worker; not a Novu-compatible production worker.
use std::{error::Error, time::Duration};
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = std::env::var("DATABASE_URL")?;
    let (mut db, connection) = tokio_postgres::connect(&url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("database connection failed: {error}");
        }
    });
    loop {
        match novu_postgres_queue::in_app::process_one(&mut db).await {
            Ok(true) => continue,
            Ok(false) => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(error) => {
                eprintln!("in-app worker failed: {error}");
                // Exit instead of spinning on a failed connection or an unhandled job.
                return Err(error);
            }
        }
    }
}
