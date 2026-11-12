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
    // Only this handler has its side effects and completion in one transaction;
    // expired jobs of other kinds must not be recovered by this process.
    let mut next_recovery = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_recovery {
            novu_postgres_queue::requeue_expired(&db, novu_postgres_queue::in_app::JOB_KIND)
                .await?;
            next_recovery = tokio::time::Instant::now() + Duration::from_secs(5);
        }
        if novu_postgres_queue::in_app::process_one_unsnooze(&db).await? {
            continue;
        }
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
