//! Experimental environment-scoped FCM worker; not a full Novu worker.
use novu_postgres_queue::{fcm::Fcm, fcm_queue};
use std::{error::Error, time::Duration};
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_FCM_ENVIRONMENT_ID")?;
    if environment.is_empty() {
        return Err("NOVU_FCM_ENVIRONMENT_ID must not be empty".into());
    }
    let project = std::env::var("NOVU_FCM_PROJECT_ID")?;
    let provider = if let Ok(token) = std::env::var("NOVU_FCM_ACCESS_TOKEN") {
        Fcm::new(&project, token)?
    } else {
        Fcm::with_service_account(
            &project,
            std::env::var("NOVU_FCM_CLIENT_EMAIL")?,
            &std::env::var("NOVU_FCM_PRIVATE_KEY")?,
        )?
    };
    let (mut db, connection) =
        tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("push worker database connection failed");
        }
    });
    let mut next_recovery = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_recovery {
            fcm_queue::recover_expired(&mut db, &environment).await?;
            next_recovery = tokio::time::Instant::now() + Duration::from_secs(5);
        }
        if !fcm_queue::process_one(&mut db, &environment, &provider).await? {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
