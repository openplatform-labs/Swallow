//! Local operator tool. Requires database access and an independently verified outcome.
use std::error::Error;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if ![2, 3, 4, 6].contains(&args.len()) {
        return Err("usage: delivery-reconcile list ENV [AFTER_JOB AFTER_TARGET|-] | evidence ENV EMAIL_JOB | sms-evidence ENV SMS_JOB | retry|accepted ENV JOB TARGET_INDEX|- OPERATOR REASON".into());
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("database connection failed");
        }
    });
    if args.len() == 2 && args[0] == "list" {
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::list(&db, &args[1]).await?
            )?
        );
    } else if args.len() == 4 && args[0] == "list" {
        let job = Uuid::parse_str(&args[2])?;
        let target = if args[3] == "-" {
            None
        } else {
            Some(args[3].parse::<i32>()?)
        };
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::list_after(
                    &db,
                    &args[1],
                    Some((job, target))
                )
                .await?
            )?
        );
    } else if args.len() == 3 && args[0] == "evidence" {
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::email_evidence(
                    &db,
                    &args[1],
                    Uuid::parse_str(&args[2])?
                )
                .await?
            )?
        );
    } else if args.len() == 3 && args[0] == "sms-evidence" {
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::sms_evidence(
                    &db,
                    &args[1],
                    Uuid::parse_str(&args[2])?
                )
                .await?
            )?
        );
    } else if args.len() == 6 && ["retry", "accepted"].contains(&args[0].as_str()) {
        let job = Uuid::parse_str(&args[2])?;
        let target = if args[3] == "-" {
            None
        } else {
            Some(args[3].parse::<i32>()?)
        };
        novu_postgres_queue::reconciliation::resolve(
            &mut db, &args[1], job, target, &args[0], &args[4], &args[5],
        )
        .await?;
        println!("reconciliation recorded");
    } else {
        return Err("invalid reconciliation command".into());
    }
    Ok(())
}
