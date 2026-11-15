//! Explicit bounded retention of ephemeral WS event rows.
use std::error::Error;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(args.len() == 4 && args[0] == "preview"
        || args.len() == 5 && args[0] == "delete"
        || args.len() == 6 && args[0] == "delete-v2"
        || args.len() == 3 && args[0] == "request")
    {
        return Err("usage: ws-event-retention preview ENV CUTOFF_UTC BATCH | delete ENV CUTOFF_UTC BATCH REASON | delete-v2 ENV CUTOFF_UTC BATCH REQUEST_ID REASON | request ENV REQUEST_ID".into());
    }
    let request = if args[0] == "request" {
        Some(Uuid::parse_str(&args[2])?)
    } else if args[0] == "delete-v2" {
        Some(Uuid::parse_str(&args[4])?)
    } else {
        None
    };
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("database connection failed");
        }
    });
    if args[0] == "request" {
        let row = db
            .query_one(
                "SELECT public.novu_rust_ws_retention_request($1,$2)",
                &[&args[1], &request.unwrap()],
            )
            .await?;
        println!("{}", row.get::<_, serde_json::Value>(0));
        return Ok(());
    }
    let cutoff = &args[2];
    let bytes = cutoff.as_bytes();
    let fixed = bytes.len() >= 20
        && bytes.len() <= 30
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[bytes.len() - 1] == b'Z'
        && [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18]
            .iter()
            .all(|i| bytes[*i].is_ascii_digit());
    let fraction = bytes.len() == 20
        || (bytes.len() >= 22
            && bytes[19] == b'.'
            && bytes[20..bytes.len() - 1].iter().all(u8::is_ascii_digit));
    if !fixed || !fraction {
        return Err("cutoff must be an ISO 8601 UTC timestamp ending in Z".into());
    }
    let limit: i32 = args[3].parse()?;
    if !(1..=1000).contains(&limit) {
        return Err("batch must be between 1 and 1000".into());
    }
    let row = if args[0] == "delete-v2" {
        db.query_one(
            "SELECT public.novu_rust_ws_retention_delete_v2($1,$2::text::timestamptz,$3,$4,$5)",
            &[&args[1], &cutoff, &limit, &request.unwrap(), &args[5]],
        )
        .await?
    } else {
        let deleting = args[0] == "delete";
        let reason = args.get(4).map(String::as_str);
        db.query_one(
            "SELECT public.novu_rust_ws_retention_batch($1,$2::text::timestamptz,$3,$4,$5)",
            &[&args[1], &cutoff, &limit, &deleting, &reason],
        )
        .await?
    };
    println!("{}", row.get::<_, serde_json::Value>(0));
    Ok(())
}
