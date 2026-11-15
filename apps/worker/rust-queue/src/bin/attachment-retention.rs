//! Opt-in, audited scrubbing of completed workflow attachment copies.
use serde_json::json;
use std::{error::Error, time::Duration};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let valid = matches!(args.as_slice(),[cmd,_,_,_] if cmd=="list" || cmd=="list-alternatives")
        || matches!(args.as_slice(),[cmd,_,_,_,_] if cmd=="scrub" || cmd=="scrub-alternatives")
        || matches!(args.as_slice(),[cmd,_] if cmd=="audit" || cmd=="audit-alternatives")
        || matches!(args.as_slice(),[cmd,_,_] if cmd=="audit" || cmd=="audit-alternatives");
    if !valid {
        return Err("usage: attachment-retention list-alternatives ENV AGE LIMIT | scrub-alternatives ENV AGE LIMIT REASON | audit-alternatives ENV [AFTER_ID] | list ENV MIN_AGE_SECONDS LIMIT | scrub ENV MIN_AGE_SECONDS LIMIT REASON | audit ENV [AFTER_ID]".into());
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (db, driver) = tokio::time::timeout(Duration::from_secs(30), config.connect(tls)).await??;
    tokio::spawn(async move {
        let _ = driver.await;
    });
    match args[0].as_str() {
        "list" | "scrub" | "list-alternatives" | "scrub-alternatives" => {
            let is_list = args[0].starts_with("list");
            let alternative = args[0].ends_with("alternatives");
            let age: i32 = args[2].parse()?;
            let limit: i32 = args[3].parse()?;
            if !(0..=31_536_000).contains(&age) || !(1..=100).contains(&limit) {
                return Err("invalid retention range".into());
            }
            let result = if is_list {
                tokio::time::timeout(
                    Duration::from_secs(30),
                    db.query(
                        if alternative { "SELECT * FROM public.novu_rust_list_alternative_retention($1,$2,$3)" } else { "SELECT * FROM public.novu_rust_list_attachment_retention($1,$2,$3)" },
                        &[&args[1], &age, &limit],
                    ),
                )
                .await
            } else {
                tokio::time::timeout(
                    Duration::from_secs(30),
                    db.query(
                        if alternative { "SELECT * FROM public.novu_rust_scrub_alternative_retention($1,$2,$3,$4)" } else { "SELECT * FROM public.novu_rust_scrub_attachment_retention($1,$2,$3,$4)" },
                        &[&args[1], &age, &limit, &args[4]],
                    ),
                )
                .await
            };
            let rows=match result { Ok(Ok(rows))=>rows, _ if !is_list=>return Err("scrub outcome unavailable; reconcile audit and current job states before retrying".into()), _=>return Err("retention listing unavailable".into()) };
            let values:Vec<_>=rows.into_iter().map(|r|if is_list {json!({"jobId":r.get::<_,Uuid>(0).to_string(),"kind":r.get::<_,String>(1),"completedAt":r.get::<_,String>(2)})} else {json!({"jobId":r.get::<_,Uuid>(0).to_string(),"kind":r.get::<_,String>(1)})}).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "audit" | "audit-alternatives" => {
            let after = args
                .get(2)
                .map(|s| s.parse::<i64>())
                .transpose()?
                .unwrap_or(0);
            let rows = tokio::time::timeout(
                Duration::from_secs(30),
                db.query(
                    if args[0] == "audit-alternatives" { "SELECT * FROM public.novu_rust_alternative_retention_audit($1,$2)" } else { "SELECT * FROM public.novu_rust_attachment_retention_audit($1,$2)" },
                    &[&args[1], &after],
                ),
            )
            .await??;
            let values:Vec<_>=rows.into_iter().map(|r|json!({"id":r.get::<_,String>(0),"jobId":r.get::<_,Uuid>(1).to_string(),"kind":r.get::<_,String>(2),"actor":r.get::<_,String>(3),"reason":r.get::<_,String>(4),"createdAt":r.get::<_,String>(5)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        _ => unreachable!(),
    }
    Ok(())
}
