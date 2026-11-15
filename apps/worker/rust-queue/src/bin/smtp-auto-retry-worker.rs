//! Opt-in SMTP at-least-once recovery; provider acceptance can be duplicated.
use std::{error::Error,time::Duration};
#[tokio::main]
async fn main() -> Result<(),Box<dyn Error+Send+Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}
async fn run(stop:novu_postgres_queue::shutdown::Stop) -> Result<(),Box<dyn Error+Send+Sync>> {
    let environment=std::env::var("NOVU_SMTP_ENVIRONMENT_ID")?;
    if environment.trim().is_empty(){return Err("environment is required".into());}
    let interval=match std::env::var("SWALLOW_SMTP_AUTO_RETRY_INTERVAL_SECONDS") {
        Ok(value)=>value.parse::<u64>().ok().filter(|v|(1..=3600).contains(v)).ok_or("invalid SMTP auto retry interval")?,
        Err(std::env::VarError::NotPresent)=>15,_=>return Err("invalid SMTP auto retry interval".into())
    };
    let batch=match std::env::var("SWALLOW_SMTP_AUTO_RETRY_BATCH_SIZE") {
        Ok(value)=>value.parse::<i32>().ok().filter(|v|(1..=25).contains(v)).ok_or("invalid SMTP auto retry batch size")?,
        Err(std::env::VarError::NotPresent)=>10,_=>return Err("invalid SMTP auto retry batch size".into())
    };
    let observed=match std::env::var("SWALLOW_SMTP_AUTO_WORKER_HEALTH").as_deref() {
        Ok("true")=>true, Ok("false")|Err(std::env::VarError::NotPresent)=>false,
        _=>return Err("invalid SMTP auto worker health setting".into())
    };
    let batch_sql=if observed {"SELECT public.novu_rust_smtp_auto_retry_batch_observed($1,$2)"} else {"SELECT public.novu_rust_smtp_auto_retry_batch($1,$2)"};
    let (config,tls)=novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    while !stop.requested(){
        if let Ok(Ok((db,driver)))=tokio::time::timeout(Duration::from_secs(10),config.connect(tls.clone())).await {
            let task=tokio::spawn(driver);
            while !stop.requested(){
                match tokio::time::timeout(Duration::from_secs(30),db.query_one(batch_sql,&[&environment,&batch])).await {
                    Ok(Ok(row))=>{let count:i32=row.get(0);if count>0 {println!("SMTP auto retry: queued {count} jobs; duplicate delivery is possible");}},
                    Ok(Err(e)) if e.code().is_some_and(|c|c.code()=="42501")=>{task.abort();return Err("SMTP auto retry permission denied".into());},
                    _=>break,
                }
                stop.wait(Duration::from_secs(interval)).await;
            }
            task.abort();let _=task.await;
        }
        if !stop.requested(){eprintln!("SMTP auto retry unavailable; reconnecting");stop.wait(Duration::from_secs(2)).await;}
    }
    Ok(())
}
