//! Experimental environment-scoped worker for explicit cancellation commands.
use novu_postgres_queue::resend::{CancelOutcome,Config,Resend};
use std::{error::Error,time::Duration};
use uuid::Uuid;
#[tokio::main]
async fn main()->Result<(),Box<dyn Error+Send+Sync>>{novu_postgres_queue::shutdown::run_until_signal(run).await}
async fn run(stop:novu_postgres_queue::shutdown::Stop)->Result<(),Box<dyn Error+Send+Sync>>{
    let environment=std::env::var("NOVU_RESEND_ENVIRONMENT_ID")?;
    if environment.trim().is_empty(){return Err("environment required".into());}
    let api_key=novu_rust_contracts::secret::from_env("NOVU_RESEND_API_KEY","NOVU_RESEND_API_KEY_FILE")?.ok_or("Resend API key required")?;
    let cfg=Config{api_key,from:String::new(),sender_name:String::new()};
    #[cfg(feature="test-mocks")]
    let provider=match std::env::var("SWALLOW_TEST_RESEND_ENDPOINT") {
        Ok(endpoint)=>{let u=reqwest::Url::parse(&endpoint)?;if u.scheme()!="http"||u.host_str()!=Some("127.0.0.1")||u.port().is_none()||!u.username().is_empty()||u.password().is_some()||u.path()!="/emails"||u.query().is_some()||u.fragment().is_some(){return Err("invalid fixture endpoint".into());}Resend::test_endpoint(cfg,&endpoint)?},
        Err(std::env::VarError::NotPresent)=>Resend::new(cfg)?,Err(_)=>return Err("invalid fixture endpoint".into()),
    };
    #[cfg(not(feature="test-mocks"))]
    let provider=Resend::new(cfg)?;
    let (config,tls)=novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout=Duration::from_secs(30);
    while !stop.requested(){
        if let Ok(Ok((db,driver)))=tokio::time::timeout(timeout,config.connect(tls.clone())).await {
            let task=tokio::spawn(driver);
            while !stop.requested(){
                let result=tokio::time::timeout(timeout,db.query_one("SELECT public.novu_rust_resend_cancel_claim($1)",&[&environment])).await;
                let row=match result {Ok(Ok(row))=>row,_=>{eprintln!("cancellation claim failed; reconnecting");break;}};
                let command:Option<serde_json::Value>=row.get(0);
                let Some(command)=command else {stop.wait(Duration::from_secs(1)).await;continue;};
                if command["state"]=="failed" {eprintln!("cancellation command failed eligibility");continue;}
                let Some(id)=command["providerId"].as_str() else {eprintln!("invalid cancellation claim");break;};
                let Some(request)=command["requestId"].as_str().and_then(|s|Uuid::parse_str(s).ok()) else {eprintln!("invalid cancellation claim");break;};
                // Claim committed the unknown marker. Never repeat the provider call.
                let outcome=provider.cancel_scheduled(id).await;
                let (state,status)=match outcome {Ok(CancelOutcome::Confirmed)=>("confirmed",None),Ok(CancelOutcome::Rejected(code))=>("rejected",Some(i32::from(code))),_=>("unknown",None)};
                if !matches!(tokio::time::timeout(timeout,db.query_one("SELECT public.novu_rust_resend_cancel_finish($1,$2,$3,$4)",&[&environment,&request,&state,&status])).await,Ok(Ok(_))){eprintln!("cancellation result not recorded; investigate unknown request");break;}
            }
            task.abort();let _=task.await;
        }
        if !stop.requested(){stop.wait(Duration::from_secs(2)).await;}
    }
    Ok(())
}
