//! Experimental environment-scoped worker for explicit cancellation commands.
use novu_postgres_queue::resend::{CancelOutcome,Config,Resend};
use std::{error::Error,time::Duration};
use uuid::Uuid;
#[tokio::main]
async fn main()->Result<(),Box<dyn Error+Send+Sync>>{novu_postgres_queue::shutdown::run_until_signal(run).await}
async fn run(stop:novu_postgres_queue::shutdown::Stop)->Result<(),Box<dyn Error+Send+Sync>>{
    let environment=std::env::var("NOVU_RESEND_ENVIRONMENT_ID")?;
    if environment.trim().is_empty(){return Err("environment required".into());}
    // Reload only between commands, before claiming an irreversible provider call.
    let reload_path=match std::env::var("NOVU_RESEND_RELOAD_API_KEY").as_deref(){
        Ok("true")=>Some(std::path::PathBuf::from(std::env::var_os("NOVU_RESEND_API_KEY_FILE").ok_or("Resend key reload requires a file")?)),
        Ok("false")|Err(std::env::VarError::NotPresent)=>None,
        _=>return Err("invalid Resend key reload setting".into()),
    };
    let exit_on_reload_failure=match std::env::var("SWALLOW_RESEND_CANCEL_RELOAD_FAILURE_POLICY").as_deref(){
        Ok("pause")|Err(std::env::VarError::NotPresent)=>false,
        Ok("exit") if reload_path.is_some()=>true,
        _=>return Err("invalid cancellation key reload failure policy".into()),
    };
    let api_key=if let Some(path)=&reload_path {
        if std::env::var_os("NOVU_RESEND_API_KEY").is_some(){return Err("Resend key sources conflict".into());}
        novu_rust_contracts::secret::read_regular_file(path)?
    } else {novu_rust_contracts::secret::from_env("NOVU_RESEND_API_KEY","NOVU_RESEND_API_KEY_FILE")?.ok_or("Resend API key required")?};
    let cfg=Config{api_key,from:String::new(),sender_name:String::new()};
    #[cfg(feature="test-mocks")]
    let mut provider=match std::env::var("SWALLOW_TEST_RESEND_ENDPOINT") {
        Ok(endpoint)=>{let u=reqwest::Url::parse(&endpoint)?;if u.scheme()!="http"||u.host_str()!=Some("127.0.0.1")||u.port().is_none()||!u.username().is_empty()||u.password().is_some()||u.path()!="/emails"||u.query().is_some()||u.fragment().is_some(){return Err("invalid fixture endpoint".into());}Resend::test_endpoint(cfg,&endpoint)?},
        Err(std::env::VarError::NotPresent)=>Resend::new(cfg)?,Err(_)=>return Err("invalid fixture endpoint".into()),
    };
    #[cfg(not(feature="test-mocks"))]
    let mut provider=Resend::new(cfg)?;
    let require_tag=match std::env::var("SWALLOW_RESEND_CANCEL_REQUIRE_ACCOUNT_TAG").as_deref(){Ok("true")=>true,Ok("false")|Err(std::env::VarError::NotPresent)=>false,_=>return Err("invalid account tag guard".into())};
    let tag=if require_tag {let v=std::env::var("SWALLOW_RESEND_ACCOUNT_TAG")?;if v.is_empty()||v.len()>128||!v.bytes().all(|b|b.is_ascii_alphanumeric()||matches!(b,b'.'|b'_'|b'-')){return Err("invalid Resend account tag".into());}Some(v)}else{None};
    let (config,tls)=novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout=Duration::from_secs(30);
    while !stop.requested(){
        if let Ok(Ok((db,driver)))=tokio::time::timeout(timeout,config.connect(tls.clone())).await {
            let task=tokio::spawn(driver);
            while !stop.requested(){
                if reload_path.as_ref().is_some_and(|path|provider.reload_api_key_file(path).is_err()) {
                    eprintln!("Resend cancellation key reload failed; claims paused");
                    if exit_on_reload_failure {task.abort();let _=task.await;return Err("Resend cancellation key reload failed".into());}
                    stop.wait(Duration::from_secs(2)).await;
                    continue;
                }
                let result=if let Some(tag)=&tag {tokio::time::timeout(timeout,db.query_one("SELECT public.novu_rust_resend_cancel_claim_tagged($1,$2)",&[&environment,tag])).await}else{tokio::time::timeout(timeout,db.query_one("SELECT public.novu_rust_resend_cancel_claim($1)",&[&environment])).await};
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
