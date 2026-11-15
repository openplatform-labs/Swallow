//! Experimental cancellation operator; one provider request per reserved UUID.
use novu_postgres_queue::resend::{CancelOutcome, Config, Resend};
use std::error::Error;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(),Box<dyn Error+Send+Sync>> {
    let args:Vec<String>=std::env::args().skip(1).collect();
    if !(args.len()==3 && (args[0]=="get" || args[0]=="resolution-get") || (args.len()==3 || args.len()==4) && args[0]=="list" || args.len()==5 && args[0]=="cancel" || args.len()==6 && args[0]=="resolve") {
        return Err("usage: resend-cancel list ENV GRACE_SECONDS [AFTER_REQUEST_UUID] | get ENV REQUEST_UUID | resolution-get ENV RESOLUTION_UUID | cancel ENV REQUEST_UUID JOB_UUID REASON | resolve ENV RESOLUTION_UUID REQUEST_UUID confirmed|not_cancelled REASON".into());
    }
    let request=if args[0]=="list" {None} else {Some(Uuid::parse_str(&args[2])?)};
    // Validate the provider configuration before committing an unknown marker.
    let provider=if args[0]=="cancel" {
        let api_key=novu_rust_contracts::secret::from_env("NOVU_RESEND_API_KEY","NOVU_RESEND_API_KEY_FILE")?.ok_or("Resend API key is required")?;
        let config=Config{api_key,from:String::new(),sender_name:String::new()};
        #[cfg(feature="test-mocks")]
        let result=match std::env::var("SWALLOW_TEST_RESEND_ENDPOINT") {
            Ok(endpoint)=>{
                let url=reqwest::Url::parse(&endpoint)?;
                if url.scheme()!="http" || url.host_str()!=Some("127.0.0.1") || url.port().is_none() || !url.username().is_empty() || url.password().is_some() || url.path()!="/emails" || url.query().is_some() || url.fragment().is_some() {return Err("invalid fixture endpoint".into());}
                Resend::test_endpoint(config,&endpoint)
            },
            Err(std::env::VarError::NotPresent)=>Resend::new(config),
            Err(_)=>return Err("invalid fixture endpoint".into()),
        };
        #[cfg(not(feature="test-mocks"))]
        let result=Resend::new(config);
        Some(result?)
    } else {None};
    let (config,tls)=novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (db,connection)=config.connect(tls).await?;
    tokio::spawn(async move {if connection.await.is_err(){eprintln!("database connection failed");}});
    if args[0]=="list" {
        let grace=args[2].parse::<i32>()?;
        let after=args.get(3).map(|value|Uuid::parse_str(value)).transpose()?;
        let rows=db.query("SELECT * FROM public.novu_rust_resend_cancel_list($1,$2,$3)",&[&args[1],&grace,&after]).await?;
        let data:Vec<_>=rows.iter().take(50).map(|row|serde_json::json!({"requestId":row.get::<_,Uuid>(0),"jobId":row.get::<_,Uuid>(1),"createdAt":row.get::<_,String>(2),"overdue":row.get::<_,bool>(3)})).collect();
        let next=if rows.len()>50 {data.last().and_then(|row|row["requestId"].as_str())} else {None};
        println!("{}",serde_json::json!({"data":data,"next":next}));return Ok(());
    }
    let request=request.expect("validated request ID");
    if args[0]=="get" || args[0]=="resolution-get" {
        let sql=if args[0]=="get" {"SELECT public.novu_rust_resend_cancel_get($1,$2)"} else {"SELECT public.novu_rust_resend_cancel_resolution_get($1,$2)"};
        let row=db.query_one(sql,&[&args[1],&request]).await?;
        let result:Option<serde_json::Value>=row.get(0);println!("{}",serde_json::json!({"request":result}));return Ok(());
    }
    if args[0]=="resolve" {
        let target=Uuid::parse_str(&args[3])?;
        let row=db.query_one("SELECT public.novu_rust_resend_cancel_resolve($1,$2,$3,$4,$5)",&[&args[1],&request,&target,&args[4],&args[5]]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));return Ok(());
    }
    let job=Uuid::parse_str(&args[3])?;
    let row=db.query_one("SELECT public.novu_rust_resend_cancel_reserve($1,$2,$3,$4)",&[&args[1],&request,&job,&args[4]]).await?;
    let reserved:serde_json::Value=row.get(0);
    if reserved["fresh"]!=true {println!("{reserved}");if reserved["state"]!="confirmed" {std::process::exit(2);}return Ok(());}
    let id=reserved["providerId"].as_str().ok_or("missing provider ID")?;
    let outcome=provider.unwrap().cancel_scheduled(id).await?;
    let (state,status)=match outcome {CancelOutcome::Confirmed=>("confirmed",None),CancelOutcome::Rejected(status)=>("rejected",Some(i32::from(status))),CancelOutcome::Unknown=>("unknown",None)};
    let row=db.query_one("SELECT public.novu_rust_resend_cancel_finish($1,$2,$3,$4)",&[&args[1],&request,&state,&status]).await?;
    println!("{}",row.get::<_,serde_json::Value>(0));
    if state!="confirmed" {std::process::exit(2);}
    Ok(())
}
