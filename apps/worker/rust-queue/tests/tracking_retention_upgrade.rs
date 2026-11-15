use std::path::Path;
use tokio_postgres::{Client,NoTls};
use uuid::Uuid;
async fn migrate(db:&Client,dir:&Path,start:u32,end:u32){let mut files:Vec<_>=std::fs::read_dir(dir).unwrap().map(|v|v.unwrap().path()).filter(|p|p.extension().is_some_and(|v|v=="sql")).collect();files.sort();for file in files{let name=file.file_name().unwrap().to_str().unwrap();let number:u32=name[..4].parse().unwrap();if (start..=end).contains(&number){db.batch_execute(&std::fs::read_to_string(&file).unwrap()).await.unwrap_or_else(|e|panic!("{name}: {e}"));}}}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL"]
async fn upgrade_backfills_tracking_for_previously_scrubbed_jobs() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);let root=Path::new(env!("CARGO_MANIFEST_DIR"));migrate(&db,&root.join("../../api/rust-api/migrations"),1,17).await;migrate(&db,&root.join("migrations"),1,241).await;
 let env=format!("tracking-upgrade-{}",Uuid::new_v4());db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')",&[&env]).await.unwrap();db.execute("INSERT INTO public.novu_rust_attachment_retention_grants SELECT $1,rolname,oid,true,true FROM pg_roles WHERE rolname=current_user",&[&env]).await.unwrap();
 let done=Uuid::new_v4();let pending=Uuid::new_v4();let body=serde_json::json!({"attachments":[{"file":[1]}],"alternatives":[{"content":"ALT"}],"tracking_settings":{"subscriptionTracking":{"text":"PRIVATE"}},"workflow_context":{"overrides":{"trackingSettings":{"subscriptionTracking":{"text":"PRIVATE"}}}}});
 db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,status,payload) VALUES($1,$3,'experimental_sendgrid_email','completed',$4),($2,$3,'experimental_sendgrid_email','pending',$4)",&[&done,&pending,&env,&body]).await.unwrap();
 assert_eq!(db.query("SELECT * FROM public.novu_rust_scrub_attachment_retention($1,0,100,'before')",&[&env]).await.unwrap().len(),1);assert_eq!(db.query("SELECT * FROM public.novu_rust_scrub_alternative_retention($1,0,100,'before')",&[&env]).await.unwrap().len(),1);
 migrate(&db,&root.join("migrations"),242,242).await;
 assert_eq!(db.query("SELECT * FROM public.novu_rust_list_tracking_retention($1,0,100)",&[&env]).await.unwrap().len(),1);assert_eq!(db.query("SELECT * FROM public.novu_rust_scrub_tracking_retention($1,0,100,'after')",&[&env]).await.unwrap().len(),1);
 assert!(db.query_one("SELECT attachment_scrubbed_at IS NOT NULL AND alternative_scrubbed_at IS NOT NULL AND tracking_scrubbed_at IS NOT NULL AND NOT payload::text LIKE '%PRIVATE%' FROM public.novu_queue_jobs WHERE id=$1",&[&done]).await.unwrap().get::<_,bool>(0));assert!(db.query_one("SELECT tracking_completed_at IS NULL AND payload::text LIKE '%PRIVATE%' FROM public.novu_queue_jobs WHERE id=$1",&[&pending]).await.unwrap().get::<_,bool>(0));
}
