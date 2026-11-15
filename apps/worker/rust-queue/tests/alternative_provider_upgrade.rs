use std::path::Path;
use tokio_postgres::{Client,NoTls};
use uuid::Uuid;
async fn migrate(db:&Client,dir:&Path,start:u32,end:u32) {
 let mut files:Vec<_>=std::fs::read_dir(dir).unwrap().map(|x|x.unwrap().path()).filter(|p|p.extension().is_some_and(|x|x=="sql")).collect();files.sort();
 for p in files {let n=p.file_name().unwrap().to_str().unwrap();let v:u32=n[..4].parse().unwrap();if (start..=end).contains(&v){db.batch_execute(&std::fs::read_to_string(&p).unwrap()).await.unwrap_or_else(|e|panic!("{n}: {e}"));}}
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL"]
async fn upgrade_rescrubs_provider_layers_in_previously_scrubbed_jobs() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
 let root=Path::new(env!("CARGO_MANIFEST_DIR"));migrate(&db,&root.join("../../api/rust-api/migrations"),1,17).await;migrate(&db,&root.join("migrations"),1,239).await;
 let env=format!("alt-upgrade-{}",Uuid::new_v4());db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')",&[&env]).await.unwrap();
 db.execute("INSERT INTO public.novu_rust_attachment_retention_grants SELECT $1,rolname,oid,true,true FROM pg_roles WHERE rolname=current_user",&[&env]).await.unwrap();
 let old=Uuid::new_v4();
 db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,status,payload) VALUES($1,$2,'experimental_static_in_app','completed',$3)",&[&old,&env,&serde_json::json!({"emailAttachments":[{"file":[1]}],"emailOverride":{"alternatives":[{"content":"channel-secret"}]},"emailProviderOverrides":{"smtp":{"alternatives":[{"content":"provider-secret"}],"subject":"keep"}},"emailStepOverrides":{"a":{"providers":{"smtp":{"alternatives":[{"content":"step-secret"}]}}}},"emailFlatProviderOverride":{"alternatives":[{"content":"flat-secret"}]}})]).await.unwrap();
 assert_eq!(db.query("SELECT * FROM public.novu_rust_scrub_attachment_retention($1,0,100,'before')",&[&env]).await.unwrap().len(),1);
 assert_eq!(db.query("SELECT * FROM public.novu_rust_scrub_alternative_retention($1,0,100,'before')",&[&env]).await.unwrap().len(),1);
 let before=db.query_one("SELECT payload::text,alternative_scrubbed_at::text FROM public.novu_queue_jobs WHERE id=$1",&[&old]).await.unwrap();assert!(before.get::<_,String>(0).contains("provider-secret"));
 migrate(&db,&root.join("migrations"),240,240).await;
 assert_eq!(db.query("SELECT * FROM public.novu_rust_list_alternative_retention($1,0,100)",&[&env]).await.unwrap().len(),1);
 assert_eq!(db.query("SELECT * FROM public.novu_rust_scrub_alternative_retention($1,0,100,'after')",&[&env]).await.unwrap().len(),1);
 let after=db.query_one("SELECT payload::text,alternative_scrubbed_at::text,attachment_scrubbed_at IS NOT NULL FROM public.novu_queue_jobs WHERE id=$1",&[&old]).await.unwrap();let payload=after.get::<_,String>(0);assert!(!payload.contains("secret"));assert!(payload.contains("keep"));assert_eq!(before.get::<_,String>(1),after.get::<_,String>(1));assert!(after.get::<_,bool>(2));
 assert!(db.execute("UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",&[&old]).await.is_err());
}
