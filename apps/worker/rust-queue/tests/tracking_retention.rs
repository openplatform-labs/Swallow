use serde_json::{Value,json};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and role creation"]
async fn tracking_cleanup_is_audited_scoped_and_composes_in_any_order() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
 let role=format!("track_ret_{}",Uuid::new_v4().simple());
 db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_list_tracking_retention(text,integer,integer),public.novu_rust_scrub_tracking_retention(text,integer,integer,text),public.novu_rust_tracking_retention_audit(text,bigint),public.novu_rust_scrub_attachment_retention(text,integer,integer,text),public.novu_rust_scrub_alternative_retention(text,integer,integer,text) TO {role}")).await.unwrap();
 let mut login=reqwest::Url::parse(&url).unwrap();login.set_username(&role).unwrap();let (client,driver)=tokio_postgres::connect(login.as_str(),NoTls).await.unwrap();tokio::spawn(driver);
 let orders=[["tracking","attachment","alternative"],["tracking","alternative","attachment"],["attachment","tracking","alternative"],["attachment","alternative","tracking"],["alternative","tracking","attachment"],["alternative","attachment","tracking"]];
 for (index,order) in orders.iter().enumerate() {
  let env=format!("track-{index}-{}",Uuid::new_v4());db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')",&[&env]).await.unwrap();db.execute("INSERT INTO public.novu_rust_attachment_retention_grants SELECT $1,rolname,oid,true,true FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
  let parent=Uuid::new_v4();let child=Uuid::new_v4();let pending=Uuid::new_v4();let standalone=Uuid::new_v4();
  let p=json!({"emailAttachments":[{"file":[1]}],"emailOverride":{"alternatives":[{"content":"ALT_PRIVATE"}]},"emailProviderOverrides":{"sendgrid":{"trackingSettings":{"subscriptionTracking":{"text":"TRACK_PRIVATE"}},"_passthrough":{"body":{"trackingSettings":{"openTracking":{"substitutionTag":"TRACK_PRIVATE"}}}}}},"emailStepOverrides":{"step":{"providers":{"sendgrid":{"trackingSettings":{"subscriptionTracking":{"html":"TRACK_PRIVATE"}},"_passthrough":{"body":{"trackingSettings":{"ganalytics":{"utmSource":"TRACK_PRIVATE"}}}}}}}},"payload":{"userContent":"KEEP_USER_CONTENT"}});
  let c=json!({"attachments":[{"file":[2]}],"alternatives":[{"content":"ALT_PRIVATE"}],"tracking_settings":{"subscriptionTracking":{"text":"TRACK_PRIVATE"}},"workflow_context":{"overrides":{"alternatives":[{"content":"ALT_PRIVATE"}],"trackingSettings":{"subscriptionTracking":{"text":"TRACK_PRIVATE"}},"_passthrough":{"body":{"trackingSettings":{"openTracking":{"substitutionTag":"TRACK_PRIVATE"}}}}}}});
  db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,status,payload) VALUES($1,$5,'experimental_static_in_app_attachments_v1','completed',$6),($2,$5,'experimental_sendgrid_email','completed',$7),($3,$5,'experimental_sendgrid_email','pending',$7),($4,$5,'experimental_sendgrid_email','completed',$8)",&[&parent,&child,&pending,&standalone,&env,&p,&c,&json!({"tracking_settings":{"subscriptionTracking":{"text":"TRACK_PRIVATE"}}})]).await.unwrap();
  if index==0 {
   db.batch_execute("CREATE FUNCTION public.test_track_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit'; END $$; CREATE TRIGGER test_track_audit_fail BEFORE INSERT ON public.novu_rust_tracking_retention_audit FOR EACH ROW EXECUTE FUNCTION public.test_track_audit_fail()").await.unwrap();
   assert!(client.query("SELECT * FROM public.novu_rust_scrub_tracking_retention($1,0,100,'test')",&[&env]).await.is_err());
   assert_eq!(client.query("SELECT * FROM public.novu_rust_list_tracking_retention($1,0,100)",&[&env]).await.unwrap().len(),2);
   db.batch_execute("DROP TRIGGER test_track_audit_fail ON public.novu_rust_tracking_retention_audit; DROP FUNCTION public.test_track_audit_fail()").await.unwrap();
  }
  for kind in order {
   if index==0 && *kind=="tracking" {
    let output=std::process::Command::new(env!("CARGO_BIN_EXE_attachment-retention")).env("DATABASE_URL",login.as_str()).env("NOVU_POSTGRES_ALLOW_INSECURE","true").args(["scrub-tracking",&env,"0","100","routine"]).output().unwrap();assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));assert_eq!(serde_json::from_slice::<Value>(&output.stdout).unwrap().as_array().unwrap().len(),2);
   } else if index==1 && *kind=="tracking" {
    let (other,driver)=tokio_postgres::connect(login.as_str(),NoTls).await.unwrap();tokio::spawn(driver);
    let sql="SELECT * FROM public.novu_rust_scrub_tracking_retention($1,0,100,'concurrent')";
    let args:&[&(dyn tokio_postgres::types::ToSql+Sync)]=&[&env];
    let (a,b)=tokio::join!(client.query(sql,args),other.query(sql,args));let rows:Vec<_>=a.unwrap().into_iter().chain(b.unwrap()).map(|r|r.get::<_,Uuid>(0)).collect();
    assert_eq!(rows.len(),2);assert_ne!(rows[0],rows[1]);
   } else {
    let sql=match *kind {"tracking"=>"SELECT * FROM public.novu_rust_scrub_tracking_retention($1,0,100,'routine')","attachment"=>"SELECT * FROM public.novu_rust_scrub_attachment_retention($1,0,100,'routine')",_=>"SELECT * FROM public.novu_rust_scrub_alternative_retention($1,0,100,'routine')"};assert_eq!(client.query(sql,&[&env]).await.unwrap().len(),2,"{kind} order {order:?}");
   }
  }
  assert_eq!(client.query("SELECT * FROM public.novu_rust_tracking_retention_audit($1,0)",&[&env]).await.unwrap().len(),2);
  assert_eq!(client.query("SELECT * FROM public.novu_rust_scrub_tracking_retention($1,0,100,'again')",&[&env]).await.unwrap().len(),0);
  for id in [parent,child] {let row=db.query_one("SELECT payload,tracking_scrubbed_at IS NOT NULL,attachment_scrubbed_at IS NOT NULL,alternative_scrubbed_at IS NOT NULL FROM public.novu_queue_jobs WHERE id=$1",&[&id]).await.unwrap();let body:Value=row.get(0);assert!(!body.to_string().contains("TRACK_PRIVATE"));assert!(!body.to_string().contains("ALT_PRIVATE"));assert!((1..=3).all(|i|row.get::<_,bool>(i)));assert!(db.execute("UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",&[&id]).await.is_err());assert!(db.execute("UPDATE public.novu_queue_jobs SET payload=payload||'{\"tracking_settings\":{}}'::jsonb WHERE id=$1",&[&id]).await.is_err());}
  assert!(db.query_one("SELECT payload::text FROM public.novu_queue_jobs WHERE id=$1",&[&parent]).await.unwrap().get::<_,String>(0).contains("KEEP_USER_CONTENT"));
  for id in [pending,standalone] {assert!(db.query_one("SELECT payload::text FROM public.novu_queue_jobs WHERE id=$1",&[&id]).await.unwrap().get::<_,String>(0).contains("TRACK_PRIVATE"));}
  if index==5 {
   let output=std::process::Command::new(env!("CARGO_BIN_EXE_attachment-retention")).env("DATABASE_URL",login.as_str()).env("NOVU_POSTGRES_ALLOW_INSECURE","true").args(["audit-tracking",&env]).output().unwrap();assert!(output.status.success());assert_eq!(serde_json::from_slice::<Value>(&output.stdout).unwrap().as_array().unwrap().len(),2);
   db.execute("DELETE FROM public.novu_rust_attachment_retention_grants WHERE environment_id=$1",&[&env]).await.unwrap();assert!(client.query("SELECT * FROM public.novu_rust_list_tracking_retention($1,0,100)",&[&env]).await.is_err());
  }
 }
 assert!(client.query("SELECT * FROM public.novu_rust_list_tracking_retention('another-env',0,100)",&[]).await.is_err());
}
