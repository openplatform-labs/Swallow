use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires disposable migrated PostgreSQL and role creation"]
async fn alternative_retention_is_independent_and_preserves_replay_fences() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
 let env=format!("alt-ret-{}",Uuid::new_v4());let other=format!("alt-other-{}",Uuid::new_v4());let role=format!("alt_ret_{}",Uuid::new_v4().simple());
 db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
 db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_list_alternative_retention(text,integer,integer),public.novu_rust_scrub_alternative_retention(text,integer,integer,text),public.novu_rust_alternative_retention_audit(text,bigint),public.novu_rust_scrub_attachment_retention(text,integer,integer,text) TO {role}")).await.unwrap();
 db.execute("INSERT INTO public.novu_rust_attachment_retention_grants SELECT $1,rolname,oid,true,true FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
 let mut login=reqwest::Url::parse(&url).unwrap();login.set_username(&role).unwrap();let (client,driver)=tokio_postgres::connect(login.as_str(),NoTls).await.unwrap();tokio::spawn(driver);
 let parent=Uuid::new_v4();let child=Uuid::new_v4();let pending=Uuid::new_v4();let foreign=Uuid::new_v4();let standalone=Uuid::new_v4();
 let parent_payload=json!({"emailAttachments":[{"file":[1]}],"emailOverride":{"alternatives":[{"contentType":"text/calendar","content":"PRIVATE"}],"subject":"safe"},"emailProviderOverrides":{"smtp":{"alternatives":[{"content":"PRIVATE"}]},"sendgrid":{"alternatives":[{"content":"PRIVATE"}]}},"emailStepOverrides":{"step":{"providers":{"smtp":{"alternatives":[{"content":"PRIVATE"}]}}}},"emailFlatProviderOverride":{"alternatives":[{"content":"PRIVATE"}]}});
 let child_payload=json!({"attachments":[{"file":[2]}],"alternatives":[{"content_type":"text/calendar","content":"PRIVATE"}],"workflow_context":{"overrides":{"alternatives":[{"contentType":"text/calendar","content":"PRIVATE"}],"subject":"safe"}}});
 db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,status,payload) VALUES($1,$6,'experimental_static_in_app_attachments_v1','completed',$7),($2,$6,'experimental_smtp_email','completed',$8),($3,$6,'experimental_smtp_email','pending',$8),($4,$9,'experimental_smtp_email','completed',$8),($5,$6,'experimental_smtp_email','completed',$10)",&[&parent,&child,&pending,&foreign,&standalone,&env,&parent_payload,&child_payload,&other,&json!({"alternatives":[{"content":"PRIVATE"}]})]).await.unwrap();
 assert!(client.query("SELECT * FROM public.novu_rust_list_alternative_retention($1,0,100)",&[&other]).await.is_err());
 assert_eq!(client.query("SELECT * FROM public.novu_rust_list_alternative_retention($1,0,100)",&[&env]).await.unwrap().len(),2);
 // Attachment-first on the parent; preserve alternatives for the separate action.
 let attach=client.query("SELECT * FROM public.novu_rust_scrub_attachment_retention($1,0,1,'attachment')",&[&env]).await.unwrap();
 let attachment_first:Uuid=attach[0].get(0);
 let remaining=if attachment_first==parent { child } else { parent };
 db.batch_execute("CREATE FUNCTION public.test_alt_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit'; END $$; CREATE TRIGGER test_alt_audit_fail BEFORE INSERT ON public.novu_rust_alternative_retention_audit FOR EACH ROW EXECUTE FUNCTION public.test_alt_audit_fail()").await.unwrap();
 assert!(client.query("SELECT * FROM public.novu_rust_scrub_alternative_retention($1,0,100,'test')",&[&env]).await.is_err());
 assert_eq!(client.query("SELECT * FROM public.novu_rust_list_alternative_retention($1,0,100)",&[&env]).await.unwrap().len(),2);
 db.batch_execute("DROP TRIGGER test_alt_audit_fail ON public.novu_rust_alternative_retention_audit; DROP FUNCTION public.test_alt_audit_fail()").await.unwrap();
 let output=std::process::Command::new(env!("CARGO_BIN_EXE_attachment-retention")).env("DATABASE_URL",login.as_str()).env("NOVU_POSTGRES_ALLOW_INSECURE","true").args(["scrub-alternatives",&env,"0","100","routine"]).output().unwrap();assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
 assert_eq!(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap().as_array().unwrap().len(),2);
 assert_eq!(client.query("SELECT * FROM public.novu_rust_alternative_retention_audit($1,0)",&[&env]).await.unwrap().len(),2);
 // Alternative-first on the other row still permits later attachment cleanup.
 assert_eq!(client.query("SELECT * FROM public.novu_rust_scrub_attachment_retention($1,0,100,'attachment')",&[&env]).await.unwrap().len(),1);
 for id in [parent,child] { let row=db.query_one("SELECT payload::text,attachment_scrubbed_at IS NOT NULL,alternative_scrubbed_at IS NOT NULL FROM public.novu_queue_jobs WHERE id=$1",&[&id]).await.unwrap();assert!(!row.get::<_,String>(0).contains("PRIVATE"));assert!(row.get::<_,bool>(1)&&row.get::<_,bool>(2));assert!(db.execute("UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",&[&id]).await.is_err());assert!(db.execute("UPDATE public.novu_queue_jobs SET payload=payload||'{\"alternatives\":[1]}'::jsonb WHERE id=$1",&[&id]).await.is_err()); }
 assert_ne!(attachment_first,remaining);
 for id in [pending,foreign,standalone] {assert!(db.query_one("SELECT payload::text FROM public.novu_queue_jobs WHERE id=$1",&[&id]).await.unwrap().get::<_,String>(0).contains("PRIVATE"));}
 db.execute("DELETE FROM public.novu_rust_attachment_retention_grants WHERE environment_id=$1",&[&env]).await.unwrap();
 assert!(client.query("SELECT * FROM public.novu_rust_list_alternative_retention($1,0,100)",&[&env]).await.is_err());
}
