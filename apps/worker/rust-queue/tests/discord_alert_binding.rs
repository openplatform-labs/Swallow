//! Tests an optional target binding using restricted DB logins.
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore="requires a fully migrated disposable PostgreSQL and role creation"]
async fn discord_alert_binding_enforces_scope_and_concurrent_first_writer() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
 let env=format!("binding-{}",Uuid::new_v4().simple());let other=format!("binding-{}",Uuid::new_v4().simple());let role=format!("binding_{}",Uuid::new_v4().simple());
 db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_bind_discord_alert_mode(text,text,text,text,integer) TO {role}")).await.unwrap();
 let mut role_url=reqwest::Url::parse(&url).unwrap();role_url.set_username(&role).unwrap();role_url.set_password(Some("fixture")).unwrap();let (a,driver)=tokio_postgres::connect(role_url.as_str(),NoTls).await.unwrap();tokio::spawn(driver);let (b,driver)=tokio_postgres::connect(role_url.as_str(),NoTls).await.unwrap();tokio::spawn(driver);
 assert!(a.query("SELECT * FROM public.novu_rust_discord_alert_mode_bindings",&[]).await.is_err());
 assert!(a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops','all',60)",&[&env]).await.is_err());
 db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
 assert!(a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops','all',60)",&[&other]).await.is_err());
 assert!(a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops','all',NULL)",&[&env]).await.is_err());
 let (left,right)=tokio::join!(async {a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops','all',60)",&[&env]).await},async {b.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops','overdue',60)",&[&env]).await});assert_ne!(left.unwrap().get::<_,bool>(0),right.unwrap().get::<_,bool>(0));
 let row=db.query_one("SELECT mode,grace_seconds FROM public.novu_rust_discord_alert_mode_bindings WHERE environment_id=$1 AND target_id='ops'",&[&env]).await.unwrap();let mode:String=row.get(0);assert_eq!(row.get::<_,i32>(1),60);
 assert!(a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops',$2,60)",&[&env,&mode]).await.unwrap().get::<_,bool>(0));assert!(!a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops',$2,120)",&[&env,&mode]).await.unwrap().get::<_,bool>(0));assert!(!a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops','none',NULL)",&[&env]).await.unwrap().get::<_,bool>(0));
 assert!(a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'email','ops','none',NULL)",&[&env]).await.unwrap().get::<_,bool>(0));assert!(!a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'email','ops','all',60)",&[&env]).await.unwrap().get::<_,bool>(0));
 db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2",&[&env,&role]).await.unwrap();assert!(a.query_one("SELECT public.novu_rust_bind_discord_alert_mode($1,'slack','ops',$2,60)",&[&env,&mode]).await.is_err());
}
