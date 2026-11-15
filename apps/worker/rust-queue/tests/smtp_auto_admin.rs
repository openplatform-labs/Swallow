//! Permission, revision and audit checks for the SMTP duplicate-risk retry policy.
use serde_json::Value;
use std::process::Command;
use tokio_postgres::{Client,NoTls};
use uuid::Uuid;
async fn connect(url:&str)->Client {let (db,driver)=tokio_postgres::connect(url,NoTls).await.unwrap();tokio::spawn(driver);db}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn scoped_smtp_auto_policy_is_audited_and_idempotent() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;
    let env=format!("smtp-auto-admin-{}",Uuid::new_v4().simple());let other=format!("smtp-auto-other-{}",Uuid::new_v4().simple());let role=format!("smtp_auto_admin_{}",Uuid::new_v4().simple());let reader=format!("smtp_auto_reader_{}",Uuid::new_v4().simple());
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; CREATE ROLE {reader} LOGIN; GRANT USAGE ON SCHEMA public TO {role},{reader}; GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_auto_policy_read(text),public.novu_rust_smtp_auto_policy_set(text,uuid,bigint,boolean,integer,integer,text,text),public.novu_rust_smtp_auto_policy_request(text,uuid),public.novu_rust_smtp_auto_policy_audit(text,bigint),public.novu_rust_smtp_auto_history(text,bigint) TO {role},{reader}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_auto_admins(environment_id,database_principal,principal_oid,can_read,can_write,can_audit) SELECT $1,rolname,oid,true,rolname::text=$2,rolname::text=$2 FROM pg_roles WHERE rolname::text=ANY($3)",&[&env,&role,&vec![role.clone(),reader.clone()]]).await.unwrap();
    let user_url=url.replacen("postgres://postgres@",&format!("postgres://{role}@"),1);let reader_url=url.replacen("postgres://postgres@",&format!("postgres://{reader}@"),1);
    let admin=connect(&user_url).await;let admin2=connect(&user_url).await;let reader=connect(&reader_url).await;
    assert!(admin.query("SELECT * FROM public.novu_rust_smtp_auto_retry_policy",&[]).await.is_err());assert!(admin.query_one("SELECT public.novu_rust_smtp_auto_policy_read($1)",&[&other]).await.is_err());
    let value:Value=reader.query_one("SELECT public.novu_rust_smtp_auto_policy_read($1)",&[&env]).await.unwrap().get(0);assert_eq!(value["enabled"],false);assert!(value["eligibleSince"].is_null());
    let id=Uuid::new_v4();assert!(reader.query_one("SELECT public.novu_rust_smtp_auto_policy_set($1,$2,0,true,1,10,'now','fixture')",&[&env,&id]).await.is_err());
    assert!(admin.query_one("SELECT public.novu_rust_smtp_auto_policy_set($1,$2,0,true,1,10,'preserve','fixture')",&[&env,&id]).await.is_err());
    let sql="SELECT public.novu_rust_smtp_auto_policy_set($1,$2,0,true,2,10,'now','fixture')";let params:[&(dyn tokio_postgres::types::ToSql+Sync);2]=[&env,&id];
    let (a,b)=tokio::join!(admin.query_one(sql,&params),admin2.query_one(sql,&params));let a:Value=a.unwrap().get(0);let b:Value=b.unwrap().get(0);
    assert_ne!(a["replayed"],b["replayed"]);assert_eq!(a["policy"],b["policy"]);assert_eq!(a["policy"]["revision"],"1");assert!(a["policy"]["eligibleSince"].is_string());
    assert!(admin.query_one("SELECT public.novu_rust_smtp_auto_policy_set($1,$2,0,true,3,10,'now','fixture')",&[&env,&id]).await.is_err());
    assert!(admin.query_one("SELECT public.novu_rust_smtp_auto_policy_set($1,$2,0,false,1,10,'all','stale')",&[&env,&Uuid::new_v4()]).await.is_err());
    let found:Value=admin.query_one("SELECT public.novu_rust_smtp_auto_policy_request($1,$2)",&[&env,&id]).await.unwrap().get(0);assert_eq!(found["cutoff"],"now");
    assert!(reader.query_one("SELECT public.novu_rust_smtp_auto_history($1,NULL)",&[&env]).await.is_err());
    db.batch_execute("CREATE FUNCTION public.fixture_auto_policy_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fixture_auto_policy_audit_fail BEFORE INSERT ON public.novu_rust_smtp_auto_policy_audit FOR EACH ROW EXECUTE FUNCTION public.fixture_auto_policy_audit_fail()").await.unwrap();
    let rolled=Uuid::new_v4();assert!(admin.query_one("SELECT public.novu_rust_smtp_auto_policy_set($1,$2,1,false,1,10,'preserve','rollback')",&[&env,&rolled]).await.is_err());
    db.batch_execute("DROP TRIGGER fixture_auto_policy_audit_fail ON public.novu_rust_smtp_auto_policy_audit; DROP FUNCTION public.fixture_auto_policy_audit_fail()").await.unwrap();
    let value:Value=admin.query_one("SELECT public.novu_rust_smtp_auto_policy_read($1)",&[&env]).await.unwrap().get(0);assert_eq!(value["revision"],"1");
    let cli=Command::new(env!("CARGO_BIN_EXE_delivery-reconcile")).args(["smtp-auto-read",&env]).env("DATABASE_URL",&user_url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").output().unwrap();assert!(cli.status.success());assert_eq!(serde_json::from_slice::<Value>(&cli.stdout).unwrap()["enabled"],true);
    let cli_id=Uuid::new_v4().to_string();let cli=Command::new(env!("CARGO_BIN_EXE_delivery-reconcile")).args(["smtp-auto-set",&env,&cli_id,"1","disabled","1","60","all","reviewed"]).env("DATABASE_URL",&user_url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").output().unwrap();assert!(cli.status.success());assert_eq!(serde_json::from_slice::<Value>(&cli.stdout).unwrap()["policy"]["eligibleSince"],"-infinity");
    let value:Value=admin.query_one("SELECT public.novu_rust_smtp_auto_policy_audit($1,NULL)",&[&env]).await.unwrap().get(0);assert_eq!(value["data"].as_array().unwrap().len(),2);
    let value:Value=admin.query_one("SELECT public.novu_rust_smtp_auto_history($1,NULL)",&[&env]).await.unwrap().get(0);assert!(value["data"].as_array().unwrap().is_empty());
    db.execute("UPDATE public.novu_rust_smtp_auto_admins SET can_write=false WHERE environment_id=$1 AND database_principal=$2::name",&[&env,&role]).await.unwrap();assert!(admin.query_one("SELECT public.novu_rust_smtp_auto_policy_request($1,$2)",&[&env,&id]).await.is_err());
}
