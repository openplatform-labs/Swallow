//! Scoped transport-policy administration on disposable PostgreSQL.
use serde_json::Value;
use std::process::Command;
use tokio_postgres::{Client,NoTls};
use uuid::Uuid;
async fn connect(url:&str)->Client {let (db,driver)=tokio_postgres::connect(url,NoTls).await.unwrap();tokio::spawn(driver);db}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn scoped_policy_admin_is_idempotent_revisioned_and_audited() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;
    let env=format!("smtp-admin-{}",Uuid::new_v4().simple());let other=format!("smtp-admin-other-{}",Uuid::new_v4().simple());
    let role=format!("smtp_admin_{}",Uuid::new_v4().simple());let reader=format!("smtp_reader_{}",Uuid::new_v4().simple());
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; CREATE ROLE {reader} LOGIN; GRANT USAGE ON SCHEMA public TO {role},{reader}; GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_transport_policy_read(text),public.novu_rust_smtp_transport_policy_set(text,uuid,bigint,text,text),public.novu_rust_smtp_transport_policy_request(text,uuid),public.novu_rust_smtp_transport_policy_audit(text,bigint) TO {role},{reader}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_transport_admins(environment_id,database_principal,principal_oid,can_read,can_write,can_audit) SELECT $1,rolname,oid,true,rolname::text=$2,rolname::text=$2 FROM pg_roles WHERE rolname::text=ANY($3)",&[&env,&role,&vec![role.clone(),reader.clone()]]).await.unwrap();
    let user_url=url.replacen("postgres://postgres@",&format!("postgres://{role}@"),1);let reader_url=url.replacen("postgres://postgres@",&format!("postgres://{reader}@"),1);
    let admin=connect(&user_url).await;let reader=connect(&reader_url).await;let admin2=connect(&user_url).await;
    assert!(admin.query("SELECT * FROM public.novu_rust_smtp_transport_policy",&[]).await.is_err());
    assert!(admin.query_one("SELECT public.novu_rust_smtp_transport_policy_read($1)",&[&other]).await.is_err());
    let current:Value=reader.query_one("SELECT public.novu_rust_smtp_transport_policy_read($1)",&[&env]).await.unwrap().get(0);assert_eq!(current["mode"],"disabled");assert_eq!(current["revision"],"0");
    let request=Uuid::new_v4();assert!(reader.query_one("SELECT public.novu_rust_smtp_transport_policy_set($1,$2,0,'tls','reason')",&[&env,&request]).await.is_err());
    let sql="SELECT public.novu_rust_smtp_transport_policy_set($1,$2,0,'starttls','deployment')";
    let params: [&(dyn tokio_postgres::types::ToSql + Sync);2]=[&env,&request];
    let (a,b)=tokio::join!(admin.query_one(sql,&params),admin2.query_one(sql,&params));
    let a:Value=a.unwrap().get(0);let b:Value=b.unwrap().get(0);assert_ne!(a["replayed"],b["replayed"]);assert_eq!(a["policy"]["revision"],"1");assert_eq!(b["policy"]["mode"],"starttls");
    assert!(admin.query_one("SELECT public.novu_rust_smtp_transport_policy_set($1,$2,0,'none','deployment')",&[&env,&request]).await.is_err());
    assert!(admin.query_one("SELECT public.novu_rust_smtp_transport_policy_set($1,$2,0,'tls','stale')",&[&env,&Uuid::new_v4()]).await.is_err());
    let found:Value=admin.query_one("SELECT public.novu_rust_smtp_transport_policy_request($1,$2)",&[&env,&request]).await.unwrap().get(0);assert_eq!(found["found"],true);
    let audit:Value=admin.query_one("SELECT public.novu_rust_smtp_transport_policy_audit($1,NULL)",&[&env]).await.unwrap().get(0);assert_eq!(audit["data"].as_array().unwrap().len(),1);assert!(reader.query_one("SELECT public.novu_rust_smtp_transport_policy_audit($1,NULL)",&[&env]).await.is_err());
    db.batch_execute("CREATE FUNCTION public.fixture_fail_smtp_transport_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fixture_fail_smtp_transport_audit BEFORE INSERT ON public.novu_rust_smtp_transport_audit FOR EACH ROW EXECUTE FUNCTION public.fixture_fail_smtp_transport_audit()").await.unwrap();
    let rolled=Uuid::new_v4();assert!(admin.query_one("SELECT public.novu_rust_smtp_transport_policy_set($1,$2,1,'tls','rollback')",&[&env,&rolled]).await.is_err());
    db.batch_execute("DROP TRIGGER fixture_fail_smtp_transport_audit ON public.novu_rust_smtp_transport_audit; DROP FUNCTION public.fixture_fail_smtp_transport_audit()").await.unwrap();
    let current:Value=reader.query_one("SELECT public.novu_rust_smtp_transport_policy_read($1)",&[&env]).await.unwrap().get(0);assert_eq!(current["revision"],"1");
    let cli=Command::new(env!("CARGO_BIN_EXE_delivery-reconcile")).args(["smtp-transport-read",&env]).env("DATABASE_URL",&user_url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").output().unwrap();assert!(cli.status.success());assert_eq!(serde_json::from_slice::<Value>(&cli.stdout).unwrap()["mode"],"starttls");
    let cli_request=Uuid::new_v4().to_string();
    let cli=Command::new(env!("CARGO_BIN_EXE_delivery-reconcile")).args(["smtp-transport-set",&env,&cli_request,"1","tls","reviewed"]).env("DATABASE_URL",&user_url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").output().unwrap();assert!(cli.status.success());assert_eq!(serde_json::from_slice::<Value>(&cli.stdout).unwrap()["policy"]["mode"],"tls");
    let cli=Command::new(env!("CARGO_BIN_EXE_delivery-reconcile")).args(["smtp-transport-request",&env,&cli_request]).env("DATABASE_URL",&user_url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").output().unwrap();assert!(cli.status.success());assert_eq!(serde_json::from_slice::<Value>(&cli.stdout).unwrap()["found"],true);
    let cli=Command::new(env!("CARGO_BIN_EXE_delivery-reconcile")).args(["smtp-transport-audit",&env]).env("DATABASE_URL",&user_url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").output().unwrap();assert!(cli.status.success());assert_eq!(serde_json::from_slice::<Value>(&cli.stdout).unwrap()["data"].as_array().unwrap().len(),2);
    db.execute("UPDATE public.novu_rust_smtp_transport_admins SET can_write=false WHERE environment_id=$1 AND database_principal=$2::name",&[&env,&role]).await.unwrap();
    assert!(admin.query_one("SELECT public.novu_rust_smtp_transport_policy_request($1,$2)",&[&env,&request]).await.is_err());
}
