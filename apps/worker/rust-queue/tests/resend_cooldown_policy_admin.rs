//! Audited cooldown policy administration with restricted local PostgreSQL roles.
use serde_json::Value;
use std::time::Duration;
use tokio::process::Command;
use tokio_postgres::NoTls;
use uuid::Uuid;

async fn cli(url:&str,args:&[&str])->std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile")).env_clear().env("DATABASE_URL",url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE","true").args(args).output().await.unwrap()
}
fn json(output:&std::process::Output)->Value {serde_json::from_slice(&output.stdout).unwrap()}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore="requires fully migrated disposable PostgreSQL and role creation"]
async fn policy_management_is_scoped_idempotent_and_preserves_deadlines() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move {driver.await.unwrap()});
    let suffix=Uuid::new_v4().simple().to_string();let environment=format!("policy-{suffix}");let other=format!("other-{suffix}");
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')", &[&environment,&other]).await.unwrap();
    let reader=format!("policy_r_{suffix}");let writer=format!("policy_w_{suffix}");let auditor=format!("policy_a_{suffix}");
    for role in [&reader,&writer,&auditor] {
        db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cooldown_policy_read(text),public.novu_rust_resend_cooldown_policy_set(text,uuid,bigint,integer,text),public.novu_rust_resend_cooldown_policy_request(text,uuid),public.novu_rust_resend_cooldown_policy_audit_read(text,bigint) TO {role}")).await.unwrap();
    }
    for (role,read,write,audit) in [(&reader,true,false,false),(&writer,false,true,false),(&auditor,false,false,true)] {
        db.execute("INSERT INTO public.novu_rust_resend_cooldown_policy_admins SELECT $1,rolname,oid,$3,$4,$5 FROM pg_roles WHERE rolname::text=$2", &[&environment,&role,&read,&write,&audit]).await.unwrap();
    }
    let role_url=|role:&str| {let mut u=reqwest::Url::parse(&url).unwrap();u.set_username(role).unwrap();u.to_string()};
    let r=role_url(&reader);let w=role_url(&writer);let a=role_url(&auditor);
    let (restricted,driver)=tokio_postgres::connect(&w,NoTls).await.unwrap();tokio::spawn(async move {driver.await.unwrap()});
    assert!(restricted.query("SELECT * FROM public.novu_rust_resend_cooldown_policies", &[]).await.is_err());
    let read=cli(&r,&["resend-cooldown-policy-read",&environment]).await;
    assert!(read.status.success());assert_eq!(json(&read)["exists"],false);
    assert!(!cli(&r,&["resend-cooldown-policy-read",&other]).await.status.success());
    assert!(!cli(&w,&["resend-cooldown-policy-read",&environment]).await.status.success());
    let id=Uuid::new_v4().to_string();
    assert!(!cli(&r,&["resend-cooldown-policy-set",&environment,&id,"-1","30","fixture"]).await.status.success());
    for invalid in ["0","86401"] {
        assert!(!cli(&w,&["resend-cooldown-policy-set",&environment,&Uuid::new_v4().to_string(),"-1",invalid,"fixture"]).await.status.success());
    }
    assert!(!cli(&w,&["resend-cooldown-policy-set",&environment,&Uuid::new_v4().to_string(),"-1","30",&format!("{}x"," ".repeat(512))]).await.status.success());
    let created=cli(&w,&["resend-cooldown-policy-set",&environment,&id,"-1","30","fixture"]).await;
    assert!(created.status.success(),"{}",String::from_utf8_lossy(&created.stderr));
    assert_eq!(json(&created)["policy"]["revision"],"0");assert_eq!(json(&created)["replayed"],false);
    let replay=cli(&w,&["resend-cooldown-policy-set",&environment,&id,"-1","30","fixture"]).await;
    assert_eq!(json(&replay)["replayed"],true);
    assert!(!cli(&w,&["resend-cooldown-policy-set",&environment,&id,"-1","31","fixture"]).await.status.success());
    assert_eq!(json(&cli(&w,&["resend-cooldown-policy-request",&environment,&id]).await)["found"],true);
    assert!(!cli(&r,&["resend-cooldown-policy-request",&environment,&id]).await.status.success());
    let deadline:String=db.query_one("UPDATE public.novu_rust_resend_cooldown_policies SET not_before=clock_timestamp()+interval '10 minutes' WHERE environment_id=$1 RETURNING not_before::text", &[&environment]).await.unwrap().get(0);
    let id1=Uuid::new_v4().to_string();let id2=Uuid::new_v4().to_string();
    let first_args=["resend-cooldown-policy-set",&environment,&id1,"0","120","first"];
    let second_args=["resend-cooldown-policy-set",&environment,&id2,"0","180","second"];
    let (one,two)=tokio::join!(cli(&w,&first_args),cli(&w,&second_args));
    assert_ne!(one.status.success(),two.status.success());
    let policy=json(&cli(&r,&["resend-cooldown-policy-read",&environment]).await);
    assert_eq!(policy["revision"],"1");assert_eq!(policy["notBefore"],deadline);
    assert!(!cli(&w,&["resend-cooldown-policy-audit",&environment]).await.status.success());
    let audit=json(&cli(&a,&["resend-cooldown-policy-audit",&environment]).await);
    assert_eq!(audit["data"].as_array().unwrap().len(),2);
    let reject_id=Uuid::new_v4().to_string();
    db.batch_execute("CREATE FUNCTION public.fixture_cooldown_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fixture_cooldown_reject_audit BEFORE INSERT ON public.novu_rust_resend_cooldown_policy_audit FOR EACH ROW EXECUTE FUNCTION public.fixture_cooldown_reject_audit()").await.unwrap();
    assert!(!cli(&w,&["resend-cooldown-policy-set",&environment,&reject_id,"1","300","audit rollback"]).await.status.success());
    db.batch_execute("DROP TRIGGER fixture_cooldown_reject_audit ON public.novu_rust_resend_cooldown_policy_audit; DROP FUNCTION public.fixture_cooldown_reject_audit()").await.unwrap();
    assert_eq!(json(&cli(&r,&["resend-cooldown-policy-read",&environment]).await)["revision"],"1");
    assert_eq!(json(&cli(&w,&["resend-cooldown-policy-request",&environment,&reject_id]).await)["found"],false);
    let (mut locked,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move {driver.await.unwrap()});
    let tx=locked.transaction().await.unwrap();
    tx.query_one("SELECT 1 FROM public.novu_rust_resend_cooldown_policies WHERE environment_id=$1 FOR SHARE", &[&environment]).await.unwrap();
    let change_id=Uuid::new_v4().to_string();
    let mut pending=tokio::spawn({let w=w.clone();let environment=environment.clone();let change_id=change_id.clone();async move {cli(&w,&["resend-cooldown-policy-set",&environment,&change_id,"1","240","lock test"]).await}});
    assert!(tokio::time::timeout(Duration::from_millis(200),&mut pending).await.is_err());
    tx.commit().await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5),pending).await.unwrap().unwrap().status.success());
    assert_eq!(json(&cli(&r,&["resend-cooldown-policy-read",&environment]).await)["notBefore"],deadline);
    db.execute("UPDATE public.novu_rust_resend_cooldown_policy_admins SET can_write=false WHERE environment_id=$1 AND database_principal=$2", &[&environment,&writer]).await.unwrap();
    assert!(!cli(&w,&["resend-cooldown-policy-request",&environment,&id]).await.status.success());
}
