//! Restricted FCM cooldown access and worker startup on disposable PostgreSQL.
use std::process::Command;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn cooldown_is_scoped_monotonic_and_worker_fails_closed() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("fcm-cool-{}", Uuid::new_v4());
    let other = format!("fcm-cool-other-{}", Uuid::new_v4());
    let role = format!("fcm_cool_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_fcm_cooldown_policies(environment_id,fallback_seconds) VALUES($1,60),($2,60)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_fcm_cooldown_check(text),public.novu_rust_fcm_cooldown_record(text,integer),public.novu_rust_fcm_cooldown_record_429(text,integer),public.novu_rust_fcm_cooldown_record_503(text,integer) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let a = connect(role_url.as_str()).await;
    let b = connect(role_url.as_str()).await;
    assert!(
        a.query("SELECT * FROM public.novu_rust_fcm_cooldown_policies", &[])
            .await
            .is_err()
    );
    assert!(
        a.query_one("SELECT public.novu_rust_fcm_cooldown_check($1)", &[&env])
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_fcm_cooldown_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(
        a.query_one("SELECT public.novu_rust_fcm_cooldown_check($1)", &[&env])
            .await
            .unwrap()
            .get::<_, Option<String>>(0),
        None
    );
    assert!(
        a.query_one("SELECT public.novu_rust_fcm_cooldown_check($1)", &[&other])
            .await
            .is_err()
    );
    let sql = "SELECT public.novu_rust_fcm_cooldown_record($1,$2)";
    let short = Some(1i32);
    let long = Some(180i32);
    let p1: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&env, &short];
    let p2: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&env, &long];
    let (r1, r2) = tokio::join!(a.query_one(sql, p1), b.query_one(sql, p2));
    r1.unwrap();
    r2.unwrap();
    assert!(db.query_one("SELECT not_before>clock_timestamp()+interval '170 seconds' FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=$1",&[&env]).await.unwrap().get::<_,bool>(0));
    // Legacy calls have unknown source; a shorter observation must not overwrite the deadline source.
    let source = |name: &str| format!("SELECT public.novu_rust_fcm_cooldown_record_{name}($1,$2)");
    a.query_one(&source("503"), &[&env, &Some(400i32)]).await.unwrap();
    a.query_one(&source("429"), &[&env, &Some(60i32)]).await.unwrap();
    let row=db.query_one("SELECT deadline_source,deadline_recorded_at IS NOT NULL FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=$1", &[&env]).await.unwrap();
    assert_eq!(row.get::<_,Option<i32>>(0),Some(503)); assert!(row.get::<_,bool>(1));
    a.query_one(&source("429"), &[&env, &Some(500i32)]).await.unwrap();
    assert_eq!(db.query_one("SELECT deadline_source FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,Option<i32>>(0),Some(429));
    a.query_one(sql, &[&env, &Some(600i32)]).await.unwrap();
    assert_eq!(db.query_one("SELECT deadline_source FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,Option<i32>>(0),None);
    // A blocked policy row must start the delay only after acquiring its lock.
    db.execute("UPDATE public.novu_rust_fcm_cooldown_policies SET not_before='-infinity' WHERE environment_id=$1", &[&env]).await.unwrap();
    let mut blocker = connect(&url).await;
    let locked = blocker.transaction().await.unwrap();
    locked.query_one("SELECT 1 FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=$1 FOR UPDATE", &[&env]).await.unwrap();
    let sixty = Some(60i32);
    let args: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&env, &sixty];
    let name = source("429");
    let waiting = a.query_one(&name, args);
    tokio::pin!(waiting);
    tokio::select! { _ = &mut waiting => panic!("recording bypassed the locked row"), _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {} }
    locked.commit().await.unwrap();
    waiting.await.unwrap();
    assert!(db.query_one("SELECT not_before>clock_timestamp()+interval '59 seconds' FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,bool>(0));
    assert!(a.query_one(sql, &[&env, &Some(86401i32)]).await.is_err());
    db.batch_execute(&format!("REVOKE EXECUTE ON FUNCTION public.novu_rust_fcm_cooldown_record_429(text,integer) FROM {role}")).await.unwrap();
    let denied = Command::new(env!("CARGO_BIN_EXE_fcm-worker")).env_clear()
        .env("DATABASE_URL", role_url.as_str()).env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_FCM_ENVIRONMENT_ID", &env).env("NOVU_FCM_PROJECT_ID", "fixture-project")
        .env("NOVU_FCM_ACCESS_TOKEN", "fixture-token").env("SWALLOW_FCM_ENFORCE_ROUTING_CONFIG", "false")
        .env("SWALLOW_FCM_SHARED_COOLDOWN", "true").output().unwrap();
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stderr).contains("recording permission denied"));
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_fcm_cooldown_record_429(text,integer) TO {role}; REVOKE EXECUTE ON FUNCTION public.novu_rust_fcm_cooldown_record_503(text,integer) FROM {role}")).await.unwrap();
    let denied_503 = Command::new(env!("CARGO_BIN_EXE_fcm-worker")).env_clear()
        .env("DATABASE_URL", role_url.as_str()).env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_FCM_ENVIRONMENT_ID", &env).env("NOVU_FCM_PROJECT_ID", "fixture-project")
        .env("NOVU_FCM_ACCESS_TOKEN", "fixture-token").env("SWALLOW_FCM_ENFORCE_ROUTING_CONFIG", "false")
        .env("SWALLOW_FCM_SHARED_COOLDOWN", "true").env("SWALLOW_FCM_SHARED_COOLDOWN_503", "true").output().unwrap();
    assert!(!denied_503.status.success());
    assert!(String::from_utf8_lossy(&denied_503.stderr).contains("recording permission denied"));
    db.execute("DELETE FROM public.novu_rust_fcm_cooldown_workers WHERE environment_id=$1 AND database_principal=$2",&[&env,&role]).await.unwrap();
    assert!(
        a.query_one("SELECT public.novu_rust_fcm_cooldown_check($1)", &[&env])
            .await
            .is_err()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_fcm-worker"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_FCM_ENVIRONMENT_ID", &env)
        .env("NOVU_FCM_PROJECT_ID", "fixture-project")
        .env("NOVU_FCM_ACCESS_TOKEN", "fixture-token")
        .env("SWALLOW_FCM_ENFORCE_ROUTING_CONFIG", "false")
        .env("SWALLOW_FCM_SHARED_COOLDOWN", "true")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("FCM shared cooldown configuration denied")
    );
}

#[test]
fn worker_rejects_invalid_unavailable_policy_before_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_fcm-worker"))
        .env_clear()
        .env("SWALLOW_FCM_MULTICAST_503_POLICY", "invalid")
        .output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid FCM multicast 503 policy"));
}
