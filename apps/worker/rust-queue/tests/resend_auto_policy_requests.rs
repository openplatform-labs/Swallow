//! Idempotent policy administration using restricted DB logins.
use std::process::Command;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
fn role_url(url: &str, role: &str) -> String {
    let mut u = reqwest::Url::parse(url).unwrap();
    u.set_username(role).unwrap();
    u.set_password(Some("test-only")).unwrap();
    u.to_string()
}
fn cli(url: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(args)
        .output()
        .unwrap()
}
async fn set(
    db: &Client,
    env: &str,
    id: Uuid,
    expected: i64,
    reason: &str,
) -> Result<serde_json::Value, tokio_postgres::Error> {
    Ok(db
        .query_one(
            "SELECT public.novu_rust_resend_auto_policy_set_v2($1,$2,$3,true,3,'now',$4)",
            &[&env, &id, &expected, &reason],
        )
        .await?
        .get(0))
}
async fn lookup(
    db: &Client,
    env: &str,
    id: Uuid,
) -> Result<serde_json::Value, tokio_postgres::Error> {
    Ok(db
        .query_one(
            "SELECT public.novu_rust_resend_auto_policy_request($1,$2)",
            &[&env, &id],
        )
        .await?
        .get(0))
}
#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL with role creation"]
async fn policy_request_is_scoped_atomic_replayable_and_queryable() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("request-policy-{}", Uuid::new_v4());
    let role = format!("request_{}", Uuid::new_v4().simple());
    let other = format!("request_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; CREATE ROLE {other} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role},{other}; GRANT EXECUTE ON FUNCTION public.novu_rust_resend_auto_policy_set_v2(text,uuid,bigint,boolean,integer,text,text),public.novu_rust_resend_auto_policy_request(text,uuid),public.novu_rust_resend_auto_policy_audit_read(text,bigint),public.novu_rust_resend_auto_policy_set(text,bigint,boolean,integer,text,text) TO {role},{other}")).await.unwrap();
    for name in [&role, &other] {
        db.execute("INSERT INTO public.novu_rust_resend_auto_policy_admins(environment_id,database_principal,principal_oid,can_write,can_audit) SELECT $1,$2,oid,true,true FROM pg_roles WHERE rolname::text=$2", &[&env,name]).await.unwrap();
    }
    let other_url = role_url(&url, &other);
    let role_url = role_url(&url, &role);
    let actor = connect(&role_url).await;
    let actor2 = connect(&role_url).await;
    let outsider = connect(&other_url).await;
    let id = Uuid::new_v4();
    assert!(
        actor
            .query(
                "SELECT * FROM public.novu_rust_resend_auto_policy_requests",
                &[]
            )
            .await
            .is_err()
    );
    let (first, second) = tokio::join!(
        set(&actor, &env, id, -1, "ticket"),
        set(&actor2, &env, id, -1, "ticket")
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_ne!(first["replayed"], second["replayed"]);
    assert_eq!(first["policy"], second["policy"]);
    assert_eq!(first["policy"]["revision"], "0");
    assert!(set(&actor, &env, id, -1, "changed-ticket").await.is_err());
    let lookup_result = cli(
        &role_url,
        &["resend-auto-policy-request", &env, &id.to_string()],
    );
    assert!(lookup_result.status.success());
    let found: serde_json::Value = serde_json::from_slice(&lookup_result.stdout).unwrap();
    assert_eq!(found["found"], true);
    assert_eq!(found["reason"], "ticket");
    assert_eq!(lookup(&outsider, &env, id).await.unwrap()["found"], false);
    assert!(lookup(&actor, "wrong-env", id).await.is_err());
    let replay = cli(
        &role_url,
        &[
            "resend-auto-policy-set-v2",
            &env,
            &id.to_string(),
            "-1",
            "enabled",
            "3",
            "now",
            "ticket",
        ],
    );
    assert!(replay.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&replay.stdout).unwrap()["replayed"],
        true
    );
    let audit = cli(&role_url, &["resend-auto-policy-audit", &env]);
    let audit: serde_json::Value = serde_json::from_slice(&audit.stdout).unwrap();
    assert_eq!(audit["data"].as_array().unwrap().len(), 1);
    assert_eq!(audit["data"][0]["requestId"], id.to_string());
    actor
        .query_one(
            "SELECT public.novu_rust_resend_auto_policy_set($1,0,false,2,'preserve','later')",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!(
        set(&actor, &env, id, -1, "ticket").await.unwrap()["policy"]["revision"],
        "0"
    );
    let stale = Uuid::new_v4();
    assert!(set(&actor, &env, stale, 0, "stale").await.is_err());
    assert_eq!(lookup(&actor, &env, stale).await.unwrap()["found"], false);
    let rollback = Uuid::new_v4();
    db.batch_execute("ALTER TABLE public.novu_rust_resend_auto_policy_audit ADD CONSTRAINT fixture_no_request CHECK(request_id IS NULL) NOT VALID").await.unwrap();
    assert!(set(&actor, &env, rollback, 1, "rollback").await.is_err());
    db.batch_execute(
        "ALTER TABLE public.novu_rust_resend_auto_policy_audit DROP CONSTRAINT fixture_no_request",
    )
    .await
    .unwrap();
    assert_eq!(
        lookup(&actor, &env, rollback).await.unwrap()["found"],
        false
    );
    assert_eq!(db.query_one("SELECT revision FROM public.novu_rust_resend_auto_retry_policy WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,i64>(0),1);
    db.execute("DELETE FROM public.novu_rust_resend_auto_policy_admins WHERE environment_id=$1 AND database_principal=$2", &[&env,&role]).await.unwrap();
    assert!(lookup(&actor, &env, id).await.is_err());
    assert!(set(&actor, &env, id, -1, "ticket").await.is_err());
}
