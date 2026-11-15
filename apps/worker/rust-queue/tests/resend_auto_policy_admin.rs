//! Restricted policy administration against a disposable, migrated PostgreSQL database.
use std::process::Command;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (client, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    client
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
    revision: i64,
    enabled: bool,
    max: i32,
    cutoff: &str,
    reason: &str,
) -> Result<serde_json::Value, tokio_postgres::Error> {
    Ok(db
        .query_one(
            "SELECT public.novu_rust_resend_auto_policy_set($1,$2,$3,$4,$5,$6)",
            &[&env, &revision, &enabled, &max, &cutoff, &reason],
        )
        .await?
        .get(0))
}

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL with role creation"]
async fn policy_administration_is_scoped_concurrent_and_audited() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("policy-admin-{}", Uuid::new_v4());
    let role = format!("policy_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resend_auto_policy_read(text),public.novu_rust_resend_auto_policy_set(text,bigint,boolean,integer,text,text),public.novu_rust_resend_auto_policy_audit_read(text,bigint) TO {role}")).await.unwrap();
    let rurl = role_url(&url, &role);
    let mut actor = connect(&rurl).await;
    let actor2 = connect(&rurl).await;
    assert!(
        !cli(&rurl, &["resend-auto-policy-read", &env])
            .status
            .success()
    );
    db.execute("INSERT INTO public.novu_rust_resend_auto_policy_admins(environment_id,database_principal,principal_oid,can_read,can_write,can_audit) SELECT $1,$2,oid,true,false,false FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    assert!(
        actor
            .query(
                "SELECT * FROM public.novu_rust_resend_auto_retry_policy",
                &[]
            )
            .await
            .is_err()
    );
    let read = cli(&rurl, &["resend-auto-policy-read", &env]);
    assert!(read.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&read.stdout).unwrap(),
        serde_json::json!({"exists":false})
    );
    assert!(
        set(&actor, &env, -1, true, 3, "now", "create")
            .await
            .is_err()
    );
    assert!(
        !cli(&rurl, &["resend-auto-policy-audit", &env])
            .status
            .success()
    );
    db.execute("UPDATE public.novu_rust_resend_auto_policy_admins SET can_write=true,can_audit=true WHERE environment_id=$1 AND database_principal=$2", &[&env,&role]).await.unwrap();
    assert!(
        set(&actor, "wrong-env", -1, true, 3, "now", "wrong")
            .await
            .is_err()
    );
    assert!(
        set(&actor, &env, -1, true, 3, "preserve", "wrong")
            .await
            .is_err()
    );
    assert!(
        set(&actor, &env, -1, true, 11, "now", "wrong")
            .await
            .is_err()
    );
    let created = cli(
        &rurl,
        &[
            "resend-auto-policy-set",
            &env,
            "-1",
            "enabled",
            "3",
            "now",
            "initial",
        ],
    );
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let current: serde_json::Value = serde_json::from_slice(&created.stdout).unwrap();
    assert_eq!(current["revision"], "0");
    assert_eq!(current["enabled"], true);
    let first = set(&actor, &env, 0, false, 2, "preserve", "change")
        .await
        .unwrap();
    assert_eq!(first["revision"], "1");
    assert_eq!(first["notBefore"], current["notBefore"]);
    assert!(set(&actor, &env, 0, true, 2, "now", "stale").await.is_err());
    let (one, two) = tokio::join!(
        set(&actor, &env, 1, true, 2, "now", "one"),
        set(&actor2, &env, 1, false, 4, "preserve", "two")
    );
    assert_ne!(one.is_ok(), two.is_ok());
    let read = cli(&rurl, &["resend-auto-policy-read", &env]);
    let state: serde_json::Value = serde_json::from_slice(&read.stdout).unwrap();
    assert_eq!(state["revision"], "2");
    let audit = cli(&rurl, &["resend-auto-policy-audit", &env]);
    assert!(audit.status.success());
    let audit: serde_json::Value = serde_json::from_slice(&audit.stdout).unwrap();
    assert_eq!(audit["data"].as_array().unwrap().len(), 3);
    db.batch_execute("ALTER TABLE public.novu_rust_resend_auto_policy_audit ADD CONSTRAINT fixture_fail_reason CHECK(reason<>'force rollback') NOT VALID").await.unwrap();
    assert!(
        set(&actor, &env, 2, true, 5, "now", "force rollback")
            .await
            .is_err()
    );
    db.batch_execute(
        "ALTER TABLE public.novu_rust_resend_auto_policy_audit DROP CONSTRAINT fixture_fail_reason",
    )
    .await
    .unwrap();
    assert_eq!(db.query_one("SELECT revision FROM public.novu_rust_resend_auto_retry_policy WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,i64>(0),2);
    // Policy updates wait while a scheduler holds its policy snapshot.
    let mut holder = connect(&url).await;
    let policy_tx = holder.transaction().await.unwrap();
    policy_tx.query_one("SELECT enabled FROM public.novu_rust_resend_auto_retry_policy WHERE environment_id=$1 FOR SHARE", &[&env]).await.unwrap();
    let policy_env = env.clone();
    let pending_policy =
        tokio::spawn(async move { set(&actor2, &policy_env, 0, true, 3, "now", "stale").await });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!pending_policy.is_finished());
    policy_tx.commit().await.unwrap();
    assert!(pending_policy.await.unwrap().is_err());
    // Revocation waits for a write already holding the administrator grant lock.
    let tx = actor.transaction().await.unwrap();
    tx.query_one(
        "SELECT public.novu_rust_resend_auto_policy_set($1,2,true,3,'preserve','locked')",
        &[&env],
    )
    .await
    .unwrap();
    let revoke_db = connect(&url).await;
    let revoke_env = env.clone();
    let revoke_role = role.clone();
    let pending = tokio::spawn(async move {
        revoke_db.execute("DELETE FROM public.novu_rust_resend_auto_policy_admins WHERE environment_id=$1 AND database_principal=$2", &[&revoke_env,&revoke_role]).await.unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), 1);
    assert!(
        set(&actor, &env, 3, false, 3, "now", "revoked")
            .await
            .is_err()
    );
}
