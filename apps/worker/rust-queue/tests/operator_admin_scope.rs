//! Tests scoped delegation with isolated PostgreSQL roles and concurrent updates.
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn delegation_scope_is_enforced_and_fenced() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut owner, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for args in [
        vec![
            "apply",
            "api",
            "../../api/rust-api/migrations",
            "--through",
            "17",
        ],
        vec!["apply", "worker", "migrations"],
    ] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .current_dir(root)
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    owner.batch_execute("INSERT INTO public.novu_rust_environments(id,organization_id) VALUES ('env','org'),('other','org')").await.unwrap();
    let migration = include_str!("../migrations/0116_operator_admin_scopes.sql");
    owner.batch_execute(migration).await.unwrap();
    owner.batch_execute(migration).await.unwrap();
    let admin = format!("scope_admin_{}", Uuid::new_v4().simple());
    let target = format!("scope_target_{}", Uuid::new_v4().simple());
    owner.batch_execute(&format!("CREATE ROLE {admin} LOGIN; CREATE ROLE {target} LOGIN; GRANT EXECUTE ON FUNCTION public.novu_rust_manage_operator_grant(text,text,text,text,text), public.novu_rust_current_operator_admin_scope(text) TO {admin}")).await.unwrap();
    owner.execute("INSERT INTO public.novu_rust_operator_grant_admins(environment_id,database_principal,principal_oid) SELECT 'env',$1,oid FROM pg_roles WHERE rolname::text=$1", &[&admin]).await.unwrap();
    let (manager, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    manager
        .batch_execute(&format!("SET SESSION AUTHORIZATION {admin}"))
        .await
        .unwrap();
    let row = manager
        .query_one(
            "SELECT * FROM public.novu_rust_current_operator_admin_scope('env')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<Vec<String>>>(0), None);
    assert!(manager.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'accepted','grant','initial')", &[&target]).await.unwrap().get::<_,bool>(0));
    owner.execute("UPDATE public.novu_rust_operator_grant_admins SET grant_scope=ARRAY['retry'],revoke_scope=ARRAY['accepted'] WHERE environment_id='env' AND database_principal=$1", &[&admin]).await.unwrap();
    assert!(
        manager
            .query_one(
                "SELECT public.novu_rust_manage_operator_grant('env',$1,'retry','grant','allowed')",
                &[&target]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(manager.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'accepted','grant','denied')", &[&target]).await.is_err());
    assert!(
        manager
            .query_one(
                "SELECT public.novu_rust_manage_operator_grant('env',$1,'retry','revoke','denied')",
                &[&target]
            )
            .await
            .is_err()
    );
    assert!(manager.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'accepted','revoke','allowed')", &[&target]).await.unwrap().get::<_,bool>(0));
    assert!(
        manager
            .query_one(
                "SELECT * FROM public.novu_rust_current_operator_admin_scope('other')",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        manager
            .execute(
                "UPDATE public.novu_rust_operator_grant_admins SET grant_scope=NULL",
                &[]
            )
            .await
            .is_err()
    );
    assert!(owner.execute("UPDATE public.novu_rust_operator_grant_admins SET grant_scope=ARRAY['retry',NULL] WHERE database_principal=$1", &[&admin]).await.is_err());
    assert!(owner.execute("UPDATE public.novu_rust_operator_grant_admins SET grant_scope=ARRAY['typo'] WHERE database_principal=$1", &[&admin]).await.is_err());
    let admin_url = url.replacen("postgres@", &format!("{admin}@"), 1);
    assert_ne!(admin_url, url);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["operator-scope", "env"])
        .env("DATABASE_URL", &admin_url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        parsed,
        serde_json::json!({"grant":["retry"],"revoke":["accepted"]})
    );
    for (permission, success) in [("retry", true), ("accepted", false)] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(["operator-grant", "env", &target, permission, "cli-check"])
            .env("DATABASE_URL", &admin_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            success,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    owner.execute("UPDATE public.novu_rust_operator_grant_admins SET grant_scope=ARRAY['retry','alert_send'] WHERE database_principal=$1", &[&admin]).await.unwrap();
    owner.batch_execute("CREATE FUNCTION public.reject_scope_test_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$; CREATE TRIGGER reject_scope_test_audit BEFORE INSERT ON public.novu_rust_operator_grant_audit FOR EACH ROW EXECUTE FUNCTION public.reject_scope_test_audit()").await.unwrap();
    assert!(manager.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'alert_send','grant','audit-failure')", &[&target]).await.is_err());
    assert_eq!(owner.query_one("SELECT count(*) FROM public.novu_rust_delivery_operator_grants WHERE database_principal=$1 AND decision='alert_send'", &[&target]).await.unwrap().get::<_,i64>(0),0);
    owner
        .batch_execute(
            "DROP TRIGGER reject_scope_test_audit ON public.novu_rust_operator_grant_audit",
        )
        .await
        .unwrap();
    let tx = owner.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_grant_admins SET grant_scope=ARRAY[]::text[] WHERE database_principal=$1", &[&admin]).await.unwrap();
    let target_name = target.clone();
    let pending = tokio::spawn(async move {
        manager
            .query_one(
                "SELECT public.novu_rust_manage_operator_grant('env',$1,'retry','grant','racing')",
                &[&target_name],
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !pending.is_finished(),
        "delegation did not wait for scope update"
    );
    tx.commit().await.unwrap();
    assert!(
        pending.await.unwrap().is_err(),
        "revoked scope must be observed"
    );
    assert_eq!(
        owner
            .query_one(
                "SELECT count(*) FROM public.novu_rust_operator_grant_audit",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        3
    );
}
