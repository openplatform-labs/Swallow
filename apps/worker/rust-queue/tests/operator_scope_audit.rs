//! Exercises audited scope changes with real migrations and restricted logins.
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn scoped_scope_administration_is_atomic_and_revocable() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (owner, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
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
        let result = std::process::Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .current_dir(root)
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    owner.batch_execute("INSERT INTO public.novu_rust_environments(id,organization_id) VALUES ('env','org'),('other','org')").await.unwrap();
    let actor = format!("scope_mgr_{}", Uuid::new_v4().simple());
    let target = format!("scope_user_{}", Uuid::new_v4().simple());
    owner.batch_execute(&format!("CREATE ROLE {actor} LOGIN; CREATE ROLE {target} LOGIN; GRANT EXECUTE ON FUNCTION public.novu_rust_set_operator_admin_scope(text,text,text[],text[],text),public.novu_rust_list_operator_scope_audit(text,bigint) TO {actor}")).await.unwrap();
    owner.execute("INSERT INTO public.novu_rust_operator_scope_admins SELECT 'env',$1,oid FROM pg_roles WHERE rolname::text=$1", &[&actor]).await.unwrap();
    owner.execute("INSERT INTO public.novu_rust_operator_grant_admins(environment_id,database_principal,principal_oid) SELECT 'env',$1,oid FROM pg_roles WHERE rolname::text=$1", &[&target]).await.unwrap();
    let (mut manager, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    manager
        .batch_execute(&format!("SET SESSION AUTHORIZATION {actor}"))
        .await
        .unwrap();
    let grants = vec!["slack_view".to_string()];
    let revokes: Vec<String> = vec![];
    let change = "SELECT public.novu_rust_set_operator_admin_scope('env',$1,$2,$3,'ticket')";
    assert!(
        manager
            .query_one(change, &[&target, &grants, &revokes])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        !manager
            .query_one(change, &[&target, &grants, &revokes])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        manager
            .query_one(
                "SELECT public.novu_rust_set_operator_admin_scope('other',$1,$2,$3,'ticket')",
                &[&target, &grants, &revokes]
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
    assert!(
        manager
            .query("SELECT * FROM public.novu_rust_operator_scope_audit", &[])
            .await
            .is_err()
    );
    let stale = format!("scope_stale_{}", Uuid::new_v4().simple());
    owner
        .batch_execute(&format!("CREATE ROLE {stale} LOGIN"))
        .await
        .unwrap();
    owner.execute("INSERT INTO public.novu_rust_operator_grant_admins(environment_id,database_principal,principal_oid) SELECT 'env',$1,oid FROM pg_roles WHERE rolname::text=$1", &[&stale]).await.unwrap();
    owner
        .batch_execute(&format!("DROP ROLE {stale}; CREATE ROLE {stale} LOGIN"))
        .await
        .unwrap();
    assert!(
        manager
            .query_one(change, &[&stale, &grants, &revokes])
            .await
            .is_err()
    );
    let invalid = vec!["not-a-permission".to_string()];
    assert!(
        manager
            .query_one(change, &[&target, &invalid, &revokes])
            .await
            .is_err()
    );
    let row = manager
        .query_one(
            "SELECT * FROM public.novu_rust_list_operator_scope_audit('env',NULL)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(1), actor);
    assert_eq!(row.get::<_, String>(2), target);
    assert_eq!(row.get::<_, Option<Vec<String>>>(3), None);
    assert_eq!(row.get::<_, Option<Vec<String>>>(5), Some(grants.clone()));
    assert!(
        manager
            .query(
                "SELECT * FROM public.novu_rust_list_operator_scope_audit('other',NULL)",
                &[]
            )
            .await
            .is_err()
    );
    let actor_url = url.replacen("postgres@", &format!("{actor}@"), 1);
    assert_ne!(url, actor_url);
    let cli = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let out = cli(&[
        "operator-scope-set",
        "env",
        &target,
        "null",
        "[\"retry\"]",
        "cli-ticket",
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = cli(&["operator-scope-audit", "env"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
    assert_eq!(rows[1]["grant"], serde_json::Value::Null);
    assert_eq!(rows[1]["revoke"], serde_json::json!(["retry"]));
    let next = cli(&[
        "operator-scope-audit",
        "env",
        rows[0]["id"].as_str().unwrap(),
    ]);
    assert!(next.status.success());
    let page: serde_json::Value = serde_json::from_slice(&next.stdout).unwrap();
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert!(!cli(&["operator-scope-audit", "env", "-1"]).status.success());
    owner
        .batch_execute(include_str!(
            "../migrations/0117_operator_admin_scope_audit.sql"
        ))
        .await
        .unwrap();
    assert_eq!(
        manager
            .query(
                "SELECT * FROM public.novu_rust_list_operator_scope_audit('env',NULL)",
                &[]
            )
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(
        !cli(&["operator-scope-set", "env", &target, "{}", "null", "bad"])
            .status
            .success()
    );
    owner.batch_execute("CREATE FUNCTION public.reject_scope_audit_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$; CREATE TRIGGER reject_scope_audit_test BEFORE INSERT ON public.novu_rust_operator_scope_audit FOR EACH ROW EXECUTE FUNCTION public.reject_scope_audit_test()").await.unwrap();
    assert!(
        manager
            .query_one(change, &[&target, &grants, &revokes])
            .await
            .is_err()
    );
    let row=owner.query_one("SELECT grant_scope,revoke_scope FROM public.novu_rust_operator_grant_admins WHERE database_principal=$1",&[&target]).await.unwrap();
    assert_eq!(row.get::<_, Option<Vec<String>>>(0), None);
    assert_eq!(
        row.get::<_, Option<Vec<String>>>(1),
        Some(vec!["retry".to_string()])
    );
    owner
        .batch_execute(
            "DROP TRIGGER reject_scope_audit_test ON public.novu_rust_operator_scope_audit",
        )
        .await
        .unwrap();
    let tx = manager.transaction().await.unwrap();
    assert!(
        tx.query_one(change, &[&target, &grants, &revokes])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let (other, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let actor_name = actor.clone();
    let revoke = tokio::spawn(async move {
        other.execute("DELETE FROM public.novu_rust_operator_scope_admins WHERE environment_id='env' AND database_principal=$1",&[&actor_name]).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !revoke.is_finished(),
        "revocation should wait for active scope change"
    );
    tx.commit().await.unwrap();
    assert_eq!(revoke.await.unwrap().unwrap(), 1);
    assert!(
        manager
            .query_one(change, &[&target, &grants, &revokes])
            .await
            .is_err()
    );
    assert_eq!(
        owner
            .query_one(
                "SELECT count(*) FROM public.novu_rust_operator_scope_audit",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        3
    );
}
