//! Scoped, read-only operator permission review on a disposable PostgreSQL database.
use std::{path::Path, process::Command, time::Duration};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and roles"]
async fn admin_lists_grants_and_audit_without_direct_table_access() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("../../api/rust-api/migrations");
    let worker = root.join("migrations");
    for args in [
        vec!["apply", "api", api.to_str().unwrap(), "--through", "17"],
        vec!["apply", "worker", worker.to_str().unwrap()],
        vec!["apply", "api", api.to_str().unwrap()],
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
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
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES ('env','org'),('other','org')",
    )
    .await
    .unwrap();
    let admin = format!("reader_{}", Uuid::new_v4().simple());
    let actor = format!("actor_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {admin} LOGIN PASSWORD 'fixture-password'; CREATE ROLE {actor} LOGIN; GRANT USAGE ON SCHEMA public TO {admin},{actor}; GRANT EXECUTE ON FUNCTION public.novu_rust_list_operator_grants(text,text,text),public.novu_rust_list_operator_grant_audit(text,bigint) TO {admin},{actor}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_grant_admins SELECT 'env',$1,oid FROM pg_roles WHERE rolname::text=$1", &[&admin]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT 'env',$1,'retry',oid FROM pg_roles WHERE rolname::text=$1", &[&actor]).await.unwrap();
    db.batch_execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'env','stale-'||lpad(i::text,3,'0'),'retry',NULL FROM generate_series(1,100) i; INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) VALUES ('other','outside','retry',NULL); INSERT INTO public.novu_rust_operator_grant_audit(environment_id,actor_session_user,target_session_user,permission,action,reason) SELECT 'env','admin','operator','retry','grant','ticket-'||i FROM generate_series(1,101) i; INSERT INTO public.novu_rust_operator_grant_audit(environment_id,actor_session_user,target_session_user,permission,action,reason) VALUES ('other','other','other','retry','grant','secret-other')").await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&admin).unwrap();
    role_url.set_password(Some("fixture-password")).unwrap();
    let (mut reader, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_delivery_operator_grants",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query("SELECT * FROM public.novu_rust_operator_grant_audit", &[])
            .await
            .is_err()
    );
    assert!(reader.query("SELECT public.novu_rust_manage_operator_grant('env','nobody','retry','grant','ticket')", &[]).await.is_err());
    db.batch_execute(&format!("REVOKE EXECUTE ON FUNCTION public.novu_rust_list_operator_grants(text,text,text) FROM {admin}")).await.unwrap();
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grants('env',NULL,NULL)",
                &[]
            )
            .await
            .is_err()
    );
    db.batch_execute(&format!(
        "GRANT EXECUTE ON FUNCTION public.novu_rust_list_operator_grants(text,text,text) TO {admin}"
    ))
    .await
    .unwrap();
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grants('other',NULL,NULL)",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grant_audit('other',NULL)",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grants('env','x',NULL)",
                &[]
            )
            .await
            .is_err()
    );
    let first = reader
        .query(
            "SELECT * FROM public.novu_rust_list_operator_grants('env',NULL,NULL)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(first.len(), 100);
    assert!(
        first
            .iter()
            .any(|r| r.get::<_, String>(0) == actor && r.get::<_, bool>(2))
    );
    assert!(first.iter().any(|r| !r.get::<_, bool>(2)));
    let last = first.last().unwrap();
    let principal: String = last.get(0);
    let permission: String = last.get(1);
    let next = reader
        .query(
            "SELECT * FROM public.novu_rust_list_operator_grants('env',$1,$2)",
            &[&principal, &permission],
        )
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    let audit = reader
        .query(
            "SELECT * FROM public.novu_rust_list_operator_grant_audit('env',NULL)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(audit.len(), 100);
    let last_id: i64 = audit.last().unwrap().get(0);
    let next = reader
        .query(
            "SELECT * FROM public.novu_rust_list_operator_grant_audit('env',$1)",
            &[&last_id],
        )
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].get::<_, String>(5), "ticket-101");
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grant_audit('env',-1)",
                &[]
            )
            .await
            .is_err()
    );
    for cmd in ["operator-grants", "operator-grant-audit"] {
        let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args([cmd, "env"])
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let values: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(values.as_array().unwrap().len(), 100);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("secret-other"));
    }
    let after_id = last_id.to_string();
    for arguments in [
        vec![
            "operator-grants",
            "env",
            principal.as_str(),
            permission.as_str(),
        ],
        vec!["operator-grant-audit", "env", &after_id],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(arguments)
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let values: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(values.as_array().unwrap().len(), 1);
    }
    let (other, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    other
        .batch_execute(&format!("SET SESSION AUTHORIZATION {actor}"))
        .await
        .unwrap();
    assert!(
        other
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grants('env',NULL,NULL)",
                &[]
            )
            .await
            .is_err()
    );
    let tx = reader.transaction().await.unwrap();
    tx.query(
        "SELECT * FROM public.novu_rust_list_operator_grants('env',NULL,NULL)",
        &[],
    )
    .await
    .unwrap();
    let actor_name = admin.clone();
    let (owner, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let revocation = tokio::spawn(async move {
        owner.execute("DELETE FROM public.novu_rust_operator_grant_admins WHERE environment_id='env' AND database_principal=$1", &[&actor_name]).await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!revocation.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(revocation.await.unwrap().unwrap(), 1);
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grant_audit('env',NULL)",
                &[]
            )
            .await
            .is_err()
    );
}
