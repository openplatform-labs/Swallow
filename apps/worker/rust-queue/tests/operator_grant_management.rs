//! Verifies scoped administration and revocation fencing in disposable PostgreSQL.
use novu_postgres_queue::{self as queue, alerts, reconciliation, slack_queue};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn scoped_administrators_audit_changes_and_fence_revocation() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO public.novu_rust_environments VALUES ('env','org'),('other','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        reconciliation::MIGRATION,
        reconciliation::OPERATOR_GRANTS_MIGRATION,
        alerts::MIGRATION,
        alerts::RESOLUTION_MIGRATION,
        reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
        slack_queue::OPERATOR_MIGRATION,
        include_str!("../migrations/0072_operator_grant_management.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute(include_str!(
        "../migrations/0072_operator_grant_management.sql"
    ))
    .await
    .unwrap();
    let admin = format!("grant_admin_{}", Uuid::new_v4().simple());
    let operator = format!("grant_operator_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {admin} LOGIN; CREATE ROLE {operator} LOGIN; GRANT EXECUTE ON FUNCTION public.novu_rust_manage_operator_grant(text,text,text,text,text) TO {admin}; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_operator_grant(text,text) TO {operator}")).await.unwrap();
    db.execute(
        "INSERT INTO public.novu_rust_operator_grant_admins VALUES ('env',$1)",
        &[&admin],
    )
    .await
    .unwrap();
    let (manager, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    manager
        .batch_execute(&format!("SET SESSION AUTHORIZATION {admin}"))
        .await
        .unwrap();
    let args: [&(dyn tokio_postgres::types::ToSql + Sync); 5] =
        [&"env", &operator, &"retry", &"grant", &"ticket-123"];
    assert!(
        manager
            .query_one(
                "SELECT public.novu_rust_manage_operator_grant($1,$2,$3,$4,$5)",
                &args
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        !manager
            .query_one(
                "SELECT public.novu_rust_manage_operator_grant($1,$2,$3,$4,$5)",
                &args
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(manager.query_one("SELECT public.novu_rust_manage_operator_grant('other',$1,'retry','grant','ticket')", &[&operator]).await.is_err());
    assert!(
        manager
            .query_one(
                "SELECT public.novu_rust_manage_operator_grant('env',$1,'owner','grant','ticket')",
                &[&operator]
            )
            .await
            .is_err()
    );
    assert!(manager.query_one("SELECT public.novu_rust_manage_operator_grant('env','missing_login','retry','grant','ticket')", &[]).await.is_err());
    assert!(
        manager
            .execute("DELETE FROM public.novu_rust_delivery_operator_grants", &[])
            .await
            .is_err()
    );
    assert!(
        manager
            .execute("DELETE FROM public.novu_rust_operator_grant_audit", &[])
            .await
            .is_err()
    );
    let (mut actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {operator}"))
        .await
        .unwrap();
    assert!(actor.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'accepted','grant','ticket')", &[&operator]).await.is_err());
    let tx = actor.transaction().await.unwrap();
    assert!(
        tx.query_one(
            "SELECT public.novu_rust_lock_operator_grant('env','retry')",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    let op = operator.clone();
    let task = tokio::spawn(async move {
        let result = manager.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'retry','revoke','ticket-456')", &[&op]).await;
        (manager, result)
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !task.is_finished(),
        "revocation must wait for active operation"
    );
    tx.commit().await.unwrap();
    let (mut manager, row) = task.await.unwrap();
    assert!(row.unwrap().get::<_, bool>(0));
    assert!(
        !actor
            .query_one(
                "SELECT public.novu_rust_lock_operator_grant('env','retry')",
                &[]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let records = db.query("SELECT actor_session_user,target_session_user,permission,action,reason FROM public.novu_rust_operator_grant_audit ORDER BY id", &[]).await.unwrap();
    assert_eq!(records.len(), 2);
    for (row, action) in records.iter().zip(["grant", "revoke"]) {
        assert_eq!(row.get::<_, String>(0), admin);
        assert_eq!(row.get::<_, String>(1), operator);
        assert_eq!(row.get::<_, String>(2), "retry");
        assert_eq!(row.get::<_, String>(3), action);
    }
    let manager_url = url.replacen("postgres@", &format!("{admin}@"), 1);
    assert_ne!(manager_url, url, "test URL must use postgres login");
    for command in ["operator-grant", "operator-revoke"] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args([command, "env", &operator, "accepted", "ticket-cli"])
            .env("DATABASE_URL", &manager_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(output.status.success(), "CLI grant management failed");
        assert!(String::from_utf8_lossy(&output.stdout).contains("operator grant changed"));
    }
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_grant_audit WHERE actor_session_user=$1 AND target_session_user=$2 AND permission='accepted' AND reason='ticket-cli'", &[&admin,&operator]).await.unwrap().get::<_,i64>(0),2);
    let management_tx = manager.transaction().await.unwrap();
    assert!(management_tx.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'retry','grant','ticket-lock')", &[&operator]).await.unwrap().get::<_,bool>(0));
    let (owner, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let admin_to_revoke = admin.clone();
    let revocation = tokio::spawn(async move {
        owner.execute("DELETE FROM public.novu_rust_operator_grant_admins WHERE environment_id='env' AND database_principal=$1", &[&admin_to_revoke]).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !revocation.is_finished(),
        "admin removal must wait for in-flight management"
    );
    management_tx.commit().await.unwrap();
    assert_eq!(revocation.await.unwrap().unwrap(), 1);
    assert!(
        manager
            .query_one(
                "SELECT public.novu_rust_manage_operator_grant('env',$1,'retry','grant','ticket')",
                &[&operator]
            )
            .await
            .is_err()
    );
}
