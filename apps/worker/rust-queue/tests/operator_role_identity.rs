//! Role names can be reused; authorization must bind to the role instance.
use novu_postgres_queue::{self as queue, alerts, reconciliation, slack_queue};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn reused_login_does_not_inherit_operator_or_admin_grants() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO public.novu_rust_environments VALUES ('env','org')").await.unwrap();
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
    let admin = format!("admin_{}", Uuid::new_v4().simple());
    let operator = format!("operator_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {admin} LOGIN; CREATE ROLE {operator} LOGIN; GRANT EXECUTE ON FUNCTION public.novu_rust_manage_operator_grant(text,text,text,text,text) TO {admin}; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_operator_grant(text,text) TO {operator}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_grant_admins(environment_id,database_principal) VALUES ('env',$1)", &[&admin]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision) VALUES ('env',$1,'retry')", &[&operator]).await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0073_operator_role_identity.sql"
    ))
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
    let (actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {operator}"))
        .await
        .unwrap();
    assert!(
        actor
            .query_one(
                "SELECT public.novu_rust_lock_operator_grant('env','retry')",
                &[]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    actor
        .batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    db.batch_execute(&format!("DROP OWNED BY {operator}; DROP ROLE {operator}; CREATE ROLE {operator} LOGIN; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_operator_grant(text,text) TO {operator}")).await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0073_operator_role_identity.sql"
    ))
    .await
    .unwrap();
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {operator}"))
        .await
        .unwrap();
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
    assert!(manager.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'retry','grant','re-authorize')", &[&operator]).await.unwrap().get::<_,bool>(0));
    assert!(
        actor
            .query_one(
                "SELECT public.novu_rust_lock_operator_grant('env','retry')",
                &[]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let audit = db.query_one("SELECT actor_oid=(SELECT oid FROM pg_roles WHERE rolname=$1),target_oid=(SELECT oid FROM pg_roles WHERE rolname=$2) FROM public.novu_rust_operator_grant_audit WHERE reason='re-authorize'", &[&admin,&operator]).await.unwrap();
    assert!(audit.get::<_, bool>(0) && audit.get::<_, bool>(1));
    manager
        .batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    db.batch_execute(&format!("DROP OWNED BY {admin}; DROP ROLE {admin}; CREATE ROLE {admin} LOGIN; GRANT EXECUTE ON FUNCTION public.novu_rust_manage_operator_grant(text,text,text,text,text) TO {admin}")).await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0073_operator_role_identity.sql"
    ))
    .await
    .unwrap();
    manager
        .batch_execute(&format!("SET SESSION AUTHORIZATION {admin}"))
        .await
        .unwrap();
    assert!(manager.query_one("SELECT public.novu_rust_manage_operator_grant('env',$1,'accepted','grant','ticket')", &[&operator]).await.is_err());
}
