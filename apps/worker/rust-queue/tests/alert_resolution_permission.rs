//! Audited alert resolution using a login without table privileges.
use novu_postgres_queue::{MIGRATION, alerts, reconciliation};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn restricted_operator_can_only_make_authorized_atomic_decisions() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (admin, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    admin.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')").await.unwrap();
    for sql in [
        MIGRATION,
        reconciliation::MIGRATION,
        reconciliation::OPERATOR_GRANTS_MIGRATION,
        alerts::MIGRATION,
        alerts::RESOLUTION_MIGRATION,
        reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
        include_str!("../migrations/0072_operator_grant_management.sql"),
        include_str!("../migrations/0073_operator_role_identity.sql"),
        alerts::RESOLVE_FUNCTION_MIGRATION,
    ] {
        admin.batch_execute(sql).await.unwrap();
    }
    admin
        .batch_execute(alerts::RESOLVE_FUNCTION_MIGRATION)
        .await
        .unwrap();
    let operator = format!("alert_operator_{}", Uuid::new_v4().simple());
    admin.batch_execute(&format!("CREATE ROLE {operator} LOGIN; GRANT USAGE ON SCHEMA public TO {operator}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_alert(text,text,text,text,text,text) TO {operator}")).await.unwrap();
    let (mut actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {operator}"))
        .await
        .unwrap();
    let mut ids = Vec::new();
    for (env, channel, target, state) in [
        ("a", "slack", "first", "unknown"),
        ("a", "email", "second", "unknown"),
        ("a", "email", "rejected", "rejected"),
        ("b", "slack", "other", "unknown"),
    ] {
        let id = Uuid::new_v4();
        ids.push(id);
        admin.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,attempt_id) VALUES ($1,$2,$3,$4,$5)",&[&env,&channel,&target,&state,&id]).await.unwrap();
        admin.execute("INSERT INTO novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES ($1,$2,$3,$4,$5)",&[&id,&env,&channel,&target,&state]).await.unwrap();
    }
    assert!(
        alerts::resolve(
            &mut actor, "a", "slack", "first", "retry", "operator", "ticket"
        )
        .await
        .is_err()
    );
    admin.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'alert_retry',oid FROM pg_roles WHERE rolname::text=$1",&[&operator]).await.unwrap();
    assert!(
        alerts::resolve(
            &mut actor, "b", "slack", "other", "retry", "operator", "ticket"
        )
        .await
        .is_err()
    );
    assert!(
        alerts::resolve(
            &mut actor, "a", "email", "second", "accepted", "operator", "ticket"
        )
        .await
        .is_err()
    );
    admin.batch_execute("ALTER TABLE novu_rust_alert_delivery_resolutions ADD CONSTRAINT fixture_reject CHECK (reason<>'reject fixture')").await.unwrap();
    assert!(
        alerts::resolve(
            &mut actor,
            "a",
            "slack",
            "first",
            "retry",
            "operator",
            "reject fixture"
        )
        .await
        .is_err()
    );
    assert_eq!(admin.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id='a' AND channel='slack' AND target_id='first'",&[]).await.unwrap().get::<_,String>(0),"unknown");
    admin
        .batch_execute(
            "ALTER TABLE novu_rust_alert_delivery_resolutions DROP CONSTRAINT fixture_reject",
        )
        .await
        .unwrap();
    alerts::resolve(
        &mut actor, "a", "slack", "first", "retry", "operator", "ticket",
    )
    .await
    .unwrap();
    assert!(
        alerts::resolve(
            &mut actor, "a", "slack", "first", "retry", "operator", "ticket"
        )
        .await
        .is_err()
    );
    let row=admin.query_one("SELECT database_user,database_session_user FROM novu_rust_alert_delivery_resolutions WHERE attempt_id=$1",&[&ids[0]]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), operator);
    assert_eq!(row.get::<_, String>(1), operator);
    assert!(
        actor
            .execute(
                "UPDATE public.novu_rust_alert_delivery_slots SET state='idle'",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        actor
            .execute(
                "DELETE FROM public.novu_rust_alert_delivery_resolutions",
                &[]
            )
            .await
            .is_err()
    );
    admin.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'alert_accepted',oid FROM pg_roles WHERE rolname::text=$1",&[&operator]).await.unwrap();
    assert!(
        alerts::resolve(
            &mut actor, "a", "email", "rejected", "accepted", "operator", "ticket"
        )
        .await
        .is_err()
    );
    alerts::resolve(
        &mut actor, "a", "email", "second", "accepted", "operator", "ticket",
    )
    .await
    .unwrap();
    assert_eq!(admin.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id='a' AND channel='email' AND target_id='second'",&[]).await.unwrap().get::<_,String>(0),"accepted");
    admin
        .execute(
            "DELETE FROM novu_rust_delivery_operator_grants WHERE database_principal=$1",
            &[&operator],
        )
        .await
        .unwrap();
    assert!(
        alerts::resolve(
            &mut actor, "a", "email", "rejected", "retry", "operator", "ticket"
        )
        .await
        .is_err()
    );
    actor
        .batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    admin
        .batch_execute(&format!("DROP OWNED BY {operator}; DROP ROLE {operator}"))
        .await
        .unwrap();
}
