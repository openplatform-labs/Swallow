//! Verifies revocation fencing with a restricted session in a disposable database.
use novu_postgres_queue::{self as queue, alerts, reconciliation};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn revocation_waits_for_reserved_operation_without_table_write_permission() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (admin, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    admin.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO public.novu_rust_environments VALUES ('env','org')").await.unwrap();
    for migration in [
        queue::MIGRATION,
        reconciliation::MIGRATION,
        reconciliation::OPERATOR_GRANTS_MIGRATION,
        alerts::MIGRATION,
        alerts::RESOLUTION_MIGRATION,
        reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
    ] {
        admin.batch_execute(migration).await.unwrap();
    }
    admin
        .batch_execute(reconciliation::OPERATOR_GRANT_LOCK_MIGRATION)
        .await
        .unwrap();
    let role = format!("alert_test_{}", Uuid::new_v4().simple());
    admin.batch_execute(&format!("CREATE ROLE {role}; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.novu_rust_environments TO {role}; GRANT SELECT,INSERT,UPDATE ON public.novu_rust_alert_delivery_slots,public.novu_rust_alert_delivery_attempts TO {role};")).await.unwrap();
    admin
        .execute(
            "INSERT INTO public.novu_rust_delivery_operator_grants VALUES ('env',$1,'alert_send')",
            &[&role],
        )
        .await
        .unwrap();
    admin.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('env','slack','ops','idle')", &[]).await.unwrap();
    let (mut actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        actor
            .query_one(
                "SELECT public.novu_rust_lock_operator_grant('env','alert_send')",
                &[]
            )
            .await
            .is_err()
    );
    admin
        .batch_execute(&format!(
            "GRANT EXECUTE ON FUNCTION public.novu_rust_lock_operator_grant(text,text) TO {role}"
        ))
        .await
        .unwrap();
    assert!(
        actor
            .execute(
                "DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='env'",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        !actor
            .query_one(
                "SELECT public.novu_rust_lock_operator_grant('other','alert_send')",
                &[]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
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
    let pid: i32 = actor
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let (mut blocker, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let lock = blocker.transaction().await.unwrap();
    lock.query_one("SELECT 1 FROM public.novu_rust_alert_delivery_slots WHERE environment_id='env' AND channel='slack' AND target_id='ops' FOR UPDATE", &[]).await.unwrap();
    let task = tokio::spawn(async move {
        let result = alerts::reserve(&mut actor, "env", "slack", "ops", 60).await;
        (actor, result)
    });
    let mut waiting = false;
    for _ in 0..200 {
        waiting = admin
            .query_one(
                "SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1",
                &[&pid],
            )
            .await
            .unwrap()
            .get::<_, Option<bool>>(0)
            == Some(true);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(waiting, "reservation did not wait on the slot lock");
    let (revoker, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let mut revoke = tokio::spawn(async move {
        revoker.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='env' AND decision='alert_send'", &[]).await.unwrap()
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut revoke)
            .await
            .is_err()
    );
    lock.commit().await.unwrap();
    let (mut actor, result) = task.await.unwrap();
    assert!(matches!(result.unwrap(), alerts::Reservation::Claimed(_)));
    assert_eq!(revoke.await.unwrap(), 1);
    assert!(
        alerts::reserve(&mut actor, "env", "slack", "other", 60)
            .await
            .is_err()
    );
    actor
        .batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    admin
        .batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .await
        .unwrap();
}
