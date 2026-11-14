//! Scoped sender tests against disposable PostgreSQL, with no external provider calls.
use novu_postgres_queue::{
    MIGRATION,
    alerts::{self, AlertError, Reservation},
    reconciliation,
};
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn isolated_senders_fence_outcomes_and_preserve_inflight_revocation() {
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
        include_str!("../migrations/0085_alert_condition_cooldown.sql"),
        include_str!("../migrations/0088_scoped_alert_dispatch.sql"),
    ] {
        admin.batch_execute(sql).await.unwrap();
    }
    admin
        .batch_execute(include_str!("../migrations/0088_scoped_alert_dispatch.sql"))
        .await
        .unwrap();
    let first = format!("alert_sender_a_{}", Uuid::new_v4().simple());
    let second = format!("alert_sender_b_{}", Uuid::new_v4().simple());
    for role in [&first, &second] {
        admin.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer) TO {role}")).await.unwrap();
    }
    let connect = |role: String| {
        let url = url.clone();
        async move {
            let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
            tokio::spawn(async move {
                let _ = driver.await;
            });
            db.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
                .await
                .unwrap();
            db
        }
    };
    let a = connect(first.clone()).await;
    let b = connect(second.clone()).await;
    assert!(
        alerts::reserve_scoped(&a, "a", "slack", "ops", 60, Some(1))
            .await
            .is_err()
    );
    for role in [&first, &second] {
        admin.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'alert_send',oid FROM pg_roles WHERE rolname::text=$1",&[role]).await.unwrap();
    }
    assert!(
        alerts::reserve_scoped(&a, "b", "slack", "ops", 60, None)
            .await
            .is_err()
    );
    assert!(
        a.query("SELECT * FROM public.novu_rust_alert_delivery_slots", &[])
            .await
            .is_err()
    );
    assert!(
        a.execute(
            "UPDATE public.novu_rust_alert_delivery_attempts SET state='accepted'",
            &[]
        )
        .await
        .is_err()
    );
    let (left, right) = tokio::join!(
        alerts::reserve_scoped(&a, "a", "slack", "ops", 60, Some(1)),
        alerts::reserve_scoped(&b, "a", "slack", "ops", 60, Some(1))
    );
    let left = left.unwrap();
    let right = right.unwrap();
    let (winner, loser, role, id) = match (left, right) {
        (Reservation::Claimed(id), Reservation::InFlight) => (&a, &b, &first, id),
        (Reservation::InFlight, Reservation::Claimed(id)) => (&b, &a, &second, id),
        _ => panic!("expected one claim"),
    };
    assert!(
        !alerts::finish_scoped(loser, "a", "slack", "ops", id, &Ok(()))
            .await
            .unwrap()
    );
    admin.execute("DELETE FROM novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=$1 AND decision='alert_send'",&[role]).await.unwrap();
    assert!(
        alerts::finish_scoped(winner, "a", "slack", "ops", id, &Ok(()))
            .await
            .unwrap()
    );
    assert!(
        alerts::reserve_scoped(winner, "a", "slack", "ops", 60, None)
            .await
            .is_err()
    );
    assert_eq!(
        alerts::reserve_scoped(loser, "a", "slack", "ops", 60, Some(1))
            .await
            .unwrap(),
        Reservation::CoolingDown
    );
    let id2 = match alerts::reserve_scoped(loser, "a", "slack", "ops", 60, Some(3))
        .await
        .unwrap()
    {
        Reservation::Claimed(id) => id,
        _ => panic!("new condition expected"),
    };
    admin.execute("UPDATE novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id='a' AND target_id='ops'",&[]).await.unwrap();
    assert!(
        !alerts::finish_scoped(loser, "a", "slack", "ops", id2, &Ok(()))
            .await
            .unwrap()
    );
    assert_eq!(
        alerts::reserve_scoped(loser, "a", "slack", "ops", 60, Some(7))
            .await
            .unwrap(),
        Reservation::Blocked
    );
    assert!(
        !alerts::finish_scoped(loser, "a", "slack", "ops", id2, &Err(AlertError::Unknown))
            .await
            .unwrap()
    );
    let state: String = admin
        .query_one(
            "SELECT state FROM novu_rust_alert_delivery_attempts WHERE id=$1",
            &[&id2],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(state, "unknown");
    a.batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    b.batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    admin
        .execute(
            "DELETE FROM novu_rust_delivery_operator_grants WHERE database_principal=ANY($1)",
            &[&vec![first.clone(), second.clone()]],
        )
        .await
        .unwrap();
    for role in [&first, &second] {
        admin
            .batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
            .await
            .unwrap();
    }
}
