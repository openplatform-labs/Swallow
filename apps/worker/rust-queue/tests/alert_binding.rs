//! Environment-scoped immutable alert destination association.
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn binding_is_scoped_and_concurrent_conflicts_fail_closed() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute(
        "INSERT INTO novu_rust_environments VALUES ('binding-a','org'),('binding-b','org')",
    )
    .await
    .unwrap();
    let role = format!("alert_binding_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_bind_alert_destination(text,text,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'binding-a',rolname,'alert_send',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let (a, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let (b, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    for client in [&a, &b] {
        client
            .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
            .await
            .unwrap();
    }
    assert!(
        a.query(
            "SELECT * FROM public.novu_rust_alert_destination_bindings",
            &[]
        )
        .await
        .is_err()
    );
    let first = "a".repeat(64);
    let second = "b".repeat(64);
    let query = "SELECT public.novu_rust_bind_alert_destination($1,$2,$3,$4)";
    let (left, right) = tokio::join!(
        async {
            a.query_one(query, &[&"binding-a", &"email", &"race", &first])
                .await
        },
        async {
            b.query_one(query, &[&"binding-a", &"email", &"race", &second])
                .await
        }
    );
    let left: bool = left.unwrap().get(0);
    let right: bool = right.unwrap().get(0);
    assert_ne!(left, right);
    let winner = if left { &first } else { &second };
    let loser = if left { &second } else { &first };
    assert!(
        a.query_one(query, &[&"binding-a", &"email", &"race", winner])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        !a.query_one(query, &[&"binding-a", &"email", &"race", loser])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        a.query_one(query, &[&"binding-b", &"email", &"race", winner])
            .await
            .is_err()
    );
    assert!(
        a.query_one(query, &[&"binding-a", &"slack", &"race", loser])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    db.execute("DELETE FROM novu_rust_delivery_operator_grants WHERE environment_id='binding-a' AND database_principal=$1", &[&role]).await.unwrap();
    assert!(
        a.query_one(query, &[&"binding-a", &"email", &"race", winner])
            .await
            .is_err()
    );
}
