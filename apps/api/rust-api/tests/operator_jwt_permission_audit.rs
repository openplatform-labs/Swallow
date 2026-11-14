//! Scoped audit review and read-only administration on disposable PostgreSQL.
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn audit_is_scoped_paginated_and_independently_granted() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let writer = format!("jwt_writer_{}", Uuid::new_v4().simple());
    let reader = format!("jwt_reader_{}", Uuid::new_v4().simple());
    let no_audit = format!("jwt_no_audit_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {writer} LOGIN; CREATE ROLE {reader} LOGIN; CREATE ROLE {no_audit} LOGIN; GRANT USAGE ON SCHEMA public TO {writer},{reader},{no_audit}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_set_permissions(text,text,text,text,boolean,boolean,boolean,boolean,boolean,text) TO {writer}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_get_permissions(text,text,text,text) TO {reader},{no_audit}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_permission_audit_page(text,bigint) TO {reader}; INSERT INTO public.novu_rust_environments VALUES('audit-a','org'),('audit-b','org'); INSERT INTO public.novu_rust_operator_jwt_admin_grants SELECT 'audit-a',rolname,oid FROM pg_roles WHERE rolname IN ('{writer}','{reader}','{no_audit}');")).await.unwrap();
    let writer_db =
        connect(&url.replacen("postgres://postgres@", &format!("postgres://{writer}@"), 1)).await;
    let reader_db =
        connect(&url.replacen("postgres://postgres@", &format!("postgres://{reader}@"), 1)).await;
    let no_audit_db = connect(&url.replacen(
        "postgres://postgres@",
        &format!("postgres://{no_audit}@"),
        1,
    ))
    .await;
    let set = "SELECT public.novu_rust_operator_jwt_set_permissions('audit-a','https://issuer.test','alice','console',true,$1,false,false,false,'ticket')";
    for i in 0..53 {
        assert!(
            writer_db
                .query_one(set, &[&(i % 2 == 0)])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
    }
    db.execute("INSERT INTO public.novu_rust_operator_jwt_permission_audit(environment_id,issuer,subject,client_id,actor,actor_oid,reason,current) VALUES('audit-b','other','other','other','postgres',1,'private','{}')", &[]).await.unwrap();
    assert!(
        reader_db
            .query(
                "SELECT * FROM public.novu_rust_operator_jwt_permission_audit",
                &[]
            )
            .await
            .is_err()
    );
    assert!(reader_db.query_one(set, &[&true]).await.is_err());
    assert!(
        no_audit_db
            .query_one(
                "SELECT public.novu_rust_operator_jwt_permission_audit_page('audit-a',NULL)",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        reader_db
            .query_one(
                "SELECT public.novu_rust_operator_jwt_permission_audit_page('audit-b',NULL)",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        reader_db
            .query_one(
                "SELECT public.novu_rust_operator_jwt_permission_audit_page('audit-a',0)",
                &[]
            )
            .await
            .is_err()
    );
    let first: serde_json::Value = reader_db
        .query_one(
            "SELECT public.novu_rust_operator_jwt_permission_audit_page('audit-a',NULL)",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let rows = first["data"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    assert_eq!(rows[0]["subject"], "alice");
    assert_eq!(rows[0]["actor"], writer);
    assert_eq!(rows[0]["reason"], "ticket");
    let cursor = first["next"].as_i64().unwrap();
    assert_eq!(cursor, rows[49]["id"].as_i64().unwrap());
    let last: serde_json::Value = reader_db
        .query_one(
            "SELECT public.novu_rust_operator_jwt_permission_audit_page('audit-a',$1)",
            &[&cursor],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(last["data"].as_array().unwrap().len(), 3);
    assert!(last["next"].is_null());
    let identity:serde_json::Value=reader_db.query_one("SELECT public.novu_rust_operator_jwt_get_permissions('audit-a','https://issuer.test','alice','console')", &[]).await.unwrap().get(0);
    assert_eq!(identity["subscriber_read"], true);
    concurrent_first_registration_records_one_change().await;
}

async fn concurrent_first_registration_records_one_change() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let writer = format!("jwt_race_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {writer} LOGIN; GRANT USAGE ON SCHEMA public TO {writer}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_set_permissions(text,text,text,text,boolean,boolean,boolean,boolean,boolean,text) TO {writer}; INSERT INTO public.novu_rust_environments VALUES('audit-race','org'); INSERT INTO public.novu_rust_operator_jwt_admin_grants SELECT 'audit-race',rolname,oid FROM pg_roles WHERE rolname='{writer}'")).await.unwrap();
    let actor_url = url.replacen("postgres://postgres@", &format!("postgres://{writer}@"), 1);
    let a = connect(&actor_url).await;
    let b = connect(&actor_url).await;
    let sql = "SELECT public.novu_rust_operator_jwt_set_permissions('audit-race','https://issuer.test','alice','console',true,true,false,false,false,'ticket')";
    let (first, second) = tokio::join!(a.query_one(sql, &[]), b.query_one(sql, &[]));
    let results = [
        first.unwrap().get::<_, bool>(0),
        second.unwrap().get::<_, bool>(0),
    ];
    assert_eq!(results.iter().filter(|&&x| x).count(), 1);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_permission_audit WHERE environment_id='audit-race'",&[]).await.unwrap().get::<_,i64>(0),1);
}
