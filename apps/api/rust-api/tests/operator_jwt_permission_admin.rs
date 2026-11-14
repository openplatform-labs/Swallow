//! Optional JWT permission administration against a disposable, migrated PostgreSQL.
use std::{sync::Arc, time::Duration};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn administration_is_scoped_audited_atomic_and_revocable() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    let admin = format!("jwt_admin_{}", Uuid::new_v4().simple());
    let api = format!("jwt_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {admin} LOGIN; CREATE ROLE {api} LOGIN; GRANT USAGE ON SCHEMA public TO {admin},{api}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_set_permissions(text,text,text,text,boolean,boolean,boolean,boolean,boolean,text), public.novu_rust_operator_jwt_get_permissions(text,text,text,text) TO {admin}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_self(text,text,text,text,bigint) TO {api}; INSERT INTO public.novu_rust_environments VALUES('admin-a','org'),('admin-b','org'); INSERT INTO public.novu_rust_operator_jwt_admin_grants SELECT 'admin-a',rolname,oid FROM pg_roles WHERE rolname='{admin}'; INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='{api}';")).await.unwrap();
    let admin_db = Arc::new(
        connect(&url.replacen("postgres://postgres@", &format!("postgres://{admin}@"), 1)).await,
    );
    let api_db =
        connect(&url.replacen("postgres://postgres@", &format!("postgres://{api}@"), 1)).await;
    let set = "SELECT public.novu_rust_operator_jwt_set_permissions($1,'https://issuer.test','alice','console',$2,$3,false,false,false,$4)";
    let get = "SELECT public.novu_rust_operator_jwt_get_permissions($1,'https://issuer.test','alice','console')";
    assert!(
        admin_db
            .query(
                "SELECT * FROM public.novu_rust_operator_jwt_identities",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        admin_db
            .query(
                "SELECT * FROM public.novu_rust_operator_jwt_permission_audit",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        admin_db
            .query_one(set, &[&"admin-b", &true, &true, &"forbidden"])
            .await
            .is_err()
    );
    assert!(
        admin_db
            .query_one(set, &[&"admin-a", &true, &true, &"  "])
            .await
            .is_err()
    );
    assert!(
        admin_db
            .query_one(set, &[&"admin-a", &true, &true, &"initial"])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let identity: serde_json::Value = admin_db.query_one(get, &[&"admin-a"]).await.unwrap().get(0);
    assert_eq!(identity["subscriber_read"], true);
    assert!(
        !admin_db
            .query_one(set, &[&"admin-a", &true, &true, &"same"])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_operator_jwt_permission_audit WHERE environment_id='admin-a'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert!(api_db.query_one("SELECT public.novu_rust_operator_jwt_self('https://issuer.test','alice','console','admin-a',4102444800)", &[]).await.is_ok());
    // A failed audit write must roll back the permission change.
    db.batch_execute("CREATE FUNCTION public.reject_permission_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER reject_permission_audit BEFORE INSERT ON public.novu_rust_operator_jwt_permission_audit FOR EACH ROW EXECUTE FUNCTION public.reject_permission_audit()").await.unwrap();
    assert!(
        admin_db
            .query_one(set, &[&"admin-a", &false, &false, &"disable"])
            .await
            .is_err()
    );
    assert!(api_db.query_one("SELECT public.novu_rust_operator_jwt_self('https://issuer.test','alice','console','admin-a',4102444800)", &[]).await.is_ok());
    db.batch_execute("DROP TRIGGER reject_permission_audit ON public.novu_rust_operator_jwt_permission_audit; DROP FUNCTION public.reject_permission_audit()").await.unwrap();
    assert!(
        admin_db
            .query_one(set, &[&"admin-a", &false, &false, &"disable"])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(api_db.query_one("SELECT public.novu_rust_operator_jwt_self('https://issuer.test','alice','console','admin-a',4102444800)", &[]).await.is_err());
    let audit = db.query_one("SELECT previous,current,actor::text,reason FROM public.novu_rust_operator_jwt_permission_audit WHERE environment_id='admin-a' ORDER BY id DESC LIMIT 1", &[]).await.unwrap();
    let previous: serde_json::Value = audit.get(0);
    let current: serde_json::Value = audit.get(1);
    assert_eq!(previous["enabled"], true);
    assert_eq!(current["enabled"], false);
    assert_eq!(audit.get::<_, String>(2), admin);
    assert_eq!(audit.get::<_, String>(3), "disable");
    // A committed revocation wins when an administrator was waiting on the grant row.
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "DELETE FROM public.novu_rust_operator_jwt_admin_grants WHERE environment_id='admin-a'",
        &[],
    )
    .await
    .unwrap();
    let actor = admin_db.clone();
    let mut waiting = tokio::spawn(async move {
        actor
            .query_one(set, &[&"admin-a", &true, &true, &"revoked"])
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut waiting)
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert!(waiting.await.unwrap().is_err());
    assert!(admin_db.query_one(get, &[&"admin-a"]).await.is_err());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_operator_jwt_permission_audit WHERE environment_id='admin-a'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
}
