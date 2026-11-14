//! Database-scoped console credential administration on disposable PostgreSQL.
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and role creation"]
async fn admin_is_scoped_audited_and_revocation_waits_for_inflight_query() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES ('admin-a','org'),('admin-b','org')",
    )
    .await
    .unwrap();
    let role = format!("console_admin_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_issue_console_credential(text,text,timestamptz,text),public.novu_rust_revoke_console_credential(text,uuid,text),public.novu_rust_list_console_credentials(text,uuid),public.novu_rust_list_console_credential_audit(text,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_console_credential_admins(environment_id,database_principal,principal_oid) SELECT 'admin-a',rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let actor = connect(&url).await;
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        actor
            .query("SELECT * FROM public.novu_rust_console_credentials", &[])
            .await
            .is_err()
    );
    assert!(
        actor
            .query(
                "SELECT * FROM public.novu_rust_console_credential_audit",
                &[]
            )
            .await
            .is_err()
    );
    db.batch_execute("CREATE FUNCTION public.console_test_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.reason='inject-failure' THEN RAISE EXCEPTION 'fixture'; END IF; RETURN NEW; END $$; CREATE TRIGGER console_test_reject_audit BEFORE INSERT ON public.novu_rust_console_credential_audit FOR EACH ROW EXECUTE FUNCTION public.console_test_reject_audit()").await.unwrap();
    let rejected_hash = "b".repeat(64);
    assert!(actor.query_one("SELECT public.novu_rust_issue_console_credential('admin-a',$1,clock_timestamp()+interval '1 hour','inject-failure')", &[&rejected_hash]).await.is_err());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_console_credentials WHERE token_hash=$1",
            &[&rejected_hash]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let hash = "a".repeat(64);
    assert!(actor.query_one("SELECT public.novu_rust_issue_console_credential('admin-b',$1,clock_timestamp()+interval '1 hour','ticket')",&[&hash]).await.is_err());
    assert!(actor.query_one("SELECT public.novu_rust_issue_console_credential('admin-a',$1,clock_timestamp()+interval '31 days','ticket')",&[&hash]).await.is_err());
    let row=actor.query_one("SELECT public.novu_rust_issue_console_credential('admin-a',$1,clock_timestamp()+interval '1 hour','ticket')",&[&hash]).await.unwrap();
    let id: Uuid = row.get(0);
    let entries = actor
        .query(
            "SELECT * FROM public.novu_rust_list_console_credentials('admin-a',NULL)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].get::<_, Uuid>(0), id);
    assert!(
        actor
            .query(
                "SELECT * FROM public.novu_rust_list_console_credentials('admin-b',NULL)",
                &[]
            )
            .await
            .is_err()
    );
    let audits = actor
        .query(
            "SELECT * FROM public.novu_rust_list_console_credential_audit('admin-a',0)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].get::<_, String>(2), "issue");
    assert_eq!(audits[0].get::<_, String>(3), role);
    assert!(
        actor
            .query_one(
                "SELECT public.novu_rust_revoke_console_credential('admin-a',$1,'inject-failure')",
                &[&id]
            )
            .await
            .is_err()
    );
    assert!(
        db.query_one(
            "SELECT enabled FROM public.novu_rust_console_credentials WHERE credential_id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    // A read that already holds the credential lock must finish before revocation commits.
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT 1 FROM public.novu_rust_console_credentials WHERE credential_id=$1 FOR SHARE",
        &[&id],
    )
    .await
    .unwrap();
    let pending = tokio::spawn(async move {
        let result = actor
            .query_one(
                "SELECT public.novu_rust_revoke_console_credential('admin-a',$1,'reviewed')",
                &[&id],
            )
            .await;
        (actor, result)
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    let (actor, result) = pending.await.unwrap();
    assert!(result.unwrap().get::<_, bool>(0));
    assert!(
        !actor
            .query_one(
                "SELECT public.novu_rust_revoke_console_credential('admin-a',$1,'duplicate')",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let audit = actor
        .query(
            "SELECT * FROM public.novu_rust_list_console_credential_audit('admin-a',0)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[1].get::<_, String>(2), "revoke");
    let tx = db.transaction().await.unwrap();
    tx.execute("DELETE FROM public.novu_rust_console_credential_admins WHERE environment_id='admin-a' AND database_principal=$1", &[&role]).await.unwrap();
    let waiting = tokio::spawn(async move {
        actor
            .query(
                "SELECT * FROM public.novu_rust_list_console_credential_audit('admin-a',0)",
                &[],
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    tx.commit().await.unwrap();
    assert!(waiting.await.unwrap().is_err());
}
