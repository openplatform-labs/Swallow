//! Disposable PostgreSQL; verifies management API grant revocation ordering.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db
}
async fn put(app: axum::Router) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/v1/experimental/workflows/w/email-provider")
            .header("authorization", "ApiKey fixture-key")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"provider":"resend"}"#))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
async fn get(app: axum::Router) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method("GET")
            .uri("/v1/experimental/workflows/w/email-provider")
            .header("authorization", "ApiKey fixture-key")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
async fn waiting(admin: &Client, pid: i32) {
    for _ in 0..200 {
        if admin
            .query_one(
                "SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1",
                &[&pid],
            )
            .await
            .unwrap()
            .get::<_, Option<bool>>(0)
            == Some(true)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("request did not reach expected row lock");
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn authorized_change_and_revocation_are_serialized() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let admin = connect(&url).await;
    for sql in [
        novu_rust_api::MIGRATION,
        include_str!("../../../worker/rust-queue/migrations/0061_workflow_email_provider.sql"),
        include_str!("../migrations/0011_workflow_provider_permissions.sql"),
        include_str!("../migrations/0014_workflow_provider_auth_lock.sql"),
        include_str!("../migrations/0015_workflow_provider_audit.sql"),
    ] {
        admin.batch_execute(sql).await.unwrap();
    }
    admin
        .batch_execute(include_str!(
            "../migrations/0014_workflow_provider_auth_lock.sql"
        ))
        .await
        .unwrap();
    let hash = format!("{:x}", Sha256::digest(b"fixture-key"));
    admin.batch_execute("INSERT INTO novu_rust_users VALUES ('u'); INSERT INTO novu_rust_environments VALUES ('e','org'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps) VALUES ('e','w',true,true)").await.unwrap();
    admin
        .execute(
            "INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'e','u')",
            &[&hash],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO novu_rust_workflow_provider_permissions VALUES ($1,'write'),($1,'read')",
            &[&hash],
        )
        .await
        .unwrap();
    let role = format!("api_test_{}", uuid::Uuid::new_v4().simple());
    admin.batch_execute(&format!("CREATE ROLE {role}; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON novu_rust_users,novu_rust_api_keys,novu_rust_workflow_provider_permissions,novu_rust_workflows TO {role}; GRANT UPDATE(email_provider) ON novu_rust_workflows TO {role}")).await.unwrap();
    let actor = connect(&url).await;
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        actor
            .query_one(
                "SELECT public.novu_rust_lock_workflow_provider_grant($1,'e','write')",
                &[&hash]
            )
            .await
            .is_err()
    );
    assert!(
        actor
            .execute(
                "DELETE FROM novu_rust_workflow_provider_permissions WHERE key_hash=$1",
                &[&hash]
            )
            .await
            .is_err()
    );
    admin.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_lock_workflow_provider_grant(text,text,text) TO {role}")).await.unwrap();
    // The API role cannot write audit rows yet: the attempted update must roll back.
    let app = novu_rust_api::router(Arc::new(actor));
    assert_eq!(put(app.clone()).await, StatusCode::INTERNAL_SERVER_ERROR);
    let provider: String = admin.query_one("SELECT email_provider FROM novu_rust_workflows WHERE environment_id='e' AND identifier='w'", &[]).await.unwrap().get(0);
    assert_eq!(provider, "sendgrid");
    let audit_count: i64 = admin
        .query_one(
            "SELECT count(*) FROM public.novu_rust_workflow_provider_audit",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(audit_count, 0);
    admin
        .batch_execute(&format!(
            "GRANT INSERT ON public.novu_rust_workflow_provider_audit TO {role}"
        ))
        .await
        .unwrap();
    // Recreate the restricted session to retain the backend PID for lock tests.
    drop(app);
    let actor = connect(&url).await;
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        actor
            .query(
                "SELECT * FROM public.novu_rust_workflow_provider_audit",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        actor
            .execute("DELETE FROM public.novu_rust_workflow_provider_audit", &[])
            .await
            .is_err()
    );
    let pid: i32 = actor
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let app = novu_rust_api::router(Arc::new(actor));
    let mut locker = connect(&url).await;
    let lock = locker.transaction().await.unwrap();
    lock.query_one("SELECT identifier FROM novu_rust_workflows WHERE environment_id='e' AND identifier='w' FOR UPDATE", &[]).await.unwrap();
    let change = tokio::spawn(put(app.clone()));
    waiting(&admin, pid).await;
    let revoker = connect(&url).await;
    let revoke_hash = hash.clone();
    let mut revoke = tokio::spawn(async move {
        revoker.execute("DELETE FROM novu_rust_workflow_provider_permissions WHERE key_hash=$1 AND permission='write'", &[&revoke_hash]).await.unwrap()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut revoke)
            .await
            .is_err(),
        "revocation overtook authorized operation"
    );
    lock.commit().await.unwrap();
    assert_eq!(change.await.unwrap(), StatusCode::OK);
    assert_eq!(revoke.await.unwrap(), 1);
    assert_eq!(put(app.clone()).await, StatusCode::FORBIDDEN);
    let row = admin.query_one("SELECT environment_id,workflow_identifier,api_key_hash,user_id,previous_provider,new_provider FROM public.novu_rust_workflow_provider_audit", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "e");
    assert_eq!(row.get::<_, String>(1), "w");
    assert_eq!(row.get::<_, String>(2), hash);
    assert_eq!(row.get::<_, String>(3), "u");
    assert_eq!(row.get::<_, String>(4), "sendgrid");
    assert_eq!(row.get::<_, String>(5), "resend");
    // Every authorized PUT is audited, including an unchanged value.
    admin
        .execute(
            "INSERT INTO novu_rust_workflow_provider_permissions VALUES ($1,'write')",
            &[&hash],
        )
        .await
        .unwrap();
    assert_eq!(put(app.clone()).await, StatusCode::OK);
    let noop:i64=admin.query_one("SELECT count(*) FROM public.novu_rust_workflow_provider_audit WHERE previous_provider='resend' AND new_provider='resend'", &[]).await.unwrap().get(0);
    assert_eq!(noop, 1);
    admin.execute("DELETE FROM novu_rust_workflow_provider_permissions WHERE key_hash=$1 AND permission='write'", &[&hash]).await.unwrap();

    // If revocation has locked the grant before the mutation, the mutation must
    // wait and then reject after commit, even if its initial read saw the grant.
    admin
        .execute(
            "INSERT INTO novu_rust_workflow_provider_permissions VALUES ($1,'write')",
            &[&hash],
        )
        .await
        .unwrap();
    let mut revoker = connect(&url).await;
    let tx = revoker.transaction().await.unwrap();
    tx.execute("DELETE FROM novu_rust_workflow_provider_permissions WHERE key_hash=$1 AND permission='write'", &[&hash]).await.unwrap();
    let change = tokio::spawn(put(app.clone()));
    waiting(&admin, pid).await;
    tx.commit().await.unwrap();
    assert_eq!(change.await.unwrap(), StatusCode::NOT_FOUND);
    let value:String=admin.query_one("SELECT email_provider FROM novu_rust_workflows WHERE environment_id='e' AND identifier='w'", &[]).await.unwrap().get(0);
    assert_eq!(value, "resend");
    let tx = revoker.transaction().await.unwrap();
    tx.execute("DELETE FROM novu_rust_workflow_provider_permissions WHERE key_hash=$1 AND permission='read'", &[&hash]).await.unwrap();
    let read = tokio::spawn(get(app.clone()));
    waiting(&admin, pid).await;
    tx.commit().await.unwrap();
    assert_eq!(read.await.unwrap(), StatusCode::NOT_FOUND);
    assert_eq!(get(app.clone()).await, StatusCode::FORBIDDEN);

    admin
        .execute(
            "INSERT INTO novu_rust_workflow_provider_permissions VALUES ($1,'write')",
            &[&hash],
        )
        .await
        .unwrap();
    let lock = locker.transaction().await.unwrap();
    lock.query_one("SELECT identifier FROM novu_rust_workflows WHERE environment_id='e' AND identifier='w' FOR UPDATE", &[]).await.unwrap();
    let change = tokio::spawn(put(app.clone()));
    waiting(&admin, pid).await;
    let disabler = connect(&url).await;
    let disable_hash = hash.clone();
    let mut disable = tokio::spawn(async move {
        disabler
            .execute(
                "UPDATE novu_rust_api_keys SET enabled=false WHERE key_hash=$1",
                &[&disable_hash],
            )
            .await
            .unwrap()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut disable)
            .await
            .is_err()
    );
    lock.commit().await.unwrap();
    assert_eq!(change.await.unwrap(), StatusCode::OK);
    assert_eq!(disable.await.unwrap(), 1);
    assert_eq!(put(app.clone()).await, StatusCode::UNAUTHORIZED);

    admin
        .execute(
            "UPDATE novu_rust_api_keys SET enabled=true WHERE key_hash=$1",
            &[&hash],
        )
        .await
        .unwrap();
    let lock = locker.transaction().await.unwrap();
    lock.query_one("SELECT identifier FROM novu_rust_workflows WHERE environment_id='e' AND identifier='w' FOR UPDATE", &[]).await.unwrap();
    let change = tokio::spawn(put(app.clone()));
    waiting(&admin, pid).await;
    let deleter = connect(&url).await;
    let mut delete = tokio::spawn(async move {
        deleter
            .execute("DELETE FROM novu_rust_users WHERE id='u'", &[])
            .await
            .unwrap()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut delete)
            .await
            .is_err()
    );
    lock.commit().await.unwrap();
    assert_eq!(change.await.unwrap(), StatusCode::OK);
    assert_eq!(delete.await.unwrap(), 1);
    assert_eq!(put(app).await, StatusCode::UNAUTHORIZED);
}
