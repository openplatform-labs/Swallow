//! Uses a disposable PostgreSQL database and the actual management router.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tower::ServiceExt;

async fn request(
    app: axum::Router,
    method: &str,
    path: &str,
    key: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("ApiKey {key}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn fcm_metadata_management_is_scoped_authorized_and_audited() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    for sql in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::in_app::MIGRATION,
        novu_postgres_queue::in_app::WORKFLOW_PREFERENCES_MIGRATION,
        include_str!("../../../worker/rust-queue/migrations/0046_experimental_push_step.sql"),
        include_str!("../../../worker/rust-queue/migrations/0047_experimental_fcm_credentials.sql"),
        include_str!("../../../worker/rust-queue/migrations/0051_multiple_fcm_channels.sql"),
        include_str!("../migrations/0018_fcm_integration_management.sql"),
        include_str!("../migrations/0019_lock_active_fcm_integration.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute("INSERT INTO novu_rust_users VALUES ('u'); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')").await.unwrap();
    let hash = |key: &[u8]| format!("{:x}", Sha256::digest(key));
    let (a, b, read) = (hash(b"a-key"), hash(b"b-key"), hash(b"read-key"));
    db.execute("INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'a','u'),($2,'b','u'),($3,'a','u')", &[&a,&b,&read]).await.unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let (mut setup, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let path = "/v1/experimental/fcm-integrations";
    assert_eq!(
        request(
            app.clone(),
            "POST",
            path,
            "a-key",
            json!({"identifier":"main"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    setup.execute("INSERT INTO novu_rust_fcm_integration_permissions VALUES ($1,'read'),($1,'write'),($2,'read'),($2,'write'),($3,'read')", &[&a,&b,&read]).await.unwrap();
    let (status, created) = request(
        app.clone(),
        "POST",
        path,
        "a-key",
        json!({"identifier":"main"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["data"]["active"], false);
    let id = created["data"]["internalId"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 24);
    assert_eq!(
        request(
            app.clone(),
            "POST",
            path,
            "a-key",
            json!({"identifier":"main"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(
            app.clone(),
            "POST",
            path,
            "a-key",
            json!({"identifier":"other","credentials":{"secret":"do-not-store"}})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            app.clone(),
            "POST",
            path,
            "a-key",
            json!({"identifier":".."})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            app.clone(),
            "PATCH",
            &format!("{path}/main"),
            "read-key",
            json!({"active":true})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            app.clone(),
            "GET",
            &format!("{path}/main"),
            "b-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            app.clone(),
            "POST",
            path,
            "b-key",
            json!({"identifier":"main","active":true})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let (status, updated) = request(
        app.clone(),
        "PATCH",
        &format!("{path}/main"),
        "a-key",
        json!({"active":true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["data"]["active"], true);
    assert_eq!(setup.query_one("SELECT active FROM novu_rust_fcm_integrations WHERE environment_id='a' AND internal_id=$1", &[&id]).await.unwrap().get::<_,bool>(0),true);
    assert_eq!(
        request(
            app.clone(),
            "PATCH",
            &format!("{path}/main"),
            "a-key",
            json!({"active":false})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_rust_fcm_integration_audit WHERE environment_id='a' AND identifier='main' AND key_hash=$1 AND user_id='u'", &[&a]).await.unwrap().get::<_,i64>(0),3);
    let transitions=setup.query("SELECT old_active,new_active FROM novu_rust_fcm_integration_audit WHERE action='update' AND environment_id='a' ORDER BY created_at",&[]).await.unwrap();
    assert_eq!(
        (
            transitions[0].get::<_, bool>(0),
            transitions[0].get::<_, bool>(1)
        ),
        (false, true)
    );
    assert_eq!(
        (
            transitions[1].get::<_, bool>(0),
            transitions[1].get::<_, bool>(1)
        ),
        (true, false)
    );
    setup.batch_execute("INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) SELECT 'a','legacy-'||lpad(x::text,3,'0'),'legacy-'||x,false FROM generate_series(1,101) x").await.unwrap();
    let (_, page) = request(app.clone(), "GET", path, "a-key", Value::Null).await;
    assert_eq!(page["data"].as_array().unwrap().len(), 100);
    assert_eq!(page["hasMore"], true);
    let cursor = page["next"].as_str().unwrap();
    let (_, next) = request(
        app.clone(),
        "GET",
        &format!("{path}?after={cursor}"),
        "a-key",
        Value::Null,
    )
    .await;
    assert_eq!(next["data"].as_array().unwrap().len(), 2);
    assert_eq!(next["hasMore"], false);
    setup.batch_execute("CREATE ROLE fcm_api_limited NOLOGIN; GRANT USAGE ON SCHEMA public TO fcm_api_limited; GRANT SELECT ON novu_rust_api_keys,novu_rust_users TO fcm_api_limited; GRANT SELECT,INSERT,UPDATE ON novu_rust_fcm_integrations TO fcm_api_limited; GRANT INSERT ON novu_rust_fcm_integration_audit TO fcm_api_limited; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_fcm_integration_grant(text,text,text) TO fcm_api_limited").await.unwrap();
    let (limited, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    limited
        .batch_execute("SET ROLE fcm_api_limited")
        .await
        .unwrap();
    assert!(
        limited
            .execute(
                "INSERT INTO novu_rust_fcm_integration_permissions VALUES ('fake','write')",
                &[]
            )
            .await
            .is_err()
    );
    let limited_app = novu_rust_api::router_with_subscriber_secret(Arc::new(limited), None);
    assert_eq!(
        request(
            limited_app,
            "POST",
            path,
            "a-key",
            json!({"identifier":"limited"})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    setup.batch_execute("CREATE ROLE fcm_credential_limited NOLOGIN; GRANT USAGE ON SCHEMA public TO fcm_credential_limited; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_active_fcm_integration(text,text,text) TO fcm_credential_limited").await.unwrap();
    let other_id: String = setup.query_one("SELECT internal_id FROM novu_rust_fcm_integrations WHERE environment_id='b' AND identifier='main'", &[]).await.unwrap().get(0);
    let (limited, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    limited
        .batch_execute("SET ROLE fcm_credential_limited")
        .await
        .unwrap();
    assert!(
        limited
            .query_one(
                "SELECT public.novu_rust_lock_active_fcm_integration($1,$2,$3)",
                &[&"b", &other_id, &"main"]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        !limited
            .query_one(
                "SELECT public.novu_rust_lock_active_fcm_integration($1,$2,$3)",
                &[&"a", &other_id, &"main"]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        limited
            .query("SELECT * FROM novu_rust_fcm_integrations", &[])
            .await
            .is_err()
    );
    let tx = setup.transaction().await.unwrap();
    tx.execute("DELETE FROM novu_rust_fcm_integration_permissions WHERE key_hash=$1 AND permission='write'", &[&a]).await.unwrap();
    let pending = tokio::spawn(async move {
        request(
            app,
            "PATCH",
            "/v1/experimental/fcm-integrations/main",
            "a-key",
            json!({"active":true}),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap().0, StatusCode::FORBIDDEN);
    assert!(!setup.query_one("SELECT active FROM novu_rust_fcm_integrations WHERE environment_id='a' AND internal_id=$1", &[&id]).await.unwrap().get::<_,bool>(0));
}
