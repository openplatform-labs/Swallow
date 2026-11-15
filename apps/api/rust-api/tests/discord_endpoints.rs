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
#[ignore = "requires a fresh disposable PostgreSQL with role creation"]
async fn scoped_crud_revision_and_audit_are_atomic() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    for sql in [
        novu_rust_api::MIGRATION,
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::in_app::MIGRATION,
        "CREATE UNIQUE INDEX novu_subscriber_identity ON novu_rust_subscribers(environment_id,subscriber_id,internal_id)",
        include_str!("../../../worker/rust-queue/migrations/0202_discord_webhook_queue.sql"),
        include_str!("../../../worker/rust-queue/migrations/0203_discord_workflow.sql"),
        include_str!("../migrations/0117_discord_endpoint_permissions.sql"),
        include_str!("../migrations/0122_discord_endpoint_audit_read.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let role = format!("discord_api_{}", uuid::Uuid::new_v4().simple());
    let a = format!("{:x}", Sha256::digest(b"discord-key-a"));
    let b = format!("{:x}", Sha256::digest(b"discord-key-b"));
    db.batch_execute(&"CREATE ROLE discord_api_test NOLOGIN; INSERT INTO novu_rust_users VALUES('u'); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES('a','alice','old'),('b','alice','other'); INSERT INTO novu_rust_discord_destinations VALUES('a','one','hash'),('a','two','hash2'),('b','one','hash3'); GRANT SELECT ON novu_rust_users,novu_rust_api_keys,novu_rust_discord_destinations TO discord_api_test; GRANT SELECT,UPDATE ON novu_rust_subscribers TO discord_api_test; GRANT SELECT,INSERT,UPDATE,DELETE ON novu_rust_discord_workflow_endpoints TO discord_api_test; GRANT INSERT ON novu_rust_discord_endpoint_audit TO discord_api_test; GRANT EXECUTE ON FUNCTION novu_rust_lock_discord_endpoint_grant(text,text,text),novu_rust_discord_endpoint_audit_list(text,text,uuid) TO discord_api_test".replace("discord_api_test", &role)).await.unwrap();
    db.execute("INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'a','u'),($2,'b','u')", &[&a,&b]).await.unwrap();
    let (api, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    api.batch_execute(&format!("SET ROLE {role}"))
        .await
        .unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(api), None);
    let path = "/v1/experimental/discord-endpoints";
    let item = "/v1/experimental/discord-endpoints/ep";
    let input = json!({"identifier":"ep","subscriberId":"alice","destinationId":"one","context":{"tenant":"acme"}});
    assert_eq!(
        request(app.clone(), "POST", path, "discord-key-a", input.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    db.execute("INSERT INTO novu_rust_discord_endpoint_permissions VALUES($1,'read'),($1,'write'),($2,'read')", &[&a,&b]).await.unwrap();
    let (status, created) =
        request(app.clone(), "POST", path, "discord-key-a", input.clone()).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["data"]["contextKeys"], json!(["tenant:acme"]));
    assert!(created.to_string().find("hash").is_none());
    assert_eq!(
        request(app.clone(), "POST", path, "discord-key-a", input)
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(app.clone(), "GET", item, "discord-key-b", Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(app.clone(), "GET", path, "discord-key-a", Value::Null)
            .await
            .1["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let revision = created["data"]["revision"].as_str().unwrap();
    let update = json!({"expectedRevision":revision,"destinationId":"two","context":null});
    let (first, second) = tokio::join!(
        request(app.clone(), "PATCH", item, "discord-key-a", update.clone()),
        request(app.clone(), "PATCH", item, "discord-key-a", update)
    );
    assert!(
        (first.0 == StatusCode::OK && second.0 == StatusCode::NOT_FOUND)
            || (second.0 == StatusCode::OK && first.0 == StatusCode::NOT_FOUND)
    );
    let current = if first.0 == StatusCode::OK {
        first.1
    } else {
        second.1
    };
    let new_revision = current["data"]["revision"].as_str().unwrap();
    assert_ne!(new_revision, revision);
    let stale = format!("{item}?revision={revision}");
    assert_eq!(
        request(app.clone(), "DELETE", &stale, "discord-key-a", Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.batch_execute(&format!(
        "REVOKE INSERT ON novu_rust_discord_endpoint_audit FROM {role}"
    ))
    .await
    .unwrap();
    let valid = format!("{item}?revision={new_revision}");
    assert_eq!(
        request(app.clone(), "DELETE", &valid, "discord-key-a", Value::Null)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM novu_rust_discord_workflow_endpoints",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.batch_execute(&format!(
        "GRANT INSERT ON novu_rust_discord_endpoint_audit TO {role}"
    ))
    .await
    .unwrap();
    assert_eq!(
        request(app.clone(), "DELETE", &valid, "discord-key-a", Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT array_agg(action ORDER BY created_at) FROM novu_rust_discord_endpoint_audit",
            &[]
        )
        .await
        .unwrap()
        .get::<_, Vec<String>>(0),
        vec!["create", "update", "delete"]
    );
    let audit_path = "/v1/experimental/discord-endpoint-audit";
    assert_eq!(request(app.clone(), "GET", audit_path, "discord-key-a", Value::Null).await.0, StatusCode::FORBIDDEN);
    db.execute("INSERT INTO novu_rust_discord_endpoint_permissions VALUES($1,'audit')", &[&a]).await.unwrap();
    let (status, audit) = request(app.clone(), "GET", audit_path, "discord-key-a", Value::Null).await;
    assert_eq!(status, StatusCode::OK);assert_eq!(audit["data"].as_array().unwrap().len(), 3);
    assert!(audit.to_string().find(&a).is_none());
    assert_eq!(request(app.clone(), "GET", audit_path, "discord-key-b", Value::Null).await.0, StatusCode::FORBIDDEN);
    db.execute("INSERT INTO novu_rust_discord_endpoint_permissions VALUES($1,'audit')", &[&b]).await.unwrap();
    let other_audit = request(app.clone(), "GET", audit_path, "discord-key-b", Value::Null).await;
    assert_eq!(other_audit.0, StatusCode::OK);
    assert_eq!(other_audit.1["data"], json!([]));
    assert_eq!(request(app.clone(), "GET", &format!("{audit_path}?after=invalid"), "discord-key-a", Value::Null).await.0, StatusCode::BAD_REQUEST);
    db.batch_execute("INSERT INTO public.novu_rust_discord_endpoint_audit(id,environment_id,identifier,action,key_hash,user_id) SELECT gen_random_uuid(),'a','ep','create','hidden','u' FROM generate_series(1,101)").await.unwrap();
    let first=request(app.clone(),"GET",audit_path,"discord-key-a",Value::Null).await.1;
    assert_eq!(first["data"].as_array().unwrap().len(),100);
    let next=first["nextCursor"].as_str().unwrap();
    let second=request(app.clone(),"GET",&format!("{audit_path}?after={next}"),"discord-key-a",Value::Null).await.1;
    assert_eq!(second["data"].as_array().unwrap().len(),4);assert!(second["nextCursor"].is_null());
    db.execute("DELETE FROM novu_rust_discord_endpoint_permissions WHERE key_hash=$1 AND permission='audit'", &[&a]).await.unwrap();
    assert_eq!(request(app.clone(), "GET", audit_path, "discord-key-a", Value::Null).await.0, StatusCode::FORBIDDEN);
    db.execute("DELETE FROM novu_rust_discord_endpoint_permissions WHERE key_hash=$1 AND permission='read'",&[&a]).await.unwrap();
    assert_eq!(
        request(app.clone(), "GET", path, "discord-key-a", Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            app,
            "POST",
            path,
            "discord-key-b",
            json!({"identifier":"other","subscriberId":"alice","destinationId":"one"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .await
        .unwrap();
}
