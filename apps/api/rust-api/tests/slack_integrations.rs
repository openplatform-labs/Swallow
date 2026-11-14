//! API metadata CRUD against PostgreSQL with scoped permissions and auditing.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc, time::Duration};
use tokio_postgres::NoTls;
use tower::ServiceExt;

async fn call(
    app: axum::Router,
    method: &str,
    path: &str,
    key: &str,
    body: Value,
) -> (StatusCode, Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("ApiKey {key}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn slack_metadata_is_scoped_audited_and_safe_to_delete() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) =
        tokio_postgres::connect(&format!("{url}?application_name=slack_metadata_api"), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (dir, start, end) in [
        (root.join("migrations"), 1, 17),
        (root.join("../../worker/rust-queue/migrations"), 1, u32::MAX),
        (root.join("migrations"), 18, u32::MAX),
    ] {
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect();
        files.sort();
        for file in files {
            let name = file.file_name().unwrap().to_str().unwrap();
            let n = name[..4].parse::<u32>().unwrap();
            if (start..=end).contains(&n) {
                db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
            }
        }
    }
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('u'); INSERT INTO public.novu_rust_environments VALUES ('a','org'),('b','org'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('a','alice','alice-id')").await.unwrap();
    let hash = |s: &[u8]| format!("{:x}", Sha256::digest(s));
    let (a, b, read) = (hash(b"a-key"), hash(b"b-key"), hash(b"read-key"));
    db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'a','u'),($2,'b','u'),($3,'a','u')",&[&a,&b,&read]).await.unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let (setup, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let base = "/v1/experimental/slack-integrations";
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"main"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    setup.execute("INSERT INTO public.novu_rust_slack_integration_permissions VALUES ($1,'read'),($1,'write'),($2,'read'),($2,'write'),($3,'read')",&[&a,&b,&read]).await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "read-key",
            json!({"identifier":"main"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"main","token":"do-not-store"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"main"})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"main"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(app.clone(), "GET", base, "b-key", Value::Null).await.1["data"],
        json!([])
    );
    let nested = format!("{base}/main/connections");
    assert_eq!(
        call(
            app.clone(),
            "GET",
            &format!("{base}/main"),
            "read-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "GET",
            &format!("{base}/main"),
            "b-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(app.clone(), "GET", &nested, "b-key", Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            &nested,
            "a-key",
            json!({"identifier":"connection"})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        call(app.clone(), "GET", &nested, "a-key", Value::Null)
            .await
            .1["data"],
        json!(["connection"])
    );
    assert_eq!(
        call(
            app.clone(),
            "GET",
            &format!("{nested}/connection"),
            "read-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "DELETE",
            &format!("{base}/main"),
            "a-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    setup.batch_execute("INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,connection_identifier,endpoint_type,endpoint) VALUES ('a','endpoint','alice','alice-id','main','connection','slack_channel','{\"channelId\":\"C123\"}')").await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "DELETE",
            &format!("{nested}/connection"),
            "a-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM public.novu_rust_slack_integration_audit WHERE environment_id='a'",&[]).await.unwrap().get::<_,i64>(0),2);
    // The restricted API DB login cannot read permissions or audit entries.
    let role = format!("slack_meta_{}", uuid::Uuid::new_v4().simple());
    setup.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.novu_rust_users,public.novu_rust_api_keys,public.novu_rust_slack_endpoint_integrations,public.novu_rust_slack_endpoint_connections TO {role}; GRANT INSERT,DELETE ON public.novu_rust_slack_endpoint_integrations,public.novu_rust_slack_endpoint_connections TO {role}; GRANT INSERT ON public.novu_rust_slack_integration_audit TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_slack_integration_grant(text,text,text) TO {role}")).await.unwrap();
    let (limited, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    limited
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        limited
            .query(
                "SELECT * FROM public.novu_rust_slack_integration_permissions",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        limited
            .query(
                "SELECT * FROM public.novu_rust_slack_integration_audit",
                &[]
            )
            .await
            .is_err()
    );
    let limited_app = novu_rust_api::router_with_subscriber_secret(Arc::new(limited), None);
    assert_eq!(
        call(
            limited_app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"restricted"})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            limited_app,
            "DELETE",
            &format!("{base}/restricted"),
            "a-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    setup.batch_execute("INSERT INTO public.novu_rust_slack_endpoint_integrations VALUES ('a','rollback-delete'); ALTER TABLE public.novu_rust_slack_integration_audit ADD CONSTRAINT fixture_reject CHECK (false) NOT VALID").await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"rollback"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM public.novu_rust_slack_endpoint_integrations WHERE environment_id='a' AND identifier='rollback'",&[]).await.unwrap().get::<_,i64>(0),0);
    assert_eq!(
        call(
            app.clone(),
            "DELETE",
            &format!("{base}/rollback-delete"),
            "a-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM public.novu_rust_slack_endpoint_integrations WHERE environment_id='a' AND identifier='rollback-delete'",&[]).await.unwrap().get::<_,i64>(0),1);
    setup.batch_execute("ALTER TABLE public.novu_rust_slack_integration_audit DROP CONSTRAINT fixture_reject; DELETE FROM public.novu_rust_slack_endpoint_integrations WHERE environment_id='a' AND identifier='rollback-delete'; INSERT INTO public.novu_rust_slack_endpoint_integrations(environment_id,identifier) SELECT 'a','item-'||lpad(i::text,3,'0') FROM generate_series(1,101) i").await.unwrap();
    let first = call(app.clone(), "GET", base, "a-key", Value::Null).await.1;
    assert_eq!(first["data"].as_array().unwrap().len(), 100);
    assert_eq!(first["hasMore"], true);
    let cursor = first["next"].as_str().unwrap();
    let second = call(
        app.clone(),
        "GET",
        &format!("{base}?after={cursor}"),
        "a-key",
        Value::Null,
    )
    .await
    .1;
    assert_eq!(second["data"].as_array().unwrap().len(), 2);
    // Legacy DB identifiers can exceed the new creation limit but still work as cursors.
    assert_eq!(
        call(
            app.clone(),
            "GET",
            &format!("{base}?after={}", "z".repeat(200)),
            "a-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    setup.execute("DELETE FROM public.novu_rust_slack_channel_endpoints WHERE environment_id='a' AND identifier='endpoint'",&[]).await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "DELETE",
            &format!("{nested}/connection"),
            "a-key",
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    // The real HTTP mutation holds the write grant while blocked on an integration row.
    let (mut holder, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let tx = holder.transaction().await.unwrap();
    tx.query_one("SELECT 1 FROM public.novu_rust_slack_endpoint_integrations WHERE environment_id='a' AND identifier='main' FOR UPDATE",&[]).await.unwrap();
    let task_app = app.clone();
    let task = tokio::spawn(async move {
        call(
            task_app,
            "DELETE",
            "/v1/experimental/slack-integrations/main",
            "a-key",
            Value::Null,
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if setup.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='slack_metadata_api' AND wait_event_type='Lock')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(waiting);
    let (revoker, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let key = a.clone();
    let revoke = tokio::spawn(async move {
        revoker.execute("DELETE FROM public.novu_rust_slack_integration_permissions WHERE key_hash=$1 AND permission='write'",&[&key]).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!revoke.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(task.await.unwrap().0, StatusCode::OK);
    assert_eq!(revoke.await.unwrap(), 1);
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"after-revoke"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}
