//! Restricted console credential isolation against disposable PostgreSQL.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tower::ServiceExt;

async fn call(app: axum::Router, path: &str, scheme: &str, token: &str) -> (StatusCode, Value) {
    let r = app
        .oneshot(
            Request::builder()
                .uri(path)
                .header("authorization", format!("{scheme} {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let code = r.status();
    let bytes = r.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}
#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL"]
async fn scoped_console_credentials_do_not_grant_api_key_access() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('ca','org'),('cb','org'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) SELECT 'ca','s-'||lpad(n::text,2,'0'),'id-'||n,jsonb_build_object('firstName','Alice','email','alice@example.test','secret','not-returned') FROM generate_series(1,25) n; INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('cb','bob','bob-id','{\"firstName\":\"Bob\"}'); INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier) VALUES ('ca','integration','fixture'); INSERT INTO novu_rust_fcm_devices(environment_id,subscriber_id,integration_internal_id,device_tokens) VALUES ('ca','s-01','integration',ARRAY['secret-device-token'])").await.unwrap();
    let token_a = "a".repeat(64);
    let token_b = "b".repeat(64);
    let token_expired = "c".repeat(64);
    for (token, env, expiration) in [
        (&token_a, "ca", "1 hour"),
        (&token_b, "cb", "1 hour"),
        (&token_expired, "ca", "-1 second"),
    ] {
        let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
        db.execute("INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at) VALUES ($1,$2,clock_timestamp()+$3::text::interval)",&[&hash,&env,&expiration]).await.unwrap();
    }
    assert!(db.execute("INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at) VALUES ($1,'ca','infinity')", &[&"f".repeat(64)]).await.is_err());
    let role = format!("console_{}", uuid::Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_console_subscribers(text,text,text,text),public.novu_rust_console_alerts(text),public.novu_rust_console_alerts_page(text,text,text) TO {role}")).await.unwrap();
    let (actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
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
            .query("SELECT * FROM public.novu_rust_subscribers", &[])
            .await
            .is_err()
    );
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(actor), None);
    let path = "/v1/experimental/console/subscribers?limit=20";
    let (status, first) = call(app.clone(), path, "SwallowConsole", &token_a).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["data"].as_array().unwrap().len(), 20);
    assert_eq!(first["next"], "s-20");
    assert_eq!(first["previous"], Value::Null);
    assert!(!first.to_string().contains("secret"));
    let (_, second) = call(
        app.clone(),
        "/v1/experimental/console/subscribers?after=s-20",
        "SwallowConsole",
        &token_a,
    )
    .await;
    assert_eq!(second["data"].as_array().unwrap().len(), 5);
    assert_eq!(second["previous"], "s-21");
    let (_, back) = call(
        app.clone(),
        "/v1/experimental/console/subscribers?before=s-21",
        "SwallowConsole",
        &token_a,
    )
    .await;
    assert_eq!(back["data"], first["data"]);
    let (_, other) = call(app.clone(), path, "SwallowConsole", &token_b).await;
    assert_eq!(other["data"][0]["subscriberId"], "bob");
    assert_eq!(other["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        call(app.clone(), path, "SwallowConsole", &token_expired)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(app.clone(), path, "ApiKey", &token_a).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(app.clone(), "/v2/subscribers", "SwallowConsole", &token_a)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            app.clone(),
            "/v1/experimental/console/subscribers?limit=21",
            "SwallowConsole",
            &token_a
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let alert_path = "/v1/experimental/console/alerts";
    assert_eq!(
        call(app.clone(), alert_path, "SwallowConsole", &token_a)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_console_alert_grants(credential_id) SELECT credential_id FROM public.novu_rust_console_credentials WHERE environment_id IN ('ca','cb'); INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) SELECT 'ca','slack','target-'||lpad(n::text,3,'0'),'unknown' FROM generate_series(1,101) n; INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('cb','email','other-env','rejected')").await.unwrap();
    let (status, alerts) = call(app.clone(), alert_path, "SwallowConsole", &token_a).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(alerts["data"].as_array().unwrap().len(), 100);
    assert_eq!(alerts["hasMore"], true);
    assert_eq!(alerts["next"]["channel"], "slack");
    assert_eq!(alerts["next"]["targetId"], "target-100");
    let (status, page2) = call(
        app.clone(),
        "/v1/experimental/console/alerts?afterChannel=slack&afterTarget=target-100",
        "SwallowConsole",
        &token_a,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page2["data"].as_array().unwrap().len(), 1);
    assert_eq!(page2["data"][0]["targetId"], "target-101");
    assert_eq!(page2["next"], Value::Null);
    assert_eq!(
        call(
            app.clone(),
            "/v1/experimental/console/alerts?afterChannel=slack",
            "SwallowConsole",
            &token_a
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            app.clone(),
            "/v1/experimental/console/alerts?afterChannel=other&afterTarget=x",
            "SwallowConsole",
            &token_a
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            app.clone(),
            "/v1/experimental/console/alerts?afterChannel=slack&afterTarget=%20",
            "SwallowConsole",
            &token_a
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );

    assert!(!alerts.to_string().contains("other-env"));
    assert!(!alerts.to_string().contains("attemptId"));
    let (_, other_alerts) = call(app.clone(), alert_path, "SwallowConsole", &token_b).await;
    assert_eq!(other_alerts["data"][0]["targetId"], "other-env");
    assert_eq!(
        call(app.clone(), alert_path, "ApiKey", &token_a).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            app.clone(),
            "/v1/experimental/console/alerts?env=cb",
            "SwallowConsole",
            &token_a
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert!(actor_direct_access_denied(&url, &role).await);
    let tx = db.transaction().await.unwrap();
    tx.execute("DELETE FROM public.novu_rust_console_alert_grants WHERE credential_id=(SELECT credential_id FROM public.novu_rust_console_credentials WHERE token_hash=$1)",&[&format!("{:x}", Sha256::digest(token_a.as_bytes()))]).await.unwrap();
    let pending_app = app.clone();
    let pending_token = token_a.clone();
    let pending = tokio::spawn(async move {
        call(pending_app, alert_path, "SwallowConsole", &pending_token)
            .await
            .0
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);

    let hash = format!("{:x}", Sha256::digest(token_a.as_bytes()));
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "UPDATE public.novu_rust_console_credentials SET subscriber_read=false WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    let request_app = app.clone();
    let request_token = token_a.clone();
    let pending = tokio::spawn(async move {
        call(
            request_app,
            "/v1/experimental/console/subscribers",
            "SwallowConsole",
            &request_token,
        )
        .await
        .0
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    db.execute(
        "UPDATE public.novu_rust_console_credentials SET subscriber_read=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    let hash = format!("{:x}", Sha256::digest(token_a.as_bytes()));
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "UPDATE public.novu_rust_console_credentials SET enabled=false WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    let request_app = app.clone();
    let pending = tokio::spawn(async move {
        call(
            request_app,
            "/v1/experimental/console/subscribers?limit=20",
            "SwallowConsole",
            &token_a,
        )
        .await
        .0
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
}

async fn actor_direct_access_denied(url: &str, role: &str) -> bool {
    let (actor, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    actor
        .query("SELECT * FROM public.novu_rust_console_alert_grants", &[])
        .await
        .is_err()
        && actor
            .query("SELECT * FROM public.novu_rust_alert_delivery_slots", &[])
            .await
            .is_err()
}
