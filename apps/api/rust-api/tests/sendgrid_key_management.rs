//! Uses a disposable PostgreSQL database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use http_body_util::BodyExt;
use ring::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::NoTls;
use tower::ServiceExt;

fn key_pair() -> (String, EcdsaKeyPair) {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng).unwrap();
    let key =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let mut der = vec![
        0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08,
        0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
    ];
    der.extend_from_slice(key.public_key().as_ref());
    (STANDARD.encode(der), key)
}
fn public_key() -> String {
    key_pair().0
}
async fn signed_webhook(app: axum::Router, key: &EcdsaKeyPair) -> StatusCode {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let raw = b"invalid-json";
    let mut signed = timestamp.as_bytes().to_vec();
    signed.extend_from_slice(raw);
    let signature = STANDARD.encode(key.sign(&SystemRandom::new(), &signed).unwrap().as_ref());
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/webhooks/organizations/org/environments/a/email/sendgrid")
            .header("x-twilio-email-event-webhook-timestamp", timestamp)
            .header("x-twilio-email-event-webhook-signature", signature)
            .body(Body::from(raw.as_slice()))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
async fn call(
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
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn scoped_key_rotation_and_concurrent_limit() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    db.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        .await
        .unwrap();
    for sql in [
        novu_rust_api::MIGRATION,
        include_str!("../migrations/0004_sendgrid_webhooks.sql"),
        include_str!("../migrations/0064_sendgrid_key_management.sql"),
        include_str!("../migrations/0065_sendgrid_key_audit_access.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute("INSERT INTO novu_rust_users VALUES ('u'); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')").await.unwrap();
    let hash = |key: &str| format!("{:x}", Sha256::digest(key.as_bytes()));
    let (a, b, r) = (hash("a-key"), hash("b-key"), hash("read-key"));
    db.execute("INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'a','u'),($2,'b','u'),($3,'a','u')", &[&a,&b,&r]).await.unwrap();
    db.execute("INSERT INTO novu_rust_sendgrid_key_permissions VALUES ($1,'read'),($1,'write'),($2,'read'),($2,'write'),($3,'read')", &[&a,&b,&r]).await.unwrap();
    let db = Arc::new(db);
    let app = novu_rust_api::router_with_subscriber_secret(db.clone(), None);
    let base = "/v1/experimental/sendgrid-webhook-keys";
    let (first, first_signing_key) = key_pair();
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "read-key",
            json!({"identifier":"first","publicKey":first})
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
            json!({"identifier":"first","publicKey":"Zm9v"})
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
            json!({"identifier":"first","publicKey":first,"secret":"ignored"})
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
            json!({"identifier":"first","publicKey":first})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        signed_webhook(app.clone(), &first_signing_key).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "b-key",
            json!({"identifier":"other","publicKey":first})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let listed = call(app.clone(), "GET", base, "read-key", json!({})).await;
    assert_eq!(listed.0, StatusCode::OK);
    assert_eq!(
        listed.1["data"],
        json!([{"identifier":"first","active":true}])
    );
    assert!(!listed.1.to_string().contains(&first));
    assert_eq!(
        call(
            app.clone(),
            "DELETE",
            &format!("{base}/first"),
            "b-key",
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for i in 0..8 {
        assert_eq!(
            call(
                app.clone(),
                "POST",
                base,
                "a-key",
                json!({"identifier":format!("key{i}"),"publicKey":public_key()})
            )
            .await
            .0,
            StatusCode::CREATED
        );
    }
    let (other, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    let other_app = novu_rust_api::router_with_subscriber_secret(Arc::new(other), None);
    let (one, two) = tokio::join!(
        call(
            app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"last1","publicKey":public_key()})
        ),
        call(
            other_app,
            "POST",
            base,
            "a-key",
            json!({"identifier":"last2","publicKey":public_key()})
        )
    );
    assert!(matches!(
        (one.0, two.0),
        (StatusCode::CREATED, StatusCode::CONFLICT) | (StatusCode::CONFLICT, StatusCode::CREATED)
    ));
    assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_sendgrid_webhook_keys WHERE environment_id='a' AND active",&[]).await.unwrap().get::<_,i64>(0),10);
    assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_sendgrid_key_audit WHERE environment_id='a' AND action='create'",&[]).await.unwrap().get::<_,i64>(0),10);
    assert_eq!(
        call(
            app.clone(),
            "DELETE",
            &format!("{base}/first"),
            "a-key",
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        signed_webhook(app.clone(), &first_signing_key).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_sendgrid_key_audit WHERE environment_id='a' AND action='delete'",&[]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("CREATE FUNCTION public.reject_key_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$; CREATE TRIGGER reject_key_audit BEFORE INSERT ON public.novu_rust_sendgrid_key_audit FOR EACH ROW EXECUTE FUNCTION public.reject_key_audit()").await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"rollback","publicKey":public_key()})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_sendgrid_webhook_keys WHERE environment_id='a' AND key_id='rollback'",&[]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("DROP TRIGGER reject_key_audit ON public.novu_rust_sendgrid_key_audit; CREATE ROLE sendgrid_key_api LOGIN; GRANT USAGE ON SCHEMA public TO sendgrid_key_api; GRANT SELECT ON public.novu_rust_api_keys,public.novu_rust_users TO sendgrid_key_api; GRANT EXECUTE ON FUNCTION public.novu_rust_sendgrid_keys_list(text,text,text),public.novu_rust_sendgrid_key_change(text,text,text,text,text,uuid),public.novu_rust_sendgrid_key_audit_list(text,text,uuid) TO sendgrid_key_api").await.unwrap();
    let (limited, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        driver.await.unwrap();
    });
    limited
        .batch_execute("SET SESSION AUTHORIZATION sendgrid_key_api")
        .await
        .unwrap();
    assert!(
        limited
            .query_one(
                "SELECT count(*) FROM public.novu_rust_sendgrid_webhook_keys",
                &[]
            )
            .await
            .is_err()
    );
    let limited_app = novu_rust_api::router_with_subscriber_secret(Arc::new(limited), None);
    assert_eq!(
        call(limited_app.clone(), "GET", base, "a-key", json!({}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            limited_app.clone(),
            "POST",
            base,
            "a-key",
            json!({"identifier":"limited","publicKey":public_key()})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            limited_app.clone(),
            "DELETE",
            &format!("{base}/limited"),
            "a-key",
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
    let audit_path = "/v1/experimental/sendgrid-webhook-key-audit".to_string();
    assert_eq!(
        call(
            limited_app.clone(),
            "GET",
            &audit_path,
            "read-key",
            json!({})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "INSERT INTO novu_rust_sendgrid_key_permissions VALUES ($1,'audit')",
        &[&r],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_sendgrid_key_audit SELECT gen_random_uuid(),'a','fixture','create',$1,'u',clock_timestamp() FROM generate_series(1,105)", &[&a]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_sendgrid_key_audit VALUES (gen_random_uuid(),'b','other','create',$1,'u',clock_timestamp())", &[&b]).await.unwrap();
    let page = call(
        limited_app.clone(),
        "GET",
        &audit_path,
        "read-key",
        json!({}),
    )
    .await;
    assert_eq!(page.0, StatusCode::OK);
    assert_eq!(page.1["data"].as_array().unwrap().len(), 100);
    assert!(!page.1.to_string().contains(&a));
    assert!(!page.1.to_string().contains("other"));
    let next = page.1["nextCursor"].as_str().unwrap();
    let second = call(
        limited_app.clone(),
        "GET",
        &format!("{audit_path}?after={next}"),
        "read-key",
        json!({}),
    )
    .await;
    assert_eq!(second.0, StatusCode::OK);
    assert!(!second.1["data"].as_array().unwrap().is_empty());
    assert!(second.1["nextCursor"].is_null());
    assert_eq!(
        call(
            limited_app.clone(),
            "GET",
            &format!("{audit_path}?after=invalid"),
            "read-key",
            json!({})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    db.execute(
        "DELETE FROM novu_rust_sendgrid_key_permissions WHERE key_hash=$1",
        &[&r],
    )
    .await
    .unwrap();
    assert_eq!(
        call(limited_app, "GET", &audit_path, "read-key", json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(app, "GET", base, "read-key", json!({})).await.0,
        StatusCode::FORBIDDEN
    );
}
