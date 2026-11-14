//! Conditional subscriber profile management against a disposable migrated DB.
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

async fn request(
    app: axum::Router,
    method: &str,
    id: &str,
    body: Value,
    key: &str,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(format!("/v1/experimental/subscribers/{id}"))
                .header(header::AUTHORIZATION, format!("ApiKey {key}"))
                .header(header::CONTENT_TYPE, "application/json")
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
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn conditional_updates_fence_recreations_and_concurrent_profile_changes() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { conn.await.unwrap() });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('conditional-env','org'),('conditional-other','org'); INSERT INTO public.novu_rust_users VALUES('conditional-user'); INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES(encode(sha256(convert_to('conditional-key','UTF8')),'hex'),'conditional-env','conditional-user'),(encode(sha256(convert_to('other-key','UTF8')),'hex'),'conditional-other','conditional-user'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES('conditional-env','person',novu_rust_new_objectid(),'{\"firstName\":\"Alice\"}');").await.unwrap();
    let (other_db, other_conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { other_conn.await.unwrap() });
    let other_db = Arc::new(other_db);
    let app2 = novu_rust_api::router(other_db.clone());
    let app = novu_rust_api::router(Arc::new(db));
    let (status, current) =
        request(app.clone(), "GET", "person", json!({}), "conditional-key").await;
    assert_eq!(status, StatusCode::OK);
    let generation = current["data"]["generation"].as_str().unwrap();
    let revision = current["data"]["revision"].as_str().unwrap();
    assert_eq!(revision, "0");
    assert_eq!(current["data"]["profile"]["firstName"], "Alice");
    assert_eq!(
        request(app.clone(), "GET", "person", json!({}), "other-key")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let body = json!({"generation":generation,"revision":revision,"profile":{"firstName":"Bob"}});
    let (status, updated) = request(
        app.clone(),
        "PATCH",
        "person",
        body.clone(),
        "conditional-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["data"]["revision"], "1");
    assert_eq!(
        request(app.clone(), "PATCH", "person", body, "conditional-key")
            .await
            .0,
        StatusCode::CONFLICT
    );
    let back = json!({"generation":generation,"revision":"1","profile":{"firstName":"Alice"}});
    assert_eq!(
        request(app.clone(), "PATCH", "person", back, "conditional-key")
            .await
            .1["data"]["revision"],
        "2"
    );
    assert_eq!(
        request(
            app.clone(),
            "DELETE",
            "person",
            json!({"generation":generation,"revision":"0"}),
            "conditional-key"
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let a = json!({"generation":generation,"revision":"2","profile":{"firstName":"Carol"}});
    let b = json!({"generation":generation,"revision":"2","profile":{"firstName":"Dave"}});
    let (a, b) = tokio::join!(
        request(app.clone(), "PATCH", "person", a, "conditional-key"),
        request(app2.clone(), "PATCH", "person", b, "conditional-key")
    );
    let mut codes = [a.0.as_u16(), b.0.as_u16()];
    codes.sort();
    assert_eq!(codes, [200, 409]);
    let latest = request(app.clone(), "GET", "person", json!({}), "conditional-key")
        .await
        .1;
    // A legacy API profile edit also advances the revision through the DB trigger.
    let legacy = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/v2/subscribers/person")
                .header(header::AUTHORIZATION, "ApiKey conditional-key")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"phone":"123"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(legacy.status(), StatusCode::OK);
    assert_eq!(
        request(
            app.clone(),
            "DELETE",
            "person",
            json!({"generation":generation,"revision":latest["data"]["revision"]}),
            "conditional-key"
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    other_db.batch_execute("INSERT INTO public.novu_rust_in_app_messages(id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload) VALUES('00000000-0000-4000-8000-000000000234','conditional-env','person','fixture','fixture','text','{}')").await.unwrap();
    let latest = request(app.clone(), "GET", "person", json!({}), "conditional-key")
        .await
        .1;
    assert_eq!(
        request(
            app.clone(),
            "DELETE",
            "person",
            json!({"generation":generation,"revision":latest["data"]["revision"]}),
            "conditional-key"
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(other_db.query_one("SELECT count(*) FROM public.novu_rust_in_app_messages WHERE id='00000000-0000-4000-8000-000000000234'",&[]).await.unwrap().get::<_,i64>(0),0);
    // Recreate through the public create endpoint, then reject the old generation even at revision zero.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/subscribers?failIfExists=true")
                .header(header::AUTHORIZATION, "ApiKey conditional-key")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"subscriberId":"person","firstName":"New"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        request(
            app.clone(),
            "DELETE",
            "person",
            json!({"generation":generation,"revision":"0"}),
            "conditional-key"
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let new = request(app.clone(), "GET", "person", json!({}), "conditional-key")
        .await
        .1;
    assert_ne!(new["data"]["generation"], generation);
    assert_eq!(new["data"]["profile"]["firstName"], "New");
    assert_eq!(request(app.clone(),"PATCH","person",json!({"generation":new["data"]["generation"],"revision":"00","profile":{"firstName":"x"}}),"conditional-key").await.0,StatusCode::BAD_REQUEST);
}
