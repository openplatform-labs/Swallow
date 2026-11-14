//! Console endpoints reject ambiguous Authorization headers before querying the database.
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL connection"]
async fn every_console_endpoint_rejects_duplicate_credentials() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let app = novu_rust_api::router(Arc::new(db));
    for (method, path, body) in [
        ("GET", "/v1/experimental/console/subscribers", ""),
        ("GET", "/v1/experimental/console/alerts", ""),
        (
            "GET",
            "/v1/experimental/console/alerts/request?requestId=1889f88e-c0a3-4ab5-b861-2fdc1f26f254",
            "",
        ),
        (
            "GET",
            "/v1/experimental/console/alerts/decisions?channel=slack&targetId=ops&attemptId=1889f88e-c0a3-4ab5-b861-2fdc1f26f254",
            "",
        ),
        ("POST", "/v1/experimental/console/alerts/resolve", "{}"),
        ("POST", "/v1/experimental/console/alerts/resolve-v2", "{}"),
    ] {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(
                header::AUTHORIZATION,
                format!("SwallowConsole {}", "a".repeat(64)),
            )
            .header(
                header::AUTHORIZATION,
                format!("SwallowConsole {}", "b".repeat(64)),
            )
            .body(Body::from(body))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );
    }
}
