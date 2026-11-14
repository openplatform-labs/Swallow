//! Operator JWT permission review endpoints against disposable PostgreSQL.
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
const PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-private.pem");
const PUBLIC: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-public.pem");
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
fn token(subject: &str, exp: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("key".into());
    encode(&h,&json!({"iss":"https://issuer.test","sub":subject,"aud":"swallow","exp":exp,"iat":now-1,"jti":"fixture","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
}
fn request(path: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}
async fn body(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn review_requires_independent_scopes_and_revocation_fences() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    let role = format!("review_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_review_identity(text,text,text,text,bigint,text,text,text),public.novu_rust_operator_jwt_review_audit(text,text,text,text,bigint,bigint) TO {role}; INSERT INTO public.novu_rust_environments VALUES('review-a','org'),('review-b','org'); INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='{role}'; INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id,subscriber_read) VALUES('https://issuer.test','reviewer','console','review-a',false),('https://issuer.test','auditor','console','review-a',false),('https://issuer.test','target','console','review-a',true),('https://issuer.test','target','console','review-b',false); INSERT INTO public.novu_rust_operator_jwt_permission_review_grants(issuer,subject,client_id,environment_id,identity_read,audit_read) VALUES('https://issuer.test','reviewer','console','review-a',true,false),('https://issuer.test','auditor','console','review-a',false,true); INSERT INTO public.novu_rust_operator_jwt_permission_audit(environment_id,issuer,subject,client_id,actor,actor_oid,reason,current) VALUES('review-a','https://issuer.test','target','console','postgres',1,'ticket','{{}}'),('review-b','other','private','console','postgres',1,'private','{{}}');")).await.unwrap();
    let api_db =
        connect(&url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1)).await;
    assert!(
        api_db
            .query(
                "SELECT * FROM public.novu_rust_operator_jwt_permission_review_grants",
                &[]
            )
            .await
            .is_err()
    );
    let verifier = Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    );
    let app = novu_rust_api::router(Arc::new(api_db)).layer(axum::Extension(verifier));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let reviewer = token("reviewer", now + 120);
    let auditor = token("auditor", now + 120);
    let identity = "/v1/experimental/operator/permissions?environmentId=review-a&issuer=https%3A%2F%2Fissuer.test&subject=target&clientId=console";
    let audit = "/v1/experimental/operator/permissions/audit?environmentId=review-a";
    let (status, result) = body(
        app.clone()
            .oneshot(request(identity, &reviewer))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["identity"]["subscriber_read"], true);
    let missing = "/v1/experimental/operator/permissions?environmentId=review-a&issuer=https%3A%2F%2Fissuer.test&subject=missing&clientId=console";
    let (status, result) = body(
        app.clone()
            .oneshot(request(missing, &reviewer))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["found"], false);
    let (status, result) = body(app.clone().oneshot(request(audit, &auditor)).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["data"].as_array().unwrap().len(), 1);
    assert_eq!(result["data"][0]["reason"], "ticket");
    assert!(result["data"][0]["id"].is_string());
    for (path, key) in [
        (identity, &auditor),
        (audit, &reviewer),
        (
            "/v1/experimental/operator/permissions/audit?environmentId=review-b",
            &auditor,
        ),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(request(path, key))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    for path in [
        "/v1/experimental/operator/permissions/audit?environmentId=review-a&before=0",
        "/v1/experimental/operator/permissions?environmentId=review-a&subject=target",
        "/v1/experimental/operator/permissions/audit?environmentId=review-a&environmentId=review-b",
    ] {
        assert_eq!(
            app.clone()
                .oneshot(request(path, &auditor))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let tx = db.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_permission_review_grants SET identity_read=false WHERE subject='reviewer'",&[]).await.unwrap();
    let app2 = app.clone();
    let token2 = reviewer.clone();
    let mut pending = tokio::spawn(async move {
        app2.oneshot(request(identity, &token2))
            .await
            .unwrap()
            .status()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut pending)
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    db.execute("UPDATE public.novu_rust_operator_jwt_permission_review_grants SET identity_read=true WHERE subject='reviewer'", &[]).await.unwrap();
    let tx = db.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_identities SET subscriber_read=subscriber_read WHERE subject='target' AND environment_id='review-a'",&[]).await.unwrap();
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3;
    let short = token("reviewer", expires);
    let app2 = app.clone();
    let mut pending = tokio::spawn(async move {
        app2.oneshot(request(identity, &short))
            .await
            .unwrap()
            .status()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut pending)
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
}
