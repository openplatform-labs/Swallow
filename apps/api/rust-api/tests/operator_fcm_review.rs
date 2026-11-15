//! Operator FCM partial failure health against a disposable database.
use axum::{
    Extension,
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
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
fn jwt(subject: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("key".into());
    encode(&h,&json!({"iss":"https://issuer.test","sub":subject,"aud":"swallow","exp":now+300,"iat":now-1,"jti":"token","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
}
fn request(token: &str, method: &str, path: &str, body: Option<Value>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
        .unwrap()
}
async fn response(
    app: axum::Router,
    token: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let r = app
        .oneshot(request(token, method, path, body))
        .await
        .unwrap();
    let status = r.status();
    let bytes = r.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn review_is_scoped_idempotent_and_fenced() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("fcm-review-{}", Uuid::new_v4());
    let other = format!("fcm-review-other-{}", Uuid::new_v4());
    let role = format!("fcm_review_api_{}", Uuid::new_v4().simple());
    let job = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','alice','console',$2)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_fcm_unreviewed(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_fcm_review(text,text,text,text,bigint,uuid,uuid,text,text),public.novu_rust_operator_jwt_fcm_review_request(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_fcm_push',jsonb_build_object('secret','fixture-private'),'completed')",&[&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES($1,$2,0,'accepted'),($1,$2,1,'rejected')",&[&job,&env]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query("SELECT * FROM public.novu_rust_fcm_attempts", &[])
            .await
            .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("alice");
    let list = format!("/v1/experimental/operator/fcm-unreviewed?environmentId={env}");
    let post = format!("/v1/experimental/operator/fcm-review?environmentId={env}");
    let id = Uuid::new_v4();
    assert_eq!(
        response(app.clone(), &token, "GET", &list, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_fcm_unreviewed_grants VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &list, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let (status, first) = response(app.clone(), &token, "GET", &list, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["data"].as_array().unwrap().len(), 1);
    assert!(!first.to_string().contains("fixture-private"));
    let revision = first["data"][0]["revision"].as_str().unwrap().to_owned();
    let input = json!({"requestId":id,"jobId":job,"revision":revision,"reason":"reviewed"});
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(input.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_fcm_review_grants VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    db.execute("UPDATE public.novu_rust_fcm_attempts SET http_status=429,updated_at=clock_timestamp()+interval '1 second' WHERE job_id=$1 AND target_index=1",&[&job]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(input.clone()))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let lookup =
        format!("/v1/experimental/operator/fcm-review-request?environmentId={env}&requestId={id}");
    assert_eq!(
        response(app.clone(), &token, "GET", &lookup, None).await.1,
        json!({"found":false})
    );
    let (_, new_list) = response(app.clone(), &token, "GET", &list, None).await;
    let revision = new_list["data"][0]["revision"].as_str().unwrap();
    let input = json!({"requestId":id,"jobId":job,"revision":revision,"reason":"reviewed"});
    let (status, value) = response(app.clone(), &token, "POST", &post, Some(input.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value, json!({"recorded":true,"replayed":false}));
    let (_, value) = response(app.clone(), &token, "POST", &post, Some(input.clone())).await;
    assert_eq!(value, json!({"recorded":true,"replayed":true}));
    let mut altered = input.clone();
    altered["reason"] = json!("different");
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(altered))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (_, record) = response(app.clone(), &token, "GET", &lookup, None).await;
    assert_eq!(record["found"], true);
    assert_eq!(record["jobId"], job.to_string());
    assert_eq!(record["reason"], "reviewed");
    let (_, empty) = response(app.clone(), &token, "GET", &list, None).await;
    assert_eq!(empty["data"], json!([]));
    let wrong = format!("/v1/experimental/operator/fcm-unreviewed?environmentId={other}");
    assert_eq!(
        response(app.clone(), &token, "GET", &wrong, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='queue_ack'",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &lookup, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(input))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "DELETE FROM public.novu_rust_operator_jwt_fcm_unreviewed_grants WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert_eq!(
        response(app, &token, "GET", &list, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
