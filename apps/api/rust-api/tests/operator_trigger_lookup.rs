use axum::{body::Body,extract::Extension,http::{Request,StatusCode}};
use jsonwebtoken::{Algorithm,EncodingKey,Header,encode};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::{Value,json};
use std::{sync::Arc,time::{SystemTime,UNIX_EPOCH},time::Duration};
use tokio_postgres::{Client,NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn connect(url:&str)->Client {let (db,driver)=tokio_postgres::connect(url,NoTls).await.unwrap();tokio::spawn(driver);db}
fn request(token:&str,query:&str)->Request<Body>{Request::builder().uri(format!("/v1/experimental/operator/workflow-trigger-result?{query}")).header("authorization",format!("Bearer {token}")).body(Body::empty()).unwrap()}
#[tokio::test]
#[ignore="requires fully migrated disposable PostgreSQL and role creation"]
async fn lookup_requires_scoped_jwt_grant_and_fences_revocation() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let mut db=connect(&url).await;let role=format!("trigger_lookup_{}",Uuid::new_v4().simple());
 db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_trigger_lookup(text,text,text,text,bigint,text) TO {role}")).await.unwrap();
 db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname=$1",&[&role]).await.unwrap();
 db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('jwt-trigger-a','org'),('jwt-trigger-b','org'); INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','person','console','jwt-trigger-a'),('https://issuer.test','person','console','jwt-trigger-b'); INSERT INTO public.novu_rust_trigger_transactions(environment_id,transaction_id) VALUES('jwt-trigger-a','fixture-txn'); INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES('11111111-1111-4111-8111-111111111111','jwt-trigger-a','experimental_static_in_app','{\"transactionId\":\"fixture-txn\",\"secret\":\"hidden\"}')").await.unwrap();
 let api_url=url.replacen("postgres://postgres@",&format!("postgres://{role}@"),1);let api=connect(&api_url).await;assert!(api.query("SELECT * FROM public.novu_queue_jobs",&[]).await.is_err());assert!(api.query("SELECT * FROM public.novu_rust_operator_jwt_trigger_lookup_grants",&[]).await.is_err());
 let app=novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",include_bytes!("fixtures/operator_jwt/test-only-public.pem"))]).unwrap())));
 let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();let mut h=Header::new(Algorithm::RS256);h.typ=Some("at+jwt".into());h.kid=Some("key".into());let token=encode(&h,&json!({"iss":"https://issuer.test","sub":"person","aud":"swallow","exp":now+120,"iat":now-1,"jti":"token","client_id":"console"}),&EncodingKey::from_rsa_pem(include_bytes!("fixtures/operator_jwt/test-only-private.pem")).unwrap()).unwrap();
 let query="environmentId=jwt-trigger-a&transactionId=fixture-txn";
 assert_eq!(app.clone().oneshot(request(&token,query)).await.unwrap().status(),StatusCode::UNAUTHORIZED);
 db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_trigger_lookup_grants VALUES('https://issuer.test','person','console','jwt-trigger-a')").await.unwrap();
 let response=app.clone().oneshot(request(&token,query)).await.unwrap();assert_eq!(response.status(),StatusCode::OK);assert_eq!(response.headers()["cache-control"],"no-store");let data:Value=serde_json::from_slice(&axum::body::to_bytes(response.into_body(),16384).await.unwrap()).unwrap();assert_eq!(data,json!({"transactionId":"fixture-txn","recorded":true,"jobCount":1,"job":{"jobId":"11111111-1111-4111-8111-111111111111","jobStatus":"pending"}}));
 assert_eq!(app.clone().oneshot(request(&token,"environmentId=jwt-trigger-b&transactionId=fixture-txn")).await.unwrap().status(),StatusCode::UNAUTHORIZED);
 assert_eq!(app.clone().oneshot(request(&token,"environmentId=jwt-trigger-a&transactionId=fixture-txn&transactionId=other")).await.unwrap().status(),StatusCode::BAD_REQUEST);
 let tx=db.transaction().await.unwrap();tx.execute("DELETE FROM public.novu_rust_operator_jwt_trigger_lookup_grants WHERE environment_id='jwt-trigger-a'",&[]).await.unwrap();
 let pending=tokio::spawn(app.clone().oneshot(request(&token,query)));tokio::time::sleep(Duration::from_millis(150)).await;assert!(!pending.is_finished());tx.rollback().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::OK);
 let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();let short=encode(&h,&json!({"iss":"https://issuer.test","sub":"person","aud":"swallow","exp":now+3,"iat":now-1,"jti":"short","client_id":"console"}),&EncodingKey::from_rsa_pem(include_bytes!("fixtures/operator_jwt/test-only-private.pem")).unwrap()).unwrap();
 let tx=db.transaction().await.unwrap();tx.execute("DELETE FROM public.novu_rust_operator_jwt_trigger_lookup_grants WHERE environment_id='jwt-trigger-a'",&[]).await.unwrap();
 let pending=tokio::spawn(app.clone().oneshot(request(&short,query)));tokio::time::sleep(Duration::from_secs(4)).await;assert!(!pending.is_finished());tx.rollback().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::UNAUTHORIZED);
 db.execute("DELETE FROM public.novu_rust_operator_jwt_trigger_lookup_grants WHERE environment_id='jwt-trigger-a'",&[]).await.unwrap();assert_eq!(app.oneshot(request(&token,query)).await.unwrap().status(),StatusCode::UNAUTHORIZED);
}
