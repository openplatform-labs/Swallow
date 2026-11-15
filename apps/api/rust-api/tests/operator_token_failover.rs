//! Controlled synchronous PostgreSQL failover; all credentials are fixtures.
use axum::{
    Extension,
    body::Body,
    http::{Request, StatusCode, header},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::json;
use std::{
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;

const PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-private.pem");
const PUBLIC: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-public.pem");

async fn connect(url: &str) -> Client {
    for _ in 0..100 {
        if let Ok((db, driver)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = driver.await;
            });
            return db;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("database unavailable")
}
fn token(jti: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("at+jwt".into());
    header.kid = Some("fixture".into());
    encode(&header,&json!({"iss":"https://issuer.test","sub":"alice","client_id":"console","jti":jti,"aud":"swallow","iat":now-1,"exp":now+300}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
}
fn request(token: &str) -> Request<Body> {
    Request::builder()
        .uri("/v1/experimental/operator/me?environmentId=operator-ha")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}
fn app(db: Client) -> axum::Router {
    let config = json!({"issuer":"https://issuer.test","audience":"swallow","keys":[{"kid":"fixture","pem":std::str::from_utf8(PUBLIC).unwrap()}],"check_token_revocation":true});
    novu_rust_api::router(Arc::new(db)).layer(Extension(Arc::new(
        AccessTokenVerifier::from_json(&config.to_string()).unwrap(),
    )))
}

#[tokio::test]
#[ignore = "requires disposable synchronously replicated PostgreSQL containers; destroys primary"]
async fn committed_operator_revocation_survives_synchronous_promotion() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let primary = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let standby = std::env::var("TEST_STANDBY_CONTAINER").unwrap();
    let db = connect(&primary_url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('operator-ha','org'); CREATE ROLE operator_ha_api LOGIN; GRANT USAGE ON SCHEMA public TO operator_ha_api; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_self(text,text,text,text,bigint),public.novu_rust_operator_token_revoked(bytea,bytea,bytea,bytea),public.novu_rust_operator_token_guard(bytea,bytea,bytea,bytea) TO operator_ha_api; INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='operator_ha_api'; INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console','operator-ha'); INSERT INTO public.novu_rust_operator_token_revocation_admins SELECT rolname,oid,true,false FROM pg_roles WHERE rolname=session_user").await.unwrap();
    let api_url =
        primary_url.replacen("postgresql://postgres@", "postgresql://operator_ha_api@", 1);
    let initial = app(connect(&api_url).await);
    let revoked = token("revoked-fixture");
    let other = token("other-fixture");
    assert_eq!(
        initial
            .clone()
            .oneshot(request(&revoked))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(db.query_one("SELECT public.novu_rust_admin_revoke_operator_token(sha256(convert_to('https://issuer.test','UTF8')),sha256(convert_to('alice','UTF8')),sha256(convert_to('console','UTF8')),sha256(convert_to('revoked-fixture','UTF8')),'failover fixture')",&[]).await.unwrap().get::<_,bool>(0));
    assert_eq!(
        initial.oneshot(request(&revoked)).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let replica = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        replicated=replica.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_rust_operator_token_revocations WHERE token_id_hash=sha256(convert_to('revoked-fixture','UTF8'))) AND EXISTS(SELECT 1 FROM public.novu_rust_operator_token_revocation_audit WHERE token_id_hash=sha256(convert_to('revoked-fixture','UTF8'))) AND EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_api_logins WHERE database_principal='operator_ha_api')",&[]).await.unwrap().get(0);
        if replicated {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        replicated,
        "committed revocation, audit and API registration must reach standby"
    );
    assert!(
        Command::new("docker")
            .args(["kill", &primary])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("docker")
            .args([
                "exec",
                "-u",
                "postgres",
                &standby,
                "pg_ctl",
                "promote",
                "-D",
                "/var/lib/postgresql/data",
                "-w"
            ])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let promoted_url =
        standby_url.replacen("postgresql://postgres@", "postgresql://operator_ha_api@", 1);
    let promoted = app(connect(&promoted_url).await);
    assert_eq!(
        promoted
            .clone()
            .oneshot(request(&revoked))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        promoted.oneshot(request(&other)).await.unwrap().status(),
        StatusCode::OK
    );
}
