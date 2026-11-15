//! Smtp JWT review through restricted API login and disposable PostgreSQL.
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
    path::Path,
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

async fn migrate(db: &Client, dir: &Path, range: std::ops::RangeInclusive<u32>) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        if range.contains(&name[..4].parse::<u32>().unwrap()) {
            db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}

#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn discord_grants_are_independent_conditional_and_audited() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    let env = format!("discord-grant-{}", Uuid::new_v4());
    let other = format!("discord-other-{}", Uuid::new_v4());
    let role = format!("discord_admin_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','admin','console',$1),('https://issuer.test','target','console',$1),('https://issuer.test','target','console',$2)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_discord_resolution_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_discord_resolution_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_discord_resolution_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_discord_resolution_grant_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_discord_resolution_request(text,text,text,text,bigint,uuid,text),public.novu_rust_operator_jwt_discord_resolution_history(text,text,text,text,bigint,uuid,oid,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_resolvers SELECT $1,rolname,oid,true,true FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_discord_resolution_grants",
            &[]
        )
        .await
        .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("admin");
    let path = format!("/v1/experimental/operator/discord-resolution-grants?environmentId={env}");
    let read = format!(
        "{path}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=target&targetClientId=console&resource=discord-resolution&permission=accepted"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &read, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_discord_resolution_grant_admins VALUES('https://issuer.test','admin','console',$1,true,false)",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &read, None).await.1,
        json!({"targetExists":true,"granted":false})
    );
    let id = Uuid::new_v4();
    let change = json!({"requestId":id,"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"discord-resolution","permission":"accepted","expected":false,"desired":true,"reason":"ticket"});
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(change.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_discord_resolution_grant_admins SET can_write=true WHERE environment_id=$1",&[&env]).await.unwrap();
    let (status, result) = response(app.clone(), &token, "PUT", &path, Some(change.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["changed"], true);
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(change.clone()))
            .await
            .1["replayed"],
        true
    );
    let mut stale = change.clone();
    stale["requestId"] = json!(Uuid::new_v4());
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(stale))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let mut own = change.clone();
    own["requestId"] = json!(Uuid::new_v4());
    own["targetSubject"] = json!("admin");
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(own))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let request = format!(
        "/v1/experimental/operator/discord-resolution-grants/request?environmentId={env}&requestId={id}"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &request, None).await.1["found"],
        true
    );
    let audit =
        format!("/v1/experimental/operator/discord-resolution-grants/audit?environmentId={env}");
    assert_eq!(
        response(app.clone(), &token, "GET", &audit, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_discord_resolution_grant_audit_viewers VALUES('https://issuer.test','admin','console',$1)",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &audit, None).await.1["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let target = jwt("target");
    let target_list = format!(
        "/v1/experimental/operator/discord-resolution-request?environmentId={env}&requestId={}&decision=accepted",
        Uuid::new_v4()
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &target_list, None)
            .await
            .0,
        StatusCode::OK
    );
    let review = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"discord-resolution","permission":"retry","expected":false,"desired":true,"reason":"ticket"});
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(review))
            .await
            .0,
        StatusCode::OK
    );
    let target_lookup = format!(
        "/v1/experimental/operator/discord-resolution-request?environmentId={env}&requestId={}&decision=retry",
        Uuid::new_v4()
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &target_lookup, None)
            .await
            .1,
        json!({"found":false})
    );
    let revoke = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"discord-resolution","permission":"accepted","expected":true,"desired":false,"reason":"done"});
    db.batch_execute("CREATE FUNCTION public.test_discord_grant_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test audit failure'; END $$; CREATE TRIGGER test_discord_grant_reject BEFORE INSERT ON public.novu_rust_operator_jwt_discord_resolution_grant_audit FOR EACH ROW EXECUTE FUNCTION public.test_discord_grant_reject()").await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(revoke.clone()))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(db.query_one("SELECT can_accept FROM public.novu_rust_operator_jwt_discord_resolution_grants WHERE environment_id=$1 AND subject='target'",&[&env]).await.unwrap().get::<_,bool>(0));
    db.batch_execute("DROP TRIGGER test_discord_grant_reject ON public.novu_rust_operator_jwt_discord_resolution_grant_audit; DROP FUNCTION public.test_discord_grant_reject()").await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(revoke))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &target_list, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &target_lookup, None)
            .await
            .1,
        json!({"found":false})
    );
    let row=db.query_one("SELECT can_accept,can_retry FROM public.novu_rust_operator_jwt_discord_resolution_grants WHERE environment_id=$1 AND subject='target'",&[&env]).await.unwrap();
    assert!(!row.get::<_, bool>(0));
    assert!(row.get::<_, bool>(1));
    let cross = format!(
        "/v1/experimental/operator/discord-resolution-grants?environmentId={other}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=target&targetClientId=console&resource=discord-resolution&permission=retry"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &cross, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_discord_resolution_audit_viewers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let history = format!(
        "/v1/experimental/operator/discord-resolution-history?environmentId={env}&jobId={}",
        Uuid::new_v4()
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &history, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let change_history = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"discord-resolution","permission":"history","expected":false,"desired":true,"reason":"audit access"});
    db.batch_execute("CREATE FUNCTION public.discord_history_grant_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit failure'; END $$; CREATE TRIGGER discord_history_grant_reject BEFORE INSERT ON public.novu_rust_operator_jwt_discord_resolution_grant_audit FOR EACH ROW EXECUTE FUNCTION public.discord_history_grant_reject()").await.unwrap();
    assert_eq!(
        response(
            app.clone(),
            &token,
            "PUT",
            &path,
            Some(change_history.clone())
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &history, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("DROP TRIGGER discord_history_grant_reject ON public.novu_rust_operator_jwt_discord_resolution_grant_audit; DROP FUNCTION public.discord_history_grant_reject()").await.unwrap();
    assert_eq!(
        response(
            app.clone(),
            &token,
            "PUT",
            &path,
            Some(change_history.clone())
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(change_history))
            .await
            .1["replayed"],
        true
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &history, None)
            .await
            .0,
        StatusCode::OK
    );
    let revoke_history = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"discord-resolution","permission":"history","expected":true,"desired":false,"reason":"audit complete"});
    assert_eq!(
        response(app.clone(), &token, "PUT", &path, Some(revoke_history))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &history, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &target, "GET", &target_lookup, None)
            .await
            .0,
        StatusCode::OK
    );
}
