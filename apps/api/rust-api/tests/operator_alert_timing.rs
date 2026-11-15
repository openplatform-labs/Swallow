//! Discord destination retry policy JWT API through restricted API login and disposable PostgreSQL.
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
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn timing_api_enforces_permissions_replay_and_atomic_audit() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;let root=Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db,&root.join("migrations"),1..=17).await;migrate(&db,&root.join("../../worker/rust-queue/migrations"),1..=u32::MAX).await;migrate(&db,&root.join("migrations"),18..=u32::MAX).await;
    let env=format!("timing-api-{}",Uuid::new_v4());let role=format!("timing_api_{}",Uuid::new_v4().simple());
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1)",&[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_alert_timing_read(text,text,text,text,bigint,text,text),public.novu_rust_operator_jwt_alert_timing_change(text,text,text,text,bigint,text,text,uuid,bigint,integer,integer,integer,text),public.novu_rust_operator_jwt_alert_timing_request(text,text,text,text,bigint,text,text,uuid),public.novu_rust_operator_jwt_alert_timing_audit(text,text,text,text,bigint,text,text,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_alert_timing_grants SELECT $1,rolname,oid,false,false FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let api=connect(&url).await;api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();
    let app=novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",PUBLIC)]).unwrap())));
    let alice=jwt("alice");let bob=jwt("bob");let path=format!("/v1/experimental/operator/alert-timing?environmentId={env}&channel=slack&targetId=ops");
    assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_alert_timing_grants VALUES('https://issuer.test','alice','console',$1,'slack','ops',true,true,true)",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("UPDATE public.novu_rust_alert_timing_grants SET can_read=true,can_write=true WHERE environment_id=$1 AND database_principal::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.1,json!({"revision":"0","grace":null,"heartbeat":null,"cooldown":null}));
    assert_eq!(response(app.clone(),&bob,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
    assert_eq!(response(app.clone(),&alice,"GET",&path.replace("targetId=ops","targetId=other"),None).await.0,StatusCode::UNAUTHORIZED);
    assert_eq!(response(app.clone(),&alice,"GET",&format!("{path}&channel=email"),None).await.0,StatusCode::BAD_REQUEST);
    let id=Uuid::new_v4().to_string();let body=json!({"requestId":id,"expectedRevision":"0","grace":60,"heartbeat":120,"cooldown":300,"reason":"reviewed"});
    let (a,b)=tokio::join!(response(app.clone(),&alice,"PUT",&path,Some(body.clone())),response(app.clone(),&alice,"PUT",&path,Some(body.clone())));
    assert_eq!(a.0,StatusCode::OK);assert_eq!(b.0,StatusCode::OK);assert_ne!(a.1["replayed"],b.1["replayed"]);assert_eq!(a.1["settings"],json!({"revision":"1","grace":60,"heartbeat":120,"cooldown":300}));
    let mut different=body.clone();different["grace"]=61.into();assert_eq!(response(app.clone(),&alice,"PUT",&path,Some(different)).await.0,StatusCode::CONFLICT);
    let mut stale=body.clone();stale["requestId"]=Uuid::new_v4().to_string().into();assert_eq!(response(app.clone(),&alice,"PUT",&path,Some(stale)).await.0,StatusCode::CONFLICT);
    let mut invalid=body.clone();invalid["cooldown"]=59.into();assert_eq!(response(app.clone(),&alice,"PUT",&path,Some(invalid)).await.0,StatusCode::BAD_REQUEST);
    let query=format!("/v1/experimental/operator/alert-timing/request?environmentId={env}&channel=slack&targetId=ops&requestId={id}");
    assert_eq!(response(app.clone(),&alice,"GET",&query,None).await.1["found"],true);assert_eq!(response(app.clone(),&bob,"GET",&query,None).await.0,StatusCode::UNAUTHORIZED);
    let audit=format!("/v1/experimental/operator/alert-timing/audit?environmentId={env}&channel=slack&targetId=ops");let (_,rows)=response(app.clone(),&alice,"GET",&audit,None).await;assert_eq!(rows["data"].as_array().unwrap().len(),1);assert_eq!(rows["data"][0]["actorSubject"],"alice");
    db.batch_execute("CREATE FUNCTION public.test_jwt_timing_audit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_jwt_timing_audit_failure BEFORE INSERT ON public.novu_rust_operator_jwt_alert_timing_audit FOR EACH ROW EXECUTE FUNCTION public.test_jwt_timing_audit_failure()").await.unwrap();
    let next=json!({"requestId":Uuid::new_v4().to_string(),"expectedRevision":"1","grace":80,"heartbeat":null,"cooldown":null,"reason":"changed"});
    assert_eq!(response(app.clone(),&alice,"PUT",&path,Some(next.clone())).await.0,StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.1["revision"],"1");
    db.batch_execute("DROP TRIGGER test_jwt_timing_audit_failure ON public.novu_rust_operator_jwt_alert_timing_audit; DROP FUNCTION public.test_jwt_timing_audit_failure()").await.unwrap();
    assert_eq!(response(app.clone(),&alice,"PUT",&path,Some(next)).await.1["settings"]["revision"],"2");
    db.execute("INSERT INTO public.novu_rust_alert_timing_grants SELECT $1,rolname,oid,true,false FROM pg_roles WHERE rolname=session_user",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_send','alert_view']) p WHERE rolname=session_user",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','ops','rejected')",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_projection_heartbeat(environment_id,last_completed_at) VALUES($1,clock_timestamp())",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES($1,$2,'experimental_resend_email','{}',clock_timestamp()-interval '100 seconds')",&[&Uuid::new_v4(),&env]).await.unwrap();
    let dir=std::env::temp_dir().join(format!("swallow-timing-api-{}",Uuid::new_v4()));std::fs::create_dir(&dir).unwrap();let secret=dir.join("slack");std::fs::write(&secret,"https://hooks.slack.com/services/A/B/C").unwrap();let config=dir.join("targets.json");
    std::fs::write(&config,serde_json::to_vec(&json!({"targets":[{"channel":"slack","id":"ops","webhook_file":secret}]})).unwrap()).unwrap();
    let fanout=std::env::var("TEST_ALERT_FANOUT_BIN").expect("TEST_ALERT_FANOUT_BIN required");
    let run=||std::process::Command::new(&fanout).args(["delivery",&env,"300","300","60"]).arg(&config).env("DATABASE_URL",&url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env("SWALLOW_ALERT_TIMING_SOURCE","postgres").env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS").env_remove("SWALLOW_ALERT_BINDING_KEY").env_remove("SWALLOW_ALERT_BINDING_KEY_FILE").output().unwrap();
    assert_eq!(run().status.code(),Some(2));
    let change=json!({"requestId":Uuid::new_v4().to_string(),"expectedRevision":"2","grace":200,"heartbeat":null,"cooldown":null,"reason":"extend grace"});
    assert_eq!(response(app.clone(),&alice,"PUT",&path,Some(change)).await.0,StatusCode::OK);assert_eq!(run().status.code(),Some(0));
    std::fs::remove_dir_all(dir).unwrap();
    db.execute("UPDATE public.novu_rust_operator_jwt_alert_timing_grants SET can_write=false WHERE environment_id=$1",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&alice,"PUT",&path,Some(body)).await.0,StatusCode::UNAUTHORIZED);assert_eq!(response(app.clone(),&alice,"GET",&query,None).await.0,StatusCode::UNAUTHORIZED);assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.0,StatusCode::OK);
    db.execute("UPDATE public.novu_rust_alert_timing_grants SET can_read=false WHERE environment_id=$1 AND database_principal::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(response(app,&alice,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
}
