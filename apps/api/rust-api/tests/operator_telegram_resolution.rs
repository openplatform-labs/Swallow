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
async fn telegram_resolution_requires_scoped_grants_and_replays_atomically() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;let root=Path::new(env!("CARGO_MANIFEST_DIR"));migrate(&db,&root.join("migrations"),1..=17).await;migrate(&db,&root.join("../../worker/rust-queue/migrations"),1..=u32::MAX).await;migrate(&db,&root.join("migrations"),18..=u32::MAX).await;
 let env=format!("resolution-{}",Uuid::new_v4());let other=format!("other-{}",Uuid::new_v4());let role=format!("telegram_resolution_{}",Uuid::new_v4().simple());
 db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
 db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1),('https://issuer.test','alice','console',$2)",&[&env,&other]).await.unwrap();
 db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_telegram_attention(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_telegram_predispatch(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_telegram_predispatch_retry(text,text,text,text,bigint,uuid,uuid,bigint,text),public.novu_rust_operator_jwt_telegram_predispatch_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_telegram_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_telegram_resolution_request(text,text,text,text,bigint,uuid,text) TO {role}")).await.unwrap();
 db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
 let make_app=||async {let api=connect(&url).await;api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",PUBLIC)]).unwrap())))};
 let app=make_app().await;let app2=make_app().await;let alice=jwt("alice");let bob=jwt("bob");let path=format!("/v1/experimental/operator/telegram-attention?environmentId={env}");let resolve=format!("/v1/experimental/operator/telegram-resolution?environmentId={env}");
 assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grants VALUES('https://issuer.test','alice','console',$1,true,true,true)",&[&env]).await.unwrap();
 assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_telegram_operator_grants SELECT $1,rolname,oid,true,true,true FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
 assert_eq!(response(app.clone(),&bob,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
 assert_eq!(response(app.clone(),&alice,"GET",&format!("/v1/experimental/operator/telegram-attention?environmentId={other}"),None).await.0,StatusCode::UNAUTHORIZED);
 let prepath=format!("/v1/experimental/operator/telegram-predispatch?environmentId={env}");
 assert_eq!(response(app.clone(),&bob,"GET",&prepath,None).await.0,StatusCode::UNAUTHORIZED);
 assert_eq!(response(app.clone(),&alice,"GET",&format!("/v1/experimental/operator/telegram-predispatch?environmentId={other}"),None).await.0,StatusCode::UNAUTHORIZED);
 assert_eq!(response(app.clone(),&alice,"GET",&format!("{prepath}&after=invalid"),None).await.0,StatusCode::BAD_REQUEST);
 db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) SELECT gen_random_uuid(),$1,'experimental_telegram_workflow','{\"secret\":\"private\"}','failed','private error' FROM generate_series(1,52)",&[&env]).await.unwrap();
 let first=response(app.clone(),&alice,"GET",&prepath,None).await;assert_eq!(first.0,StatusCode::OK);assert_eq!(first.1["data"].as_array().unwrap().len(),50);assert_eq!(first.1["hasMore"],true);assert!(!first.1.to_string().contains("private"));
 let second=response(app.clone(),&alice,"GET",&format!("{prepath}&after={}",first.1["next"].as_str().unwrap()),None).await;assert_eq!(second.1["data"].as_array().unwrap().len(),2);assert_eq!(second.1["hasMore"],false);
 db.execute("UPDATE public.novu_rust_operator_jwt_telegram_resolution_grants SET can_list=false WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&prepath,None).await.0,StatusCode::UNAUTHORIZED);db.execute("UPDATE public.novu_rust_operator_jwt_telegram_resolution_grants SET can_list=true WHERE environment_id=$1",&[&env]).await.unwrap();
 db.execute("UPDATE public.novu_rust_telegram_operator_grants SET can_view=false WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&prepath,None).await.0,StatusCode::UNAUTHORIZED);db.execute("UPDATE public.novu_rust_telegram_operator_grants SET can_view=true WHERE environment_id=$1",&[&env]).await.unwrap();
 let prejob=Uuid::new_v4();let prerequest=Uuid::new_v4();let bigrevision:i64=9007199254740993;
 db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error,failure_revision) VALUES($1,$2,'experimental_telegram','{}','failed','invalid Telegram job',$3)",&[&prejob,&env,&bigrevision]).await.unwrap();
 let prepath_after=format!("{prepath}&after={}",Uuid::nil());let page=response(app.clone(),&alice,"GET",&prepath_after,None).await.1;
 let mut found=page["data"].as_array().unwrap().iter().find(|r|r["jobId"]==prejob.to_string()).cloned();if found.is_none(){let next=page["next"].as_str().unwrap();let page2=response(app.clone(),&alice,"GET",&format!("{prepath}&after={next}"),None).await.1;found=page2["data"].as_array().unwrap().iter().find(|r|r["jobId"]==prejob.to_string()).cloned();}assert_eq!(found.unwrap()["failureRevisionText"],bigrevision.to_string());
 let preretry=format!("/v1/experimental/operator/telegram-predispatch-retry?environmentId={env}");let prebody=json!({"requestId":prerequest,"jobId":prejob,"failureRevision":bigrevision.to_string(),"reason":"investigated"});
 assert_eq!(response(app.clone(),&bob,"POST",&preretry,Some(prebody.clone())).await.0,StatusCode::UNAUTHORIZED);
 let mut bad=prebody.clone();bad["failureRevision"]=json!("000");assert_eq!(response(app.clone(),&alice,"POST",&preretry,Some(bad)).await.0,StatusCode::BAD_REQUEST);
 db.batch_execute("CREATE FUNCTION public.reject_telegram_pre_jwt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test'; END $$; CREATE TRIGGER reject_telegram_pre_jwt BEFORE INSERT ON public.novu_rust_operator_jwt_telegram_predispatch_requests FOR EACH ROW EXECUTE FUNCTION public.reject_telegram_pre_jwt()").await.unwrap();
 assert_eq!(response(app.clone(),&alice,"POST",&preretry,Some(prebody.clone())).await.0,StatusCode::INTERNAL_SERVER_ERROR);assert_eq!(db.query_one("SELECT status FROM public.novu_queue_jobs WHERE id=$1",&[&prejob]).await.unwrap().get::<_,String>(0),"failed");db.batch_execute("DROP TRIGGER reject_telegram_pre_jwt ON public.novu_rust_operator_jwt_telegram_predispatch_requests").await.unwrap();
 let (a,b)=tokio::join!(response(app.clone(),&alice,"POST",&preretry,Some(prebody.clone())),response(app2.clone(),&alice,"POST",&preretry,Some(prebody.clone())));assert_eq!(a.0,StatusCode::OK);assert_eq!(b.0,StatusCode::OK);assert_ne!(a.1["replayed"],b.1["replayed"]);
 let mut conflict=prebody.clone();conflict["reason"]=json!("changed");assert_eq!(response(app.clone(),&alice,"POST",&preretry,Some(conflict)).await.0,StatusCode::CONFLICT);
 let prelookup=format!("/v1/experimental/operator/telegram-predispatch-request?environmentId={env}&requestId={prerequest}");let prefound=response(app.clone(),&alice,"GET",&prelookup,None).await;assert_eq!(prefound.1["failureRevision"],bigrevision.to_string());assert_eq!(response(app.clone(),&bob,"GET",&prelookup,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("UPDATE public.novu_rust_operator_jwt_telegram_resolution_grants SET can_retry=false WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&prelookup,None).await.0,StatusCode::UNAUTHORIZED);db.execute("UPDATE public.novu_rust_operator_jwt_telegram_resolution_grants SET can_retry=true WHERE environment_id=$1",&[&env]).await.unwrap();
 db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_cancel(text,text,text,text,bigint,uuid,uuid,bigint,text),public.novu_rust_operator_jwt_telegram_predispatch_cancel_request(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
 let canceljob=Uuid::new_v4();let cancelrequest=Uuid::new_v4();db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES($1,$2,'experimental_telegram','{}','failed','invalid Telegram job')",&[&canceljob,&env]).await.unwrap();
 let cancelpath=format!("/v1/experimental/operator/telegram-predispatch-cancel?environmentId={env}");let cancelbody=json!({"requestId":cancelrequest,"jobId":canceljob,"failureRevision":"0","reason":"not needed"});
 assert_eq!(response(app.clone(),&alice,"POST",&cancelpath,Some(cancelbody.clone())).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_operator_jwt_telegram_predispatch_cancellers VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"POST",&cancelpath,Some(cancelbody.clone())).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_telegram_predispatch_cancel_grants SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();assert_eq!(response(app.clone(),&bob,"POST",&cancelpath,Some(cancelbody.clone())).await.0,StatusCode::UNAUTHORIZED);
 let othercancel=format!("/v1/experimental/operator/telegram-predispatch-cancel?environmentId={other}");assert_eq!(response(app.clone(),&alice,"POST",&othercancel,Some(cancelbody.clone())).await.0,StatusCode::UNAUTHORIZED);
 db.batch_execute("CREATE FUNCTION public.reject_telegram_cancel_jwt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test'; END $$; CREATE TRIGGER reject_telegram_cancel_jwt BEFORE INSERT ON public.novu_rust_operator_jwt_telegram_predispatch_cancellations FOR EACH ROW EXECUTE FUNCTION public.reject_telegram_cancel_jwt()").await.unwrap();
 assert_eq!(response(app.clone(),&alice,"POST",&cancelpath,Some(cancelbody.clone())).await.0,StatusCode::INTERNAL_SERVER_ERROR);assert_eq!(db.query_one("SELECT status FROM public.novu_queue_jobs WHERE id=$1",&[&canceljob]).await.unwrap().get::<_,String>(0),"failed");db.batch_execute("DROP TRIGGER reject_telegram_cancel_jwt ON public.novu_rust_operator_jwt_telegram_predispatch_cancellations").await.unwrap();
 let (a,b)=tokio::join!(response(app.clone(),&alice,"POST",&cancelpath,Some(cancelbody.clone())),response(app2.clone(),&alice,"POST",&cancelpath,Some(cancelbody.clone())));assert_eq!(a.0,StatusCode::OK);assert_eq!(b.0,StatusCode::OK);assert_ne!(a.1["replayed"],b.1["replayed"]);assert_eq!(db.query_one("SELECT status FROM public.novu_queue_jobs WHERE id=$1",&[&canceljob]).await.unwrap().get::<_,String>(0),"cancelled");
 let mut cancelconflict=cancelbody.clone();cancelconflict["reason"]=json!("changed");assert_eq!(response(app.clone(),&alice,"POST",&cancelpath,Some(cancelconflict)).await.0,StatusCode::CONFLICT);
 let cancellookup=format!("/v1/experimental/operator/telegram-predispatch-cancel-request?environmentId={env}&requestId={cancelrequest}");assert_eq!(response(app.clone(),&alice,"GET",&cancellookup,None).await.1["jobId"],canceljob.to_string());assert_eq!(response(app.clone(),&bob,"GET",&cancellookup,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("DELETE FROM public.novu_rust_telegram_predispatch_cancel_grants WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&cancellookup,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_telegram_predispatch_cancel_grants SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();db.execute("DELETE FROM public.novu_rust_operator_jwt_telegram_predispatch_cancellers WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&cancellookup,None).await.0,StatusCode::UNAUTHORIZED);
 let job=Uuid::new_v4();db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_telegram_workflow','{\"destination\":\"private\"}','failed')",&[&job,&env]).await.unwrap();let attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown') RETURNING attempt_id",&[&job,&env]).await.unwrap().get(0);
 let listed=response(app.clone(),&alice,"GET",&path,None).await;assert_eq!(listed.0,StatusCode::OK);assert_eq!(listed.1["data"][0]["attemptId"],attempt.to_string());assert!(!listed.1.to_string().contains("private"));
 let id=Uuid::new_v4();let body=json!({"requestId":id,"jobId":job,"attemptId":attempt,"decision":"retry","reason":"checked"});
 db.batch_execute("CREATE FUNCTION public.reject_telegram_jwt_resolution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test'; END $$; CREATE TRIGGER reject_telegram_jwt_resolution BEFORE INSERT ON public.novu_rust_operator_jwt_telegram_resolution_requests FOR EACH ROW EXECUTE FUNCTION public.reject_telegram_jwt_resolution()").await.unwrap();
 assert_eq!(response(app.clone(),&alice,"POST",&resolve,Some(body.clone())).await.0,StatusCode::INTERNAL_SERVER_ERROR);assert_eq!(db.query_one("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id=$1",&[&job]).await.unwrap().get::<_,String>(0),"unknown");
 db.batch_execute("DROP TRIGGER reject_telegram_jwt_resolution ON public.novu_rust_operator_jwt_telegram_resolution_requests").await.unwrap();
 let (a,b)=tokio::join!(response(app.clone(),&alice,"POST",&resolve,Some(body.clone())),response(app2.clone(),&alice,"POST",&resolve,Some(body.clone())));assert_eq!(a.0,StatusCode::OK);assert_eq!(b.0,StatusCode::OK);assert_ne!(a.1["replayed"],b.1["replayed"]);
 let mut conflict=body.clone();conflict["reason"]=json!("different");assert_eq!(response(app.clone(),&alice,"POST",&resolve,Some(conflict)).await.0,StatusCode::CONFLICT);
 let lookup=format!("/v1/experimental/operator/telegram-resolution-request?environmentId={env}&requestId={id}&decision=retry");assert_eq!(response(app.clone(),&alice,"GET",&lookup,None).await.1["jobId"],job.to_string());assert_eq!(response(app.clone(),&bob,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
 assert_eq!(db.query_one("SELECT status FROM public.novu_queue_jobs WHERE id=$1",&[&job]).await.unwrap().get::<_,String>(0),"pending");
 let accepted_job=Uuid::new_v4();db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_telegram','{}','failed')",&[&accepted_job,&env]).await.unwrap();let accepted_attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown') RETURNING attempt_id",&[&accepted_job,&env]).await.unwrap().get(0);
 let accepted=json!({"requestId":Uuid::new_v4(),"jobId":accepted_job,"attemptId":accepted_attempt,"decision":"accepted","reason":"provider checked"});assert_eq!(response(app.clone(),&alice,"POST",&resolve,Some(accepted)).await.0,StatusCode::OK);assert_eq!(db.query_one("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id=$1",&[&accepted_job]).await.unwrap().get::<_,String>(0),"accepted");
 let rejected_job=Uuid::new_v4();db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_telegram','{}','failed')",&[&rejected_job,&env]).await.unwrap();let rejected_attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES($1,$2,'rejected') RETURNING attempt_id",&[&rejected_job,&env]).await.unwrap().get(0);let invalid=json!({"requestId":Uuid::new_v4(),"jobId":rejected_job,"attemptId":rejected_attempt,"decision":"accepted","reason":"checked"});assert_eq!(response(app.clone(),&alice,"POST",&resolve,Some(invalid)).await.0,StatusCode::BAD_REQUEST);
 db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_history(text,text,text,text,bigint,uuid,uuid),public.novu_rust_telegram_resolution_history(text,uuid,uuid),public.novu_rust_telegram_resolve(text,uuid,uuid,uuid,text,text) TO {role}")).await.unwrap();
 let history=format!("/v1/experimental/operator/telegram-resolution-history?environmentId={env}&jobId={job}");assert_eq!(response(app.clone(),&alice,"GET",&history,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_history_viewers VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&history,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_telegram_history_viewers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
 let cli=connect(&url).await;cli.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();
 db.execute("UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",&[&job]).await.unwrap();let cli_attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown') RETURNING attempt_id",&[&job,&env]).await.unwrap().get(0);let cli_id=Uuid::new_v4();cli.query_one("SELECT public.novu_rust_telegram_resolve($1,$2,$3,$4,'accepted','cli checked')",&[&env,&cli_id,&job,&cli_attempt]).await.unwrap();
 let data=response(app.clone(),&alice,"GET",&history,None).await;assert_eq!(data.0,StatusCode::OK);let entries=data.1["data"].as_array().unwrap();assert_eq!(entries.len(),2);let jwt_entry=entries.iter().find(|v|v["operator"].is_object()).unwrap();assert_eq!(jwt_entry["operator"]["subject"],"alice");assert_eq!(jwt_entry["operator"]["requestId"],id.to_string());assert!(entries.iter().any(|v|v["requestId"]==cli_id.to_string()&&v["operator"].is_null()));
 db.execute("INSERT INTO public.novu_rust_telegram_resolutions(environment_id,request_id,job_id,attempt_id,previous_state,decision,reason,database_principal,principal_oid,previous_updated_at) SELECT $1,gen_random_uuid(),$2,$3,'unknown','accepted','paging',$4::text::name,(SELECT oid FROM pg_roles WHERE rolname::text=$4::text),clock_timestamp() FROM generate_series(1,51)",&[&env,&job,&cli_attempt,&role]).await.unwrap();
 let first=response(app.clone(),&alice,"GET",&history,None).await.1;assert_eq!(first["data"].as_array().unwrap().len(),50);assert_eq!(first["hasMore"],true);let second=response(app.clone(),&alice,"GET",&format!("{history}&after={}",first["next"].as_str().unwrap()),None).await.1;assert_eq!(second["data"].as_array().unwrap().len(),3);assert_eq!(second["hasMore"],false);
 assert_eq!(response(app.clone(),&bob,"GET",&history,None).await.0,StatusCode::UNAUTHORIZED);assert!(cli.query("SELECT * FROM public.novu_rust_telegram_resolutions",&[]).await.is_err());
 db.execute("DELETE FROM public.novu_rust_telegram_history_viewers WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&history,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("UPDATE public.novu_rust_operator_jwt_telegram_resolution_grants SET can_retry=false WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app,&alice,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
}
