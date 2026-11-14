//! JWT alert scope, pagination and revocation in a disposable PostgreSQL database.
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
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
const PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-private.pem");
const PUBLIC: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-public.pem");
struct ApiProcess(Child);
impl Drop for ApiProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn connect(url: &str) -> Client {
    let (client, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    client
}
fn request(token: &str, query: &str) -> Request<Body> {
    Request::builder()
        .uri(format!("/v1/experimental/operator/alerts?{query}"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn jwt_alert_read_is_scoped_paginated_and_fenced() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('jwt-alert-a','org'),('jwt-alert-b','org')").await.unwrap();
    let role = format!("jwt_alert_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_alerts(text,text,text,text,bigint,text,text),public.novu_rust_console_alerts_page(text,text,text),public.novu_rust_operator_jwt_resend_result(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_jobs(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_workflow_read(text,text,text,text,bigint,text,text) TO {role}")).await.unwrap();
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_workflow_write(text,text,text,text,bigint,text,text,jsonb,jsonb,text) TO {role}")).await.unwrap();
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_workflow_audit_read(text,text,text,text,bigint,text,text,uuid) TO {role}")).await.unwrap();
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_workflow_write_v2(text,text,text,text,bigint,uuid,text,text,jsonb,jsonb,text),public.novu_rust_operator_jwt_workflow_request_read(text,text,text,text,bigint,text,uuid) TO {role}")).await.unwrap();
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_workflow_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_workflow_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_workflow_grant_request(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_workflow_grant_audit_read(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id,alert_read) VALUES('https://issuer.test','person','console','jwt-alert-a',true),('https://issuer.test','reader','console','jwt-alert-a',false); INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,attempt_id) SELECT 'jwt-alert-a','slack','target-'||lpad(n::text,3,'0'),'unknown',gen_random_uuid() FROM generate_series(1,101) n; INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES('jwt-alert-b','email','private','unknown')").await.unwrap();
    db.batch_execute("WITH c AS (INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at,subscriber_read) VALUES(encode(sha256(convert_to(repeat('a',64),'UTF8')),'hex'),'jwt-alert-a',clock_timestamp()+interval '1 hour',false) RETURNING credential_id) INSERT INTO public.novu_rust_console_alert_grants SELECT credential_id FROM c").await.unwrap();
    let api_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let api = connect(&api_url).await;
    assert!(
        api.query("SELECT * FROM public.novu_rust_alert_delivery_slots", &[])
            .await
            .is_err()
    );
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_resend_auto_correlations",
            &[]
        )
        .await
        .is_err()
    );
    assert!(
        api.query("SELECT * FROM public.novu_rust_workflows", &[])
            .await
            .is_err()
    );
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_workflow_grants",
            &[]
        )
        .await
        .is_err()
    );
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_workflow_grant_admins",
            &[]
        )
        .await
        .is_err()
    );
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_workflow_grant_audit",
            &[]
        )
        .await
        .is_err()
    );
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_workflow_audit",
            &[]
        )
        .await
        .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let make = |sub: &str| {
        let mut h = Header::new(Algorithm::RS256);
        h.typ = Some("at+jwt".into());
        h.kid = Some("key".into());
        encode(&h,&json!({"iss":"https://issuer.test","sub":sub,"aud":"swallow","exp":now+120,"iat":now-1,"jti":"token","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
    };
    let token = make("person");
    db.batch_execute("INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html) VALUES('jwt-alert-a','grant-fixture',true,true,'Subject','Body')").await.unwrap();
    let grant_get = |jwt: &str, env: &str, resource: &str, permission: &str| {
        Request::builder().uri(format!("/v1/experimental/operator/workflow-grants?environmentId={env}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=reader&targetClientId=console&resource={resource}&permission={permission}")).header(header::AUTHORIZATION,format!("Bearer {jwt}")).body(Body::empty()).unwrap()
    };
    let grant_put = |jwt: &str, body: Value| {
        Request::builder()
            .method("PUT")
            .uri("/v1/experimental/operator/workflow-grants?environmentId=jwt-alert-a")
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let grant_lookup = |jwt: &str, id: Uuid| {
        Request::builder().uri(format!("/v1/experimental/operator/workflow-grants/request?environmentId=jwt-alert-a&requestId={id}")).header(header::AUTHORIZATION,format!("Bearer {jwt}")).body(Body::empty()).unwrap()
    };
    assert_eq!(
        app.clone()
            .oneshot(grant_get(&token, "jwt-alert-a", "email", "read"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_workflow_grant_admins VALUES('https://issuer.test','person','console','jwt-alert-a',true,true)").await.unwrap();
    let response = app
        .clone()
        .oneshot(grant_get(&token, "jwt-alert-a", "email", "read"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["targetExists"], true);
    assert_eq!(value["granted"], false);
    let grant_id = Uuid::new_v4();
    let grant = json!({"requestId":grant_id,"targetIssuer":"https://issuer.test","targetSubject":"reader","targetClientId":"console","resource":"email","permission":"read","expected":false,"desired":true,"reason":"ticket"});
    let response = app
        .clone()
        .oneshot(grant_put(&token, grant.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["changed"], true);
    assert_eq!(value["replayed"], false);
    let audit_get = |jwt: &str, env: &str, after: &str| {
        Request::builder()
            .uri(format!(
                "/v1/experimental/operator/workflow-grants/audit?environmentId={env}{after}"
            ))
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        app.clone()
            .oneshot(audit_get(&token, "jwt-alert-a", ""))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_workflow_grant_audit_viewers VALUES('https://issuer.test','person','console','jwt-alert-a'); INSERT INTO public.novu_rust_operator_jwt_workflow_grant_audit(id,environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason) SELECT ('00000000-0000-0000-0000-'||lpad(n::text,12,'0'))::uuid,'jwt-alert-a','issuer','actor','console','postgres',gen_random_uuid(),'issuer','target','console','email','read',false,true,'fixture' FROM generate_series(1,51)n; INSERT INTO public.novu_rust_operator_jwt_workflow_grant_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason) VALUES('jwt-alert-b','issuer','private','console','postgres',gen_random_uuid(),'issuer','target','console','email','read',false,true,'private')").await.unwrap();
    let response = app
        .clone()
        .oneshot(audit_get(&token, "jwt-alert-a", ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let page: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(page["data"].as_array().unwrap().len(), 50);
    assert_eq!(page["hasMore"], true);
    assert!(!page.to_string().contains("private"));
    let after = format!("&after={}", page["next"].as_str().unwrap());
    let response = app
        .clone()
        .oneshot(audit_get(&token, "jwt-alert-a", &after))
        .await
        .unwrap();
    let page: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(page["data"].as_array().unwrap().len(), 2);
    assert_eq!(page["hasMore"], false);
    assert!(page["next"].is_null());
    assert_eq!(
        app.clone()
            .oneshot(audit_get(&token, "jwt-alert-b", ""))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(audit_get(&make("reader"), "jwt-alert-a", ""))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(audit_get(&token, "jwt-alert-a", "&after=bad"))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("DELETE FROM public.novu_rust_operator_jwt_workflow_grant_audit_viewers WHERE subject='person'").await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(audit_get(&token, "jwt-alert-a", ""))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let target_read = || {
        Request::builder().uri("/v1/experimental/operator/workflow?environmentId=jwt-alert-a&workflow=grant-fixture&resource=email").header(header::AUTHORIZATION,format!("Bearer {}",make("reader"))).body(Body::empty()).unwrap()
    };
    assert_eq!(
        app.clone().oneshot(target_read()).await.unwrap().status(),
        StatusCode::OK
    );
    let response = app
        .clone()
        .oneshot(grant_put(&token, grant.clone()))
        .await
        .unwrap();
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["replayed"], true);
    let mut altered = grant.clone();
    altered["reason"] = json!("changed");
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, altered))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let response = app
        .clone()
        .oneshot(grant_lookup(&token, grant_id))
        .await
        .unwrap();
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["found"], true);
    assert_eq!(value["targetSubject"], "reader");
    assert_eq!(
        app.clone()
            .oneshot(grant_get(&make("reader"), "jwt-alert-a", "email", "read"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(grant_get(&token, "jwt-alert-b", "email", "read"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(grant_get(&token, "jwt-alert-a", "audit", "write"))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let mut self_change = grant.clone();
    self_change["requestId"] = json!(Uuid::new_v4());
    self_change["targetSubject"] = json!("person");
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, self_change.clone()))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    self_change["targetClientId"] = json!("different-client");
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, self_change))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let mut stale = grant.clone();
    stale["requestId"] = json!(Uuid::new_v4());
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, stale))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let mut revoke = grant.clone();
    revoke["requestId"] = json!(Uuid::new_v4());
    revoke["expected"] = json!(true);
    revoke["desired"] = json!(false);
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, revoke))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone().oneshot(target_read()).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let mut audit_grant = grant.clone();
    audit_grant["requestId"] = json!(Uuid::new_v4());
    audit_grant["resource"] = json!("audit");
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, audit_grant.clone()))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    audit_grant["requestId"] = json!(Uuid::new_v4());
    audit_grant["expected"] = json!(true);
    audit_grant["desired"] = json!(false);
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, audit_grant))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let race_id = Uuid::new_v4();
    let mut race = grant.clone();
    race["requestId"] = json!(race_id);
    race["resource"] = json!("push");
    let api2 = connect(&api_url).await;
    let app2 = novu_rust_api::router(Arc::new(api2)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let (first, second) = tokio::join!(
        app.clone().oneshot(grant_put(&token, race.clone())),
        app2.oneshot(grant_put(&token, race.clone()))
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);
    let first: Value =
        serde_json::from_slice(&first.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let second: Value =
        serde_json::from_slice(&second.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_ne!(first["replayed"], second["replayed"]);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_workflow_grant_audit WHERE request_id=$1",&[&race_id]).await.unwrap().get::<_,i64>(0),1);
    let mut remove_race = race.clone();
    remove_race["requestId"] = json!(Uuid::new_v4());
    remove_race["expected"] = json!(true);
    remove_race["desired"] = json!(false);
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, remove_race))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let mut fail = grant.clone();
    fail["requestId"] = json!(Uuid::new_v4());
    fail["resource"] = json!("push");
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_workflow_grant_audit ADD CONSTRAINT fixture_grant_audit_failure CHECK(false) NOT VALID").await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(grant_put(&token, fail.clone()))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_workflow_grant_audit DROP CONSTRAINT fixture_grant_audit_failure").await.unwrap();
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_workflow_grants WHERE subject='reader' AND resource='push'",&[]).await.unwrap().get::<_,i64>(0),0);
    let mut revoker = connect(&url).await;
    let tx = revoker.transaction().await.unwrap();
    tx.batch_execute(
        "DELETE FROM public.novu_rust_operator_jwt_workflow_grant_admins WHERE subject='person'",
    )
    .await
    .unwrap();
    let pending_request = grant_put(&token, fail);
    let pending = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(pending_request).await.unwrap().status() }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_workflow_grant_change%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    db.batch_execute("INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,sms_content,push_title,push_content,chat_content,email_provider) VALUES ('jwt-alert-a','wf',true,true,'Subject','Email','SMS','Push title','Push','Slack','resend'),('jwt-alert-b','wf',true,true,'other','other','other','other','other','other','sendgrid')").await.unwrap();
    let workflow_request = |jwt: &str, query: &str| {
        Request::builder()
            .uri(format!("/v1/experimental/operator/workflow?{query}"))
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap()
    };
    let query = "environmentId=jwt-alert-a&workflow=wf&resource=email";
    assert_eq!(
        app.clone()
            .oneshot(workflow_request(&token, query))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_workflow_grants SELECT 'https://issuer.test','person','console','jwt-alert-a',r,'read' FROM unnest(ARRAY['email','sms','push','slack','email-provider']) r").await.unwrap();
    for (resource, field, value) in [
        ("email", "content", "Email"),
        ("sms", "content", "SMS"),
        ("push", "content", "Push"),
        ("slack", "content", "Slack"),
        ("email-provider", "provider", "resend"),
    ] {
        let response = app
            .clone()
            .oneshot(workflow_request(
                &token,
                &format!("environmentId=jwt-alert-a&workflow=wf&resource={resource}"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["data"][field], value);
    }
    assert_eq!(
        app.clone()
            .oneshot(workflow_request(&make("reader"), query))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(workflow_request(
                &token,
                "environmentId=jwt-alert-b&workflow=wf&resource=email"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(workflow_request(
                &token,
                "environmentId=jwt-alert-a&workflow=wf&resource=bad"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let workflow_put = |jwt: &str, env: &str, body: Value| {
        Request::builder()
            .method("PUT")
            .uri(format!(
                "/v1/experimental/operator/workflow?environmentId={env}"
            ))
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let initial = json!({"workflow":"wf","resource":"email","expected":{"title":"Subject","content":"Email","active":true},"desired":{"title":"New","content":"New email","active":false},"reason":"ticket-1"});
    assert_eq!(
        app.clone()
            .oneshot(workflow_put(&token, "jwt-alert-a", initial.clone()))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_workflow_grants SELECT 'https://issuer.test','person','console','jwt-alert-a',r,'write' FROM unnest(ARRAY['email','sms','push','slack','email-provider']) r").await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(workflow_put(
                &make("reader"),
                "jwt-alert-a",
                initial.clone()
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(workflow_put(&token, "jwt-alert-b", initial.clone()))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let changes = [
        initial.clone(),
        json!({"workflow":"wf","resource":"sms","expected":{"title":null,"content":"SMS","active":true},"desired":{"title":null,"content":"New SMS","active":false},"reason":"ticket-2"}),
        json!({"workflow":"wf","resource":"push","expected":{"title":"Push title","content":"Push","active":true},"desired":{"title":"Push2","content":"New Push","active":false},"reason":"ticket-3"}),
        json!({"workflow":"wf","resource":"slack","expected":{"content":"Slack","active":true},"desired":{"content":"New Slack","active":false},"reason":"ticket-4"}),
        json!({"workflow":"wf","resource":"email-provider","expected":{"provider":"resend"},"desired":{"provider":"sendgrid"},"reason":"ticket-5"}),
    ];
    for body in changes {
        let response = app
            .clone()
            .oneshot(workflow_put(&token, "jwt-alert-a", body.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(
            app.clone()
                .oneshot(workflow_put(&token, "jwt-alert-a", body))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
    }
    let mut invalid = initial.clone();
    invalid["desired"]["content"] = json!("bad\u{0}");
    assert_eq!(
        app.clone()
            .oneshot(workflow_put(&token, "jwt-alert-a", invalid))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let audit=db.query_one("SELECT count(*), bool_and(subject='person' AND database_principal::text=$1) FROM public.novu_rust_operator_jwt_workflow_audit",&[&role]).await.unwrap();
    assert_eq!(audit.get::<_, i64>(0), 5);
    assert!(audit.get::<_, bool>(1));
    assert!(db.query_one("SELECT row_to_json(a)::text FROM public.novu_rust_operator_jwt_workflow_audit a WHERE resource='email'",&[]).await.unwrap().get::<_,String>(0).find("New email").is_none());
    let mut workflow_revoker = connect(&url).await;
    let write_body = json!({"workflow":"wf","resource":"slack","expected":{"content":"New Slack","active":false},"desired":{"content":"Other","active":true},"reason":"ticket"});
    let tx = workflow_revoker.transaction().await.unwrap();
    tx.batch_execute("DELETE FROM public.novu_rust_operator_jwt_workflow_grants WHERE subject='person' AND resource='slack' AND permission='write'").await.unwrap();
    let workflow_http_request = workflow_put(&token, "jwt-alert-a", write_body.clone());
    let pending = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(workflow_http_request).await.unwrap().status() }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_workflow_write%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_workflow_grants VALUES('https://issuer.test','person','console','jwt-alert-a','slack','write')").await.unwrap();
    let tx = workflow_revoker.transaction().await.unwrap();
    tx.batch_execute("UPDATE public.novu_rust_workflows SET chat_content='competing' WHERE environment_id='jwt-alert-a' AND identifier='wf'").await.unwrap();
    let workflow_http_request = workflow_put(&token, "jwt-alert-a", write_body.clone());
    let pending = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(workflow_http_request).await.unwrap().status() }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_workflow_write%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::CONFLICT);
    let mut rollback_body = write_body.clone();
    rollback_body["expected"]["content"] = json!("competing");
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_workflow_audit ADD CONSTRAINT fixture_reject_workflow_audit CHECK(false) NOT VALID").await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(workflow_put(&token, "jwt-alert-a", rollback_body))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_workflow_audit DROP CONSTRAINT fixture_reject_workflow_audit").await.unwrap();
    assert_eq!(db.query_one("SELECT chat_content FROM public.novu_rust_workflows WHERE environment_id='jwt-alert-a' AND identifier='wf'",&[]).await.unwrap().get::<_,String>(0),"competing");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_operator_jwt_workflow_audit",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        5
    );
    let hold = workflow_revoker.transaction().await.unwrap();
    hold.query_one("SELECT identifier FROM public.novu_rust_workflows WHERE environment_id='jwt-alert-a' AND identifier='wf' FOR UPDATE",&[]).await.unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expires = now + 3;
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("key".into());
    let short=encode(&h,&json!({"iss":"https://issuer.test","sub":"person","aud":"swallow","exp":expires,"iat":now-1,"jti":"short-workflow","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap();
    let mut expiring_body = write_body.clone();
    expiring_body["expected"]["content"] = json!("competing");
    let expiring_request = workflow_put(&short, "jwt-alert-a", expiring_body);
    let pending = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(expiring_request).await.unwrap().status() }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_workflow_write%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    while SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        <= expires
    {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    hold.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(db.query_one("SELECT chat_content FROM public.novu_rust_workflows WHERE environment_id='jwt-alert-a' AND identifier='wf'",&[]).await.unwrap().get::<_,String>(0),"competing");
    let tx = workflow_revoker.transaction().await.unwrap();
    tx.batch_execute("DELETE FROM public.novu_rust_operator_jwt_workflow_grants WHERE subject='person' AND resource='email' AND permission='read'").await.unwrap();
    let workflow_http_request = workflow_request(&token, query);
    let pending = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(workflow_http_request).await.unwrap().status() }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_workflow_read%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        app.clone()
            .oneshot(workflow_request(&token, query))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html) VALUES('jwt-alert-a','wf-v2',true,true,'Old','Old body')").await.unwrap();
    let request_id = Uuid::new_v4();
    let v2_body = json!({"requestId":request_id,"workflow":"wf-v2","resource":"email","expected":{"title":"Old","content":"Old body","active":true},"desired":{"title":"New","content":"Private new body","active":true},"reason":"ticket-v2"});
    let v2_request = |jwt: &str, body: Value| {
        Request::builder()
            .method("PUT")
            .uri("/v1/experimental/operator/workflow-v2?environmentId=jwt-alert-a")
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let lookup_request = |jwt: &str, resource: &str, id: Uuid| {
        Request::builder().uri(format!("/v1/experimental/operator/workflow-request?environmentId=jwt-alert-a&resource={resource}&requestId={id}")).header(header::AUTHORIZATION,format!("Bearer {jwt}")).body(Body::empty()).unwrap()
    };
    let before = app
        .clone()
        .oneshot(lookup_request(&token, "email", request_id))
        .await
        .unwrap();
    assert_eq!(before.status(), StatusCode::OK);
    let before: Value =
        serde_json::from_slice(&before.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(before["found"], false);
    let response = app
        .clone()
        .oneshot(v2_request(&token, v2_body.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let first: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(first["replayed"], false);
    let response = app
        .clone()
        .oneshot(v2_request(&token, v2_body.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let replay: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(replay["replayed"], true);
    let fresh_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut fresh_header = Header::new(Algorithm::RS256);
    fresh_header.typ = Some("at+jwt".into());
    fresh_header.kid = Some("key".into());
    let fresh_token=encode(&fresh_header,&json!({"iss":"https://issuer.test","sub":"person","aud":"swallow","exp":fresh_now+120,"iat":fresh_now,"jti":"fresh-workflow-token","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap();
    let fresh_response = app
        .clone()
        .oneshot(lookup_request(&fresh_token, "email", request_id))
        .await
        .unwrap();
    assert_eq!(fresh_response.status(), StatusCode::OK);
    let fresh_result: Value = serde_json::from_slice(
        &fresh_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes(),
    )
    .unwrap();
    assert_eq!(fresh_result["found"], true);
    let mut altered = v2_body.clone();
    altered["desired"]["content"] = json!("different");
    assert_eq!(
        app.clone()
            .oneshot(v2_request(&token, altered))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let response = app
        .clone()
        .oneshot(lookup_request(&token, "email", request_id))
        .await
        .unwrap();
    let found: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(found["found"], true);
    assert_eq!(found["workflow"], "wf-v2");
    assert!(!found.to_string().contains("Private new body"));
    assert_eq!(
        app.clone()
            .oneshot(lookup_request(&make("reader"), "email", request_id))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_workflow_audit WHERE workflow_identifier='wf-v2'",&[]).await.unwrap().get::<_,i64>(0),1);
    let stored=db.query_one("SELECT request_digest,reason FROM public.novu_rust_operator_jwt_workflow_requests WHERE request_id=$1",&[&request_id]).await.unwrap();
    assert_eq!(stored.get::<_, Vec<u8>>(0).len(), 32);
    assert_eq!(stored.get::<_, String>(1), "ticket-v2");
    let failed_id = Uuid::new_v4();
    let mut failed = v2_body.clone();
    failed["requestId"] = json!(failed_id);
    failed["expected"] = v2_body["desired"].clone();
    failed["desired"]["content"] = json!("other");
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_workflow_audit ADD CONSTRAINT fixture_reject_v2_audit CHECK(false) NOT VALID").await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(v2_request(&token, failed))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_workflow_audit DROP CONSTRAINT fixture_reject_v2_audit").await.unwrap();
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_workflow_requests WHERE request_id=$1",&[&failed_id]).await.unwrap().get::<_,i64>(0),0);
    assert_eq!(db.query_one("SELECT email_html FROM public.novu_rust_workflows WHERE environment_id='jwt-alert-a' AND identifier='wf-v2'",&[]).await.unwrap().get::<_,String>(0),"Private new body");
    let concurrent_id = Uuid::new_v4();
    let mut concurrent = v2_body.clone();
    concurrent["requestId"] = json!(concurrent_id);
    concurrent["expected"] = v2_body["desired"].clone();
    concurrent["desired"]["content"] = json!("Concurrent update");
    let api2 = connect(&api_url).await;
    assert!(
        api2.query(
            "SELECT * FROM public.novu_rust_operator_jwt_workflow_requests",
            &[]
        )
        .await
        .is_err()
    );
    let app2 = novu_rust_api::router(Arc::new(api2)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let (one, two) = tokio::join!(
        app.clone().oneshot(v2_request(&token, concurrent.clone())),
        app2.oneshot(v2_request(&token, concurrent.clone()))
    );
    let one = one.unwrap();
    let two = two.unwrap();
    assert_eq!(one.status(), StatusCode::OK);
    assert_eq!(two.status(), StatusCode::OK);
    let one: Value =
        serde_json::from_slice(&one.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let two: Value =
        serde_json::from_slice(&two.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_ne!(one["replayed"], two["replayed"]);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_workflow_audit WHERE workflow_identifier='wf-v2'",&[]).await.unwrap().get::<_,i64>(0),2);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_workflow_requests WHERE request_id=$1",&[&concurrent_id]).await.unwrap().get::<_,i64>(0),1);
    let mut different_resource = concurrent.clone();
    different_resource["resource"] = json!("slack");
    different_resource["expected"] = json!({"content":"competing","active":false});
    different_resource["desired"] = json!({"content":"different","active":true});
    assert_eq!(
        app.clone()
            .oneshot(v2_request(&token, different_resource))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    db.batch_execute("DELETE FROM public.novu_rust_operator_jwt_workflow_grants WHERE subject='person' AND resource='email' AND permission='write'").await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(lookup_request(&token, "email", request_id))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(v2_request(&token, v2_body.clone()))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let audit_request = |jwt: &str, query: &str| {
        Request::builder()
            .uri(format!("/v1/experimental/operator/workflow-audit?{query}"))
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap()
    };
    let audit_query = "environmentId=jwt-alert-a&workflow=wf&resource=email";
    assert_eq!(
        app.clone()
            .oneshot(audit_request(&token, audit_query))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_workflow_audit_grants VALUES('https://issuer.test','person','console','jwt-alert-a'); INSERT INTO public.novu_rust_operator_jwt_workflow_audit(environment_id,workflow_identifier,resource,issuer,subject,client_id,database_principal,reason) SELECT 'jwt-alert-a','wf','email','https://issuer.test','test-actor','console',session_user,'fixture' FROM generate_series(1,51); INSERT INTO public.novu_rust_operator_jwt_workflow_audit(environment_id,workflow_identifier,resource,issuer,subject,client_id,database_principal,reason) VALUES ('jwt-alert-b','wf','email','secret-issuer','other','console',session_user,'private')").await.unwrap();
    let response = app
        .clone()
        .oneshot(audit_request(&token, audit_query))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let first: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(first["data"].as_array().unwrap().len(), 50);
    assert_eq!(first["hasMore"], true);
    let cursor = first["next"].as_str().unwrap();
    let response = app
        .clone()
        .oneshot(audit_request(
            &token,
            &format!("{audit_query}&after={cursor}"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let second: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(second["data"].as_array().unwrap().len(), 2);
    assert_eq!(second["hasMore"], false);
    assert!(second["next"].is_null());
    let ids: Vec<&str> = first["data"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["data"].as_array().unwrap())
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(ids.windows(2).all(|x| x[0] < x[1]));
    assert!(
        !first.to_string().contains("New email") && !first.to_string().contains("secret-issuer")
    );
    assert_eq!(
        app.clone()
            .oneshot(audit_request(&make("reader"), audit_query))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(audit_request(
                &token,
                "environmentId=jwt-alert-b&workflow=wf&resource=email"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(audit_request(&token, &format!("{audit_query}&after=bad")))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let tx = workflow_revoker.transaction().await.unwrap();
    tx.batch_execute(
        "DELETE FROM public.novu_rust_operator_jwt_workflow_audit_grants WHERE subject='person'",
    )
    .await
    .unwrap();
    let req = audit_request(&token, audit_query);
    let pending = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(req).await.unwrap().status() }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_workflow_audit_read%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    let job = Uuid::new_v4();
    let tag = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        job,
        "jwt-alert-a",
        novu_postgres_queue::resend_queue::JOB_KIND,
        &json!({"private":"payload-secret"}),
    )
    .await
    .unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='completed',resend_delivery_result='delivered' WHERE id=$1", &[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id,correlation_id) VALUES ($1,'jwt-alert-a','accepted','private-provider',$2)", &[&job,&tag]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_recipients VALUES ($1,'jwt-alert-a','to',0,'private-recipient-hash')", &[&job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash,correlation_id) VALUES ('jwt-alert-a','result-event','private-provider','email.delivered','private-recipient-hash',$1)", &[&tag]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_auto_correlations(job_id,environment_id,correlation_id,provider_message_id,evidence_count) VALUES ($1,'jwt-alert-a',$2,'private-provider',1)", &[&job,&tag]).await.unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let result_request = |jwt: &str, query: &str| {
        Request::builder()
            .uri(format!("/v1/experimental/operator/resend-result?{query}"))
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap()
    };
    let result_query = format!("environmentId=jwt-alert-a&jobId={job}");
    assert_eq!(
        app.clone()
            .oneshot(result_request(&token, &result_query))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_resend_result_grants VALUES('https://issuer.test','person','console','jwt-alert-a'),('https://issuer.test','reader','console','jwt-alert-a')").await.unwrap();
    let response = app
        .clone()
        .oneshot(result_request(&token, &result_query))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["jobId"], job.to_string());
    assert_eq!(value["result"], "delivered");
    assert_eq!(value["autoCorrelated"], true);
    assert_eq!(value["autoCorrelationConflict"], false);
    db.batch_execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) SELECT gen_random_uuid(),'jwt-alert-a','experimental_resend_email','{\"private\":\"hidden\"}'::jsonb FROM generate_series(1,21); INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES(gen_random_uuid(),'jwt-alert-b','experimental_resend_email','{}')").await.unwrap();
    let jobs_request = |jwt: &str, query: &str| {
        Request::builder()
            .uri(format!("/v1/experimental/operator/resend-jobs?{query}"))
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        app.clone()
            .oneshot(jobs_request(&token, "environmentId=jwt-alert-a"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_resend_jobs_grants VALUES('https://issuer.test','person','console','jwt-alert-a')").await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(jobs_request(&make("reader"), "environmentId=jwt-alert-a"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let first = app
        .clone()
        .oneshot(jobs_request(&token, "environmentId=jwt-alert-a"))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()[header::CACHE_CONTROL], "no-store");
    let first: Value =
        serde_json::from_slice(&first.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(first["data"].as_array().unwrap().len(), 20);
    assert_eq!(first["hasMore"], true);
    let cursor = first["next"].as_str().unwrap();
    let second = app
        .clone()
        .oneshot(jobs_request(
            &token,
            &format!("environmentId=jwt-alert-a&after={cursor}"),
        ))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second: Value =
        serde_json::from_slice(&second.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(second["data"].as_array().unwrap().len(), 2);
    assert_eq!(second["hasMore"], false);
    assert!(second["next"].is_null());
    let ids: Vec<&str> = first["data"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["data"].as_array().unwrap())
        .map(|item| item["jobId"].as_str().unwrap())
        .collect();
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        !first.to_string().contains("private-provider")
            && !first.to_string().contains("hidden")
            && !first.to_string().contains(&tag.to_string())
    );
    assert_eq!(
        app.clone()
            .oneshot(jobs_request(&token, "environmentId=jwt-alert-b"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(jobs_request(&token, "environmentId=jwt-alert-a&after=bad"))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert!(
        !value.to_string().contains("private-provider")
            && !value.to_string().contains("private-recipient-hash")
            && !value.to_string().contains("payload-secret")
            && !value.to_string().contains(&tag.to_string())
    );
    // The new grant is separate from the existing alert-read permission.
    assert_eq!(
        app.clone()
            .oneshot(result_request(&make("reader"), &result_query))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(result_request(
                &token,
                &format!("environmentId=jwt-alert-b&jobId={job}")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(result_request(
                &token,
                "environmentId=jwt-alert-a&jobId=bad"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.clone()
            .oneshot(result_request(
                &token,
                &format!("{result_query}&jobId={job}")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash,correlation_id) VALUES ('jwt-alert-a','result-conflict','private-provider','email.delivered','private-recipient-hash',$1)", &[&Uuid::new_v4()]).await.unwrap();
    let response = app
        .clone()
        .oneshot(result_request(&token, &result_query))
        .await
        .unwrap();
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["result"], "unknown");
    assert_eq!(value["autoCorrelationConflict"], true);
    db.execute("DELETE FROM public.novu_rust_operator_jwt_resend_result_grants WHERE subject='person' AND environment_id='jwt-alert-a'", &[]).await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(result_request(&token, &result_query))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_resend_result_grants VALUES('https://issuer.test','person','console','jwt-alert-a')").await.unwrap();
    let mut revoker = connect(&url).await;
    let revocation = revoker.transaction().await.unwrap();
    revocation.execute("DELETE FROM public.novu_rust_operator_jwt_resend_result_grants WHERE subject='person' AND environment_id='jwt-alert-a'", &[]).await.unwrap();
    let pending_request = result_request(&token, &result_query);
    let pending = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(pending_request).await.unwrap().status() }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_resend_result%')",&[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    revocation.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_resend_result_grants VALUES('https://issuer.test','person','console','jwt-alert-a')").await.unwrap();
    let hold = revoker.transaction().await.unwrap();
    hold.query_one("SELECT subject FROM public.novu_rust_operator_jwt_resend_result_grants WHERE subject='person' AND environment_id='jwt-alert-a' FOR UPDATE", &[]).await.unwrap();
    let current = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expiry = current + 3;
    let mut head = Header::new(Algorithm::RS256);
    head.typ = Some("at+jwt".into());
    head.kid = Some("key".into());
    let short_token=encode(&head,&json!({"iss":"https://issuer.test","sub":"person","aud":"swallow","exp":expiry,"iat":current,"jti":"short","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap();
    let short_request = result_request(&short_token, &result_query);
    let pending = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(short_request).await.unwrap().status() }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_resend_result%')",&[]).await.unwrap().get::<_,bool>(0) {waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    while SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        <= expiry
    {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    hold.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        app.clone()
            .oneshot(request(&make("reader"), "environmentId=jwt-alert-a"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(request(&token, "environmentId=jwt-alert-b"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                &token,
                "environmentId=jwt-alert-a&afterChannel=slack"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let first = app
        .clone()
        .oneshot(request(&token, "environmentId=jwt-alert-a"))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()[header::CACHE_CONTROL], "no-store");
    let first: Value =
        serde_json::from_slice(&first.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(first["data"].as_array().unwrap().len(), 100);
    assert_eq!(first["hasMore"], true);
    assert_eq!(first["next"]["targetId"], "target-100");
    assert!(first["data"][0]["attemptId"].is_string());
    let legacy = Request::builder()
        .uri("/v1/experimental/console/alerts")
        .header(
            header::AUTHORIZATION,
            format!("SwallowConsole {}", "a".repeat(64)),
        )
        .body(Body::empty())
        .unwrap();
    let legacy = app.clone().oneshot(legacy).await.unwrap();
    assert_eq!(legacy.status(), StatusCode::OK);
    let legacy: Value =
        serde_json::from_slice(&legacy.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(first, legacy);
    let second = app
        .clone()
        .oneshot(request(
            &token,
            "environmentId=jwt-alert-a&afterChannel=slack&afterTarget=target-100",
        ))
        .await
        .unwrap();
    let second: Value =
        serde_json::from_slice(&second.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(second["data"].as_array().unwrap().len(), 1);
    assert_eq!(second["data"][0]["targetId"], "target-101");
    assert_eq!(second["hasMore"], false);
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let config=json!({"issuer":"https://issuer.test","audience":"swallow","keys":[{"kid":"key","pem":std::str::from_utf8(PUBLIC).unwrap()}]}).to_string();
    let _server = ApiProcess(
        Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", &api_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_OPERATOR_JWT_CONFIG", config)
            .env_remove("SWALLOW_OPERATOR_JWT_CONFIG_FILE")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut success = false;
    for _ in 0..100 {
        let output=Command::new("curl").args(["--silent","--max-time","1","--write-out","%{http_code}","--output","/dev/null","--header",&format!("Authorization: Bearer {token}"),&format!("http://127.0.0.1:{port}/v1/experimental/operator/alerts?environmentId=jwt-alert-a")]).output().unwrap();
        if output.status.success() && output.stdout == b"200" {
            success = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        success,
        "configured API process failed to return JWT alert page"
    );
    let mut blocker = connect(&url).await;
    let tx = blocker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_identities SET alert_read=false WHERE subject='person' AND environment_id='jwt-alert-a'",&[]).await.unwrap();
    let pending = tokio::spawn({
        let app = app.clone();
        let token = token.clone();
        async move {
            app.oneshot(request(&token, "environmentId=jwt-alert-a"))
                .await
                .unwrap()
                .status()
        }
    });
    let mut waiting = false;
    for _ in 0..100 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_alerts%')",&[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
}
