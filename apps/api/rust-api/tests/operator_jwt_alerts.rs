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
    let mut workflow_revoker = connect(&url).await;
    let tx = workflow_revoker.transaction().await.unwrap();
    tx.batch_execute("DELETE FROM public.novu_rust_operator_jwt_workflow_grants WHERE subject='person' AND resource='email'").await.unwrap();
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
