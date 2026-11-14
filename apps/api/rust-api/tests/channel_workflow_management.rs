//! Restricted channel-step management and real trigger-to-queue integration.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;

async fn call(
    app: axum::Router,
    method: &str,
    path: &str,
    key: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("ApiKey {key}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
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
#[ignore = "requires disposable PostgreSQL"]
async fn channels_are_authorized_audited_and_create_delivery_jobs() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    db.batch_execute("INSERT INTO novu_rust_users VALUES ('u'); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active) VALUES ('a','workflow',true,true,false),('b','workflow',true,true,false); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('a','alice','alice-id','{\"firstName\":\"Jane\",\"email\":\"test@example.test\",\"phone\":\"+15555550123\"}'); INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier) VALUES ('a','fcm-id','fcm'); INSERT INTO novu_rust_fcm_devices(environment_id,subscriber_id,integration_internal_id,device_tokens) VALUES ('a','alice','fcm-id',ARRAY['fixture-token'])").await.unwrap();
    db.batch_execute("UPDATE novu_rust_workflows SET inbox_metadata='{\"id\":\"workflow-id\"}'::jsonb WHERE environment_id='a' AND identifier='workflow'").await.unwrap();
    let hash = |s: &[u8]| format!("{:x}", Sha256::digest(s));
    let (a, b, reader) = (hash(b"a-key"), hash(b"b-key"), hash(b"read-key"));
    db.execute("INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'a','u'),($2,'b','u'),($3,'a','u')",&[&a,&b,&reader]).await.unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let (mut setup, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let email = "/v1/experimental/workflows/workflow/channel-steps/email";
    let sms = "/v1/experimental/workflows/workflow/channel-steps/sms";
    let push = "/v1/experimental/workflows/workflow/channel-steps/push";
    assert_eq!(
        call(app.clone(), "GET", email, "a-key", Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    setup.execute("INSERT INTO novu_rust_channel_workflow_permissions SELECT $1,c,p FROM unnest(ARRAY['email','sms','push']) c CROSS JOIN unnest(ARRAY['read','write']) p",&[&a]).await.unwrap();
    setup.execute("INSERT INTO novu_rust_channel_workflow_permissions VALUES ($1,'email','read'),($2,'sms','read')",&[&b,&reader]).await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            email,
            "read-key",
            json!({"title":"x","content":"x","active":true})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(app.clone(), "GET", email, "b-key", Value::Null)
            .await
            .1["data"]["content"],
        Value::Null
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            sms,
            "a-key",
            json!({"title":"bad","content":"x","active":true})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            push,
            "a-key",
            json!({"title":"x","content":null,"active":true})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            email,
            "a-key",
            json!({"title":"x","content":"\u{0000}","active":true})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            "/v1/experimental/workflows/workflow/channel-steps/other",
            "a-key",
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for (path, title, content) in [
        (email, Some("Subject"), "Email {{subscriber.firstName}}"),
        (sms, None, "SMS {{subscriber.firstName}}"),
        (push, Some("Push"), "Push {{subscriber.firstName}}"),
    ] {
        let (status, body) = call(
            app.clone(),
            "PUT",
            path,
            "a-key",
            json!({"title":title,"content":content,"active":true}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["content"], content);
        assert_eq!(
            call(app.clone(), "GET", path, "a-key", Value::Null).await.1,
            body
        );
    }
    let role = format!("channel_api_{}", uuid::Uuid::new_v4().simple());
    setup.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.novu_rust_users,public.novu_rust_api_keys,public.novu_rust_workflows TO {role}; GRANT UPDATE(email_subject,email_html,email_step_active,sms_content,sms_step_active,push_title,push_content,push_step_active) ON public.novu_rust_workflows TO {role}; GRANT INSERT ON public.novu_rust_channel_workflow_audit TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_channel_workflow_grant(text,text,text,text) TO {role}")).await.unwrap();
    let (limited, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    limited
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        limited
            .query(
                "SELECT * FROM public.novu_rust_channel_workflow_permissions",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        limited
            .query("SELECT * FROM public.novu_rust_channel_workflow_audit", &[])
            .await
            .is_err()
    );
    let limited_app = novu_rust_api::router_with_subscriber_secret(Arc::new(limited), None);
    assert_eq!(
        call(
            limited_app,
            "PUT",
            sms,
            "a-key",
            json!({"title":null,"content":"SMS {{subscriber.firstName}}","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        setup
            .query_one(
                "SELECT count(*) FROM novu_rust_channel_workflow_audit WHERE environment_id='a'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        4
    );
    setup.batch_execute("CREATE FUNCTION public.fail_channel_audit_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit failure'; END $$; CREATE TRIGGER fail_channel_audit_test BEFORE INSERT ON novu_rust_channel_workflow_audit FOR EACH ROW EXECUTE FUNCTION public.fail_channel_audit_test()").await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            sms,
            "a-key",
            json!({"title":null,"content":"bad","active":false})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    setup
        .batch_execute("DROP TRIGGER fail_channel_audit_test ON novu_rust_channel_workflow_audit")
        .await
        .unwrap();
    assert_eq!(
        call(app.clone(), "GET", sms, "a-key", Value::Null).await.1["data"]["content"],
        "SMS {{subscriber.firstName}}"
    );
    let revocation = setup.transaction().await.unwrap();
    revocation.execute("DELETE FROM public.novu_rust_channel_workflow_permissions WHERE key_hash=$1 AND channel='push' AND permission='write'",&[&a]).await.unwrap();
    let revoke_app = app.clone();
    let pending = tokio::spawn(async move {
        call(
            revoke_app,
            "PUT",
            push,
            "a-key",
            json!({"title":"bad","content":"bad","active":true}),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !pending.is_finished(),
        "request must wait for permission revocation"
    );
    revocation.commit().await.unwrap();
    assert_eq!(pending.await.unwrap().0, StatusCode::FORBIDDEN);
    assert_eq!(
        call(app.clone(), "GET", push, "a-key", Value::Null).await.1["data"]["content"],
        "Push {{subscriber.firstName}}"
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"channels-1","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    for kind in [
        novu_postgres_queue::sendgrid_queue::JOB_KIND,
        novu_postgres_queue::twilio_queue::JOB_KIND,
        novu_postgres_queue::fcm_queue::JOB_KIND,
    ] {
        assert_eq!(
            setup
                .query_one(
                    "SELECT count(*) FROM novu_queue_jobs WHERE environment_id='a' AND kind=$1",
                    &[&kind]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
            1,
            "{kind}"
        );
    }
    use novu_postgres_queue::{
        email_step_policy::Policy,
        sendgrid::{Config as SendgridConfig, Region, Sendgrid},
    };
    let provider = Sendgrid::new(SendgridConfig {
        api_key: "fixture".into(),
        from: "sender@example.test".into(),
        sender_name: "Fixture".into(),
        region: Region::Global,
    })
    .unwrap();
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            email,
            "a-key",
            json!({"title":"Changed","content":"Email {{subscriber.firstName}}","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(
        novu_postgres_queue::sendgrid_queue::process_one_with_step_policy(
            &mut setup,
            "a",
            &provider,
            Policy::Unchanged
        )
        .await
        .unwrap()
    );
    assert_eq!(setup.query_one("SELECT last_error FROM novu_queue_jobs WHERE environment_id='a' AND kind=$1 AND status='cancelled'",&[&novu_postgres_queue::sendgrid_queue::JOB_KIND]).await.unwrap().get::<_,String>(0),"email workflow step changed");
    assert_eq!(
        setup
            .query_one("SELECT count(*) FROM novu_rust_sendgrid_attempts", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            sms,
            "a-key",
            json!({"title":null,"content":null,"active":false})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"channels-2","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    assert_eq!(
        setup
            .query_one(
                "SELECT count(*) FROM novu_queue_jobs WHERE environment_id='a' AND kind=$1",
                &[&novu_postgres_queue::twilio_queue::JOB_KIND]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            email,
            "a-key",
            json!({"title":"Changed","content":"Email {{subscriber.firstName}}","active":false})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(
        novu_postgres_queue::sendgrid_queue::process_one_with_step_policy(
            &mut setup,
            "a",
            &provider,
            Policy::Active
        )
        .await
        .unwrap()
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_queue_jobs WHERE environment_id='a' AND kind=$1 AND last_error='email workflow step disabled'",&[&novu_postgres_queue::sendgrid_queue::JOB_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            email,
            "a-key",
            json!({"title":"Changed","content":"Email {{subscriber.firstName}}","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    setup.batch_execute("UPDATE novu_rust_workflows SET email_provider='resend' WHERE environment_id='a' AND identifier='workflow'").await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"channels-3","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            email,
            "a-key",
            json!({"title":"Changed","content":"Email {{subscriber.firstName}}","active":false})
        )
        .await
        .0,
        StatusCode::OK
    );
    let resend = novu_postgres_queue::resend::Resend::new(novu_postgres_queue::resend::Config {
        api_key: "fixture".into(),
        from: "sender@example.test".into(),
        sender_name: "Fixture".into(),
    })
    .unwrap();
    assert!(
        novu_postgres_queue::resend_queue::process_one_with_policies(
            &mut setup,
            "a",
            &resend,
            false,
            Policy::Active
        )
        .await
        .unwrap()
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_queue_jobs WHERE environment_id='a' AND kind=$1 AND last_error='email workflow step disabled'",&[&novu_postgres_queue::resend_queue::JOB_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        setup
            .query_one("SELECT count(*) FROM novu_rust_resend_attempts", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            email,
            "a-key",
            json!({"title":"Changed","content":"Email {{subscriber.firstName}}","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"channels-4","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    let tx = setup.transaction().await.unwrap();
    tx.execute("UPDATE novu_rust_workflows SET email_step_active=false WHERE environment_id='a' AND identifier='workflow'",&[]).await.unwrap();
    let (mut other, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let waiting = tokio::spawn(async move {
        let provider =
            novu_postgres_queue::resend::Resend::new(novu_postgres_queue::resend::Config {
                api_key: "fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Fixture".into(),
            })
            .unwrap();
        novu_postgres_queue::resend_queue::process_one_with_policies(
            &mut other,
            "a",
            &provider,
            false,
            Policy::Active,
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !waiting.is_finished(),
        "worker must wait for workflow update"
    );
    tx.commit().await.unwrap();
    assert!(waiting.await.unwrap().unwrap());
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_queue_jobs WHERE environment_id='a' AND kind=$1 AND last_error='email workflow step disabled'",&[&novu_postgres_queue::resend_queue::JOB_KIND]).await.unwrap().get::<_,i64>(0),2);
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            email,
            "a-key",
            json!({"title":"Changed","content":"Email {{subscriber.firstName}}","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    setup.batch_execute("UPDATE novu_rust_workflows SET email_provider='sendgrid' WHERE environment_id='a' AND identifier='workflow'").await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"channels-5","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    setup.execute("UPDATE novu_queue_jobs SET payload=payload #- '{workflow_context,workflow_internal_id}' WHERE environment_id='a' AND kind=$1 AND status='pending'",&[&novu_postgres_queue::sendgrid_queue::JOB_KIND]).await.unwrap();
    assert!(
        novu_postgres_queue::sendgrid_queue::process_one_with_step_policy(
            &mut setup,
            "a",
            &provider,
            Policy::Active
        )
        .await
        .unwrap()
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_queue_jobs WHERE environment_id='a' AND kind=$1 AND last_error='email workflow identity missing'",&[&novu_postgres_queue::sendgrid_queue::JOB_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        setup
            .query_one("SELECT count(*) FROM novu_rust_resend_attempts", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    use novu_postgres_queue::sms_step_policy::Policy as SmsPolicy;
    let twilio = novu_postgres_queue::twilio::Twilio::new(novu_postgres_queue::twilio::Config {
        account_sid: "AC11111111111111111111111111111111".into(),
        auth_token: "fixture".into(),
        api_key_sid: None,
        api_key_secret: None,
        from: "+15550000001".into(),
        region: novu_postgres_queue::twilio::Region::Us,
    })
    .unwrap();
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            sms,
            "a-key",
            json!({"title":null,"content":"new SMS","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(
        novu_postgres_queue::twilio_queue::process_one_with_step_policy(
            &mut setup,
            "a",
            &twilio,
            SmsPolicy::Unchanged
        )
        .await
        .unwrap()
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_queue_jobs WHERE kind=$1 AND last_error='sms workflow step changed'", &[&novu_postgres_queue::twilio_queue::JOB_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"sms-policy-2","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            sms,
            "a-key",
            json!({"title":null,"content":"new SMS","active":false})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(
        novu_postgres_queue::twilio_queue::process_one_with_step_policy(
            &mut setup,
            "a",
            &twilio,
            SmsPolicy::Active
        )
        .await
        .unwrap()
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_queue_jobs WHERE kind=$1 AND last_error='sms workflow step disabled'", &[&novu_postgres_queue::twilio_queue::JOB_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            sms,
            "a-key",
            json!({"title":null,"content":"new SMS","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"sms-policy-3","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    let tx = setup.transaction().await.unwrap();
    tx.execute("UPDATE novu_rust_workflows SET sms_step_active=false WHERE environment_id='a' AND identifier='workflow'",&[]).await.unwrap();
    let (mut other, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let waiting = tokio::spawn(async move {
        novu_postgres_queue::twilio_queue::process_one_with_step_policy(
            &mut other,
            "a",
            &twilio,
            SmsPolicy::Active,
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !waiting.is_finished(),
        "worker must wait for workflow update"
    );
    tx.commit().await.unwrap();
    assert!(waiting.await.unwrap().unwrap());
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_queue_jobs WHERE kind=$1 AND last_error='sms workflow step disabled'", &[&novu_postgres_queue::twilio_queue::JOB_KIND]).await.unwrap().get::<_,i64>(0),2);
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            sms,
            "a-key",
            json!({"title":null,"content":"new SMS","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"sms-policy-4","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    setup.execute("UPDATE novu_queue_jobs SET payload=payload #- '{workflow_context,workflow_internal_id}' WHERE environment_id='a' AND kind=$1 AND status='pending'", &[&novu_postgres_queue::twilio_queue::JOB_KIND]).await.unwrap();
    let twilio = novu_postgres_queue::twilio::Twilio::new(novu_postgres_queue::twilio::Config {
        account_sid: "AC11111111111111111111111111111111".into(),
        auth_token: "fixture".into(),
        api_key_sid: None,
        api_key_secret: None,
        from: "+15550000001".into(),
        region: novu_postgres_queue::twilio::Region::Us,
    })
    .unwrap();
    assert!(
        novu_postgres_queue::twilio_queue::process_one_with_step_policy(
            &mut setup,
            "a",
            &twilio,
            SmsPolicy::Active
        )
        .await
        .unwrap()
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM novu_queue_jobs WHERE kind=$1 AND last_error='sms workflow identity missing'", &[&novu_postgres_queue::twilio_queue::JOB_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        setup
            .query_one("SELECT count(*) FROM novu_rust_twilio_attempts", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    setup.batch_execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',session_user,p.permission,r.oid FROM pg_roles r CROSS JOIN (VALUES ('alert_view'),('queue_ack')) p(permission) WHERE r.rolname=session_user").await.unwrap();
    assert_eq!(
        novu_postgres_queue::reconciliation::channel_policy_count(&setup, "a", "sms")
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        novu_postgres_queue::reconciliation::channel_policy_count(&setup, "a", "email")
            .await
            .unwrap(),
        5
    );
    let sms_alerts =
        novu_postgres_queue::reconciliation::channel_policy_list(&setup, "a", "sms", None)
            .await
            .unwrap();
    let job = uuid::Uuid::parse_str(sms_alerts[0]["jobId"].as_str().unwrap()).unwrap();
    assert!(
        novu_postgres_queue::reconciliation::acknowledge_channel_policy(
            &setup,
            "a",
            job,
            sms_alerts[0]["revision"].as_str().unwrap().parse().unwrap(),
            "operator",
            "reviewed"
        )
        .await
        .unwrap()
    );
    assert_eq!(
        novu_postgres_queue::reconciliation::channel_policy_count(&setup, "a", "sms")
            .await
            .unwrap(),
        3
    );
}
