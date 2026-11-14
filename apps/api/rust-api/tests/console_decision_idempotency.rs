//! Concurrent idempotent console decisions on disposable PostgreSQL.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
async fn json(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn same_request_is_atomic_and_reusable_only_with_identical_payload() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES ('idem-a','org'),('idem-b','org')",
    )
    .await
    .unwrap();
    let role = format!("idem_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_console_resolve_alert_v2(text,uuid,text,text,uuid,text,text),public.novu_rust_console_decision_request(text,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'idem-a',rolname,'alert_retry',oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    for (byte, env) in [('c', "idem-a"), ('d', "idem-b"), ('e', "idem-a")] {
        let token = byte.to_string().repeat(64);
        let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
        let credential:Uuid=db.query_one("INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at,subscriber_read) VALUES($1,$2,clock_timestamp()+interval '1 hour',false) RETURNING credential_id",&[&hash,&env]).await.unwrap().get(0);
        db.execute(
            "INSERT INTO public.novu_rust_console_alert_decision_grants VALUES($1,'retry')",
            &[&credential],
        )
        .await
        .unwrap();
    }
    let api_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let router = novu_rust_api::router(Arc::new(connect(&api_url).await));
    let second = novu_rust_api::router(Arc::new(connect(&api_url).await));
    let attempt = Uuid::new_v4();
    let request_id = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,attempt_id) VALUES('idem-a','slack','ops','unknown',$1)",&[&attempt]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES($1,'idem-a','slack','ops','unknown')",&[&attempt]).await.unwrap();
    let request = |id: Uuid, reason: &str| {
        Request::builder().method("POST").uri("/v1/experimental/console/alerts/resolve-v2").header("authorization",format!("SwallowConsole {}","c".repeat(64))).body(Body::from(serde_json::json!({"requestId":id.to_string(),"channel":"slack","targetId":"ops","attemptId":attempt.to_string(),"decision":"retry","reason":reason}).to_string())).unwrap()
    };
    let lookup = |token: char, id: Uuid| {
        Request::builder()
            .uri(format!(
                "/v1/experimental/console/alerts/request?requestId={id}"
            ))
            .header(
                "authorization",
                format!("SwallowConsole {}", token.to_string().repeat(64)),
            )
            .body(Body::empty())
            .unwrap()
    };
    let (one, two) = tokio::join!(
        router.clone().oneshot(request(request_id, "review")),
        second.clone().oneshot(request(request_id, "review"))
    );
    let (s1, v1) = json(one.unwrap()).await;
    let (s2, v2) = json(two.unwrap()).await;
    assert_eq!((s1, s2), (StatusCode::OK, StatusCode::OK));
    assert_ne!(v1["replayed"], v2["replayed"]);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_console_alert_decision_audit WHERE environment_id='idem-a'",&[]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        json(
            router
                .clone()
                .oneshot(request(request_id, "changed"))
                .await
                .unwrap()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, found) = json(
        router
            .clone()
            .oneshot(lookup('c', request_id))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(found["found"], true);
    assert_eq!(found["requestId"], request_id.to_string());
    let (_, foreign) = json(
        second
            .clone()
            .oneshot(lookup('d', request_id))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(foreign["found"], false);
    let (_, other_credential) = json(
        second
            .clone()
            .oneshot(lookup('e', request_id))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(other_credential["found"], false);
    // An uncommitted request is not visible to the read path.
    let next_attempt = Uuid::new_v4();
    let next_id = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES($1,'idem-a','slack','ops','unknown')",&[&next_attempt]).await.unwrap();
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='unknown',attempt_id=$1 WHERE environment_id='idem-a' AND target_id='ops'",&[&next_attempt]).await.unwrap();
    let mut blocker = connect(&url).await;
    let tx = blocker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_alert_delivery_slots SET state=state WHERE environment_id='idem-a' AND target_id='ops'",&[]).await.unwrap();
    let delayed=Request::builder().method("POST").uri("/v1/experimental/console/alerts/resolve-v2").header("authorization",format!("SwallowConsole {}","c".repeat(64))).body(Body::from(serde_json::json!({"requestId":next_id.to_string(),"channel":"slack","targetId":"ops","attemptId":next_attempt.to_string(),"decision":"retry","reason":"pending"}).to_string())).unwrap();
    let pending = tokio::spawn(async move { router.oneshot(delayed).await.unwrap() });
    let mut waiting = false;
    for _ in 0..100 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_console_resolve_alert_v2%')",&[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    let (_, uncommitted) = json(second.clone().oneshot(lookup('c', next_id)).await.unwrap()).await;
    assert_eq!(uncommitted["found"], false);
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap().status(), StatusCode::OK);
    let (_, committed) = json(second.clone().oneshot(lookup('c', next_id)).await.unwrap()).await;
    assert_eq!(committed["found"], true);
    // Audit failure leaves no idempotency record.
    db.batch_execute("CREATE FUNCTION public.test_idem_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_idem_reject BEFORE INSERT ON public.novu_rust_console_alert_decision_audit FOR EACH ROW EXECUTE FUNCTION public.test_idem_reject()").await.unwrap();
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='unknown' WHERE environment_id='idem-a' AND target_id='ops'",&[]).await.unwrap();
    let failed = Uuid::new_v4();
    let fail_req=Request::builder().method("POST").uri("/v1/experimental/console/alerts/resolve-v2").header("authorization",format!("SwallowConsole {}","c".repeat(64))).body(Body::from(serde_json::json!({"requestId":failed.to_string(),"channel":"slack","targetId":"ops","attemptId":next_attempt.to_string(),"decision":"retry","reason":"failure"}).to_string())).unwrap();
    assert_eq!(
        second.clone().oneshot(fail_req).await.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let (_, not_found) = json(second.clone().oneshot(lookup('c', failed)).await.unwrap()).await;
    assert_eq!(not_found["found"], false);
    db.batch_execute("DROP TRIGGER test_idem_reject ON public.novu_rust_console_alert_decision_audit; DROP FUNCTION public.test_idem_reject()").await.unwrap();
    let revoked_hash = format!("{:x}", Sha256::digest("c".repeat(64).as_bytes()));
    db.execute("DELETE FROM public.novu_rust_console_alert_decision_grants g USING public.novu_rust_console_credentials c WHERE g.credential_id=c.credential_id AND c.token_hash=$1",&[&revoked_hash]).await.unwrap();
    assert_eq!(
        second
            .clone()
            .oneshot(lookup('c', request_id))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        second
            .oneshot(request(request_id, "review"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}
