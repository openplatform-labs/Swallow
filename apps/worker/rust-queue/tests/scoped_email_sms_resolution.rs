//! Scoped operator decisions against disposable PostgreSQL with real service migrations.
use novu_postgres_queue as queue;
use serde_json::json;
use std::{path::Path, process::Command};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
async fn create_job(db: &Client, kind: &str) -> Uuid {
    let id = Uuid::new_v4();
    queue::enqueue(db, id, "a", kind, &json!({"secret":"payload"}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='failed',last_error='unknown',sendgrid_delivery_result='delivered' WHERE id=$1", &[&id]).await.unwrap();
    let table = if kind == queue::sendgrid_queue::JOB_KIND {
        "novu_rust_sendgrid_attempts"
    } else if kind == queue::resend_queue::JOB_KIND {
        "novu_rust_resend_attempts"
    } else {
        "novu_rust_twilio_attempts"
    };
    db.execute(&format!("INSERT INTO {table}(job_id,environment_id,state,http_status) VALUES ($1,'a','unknown',500)"), &[&id]).await.unwrap();
    if kind == queue::twilio_queue::JOB_KIND {
        db.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status,sms_job_id,delivery_state,twilio_delivery_result) VALUES ($1,'a','s','s','n','w','t','error',$2,'unknown','delivered')", &[&id.to_string(),&id]).await.unwrap();
    } else {
        db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state,sendgrid_delivery_result,resend_delivery_result) VALUES ($1,'a','s','s','n','w','t','subject','error',$2,'unknown','delivered','delivered')", &[&id.to_string(),&id]).await.unwrap();
    }
    id
}
async fn snapshot(db: &Client, job: Uuid, kind: &str) -> serde_json::Value {
    let table = if kind == queue::sendgrid_queue::JOB_KIND {
        "novu_rust_sendgrid_attempts"
    } else if kind == queue::resend_queue::JOB_KIND {
        "novu_rust_resend_attempts"
    } else {
        "novu_rust_twilio_attempts"
    };
    let message_table = if kind == queue::twilio_queue::JOB_KIND {
        "novu_rust_sms_messages"
    } else {
        "novu_rust_email_messages"
    };
    let message_key = if kind == queue::twilio_queue::JOB_KIND {
        "sms_job_id"
    } else {
        "email_job_id"
    };
    let query = format!(
        "SELECT jsonb_build_object('status',j.status,'sendgrid',j.sendgrid_delivery_result,'resend',j.resend_delivery_result,'twilio',j.twilio_delivery_result,'attempt',(SELECT state FROM {table} WHERE job_id=j.id),'message',(SELECT jsonb_build_array(status,delivery_state,error_id,error_text,identifier) FROM {message_table} WHERE {message_key}=j.id),'audit',(SELECT jsonb_build_array(decision,previous_http_status,retry_dispatch_id) FROM novu_rust_delivery_reconciliations WHERE job_id=j.id)) FROM novu_queue_jobs j WHERE j.id=$1"
    );
    db.query_one(&query, &[&job]).await.unwrap().get(0)
}
async fn fcm_job(db: &Client, payload: serde_json::Value, error: &str, states: &[&str]) -> Uuid {
    let job = Uuid::new_v4();
    queue::enqueue(db, job, "a", queue::fcm_queue::JOB_KIND, &payload)
        .await
        .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed',last_error=$2 WHERE id=$1",
        &[&job, &error],
    )
    .await
    .unwrap();
    for (index, state) in states.iter().enumerate() {
        db.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state,http_status,skip_reason) VALUES ($1,'a',$2,$3,500,CASE WHEN $3='skipped' THEN 'unregistered' ELSE NULL END)", &[&job,&(index as i32),state]).await.unwrap();
    }
    db.execute("INSERT INTO novu_rust_push_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,device_tokens,title,status,push_job_id,target_index,delivery_state) VALUES ($1,'a','s','s','n','w','t',ARRAY['secret-token'],'title','error',$2,0,'unknown')", &[&job.to_string(),&job]).await.unwrap();
    job
}
async fn fcm_snapshot(db: &Client, job: Uuid) -> serde_json::Value {
    db.query_one("SELECT jsonb_build_object('status',j.status,'error',j.last_error,'attempts',(SELECT jsonb_agg(jsonb_build_array(target_index,state,http_status) ORDER BY target_index) FROM novu_rust_fcm_attempts WHERE job_id=j.id),'message',(SELECT jsonb_build_array(status,delivery_state,error_id,error_text,identifier IS NOT NULL) FROM novu_rust_push_messages WHERE push_job_id=j.id),'audit',(SELECT jsonb_build_array(target_index,decision,previous_http_status) FROM novu_rust_delivery_reconciliations WHERE job_id=j.id)) FROM novu_queue_jobs j WHERE j.id=$1", &[&job]).await.unwrap().get(0)
}
async fn scoped_fcm(
    db: &Client,
    env: &str,
    job: Uuid,
    target: i32,
    decision: &str,
) -> Result<(), tokio_postgres::Error> {
    db.query_one(
        "SELECT public.novu_rust_resolve_fcm($1,$2,$3,$4,'operator','ticket')",
        &[&env, &job, &target, &decision],
    )
    .await
    .map(|_| ())
}

async fn resolve(
    db: &Client,
    env: &str,
    id: Uuid,
    decision: &str,
) -> Result<(), tokio_postgres::Error> {
    db.query_one(
        "SELECT public.novu_rust_resolve_email_sms($1,$2,$3,'operator','ticket')",
        &[&env, &id, &decision],
    )
    .await
    .map(|_| ())
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL and roles"]
async fn scoped_email_sms_decisions_are_authorized_atomic_and_fenced() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("../../api/rust-api/migrations");
    let worker = root.join("migrations");
    for args in [
        vec!["apply", "api", api.to_str().unwrap(), "--through", "17"],
        vec!["apply", "worker", worker.to_str().unwrap()],
        vec!["apply", "api", api.to_str().unwrap()],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let mut db = connect(&url).await;
    db.batch_execute(include_str!("../migrations/0135_resend_correlation_retry.sql"))
        .await
        .unwrap();
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')")
        .await
        .unwrap();
    let role = format!("operator_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture-password'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_email_sms(text,uuid,text,text,text) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("fixture-password")).unwrap();
    let actor = connect(role_url.as_str()).await;
    let mail = create_job(&db, queue::sendgrid_queue::JOB_KIND).await;
    assert!(resolve(&actor, "a", mail, "retry").await.is_err());
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'retry',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    assert!(resolve(&actor, "b", mail, "retry").await.is_err());
    assert!(resolve(&actor, "a", mail, "accepted").await.is_err());
    assert!(
        actor
            .execute(
                "UPDATE public.novu_queue_jobs SET status='completed' WHERE id=$1",
                &[&mail]
            )
            .await
            .is_err()
    );
    assert!(
        actor
            .query("SELECT payload FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    // A recorded authenticated callback blocks a retry and leaves no audit entry.
    db.execute("INSERT INTO novu_rust_sendgrid_delivery_events(environment_id,event_id,message_internal_id,event_type,event_timestamp,recipient_hash) VALUES ('a','e',$1,'delivered',1,'hash')", &[&mail.to_string()]).await.unwrap();
    assert!(resolve(&actor, "a", mail, "retry").await.is_err());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM novu_rust_delivery_reconciliations WHERE job_id=$1",
            &[&mail]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.execute(
        "DELETE FROM novu_rust_sendgrid_delivery_events WHERE event_id='e'",
        &[],
    )
    .await
    .unwrap();
    let actor2 = connect(role_url.as_str()).await;
    let (one, two) = tokio::join!(
        resolve(&actor, "a", mail, "retry"),
        resolve(&actor2, "a", mail, "retry")
    );
    assert_ne!(one.is_ok(), two.is_ok());
    let row=db.query_one("SELECT j.status,j.sendgrid_delivery_result,m.delivery_state,m.sendgrid_delivery_result,(SELECT count(*) FROM novu_rust_sendgrid_attempts WHERE job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&mail]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(row.get::<_, String>(2), "not_attempted");
    assert_eq!(row.get::<_, String>(3), "unknown");
    assert_eq!(row.get::<_, i64>(4), 0);
    let row=db.query_one("SELECT database_user,database_session_user,decision,previous_http_status FROM novu_rust_delivery_reconciliations WHERE job_id=$1", &[&mail]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), role);
    assert_eq!(row.get::<_, String>(1), role);
    assert_eq!(row.get::<_, String>(2), "retry");
    assert_eq!(row.get::<_, Option<i32>>(3), Some(500));
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'accepted',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    for kind in [
        queue::sendgrid_queue::JOB_KIND,
        queue::resend_queue::JOB_KIND,
        queue::twilio_queue::JOB_KIND,
    ] {
        let accepted = create_job(&db, kind).await;
        resolve(&actor, "a", accepted, "accepted").await.unwrap();
        assert_eq!(
            db.query_one(
                "SELECT status FROM novu_queue_jobs WHERE id=$1",
                &[&accepted]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "completed"
        );
        assert!(resolve(&actor, "a", accepted, "accepted").await.is_err());
        if kind != queue::sendgrid_queue::JOB_KIND {
            let retry = create_job(&db, kind).await;
            if kind == queue::twilio_queue::JOB_KIND {
                db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id) VALUES ('a','AC','SM','delivered',$1)", &[&retry]).await.unwrap();
                assert!(resolve(&actor, "a", retry, "retry").await.is_err());
                db.execute(
                    "DELETE FROM novu_rust_twilio_delivery_events WHERE job_id=$1",
                    &[&retry],
                )
                .await
                .unwrap();
            }
            resolve(&actor, "a", retry, "retry").await.unwrap();
            assert_eq!(
                db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&retry])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "pending"
            );
        }
    }
    // Compare final state with the established direct-table implementation.
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',session_user,permission,oid FROM pg_roles CROSS JOIN unnest(ARRAY['retry','accepted']) permission WHERE rolname=session_user", &[]).await.unwrap();
    for kind in [
        queue::sendgrid_queue::JOB_KIND,
        queue::resend_queue::JOB_KIND,
        queue::twilio_queue::JOB_KIND,
    ] {
        for decision in ["retry", "accepted"] {
            let legacy = create_job(&db, kind).await;
            let scoped = create_job(&db, kind).await;
            queue::reconciliation::resolve(
                &mut db, "a", legacy, None, decision, "operator", "ticket",
            )
            .await
            .unwrap();
            resolve(&actor, "a", scoped, decision).await.unwrap();
            assert_eq!(
                snapshot(&db, legacy, kind).await,
                snapshot(&db, scoped, kind).await,
                "{kind} {decision}"
            );
        }
    }
    // Acceptance updates message history without claiming recipient delivery.
    for scoped in [false, true] {
        for provider_id in [None, Some("provider-confirmed-id")] {
            let accepted = create_job(&db, queue::resend_queue::JOB_KIND).await;
            db.execute(
                "UPDATE novu_queue_jobs SET resend_delivery_result='delivered' WHERE id=$1",
                &[&accepted],
            )
            .await
            .unwrap();
            db.execute(
                "UPDATE novu_rust_resend_attempts SET provider_message_id=$2 WHERE job_id=$1",
                &[&accepted, &provider_id],
            )
            .await
            .unwrap();
            db.execute("UPDATE novu_rust_email_messages SET error_id='mail_unexpected_error',error_text='old failure',resend_delivery_result='delivered' WHERE email_job_id=$1", &[&accepted]).await.unwrap();
            let cross = Uuid::new_v4().to_string();
            db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state,resend_delivery_result) VALUES ($1,'b','s','s','n','w','t','subject','error',$2,'unknown','delivered')", &[&cross,&accepted]).await.unwrap();
            if scoped {
                resolve(&actor, "a", accepted, "accepted").await.unwrap();
            } else {
                queue::reconciliation::resolve(
                    &mut db, "a", accepted, None, "accepted", "operator", "ticket",
                )
                .await
                .unwrap();
            }
            let row=db.query_one("SELECT j.status,j.resend_delivery_result,a.state,m.status,m.delivery_state,m.error_id,m.error_text,m.identifier,m.resend_delivery_result FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id AND m.environment_id=j.environment_id WHERE j.id=$1", &[&accepted]).await.unwrap();
            assert_eq!(row.get::<_, String>(0), "completed");
            assert!(row.get::<_, Option<String>>(1).is_none());
            assert_eq!(row.get::<_, String>(2), "accepted");
            assert_eq!(row.get::<_, String>(3), "sent");
            assert_eq!(row.get::<_, String>(4), "accepted");
            assert!(row.get::<_, Option<String>>(5).is_none());
            assert!(row.get::<_, Option<String>>(6).is_none());
            assert_eq!(row.get::<_, Option<String>>(7).as_deref(), provider_id);
            assert!(row.get::<_, Option<String>>(8).is_none());
            let untouched=db.query_one("SELECT status,delivery_state,resend_delivery_result FROM novu_rust_email_messages WHERE internal_id=$1", &[&cross]).await.unwrap();
            assert_eq!(untouched.get::<_, String>(0), "error");
            assert_eq!(untouched.get::<_, String>(1), "unknown");
            assert_eq!(untouched.get::<_, String>(2), "delivered");
        }
    }
    // Known Resend callbacks block a full retry in both operator modes.
    for scoped in [false, true] {
        let blocked = create_job(&db, queue::resend_queue::JOB_KIND).await;
        let provider = format!("evidence-{blocked}");
        db.execute(
            "UPDATE novu_rust_resend_attempts SET provider_message_id=$2 WHERE job_id=$1",
            &[&blocked, &provider],
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ('a',$1,$2,'email.delivered')", &[&format!("same-{blocked}"),&provider]).await.unwrap();
        let denied = if scoped {
            resolve(&actor, "a", blocked, "retry").await.is_err()
        } else {
            queue::reconciliation::resolve(
                &mut db, "a", blocked, None, "retry", "operator", "ticket",
            )
            .await
            .is_err()
        };
        assert!(denied);
        let row=db.query_one("SELECT j.status,a.state,(SELECT count(*) FROM novu_rust_delivery_reconciliations WHERE job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&blocked]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert_eq!(row.get::<_, i64>(2), 0);
        db.execute(
            "DELETE FROM novu_rust_resend_delivery_events WHERE environment_id='a' AND email_id=$1",
            &[&provider],
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ('b',$1,$2,'email.delivered')", &[&format!("cross-{blocked}"),&provider]).await.unwrap();
        if scoped {
            resolve(&actor, "a", blocked, "retry").await.unwrap();
        } else {
            queue::reconciliation::resolve(
                &mut db, "a", blocked, None, "retry", "operator", "ticket",
            )
            .await
            .unwrap();
        }
        assert_eq!(
            db.query_one(
                "SELECT status FROM novu_queue_jobs WHERE id=$1",
                &[&blocked]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "pending"
        );
        let prior:Option<String>=db.query_one("SELECT retry_provider_message_id FROM novu_rust_delivery_reconciliations WHERE job_id=$1 AND decision='retry'", &[&blocked]).await.unwrap().get(0);
        assert_eq!(prior.as_deref(), Some(provider.as_str()));
    }
    // A signed callback can identify an attempt even if its HTTP response and
    // provider ID were lost. Both operator paths must refuse that retry.
    for scoped in [false, true] {
        let blocked = create_job(&db, queue::resend_queue::JOB_KIND).await;
        let tag = Uuid::new_v4();
        db.execute(
            "UPDATE novu_rust_resend_attempts SET correlation_id=$2 WHERE job_id=$1",
            &[&blocked, &tag],
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,correlation_id) VALUES ('a',$1,'tagged-id','email.delivered',$2)", &[&format!("tag-{blocked}"),&tag]).await.unwrap();
        let denied = if scoped {
            resolve(&actor, "a", blocked, "retry").await.is_err()
        } else {
            queue::reconciliation::resolve(
                &mut db, "a", blocked, None, "retry", "operator", "ticket",
            )
            .await
            .is_err()
        };
        assert!(denied);
        let row=db.query_one("SELECT j.status,a.state,a.correlation_id,(SELECT count(*) FROM novu_rust_delivery_reconciliations WHERE job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&blocked]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert_eq!(row.get::<_, Uuid>(2), tag);
        assert_eq!(row.get::<_, i64>(3), 0);
        db.execute("DELETE FROM novu_rust_resend_delivery_events WHERE environment_id='a' AND correlation_id=$1", &[&tag]).await.unwrap();
        db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,correlation_id) VALUES ('b',$1,'tagged-id','email.delivered',$2),('a',$3,'different','email.delivered',$4)", &[&format!("cross-tag-{blocked}"),&tag,&format!("other-tag-{blocked}"),&Uuid::new_v4()]).await.unwrap();
        if scoped {
            resolve(&actor, "a", blocked, "retry").await.unwrap();
        } else {
            queue::reconciliation::resolve(
                &mut db, "a", blocked, None, "retry", "operator", "ticket",
            )
            .await
            .unwrap();
        }
        let row=db.query_one("SELECT j.status,(SELECT count(*) FROM novu_rust_resend_attempts WHERE job_id=j.id),r.retry_correlation_id FROM novu_queue_jobs j JOIN novu_rust_delivery_reconciliations r ON r.job_id=j.id WHERE j.id=$1", &[&blocked]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "pending");
        assert_eq!(row.get::<_, i64>(1), 0);
        assert_eq!(row.get::<_, Uuid>(2), tag);
    }
    // FCM target/group state is compared to the existing implementation.
    let no_function = fcm_job(
        &db,
        json!({}),
        "push delivery outcome unknown",
        &["unknown"],
    )
    .await;
    assert!(
        scoped_fcm(&actor, "a", no_function, 0, "retry")
            .await
            .is_err()
    );
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_fcm(text,uuid,integer,text,text,text) TO {role}")).await.unwrap();
    db.execute("DELETE FROM novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=$1 AND decision='retry'", &[&role]).await.unwrap();
    assert!(
        scoped_fcm(&actor, "a", no_function, 0, "retry")
            .await
            .is_err()
    );
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'retry',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let grouped = json!({"prepared_messages":[{},{},{}],"prepared_context":{"target_metadata":[{"history_index":0,"group_size":3},{"history_index":0,"group_size":3},{"history_index":0,"group_size":3}]}});
    let cases = [
        (
            "accepted",
            "push delivery outcome unknown",
            vec!["unknown", "skipped", "accepted"],
            grouped.clone(),
        ),
        (
            "retry",
            "push delivery outcome unknown",
            vec!["unknown", "skipped", "accepted"],
            grouped.clone(),
        ),
        (
            "accepted",
            "push delivery outcome unknown",
            vec!["unknown", "skipped", "accepted"],
            json!({"prepared_messages":[{},{},{}],"prepared_context":{"target_metadata":[{"history_index":0,"group_size":1},{"history_index":1,"group_size":1},{"history_index":2,"group_size":1}]}}),
        ),
        (
            "accepted",
            "push delivery outcome unknown",
            vec!["unknown", "accepted", "accepted"],
            grouped.clone(),
        ),
        (
            "accepted",
            "push delivery outcome unknown",
            vec!["unknown", "accepted"],
            grouped.clone(),
        ),
        (
            "accepted",
            "push delivery outcome unknown",
            vec!["unknown", "unknown"],
            grouped.clone(),
        ),
        (
            "accepted",
            "subscriber identity changed",
            vec!["unknown", "accepted"],
            grouped.clone(),
        ),
        (
            "retry",
            "push delivery outcome unknown",
            vec!["unknown", "unknown"],
            grouped.clone(),
        ),
        (
            "retry",
            "push delivery outcome unknown",
            vec!["unknown", "accepted"],
            grouped.clone(),
        ),
        (
            "accepted",
            "push delivery outcome unknown",
            vec!["unknown"],
            json!({}),
        ),
        (
            "accepted",
            "push delivery outcome unknown",
            vec!["unknown", "accepted"],
            json!({"prepared_messages":[{},{}],"prepared_context":{"target_metadata":[{"history_index":0.5,"group_size":999999999999_i64}]}}),
        ),
    ];
    for (decision, error, states, payload) in cases {
        let legacy = fcm_job(&db, payload.clone(), error, &states).await;
        let scoped = fcm_job(&db, payload, error, &states).await;
        queue::reconciliation::resolve(
            &mut db,
            "a",
            legacy,
            Some(0),
            decision,
            "operator",
            "ticket",
        )
        .await
        .unwrap();
        scoped_fcm(&actor, "a", scoped, 0, decision).await.unwrap();
        assert_eq!(
            fcm_snapshot(&db, legacy).await,
            fcm_snapshot(&db, scoped).await,
            "FCM {decision} {error} {states:?}"
        );
        if states.contains(&"skipped") && decision == "accepted" {
            let snapshot = fcm_snapshot(&db, scoped).await;
            assert_eq!(snapshot["status"], "failed");
            assert_eq!(snapshot["error"], "push targets skipped by policy");
            // A different group may have its own successful history entry.
            if snapshot["message"][0] == "error" {
                assert_eq!(snapshot["message"][1], "unknown");
            }
        }
    }
    let refused = fcm_job(
        &db,
        grouped.clone(),
        "subscriber identity changed",
        &["unknown"],
    )
    .await;
    assert!(scoped_fcm(&actor, "a", refused, 0, "retry").await.is_err());
    assert!(
        scoped_fcm(&actor, "b", refused, 0, "accepted")
            .await
            .is_err()
    );
    assert!(
        scoped_fcm(&actor, "a", refused, -1, "accepted")
            .await
            .is_err()
    );
    assert!(
        actor
            .query(
                "SELECT device_tokens FROM public.novu_rust_push_messages",
                &[]
            )
            .await
            .is_err()
    );
    let race = fcm_job(&db, grouped, "push delivery outcome unknown", &["unknown"]).await;
    let (one, two) = tokio::join!(
        scoped_fcm(&actor, "a", race, 0, "accepted"),
        scoped_fcm(&actor2, "a", race, 0, "accepted")
    );
    assert_ne!(one.is_ok(), two.is_ok());
    // Grant revocation is serialized behind a decision already in flight.
    let gate_job = fcm_job(
        &db,
        json!({"prepared_messages":[{}]}),
        "push delivery outcome unknown",
        &["unknown"],
    )
    .await;
    let mut gate_actor = connect(role_url.as_str()).await;
    let tx = gate_actor.transaction().await.unwrap();
    tx.query_one(
        "SELECT public.novu_rust_resolve_fcm('a',$1,0,'retry','operator','ticket')",
        &[&gate_job],
    )
    .await
    .unwrap();
    let revoker = connect(&url).await;
    revoker
        .batch_execute("SET application_name='swallow_permission_revoker'")
        .await
        .unwrap();
    let revoke_role = role.clone();
    let revoke = tokio::spawn(async move {
        revoker.execute("DELETE FROM novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=$1 AND decision='retry'", &[&revoke_role]).await.unwrap()
    });
    let mut blocked = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='swallow_permission_revoker' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0) { blocked=true; break; }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(blocked && !revoke.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(revoke.await.unwrap(), 1);
    let later = fcm_job(
        &db,
        json!({}),
        "push delivery outcome unknown",
        &["unknown"],
    )
    .await;
    assert!(scoped_fcm(&actor, "a", later, 0, "retry").await.is_err());
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,'retry',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let cli_fcm = fcm_job(
        &db,
        json!({"prepared_messages":[{}]}),
        "push delivery outcome unknown",
        &["unknown"],
    )
    .await;
    let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args([
            "retry",
            "a",
            &cli_fcm.to_string(),
            "0",
            "operator",
            "ticket",
        ])
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_RECONCILIATION_SCOPED_ACCESS", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let fcm = Uuid::new_v4();
    queue::enqueue(&db, fcm, "a", queue::fcm_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&fcm],
    )
    .await
    .unwrap();
    assert!(resolve(&actor, "a", fcm, "retry").await.is_err());
    let cli_job = create_job(&db, queue::resend_queue::JOB_KIND).await;
    let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args([
            "retry",
            "a",
            &cli_job.to_string(),
            "-",
            "operator",
            "ticket",
        ])
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_RECONCILIATION_SCOPED_ACCESS", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    db.execute("DELETE FROM novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=$1 AND decision='retry'", &[&role]).await.unwrap();
    let denied = create_job(&db, queue::resend_queue::JOB_KIND).await;
    assert!(resolve(&actor, "a", denied, "retry").await.is_err());
    assert!(
        actor
            .execute("DELETE FROM public.novu_rust_delivery_reconciliations", &[])
            .await
            .is_err()
    );
}
