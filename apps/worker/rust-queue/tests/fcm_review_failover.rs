//! Local synchronous promotion with a committed FCM review and request record.
use std::{
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
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
fn user_url(url: &str, role: &str) -> String {
    let mut u = reqwest::Url::parse(url).unwrap();
    u.set_username(role).unwrap();
    u.to_string()
}
#[tokio::test]
#[ignore = "requires disposable synchronous local PostgreSQL; stops its primary"]
async fn review_replay_and_revocation_survive_promotion() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let primary = connect(&primary_url).await;
    let env = format!("fcm-review-ha-{}", Uuid::new_v4());
    let role = format!("fcm_review_ha_{}", Uuid::new_v4().simple());
    let job = Uuid::new_v4();
    let request = Uuid::new_v4();
    let issuer = "https://issuer.test";
    let subject = "operator";
    let client = "console";
    let exp = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600) as i64;
    primary
        .execute(
            "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
            &[&env],
        )
        .await
        .unwrap();
    primary.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES($1,$2,$3,$4)",&[&issuer,&subject,&client,&env]).await.unwrap();
    primary.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_fcm_unreviewed(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_fcm_review(text,text,text,text,bigint,uuid,uuid,text,text),public.novu_rust_operator_jwt_fcm_review_request(text,text,text,text,bigint,uuid),public.novu_rust_fcm_unreviewed_health(text) TO {role}")).await.unwrap();
    primary.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    primary.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    primary
        .execute(
            "INSERT INTO public.novu_rust_operator_jwt_fcm_unreviewed_grants VALUES($1,$2,$3,$4)",
            &[&issuer, &subject, &client, &env],
        )
        .await
        .unwrap();
    primary
        .execute(
            "INSERT INTO public.novu_rust_operator_jwt_fcm_review_grants VALUES($1,$2,$3,$4)",
            &[&issuer, &subject, &client, &env],
        )
        .await
        .unwrap();
    primary.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_fcm_push','{}','completed')",&[&job,&env]).await.unwrap();
    primary.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES($1,$2,0,'accepted'),($1,$2,1,'rejected')",&[&job,&env]).await.unwrap();
    let worker = connect(&user_url(&primary_url, &role)).await;
    assert!(
        worker
            .query("SELECT * FROM public.novu_rust_fcm_attempts", &[])
            .await
            .is_err()
    );
    let listing: serde_json::Value = worker
        .query_one(
            "SELECT public.novu_rust_operator_jwt_fcm_unreviewed($1,$2,$3,$4,$5,NULL)",
            &[&issuer, &subject, &client, &env, &exp],
        )
        .await
        .unwrap()
        .get(0);
    let revision = listing["data"][0]["revision"].as_str().unwrap().to_owned();
    let reason = "operator review";
    let sql = "SELECT public.novu_rust_operator_jwt_fcm_review($1,$2,$3,$4,$5,$6,$7,$8,$9)";
    let initial: serde_json::Value = worker
        .query_one(
            sql,
            &[
                &issuer, &subject, &client, &env, &exp, &request, &job, &revision, &reason,
            ],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        initial,
        serde_json::json!({"recorded":true,"replayed":false})
    );
    let standby = connect(&standby_url).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row=standby.query_one("SELECT pg_is_in_recovery(),EXISTS(SELECT 1 FROM public.novu_rust_fcm_partial_reviews WHERE job_id=$1 AND revision=$2),EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_review_requests WHERE environment_id=$3 AND request_id=$4 AND recorded)",&[&job,&revision,&env,&request]).await.unwrap();
        if row.get::<_, bool>(0) && row.get::<_, bool>(1) && row.get::<_, bool>(2) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "review did not replay on standby"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ctl = std::env::var("TEST_LOCAL_PG_CTL").unwrap();
    let primary_data = std::env::var("TEST_LOCAL_PRIMARY_DATA").unwrap();
    let standby_data = std::env::var("TEST_LOCAL_STANDBY_DATA").unwrap();
    assert!(
        Command::new(&ctl)
            .args(["-D", &primary_data, "-m", "immediate", "stop"])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new(&ctl)
            .args(["-D", &standby_data, "promote", "-w"])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let promoted = connect(&standby_url).await;
    let a = connect(&user_url(&standby_url, &role)).await;
    let b = connect(&user_url(&standby_url, &role)).await;
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
        &issuer, &subject, &client, &env, &exp, &request, &job, &revision, &reason,
    ];
    let (r1, r2) = tokio::join!(a.query_one(sql, params), b.query_one(sql, params));
    for row in [r1.unwrap(), r2.unwrap()] {
        assert_eq!(
            row.get::<_, serde_json::Value>(0),
            serde_json::json!({"recorded":true,"replayed":true})
        );
    }
    let state=promoted.query_one("SELECT (SELECT count(*) FROM public.novu_rust_fcm_partial_reviews WHERE job_id=$1),(SELECT count(*) FROM public.novu_rust_operator_jwt_fcm_review_requests WHERE request_id=$2)",&[&job,&request]).await.unwrap();
    assert_eq!(state.get::<_, i64>(0), 1);
    assert_eq!(state.get::<_, i64>(1), 1);
    assert_eq!(
        a.query_one("SELECT public.novu_rust_fcm_unreviewed_health($1)", &[&env])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let lookup: serde_json::Value = a
        .query_one(
            "SELECT public.novu_rust_operator_jwt_fcm_review_request($1,$2,$3,$4,$5,$6)",
            &[&issuer, &subject, &client, &env, &exp, &request],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(lookup["found"], true);
    assert_eq!(lookup["recorded"], true);
    let bad = "different reason";
    let conflict = a
        .query_one(
            sql,
            &[
                &issuer, &subject, &client, &env, &exp, &request, &job, &revision, &bad,
            ],
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.code().map(|c| c.code()), Some("P4090"));
    promoted.execute("DELETE FROM public.novu_rust_operator_jwt_fcm_review_grants WHERE issuer=$1 AND subject=$2 AND client_id=$3 AND environment_id=$4",&[&issuer,&subject,&client,&env]).await.unwrap();
    let err = a.query_one(sql, params).await.unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("42501"));
}
