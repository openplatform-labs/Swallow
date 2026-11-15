use novu_postgres_queue as queue;
use serde_json::json;
use std::{path::Path, process::Command};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

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
fn alert(url: &str, env: &str, v2: bool) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_SMTP_ENABLED", "true")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env(
            "SWALLOW_ALERT_COOLDOWN_POLICY",
            if v2 { "new-condition" } else { "destination" },
        )
        .env(
            "SWALLOW_ALERT_CONDITIONS_V2",
            if v2 { "true" } else { "false" },
        )
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([env, "300", "120", "60"])
        .output()
        .unwrap()
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn scoped_health_and_alert_selection() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("../../api/rust-api/migrations"), 1..=17).await;
    migrate(&db, &root.join("migrations"), 1..=u32::MAX).await;
    let env = format!("smtp-health-{}", Uuid::new_v4());
    let other = format!("smtp-other-{}", Uuid::new_v4());
    let role = format!("smtp_health_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_health(text),public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let (reader, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    assert!(
        reader
            .query_one("SELECT * FROM public.novu_rust_smtp_health($1)", &[&env])
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_smtp_health($1)",
                &[&other]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_smtp_attempts LIMIT 1",
                &[]
            )
            .await
            .is_err()
    );
    let healthy=alert(role_url.as_str(), &env, false);
    assert_eq!(healthy.status.code(), Some(0), "{}", String::from_utf8_lossy(&healthy.stderr));
    let check = |url: &str, env: &str| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .env_clear()
            .env("DATABASE_URL", url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .args(["check-smtp", env])
            .output()
            .unwrap()
    };
    assert_eq!(check(role_url.as_str(), &env).status.code(), Some(0));
    assert_eq!(check(role_url.as_str(), &other).status.code(), Some(1));

    let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    for (id, e) in [
        (ids[0], &env),
        (ids[1], &other),
        (ids[2], &env),
        (ids[3], &env),
    ] {
        queue::enqueue(
            &db,
            id,
            e,
            queue::smtp_queue::JOB_KIND,
            &json!({"destination":"x","secret":"fixture"}),
        )
        .await
        .unwrap();
    }
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=ANY($1)",
        &[&vec![ids[0], ids[1], ids[3]]],
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&ids[2],&Uuid::new_v4()]).await.unwrap();
    for (id, e, state) in [
        (ids[0], &env, "unknown"),
        (ids[1], &other, "unknown"),
        (ids[2], &env, "dispatching"),
    ] {
        db.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,$3)",&[&id,e,&state]).await.unwrap();
    }
    let row = reader
        .query_one("SELECT * FROM public.novu_rust_smtp_health($1)", &[&env])
        .await
        .unwrap();
    assert_eq!(
        (
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2)
        ),
        (1, 1, 1)
    );
    let report = check(role_url.as_str(), &env);
    assert_eq!(report.status.code(), Some(2));
    let json: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert_eq!(
        json,
        json!({"unknownAttempts":1,"strandedDispatches":1,"preDispatchFailures":1})
    );
    assert!(!String::from_utf8_lossy(&report.stdout).contains("fixture"));
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')", &[&env]).await.unwrap();
    for v2 in [false, true] {
        let result = alert(role_url.as_str(), &env, v2);
        assert_eq!(
            result.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stderr).contains("requires operator review"));
    }
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_attention(text,uuid),public.novu_rust_ack_smtp(text,uuid,uuid,bigint,text,text) TO {role}")).await.unwrap();
    assert!(reader.query("SELECT * FROM public.novu_rust_smtp_attention($1,NULL)",&[&other]).await.is_err());
    let listed=Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear().env("DATABASE_URL",role_url.as_str()).env("NOVU_POSTGRES_ALLOW_INSECURE","true")
        .args(["list-smtp",&env]).output().unwrap();
    assert_eq!(listed.status.code(),Some(0));
    let listing:serde_json::Value=serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listing["data"].as_array().unwrap().len(),3);
    assert!(!String::from_utf8_lossy(&listed.stdout).contains("fixture"));
    let rows=reader.query("SELECT * FROM public.novu_rust_smtp_attention($1,NULL)",&[&env]).await.unwrap();
    let unknown=rows.iter().find(|r|r.get::<_,Uuid>(0)==ids[0]).unwrap();
    let attempt:Option<Uuid>=unknown.get(3); let failure:i64=unknown.get(4);
    let stmt="SELECT public.novu_rust_ack_smtp($1,$2,$3,$4,'operator','verified externally')";
    assert!(reader.query_one(stmt,&[&env,&ids[0],&attempt,&failure]).await.is_err());
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'queue_ack',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    let stranded=rows.iter().find(|r|r.get::<_,Uuid>(0)==ids[2]).unwrap();
    let stranded_attempt:Option<Uuid>=stranded.get(3); let stranded_failure:i64=stranded.get(4);
    assert!(reader.query_one(stmt,&[&env,&ids[2],&stranded_attempt,&stranded_failure]).await.is_err());
    assert!(reader.query_one(stmt,&[&env,&ids[0],&Some(Uuid::new_v4()),&failure]).await.is_err());
    assert!(reader.query_one(stmt,&[&env,&ids[0],&attempt,&(failure+1)]).await.is_err());
    db.batch_execute("CREATE FUNCTION public.smtp_review_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test audit failure'; END $$; CREATE TRIGGER smtp_review_fail BEFORE INSERT ON public.novu_rust_smtp_reviews FOR EACH ROW EXECUTE FUNCTION public.smtp_review_fail()").await.unwrap();
    assert!(reader.query_one(stmt,&[&env,&ids[0],&attempt,&failure]).await.is_err());
    assert_eq!(reader.query_one("SELECT * FROM public.novu_rust_smtp_health($1)",&[&env]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("DROP TRIGGER smtp_review_fail ON public.novu_rust_smtp_reviews; DROP FUNCTION public.smtp_review_fail()").await.unwrap();
    let (reader2,driver)=tokio_postgres::connect(role_url.as_str(),NoTls).await.unwrap(); tokio::spawn(driver);
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&env,&ids[0],&attempt,&failure];
    let (a,b)=tokio::join!(reader.query_one(stmt,params),reader2.query_one(stmt,params));
    let values=[a.unwrap().get::<_,bool>(0),b.unwrap().get::<_,bool>(0)]; assert_eq!(values.iter().filter(|x|**x).count(),1);
    let after=reader.query_one("SELECT * FROM public.novu_rust_smtp_health($1)",&[&env]).await.unwrap();
    assert_eq!((after.get::<_,i64>(0),after.get::<_,i64>(1),after.get::<_,i64>(2)),(0,1,1));
    let no_attempt:Option<Uuid>=None;
    let pre=rows.iter().find(|r|r.get::<_,Uuid>(0)==ids[3]).unwrap(); let pre_revision:i64=pre.get(4);
    let ack=Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear().env("DATABASE_URL",role_url.as_str()).env("NOVU_POSTGRES_ALLOW_INSECURE","true")
        .args(["ack-smtp",&env,&ids[3].to_string(),"-",&pre_revision.to_string(),"operator","checked"]).output().unwrap();
    assert_eq!(ack.status.code(),Some(0),"{}",String::from_utf8_lossy(&ack.stderr));
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&ack.stdout).unwrap(),json!({"recorded":true}));
    assert_eq!(reader.query_one("SELECT * FROM public.novu_rust_smtp_health($1)",&[&env]).await.unwrap().get::<_,i64>(2),0);
    let unchanged=db.query_one("SELECT j.status,a.state FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&ids[0]]).await.unwrap();
    assert_eq!((unchanged.get::<_,String>(0),unchanged.get::<_,String>(1)),("failed".into(),"unknown".into()));
    db.execute("UPDATE public.novu_rust_smtp_attempts SET state=state WHERE job_id=$1", &[&ids[0]]).await.unwrap();
    assert_eq!(reader.query_one("SELECT * FROM public.novu_rust_smtp_health($1)",&[&env]).await.unwrap().get::<_,i64>(0),1);
    assert!(reader.query_one(stmt,&[&env,&ids[0],&attempt,&failure]).await.is_err());
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='queue_ack'", &[&env,&role]).await.unwrap();
    assert!(reader.query_one(stmt,&[&env,&ids[3],&no_attempt,&pre_revision]).await.is_err());
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'", &[&env,&role]).await.unwrap();
    assert!(reader.query_one("SELECT * FROM public.novu_rust_smtp_health($1)", &[&env]).await.is_err());
    assert_eq!(alert(role_url.as_str(), &env, true).status.code(), Some(1));
}
