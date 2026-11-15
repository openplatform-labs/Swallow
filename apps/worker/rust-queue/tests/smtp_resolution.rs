use std::{path::Path, process::Command};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn migrate(db: &Client, dir: &Path, max: u32) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        if name[..4].parse::<u32>().unwrap() <= max {
            db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}

#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn resolution_is_scoped_fenced_and_transactional() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("../../api/rust-api/migrations"), 17).await;
    migrate(&db, &root.join("migrations"), u32::MAX).await;
    let env = format!("smtp-resolve-{}", Uuid::new_v4());
    let other = format!("smtp-other-{}", Uuid::new_v4());
    let role = format!("smtp_resolve_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_smtp(text,uuid,uuid,uuid,bigint,text,text) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let (operator, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let (operator2, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let job = Uuid::new_v4();
    let second = Uuid::new_v4();
    for id in [job, second] {
        db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES($1,$2,'experimental_smtp_email','{}','failed','unknown')",&[&id,&env]).await.unwrap();
        db.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown')",&[&id,&env]).await.unwrap();
        db.execute("INSERT INTO public.novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES($1,$2,'sub','sub-internal','notification','wf','tx','subject','error',$3,'unknown')",&[&id.to_string(),&env,&id]).await.unwrap();
    }
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE a.job_id=$1",&[&job]).await.unwrap();
    let rev: Uuid = row.get(0);
    let failure: i64 = row.get(1);
    let request = Uuid::new_v4();
    let sql = "SELECT public.novu_rust_resolve_smtp($1,$2,$3,$4,$5,$6,$7)";
    assert!(
        operator
            .query_one(
                sql,
                &[
                    &env,
                    &request,
                    &job,
                    &rev,
                    &failure,
                    &"accepted",
                    &"relay evidence"
                ]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_smtp_resolvers SELECT $1,rolname,oid,true,false FROM pg_roles WHERE rolname=$2",&[&env,&role]).await.unwrap();
    assert!(
        operator
            .query_one(
                sql,
                &[
                    &other,
                    &request,
                    &job,
                    &rev,
                    &failure,
                    &"accepted",
                    &"relay evidence"
                ]
            )
            .await
            .is_err()
    );
    assert!(
        operator
            .query_one(
                sql,
                &[
                    &env,
                    &request,
                    &job,
                    &rev,
                    &failure,
                    &"retry",
                    &"not accepted"
                ]
            )
            .await
            .is_err()
    );
    assert!(
        operator
            .query_one(
                sql,
                &[
                    &env,
                    &request,
                    &job,
                    &Uuid::new_v4(),
                    &failure,
                    &"accepted",
                    &"relay evidence"
                ]
            )
            .await
            .is_err()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_smtp_resolutions",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("CREATE FUNCTION public.smtp_resolution_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$; CREATE TRIGGER smtp_resolution_test_fail BEFORE INSERT ON public.novu_rust_smtp_resolutions FOR EACH ROW EXECUTE FUNCTION public.smtp_resolution_test_fail()").await.unwrap();
    assert!(
        operator
            .query_one(
                sql,
                &[
                    &env,
                    &request,
                    &job,
                    &rev,
                    &failure,
                    &"accepted",
                    &"relay evidence"
                ]
            )
            .await
            .is_err()
    );
    let state=db.query_one("SELECT j.status,a.state FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(
        (state.get::<_, String>(0), state.get::<_, String>(1)),
        ("failed".into(), "unknown".into())
    );
    db.batch_execute("DROP TRIGGER smtp_resolution_test_fail ON public.novu_rust_smtp_resolutions; DROP FUNCTION public.smtp_resolution_test_fail()").await.unwrap();
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
        &env,
        &request,
        &job,
        &rev,
        &failure,
        &"accepted",
        &"relay evidence",
    ];
    let (a, b) = tokio::join!(
        operator.query_one(sql, params),
        operator2.query_one(sql, params)
    );
    let mut results = [a.unwrap().get::<_, bool>(0), b.unwrap().get::<_, bool>(0)];
    results.sort();
    assert_eq!(results, [false, true]);
    assert!(
        operator
            .query_one(
                sql,
                &[
                    &env,
                    &request,
                    &job,
                    &rev,
                    &failure,
                    &"accepted",
                    &"changed reason"
                ]
            )
            .await
            .is_err()
    );
    let row=db.query_one("SELECT j.status,a.state,m.status,m.delivery_state FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(2),
            row.get::<_, String>(3)
        ),
        (
            "completed".into(),
            "accepted".into(),
            "sent".into(),
            "accepted".into()
        )
    );
    db.execute(
        "UPDATE public.novu_rust_smtp_resolvers SET can_retry=true WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE a.job_id=$1",&[&second]).await.unwrap();
    let second_rev: Uuid = row.get(0);
    let second_failure: i64 = row.get(1);
    let request2 = Uuid::new_v4();
    let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "resolve-smtp",
            &env,
            &request2.to_string(),
            &second.to_string(),
            &second_rev.to_string(),
            &second_failure.to_string(),
            "retry",
            "relay confirmed non-acceptance",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["recorded"],
        true
    );
    let row=db.query_one("SELECT j.status,m.status,m.delivery_state,EXISTS(SELECT 1 FROM public.novu_rust_smtp_attempts a WHERE a.job_id=j.id) FROM public.novu_queue_jobs j JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&second]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(2),
            row.get::<_, bool>(3)
        ),
        (
            "pending".into(),
            "warning".into(),
            "not_attempted".into(),
            false
        )
    );
    assert!(
        operator
            .query_one(
                sql,
                &[
                    &env,
                    &Uuid::new_v4(),
                    &second,
                    &second_rev,
                    &second_failure,
                    &"retry",
                    &"duplicate"
                ]
            )
            .await
            .is_err()
    );
    db.execute(
        "UPDATE public.novu_rust_smtp_resolvers SET can_retry=false WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert!(
        operator
            .query_one(
                sql,
                &[
                    &env,
                    &request2,
                    &second,
                    &second_rev,
                    &second_failure,
                    &"retry",
                    &"relay confirmed non-acceptance"
                ]
            )
            .await
            .is_err()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_smtp_resolutions",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
}
