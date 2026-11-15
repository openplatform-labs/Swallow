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
async fn history_is_scoped_paginated_and_revocable() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("../../api/rust-api/migrations"), 17).await;
    migrate(&db, &root.join("migrations"), u32::MAX).await;
    let env = format!("discord-history-{}", Uuid::new_v4());
    let other = format!("discord-history-{}", Uuid::new_v4());
    let role = format!("discord_history_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_resolution_history(text,uuid,oid,uuid) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let (reader, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let job = Uuid::new_v4();
    let other_job = Uuid::new_v4();
    let sql = "SELECT public.novu_rust_discord_resolution_history($1,$2,$3,$4)";
    let no_oid: Option<u32> = None;
    let no_request: Option<Uuid> = None;
    assert!(
        reader
            .query_one(sql, &[&env, &job, &no_oid, &no_request])
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_discord_resolution_audit_viewers SELECT $1,rolname,oid FROM pg_roles WHERE rolname=$2",&[&env,&role]).await.unwrap();
    assert!(
        reader
            .query_one(sql, &[&other, &job, &no_oid, &no_request])
            .await
            .is_err()
    );
    assert!(
        reader
            .query("SELECT * FROM public.novu_rust_discord_resolutions", &[])
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_discord_resolutions(environment_id,actor,actor_oid,request_id,job_id,attempt_revision,failure_revision,decision,reason,created_at) SELECT $1,'fixture',42,gen_random_uuid(),$2,gen_random_uuid(),0,'retry','fixture evidence',timestamptz '2024-01-01 00:00:00+00' FROM generate_series(1,103)",&[&env,&job]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_resolutions(environment_id,actor,actor_oid,request_id,job_id,attempt_revision,failure_revision,decision,reason) VALUES($1,'fixture',42,gen_random_uuid(),$2,gen_random_uuid(),0,'accepted','other secret'),($3,'fixture',42,gen_random_uuid(),$2,gen_random_uuid(),0,'accepted','other env secret')",&[&env,&other_job,&other]).await.unwrap();
    let first: serde_json::Value = reader
        .query_one(sql, &[&env, &job, &no_oid, &no_request])
        .await
        .unwrap()
        .get(0);
    assert_eq!(first["data"].as_array().unwrap().len(), 100);
    assert!(!first.to_string().contains("other secret"));
    let oid = first["next"]["actorOid"]
        .as_str()
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let req = Uuid::parse_str(first["next"]["requestId"].as_str().unwrap()).unwrap();
    let second: serde_json::Value = reader
        .query_one(sql, &[&env, &job, &Some(oid), &Some(req)])
        .await
        .unwrap()
        .get(0);
    assert_eq!(second["data"].as_array().unwrap().len(), 3);
    assert!(second["next"].is_null());
    let ids: std::collections::HashSet<_> = first["data"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["data"].as_array().unwrap())
        .map(|r| r["requestId"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 103);
    assert!(
        reader
            .query_one(sql, &[&env, &other_job, &Some(oid), &Some(req)])
            .await
            .is_err()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "discord-resolution-history",
            &env,
            &job.to_string(),
            &oid.to_string(),
            &req.to_string(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["data"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    db.execute(
        "DELETE FROM public.novu_rust_discord_resolution_audit_viewers WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert!(
        reader
            .query_one(sql, &[&env, &job, &no_oid, &no_request])
            .await
            .is_err()
    );
}
