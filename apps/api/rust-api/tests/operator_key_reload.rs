use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

const PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-private.pem");
const PUBLIC: &str = include_str!("fixtures/operator_jwt/test-only-public.pem");
const OTHER_PUBLIC: &str = include_str!("fixtures/operator_jwt/test-only-other-public.pem");
const OTHER_PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-other-private.pem");

fn config(kids: &[&str]) -> Value {
    json!({"issuer":"https://issuer.test","audience":"swallow",
        "keys":kids.iter().map(|kid| json!({"kid":kid,"pem":if *kid == "new" { OTHER_PUBLIC } else { PUBLIC }})).collect::<Vec<_>>()})
}
fn token(kid: &str) -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("at+jwt".into());
    header.kid = Some(kid.into());
    encode(&header, &json!({"iss":"https://issuer.test","aud":"swallow","sub":"alice",
        "client_id":"console","jti":"test","iat":now-1,"exp":now+300}),
        &EncodingKey::from_rsa_pem(if kid == "new" { OTHER_PRIVATE } else { PRIVATE }).unwrap()).unwrap()
}

#[test]
fn key_rotation_rejects_invalid_config_and_preserves_policy() {
    let verifier = AccessTokenVerifier::from_json(&config(&["old"]).to_string()).unwrap();
    let old = token("old");
    let new = token("new");
    assert!(verifier.verify(&old).is_some());
    assert!(verifier.verify(&new).is_none());
    verifier.reload_keys(&config(&["old", "new"]).to_string()).unwrap();
    assert!(verifier.verify(&old).is_some());
    assert!(verifier.verify(&new).is_some());
    verifier.reload_keys(&config(&["new"]).to_string()).unwrap();
    assert!(verifier.verify(&old).is_none());
    assert!(verifier.verify(&new).is_some());
    assert!(verifier.reload_keys("{").is_err());
    let mut changed = config(&["old"]);
    changed["issuer"] = json!("https://other.test");
    assert!(verifier.reload_keys(&changed.to_string()).is_err());
    changed = config(&["old"]);
    changed["check_token_revocation"] = json!(true);
    assert!(verifier.reload_keys(&changed.to_string()).is_err());
    assert!(verifier.verify(&old).is_none());
    assert!(verifier.verify(&new).is_some());
    verifier.reject_until_reloaded();
    assert!(!verifier.is_available());
    assert!(verifier.verify(&new).is_none());
    assert!(verifier.reload_keys("{}").is_err());
    assert!(!verifier.is_available());
    verifier.reload_keys(&config(&["new"]).to_string()).unwrap();
    assert!(verifier.is_available());
    assert!(verifier.verify(&new).is_some());
}

// The fixture is deliberately a disposable local database; no production role or data is used.
#[tokio::test]
#[ignore = "requires a fully migrated disposable PostgreSQL database and role creation"]
async fn process_reloads_keys_and_applies_each_failure_policy() {
    use std::{fs, net::TcpListener, path::Path, process::{Child, Command, Stdio}, time::Duration};
    use tokio_postgres::NoTls;
    use uuid::Uuid;
    struct Process(Child);
    impl Drop for Process { fn drop(&mut self) { let _ = self.0.kill(); let _ = self.0.wait(); } }
    struct Directory(std::path::PathBuf);
    impl Drop for Directory { fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); } }
    fn replace(path: &Path, value: &str) {
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, value).unwrap();
        fs::rename(tmp, path).unwrap();
    }
    fn free_port() -> u16 { TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port() }
    async fn wait_status(client: &reqwest::Client, url: &str, token: &str, expected: u16) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
        loop {
            let status = client.get(url).bearer_auth(token).send().await.ok().map(|r| r.status().as_u16());
            if status == Some(expected) { return; }
            assert!(tokio::time::Instant::now() < deadline, "expected HTTP {expected}, last status {status:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let database_url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let suffix = Uuid::new_v4().simple().to_string();
    let role = format!("jwt_reload_{suffix}");
    let environment = format!("jwt-reload-{suffix}");
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_self(text,text,text,text,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')", &[&environment]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1)", &[&environment]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let api_url = database_url.replace("postgres://postgres@", &format!("postgres://{role}@")).replace("postgresql://postgres@", &format!("postgresql://{role}@"));
    assert_ne!(api_url, database_url, "fixture URL must specify postgres user");
    let dir = Directory(std::env::temp_dir().join(format!("swallow-key-reload-{suffix}")));
    fs::create_dir(&dir.0).unwrap();
    let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(2)).build().unwrap();
    let mut processes = Vec::new();
    for policy in ["retain", "reject"] {
        let path = dir.0.join(policy);
        replace(&path, &config(&["old"]).to_string());
        let port = free_port();
        let child = Command::new(env!("CARGO_BIN_EXE_novu-rust-api")).env_clear()
            .env("DATABASE_URL", &api_url).env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_OPERATOR_JWT_CONFIG_FILE", &path)
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
            .env("SWALLOW_API_OPERATOR_KEY_HEALTH_ENABLED", "true")
            .env("SWALLOW_OPERATOR_JWT_RELOAD_SECONDS", "1")
            .env("SWALLOW_OPERATOR_JWT_RELOAD_FAILURE", policy)
            .stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        let url = format!("http://127.0.0.1:{port}/v1/experimental/operator/me?environmentId={environment}");
        processes.push((Process(child), path, url));
    }
    let old = token("old");
    let new = token("new");
    let health = |url: &str| format!("{}/health/operator-keys", url.split("/v1/").next().unwrap());
    for (_, _, url) in &processes { wait_status(&client, &health(url), "", 200).await; }
    for (_, _, url) in &processes { wait_status(&client, url, &old, 200).await; wait_status(&client, url, &new, 401).await; }
    for (_, path, _) in &processes { replace(path, &config(&["old", "new"]).to_string()); }
    for (_, _, url) in &processes { wait_status(&client, url, &new, 200).await; wait_status(&client, url, &old, 200).await; }
    for (_, path, _) in &processes { replace(path, &config(&["new"]).to_string()); }
    for (_, _, url) in &processes { wait_status(&client, url, &old, 401).await; wait_status(&client, url, &new, 200).await; }
    for (_, path, _) in &processes { replace(path, "{"); }
    wait_status(&client, &processes[1].2, &new, 503).await;
    for (_, _, url) in &processes { wait_status(&client, &health(url), "", 503).await; }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    wait_status(&client, &processes[0].2, &new, 200).await;
    let mut changed = config(&["old"]); changed["check_token_revocation"] = json!(true);
    for (_, path, _) in &processes { replace(path, &changed.to_string()); }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    wait_status(&client, &processes[1].2, &new, 503).await;
    wait_status(&client, &processes[0].2, &new, 200).await;
    for (_, path, _) in &processes { replace(path, &config(&["new"]).to_string()); }
    wait_status(&client, &processes[1].2, &new, 200).await;
    for (_, _, url) in &processes {
        wait_status(&client, &health(url), "", 200).await;
        let response = client.get(health(url)).send().await.unwrap();
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    }
    drop(processes);
    db.execute("DELETE FROM public.novu_rust_operator_jwt_identities WHERE environment_id=$1", &[&environment]).await.unwrap();
    db.execute("DELETE FROM public.novu_rust_operator_jwt_api_logins WHERE database_principal=$1", &[&role]).await.unwrap();
    db.execute("DELETE FROM public.novu_rust_environments WHERE id=$1", &[&environment]).await.unwrap();
    db.batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE {role}")).await.unwrap();
}

#[test]
fn key_health_requires_explicit_health_and_reload_configuration() {
    use std::process::{Command, Stdio};
    for (health, key_health) in [("false", "true"), ("true", "true"), ("true", "invalid")] {
        let status = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env_clear().env("DATABASE_URL", "postgres://postgres@127.0.0.1:1/postgres")
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", health)
            .env("SWALLOW_API_OPERATOR_KEY_HEALTH_ENABLED", key_health)
            .stdout(Stdio::null()).stderr(Stdio::null()).status().unwrap();
        assert!(!status.success());
    }
}
