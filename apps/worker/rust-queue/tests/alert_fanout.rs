//! Real alert commands and PostgreSQL, with blocked destinations and no provider traffic.
use std::{process::Command, time::Duration};
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn per_target_delivery_checks_control_real_alert_children() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();
    tokio::spawn(async move { let _=driver.await; });
    let env="fanout-selected-checks";
    db.execute("INSERT INTO novu_rust_environments VALUES ($1,'org')", &[&env]).await.unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,permission,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_send','alert_view']) permission WHERE rolname=session_user", &[&env]).await.unwrap();
    db.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ($1,'slack','failed','rejected'),($1,'slack','attempts','rejected')", &[&env]).await.unwrap();
    let job=Uuid::new_v4();
    novu_postgres_queue::enqueue(&db,job,env,"fixture",&serde_json::json!({})).await.unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='failed' WHERE id=$1", &[&job]).await.unwrap();
    let dir=std::env::temp_dir().join(format!("swallow-alert-checks-db-{}",Uuid::new_v4()));std::fs::create_dir(&dir).unwrap();
    let secret=dir.join("slack");std::fs::write(&secret,"https://hooks.slack.com/services/A/B/C").unwrap();
    let config=dir.join("config.json");
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[
      {"channel":"slack","id":"failed","webhook_file":secret,"delivery_checks":{"resend":false,"failed_jobs":true}},
      {"channel":"slack","id":"attempts","webhook_file":secret,"delivery_checks":{"resend":false,"attempts":true}}
    ]})).unwrap()).unwrap();
    let output=Command::new(env!("CARGO_BIN_EXE_alert-fanout")).args(["delivery",env,"300","120","60"]).arg(&config).env("DATABASE_URL",&url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS").env_remove("SWALLOW_ALERT_BINDING_KEY").env_remove("SWALLOW_ALERT_BINDING_KEY_FILE").output().unwrap();
    assert_eq!(output.status.code(),Some(2),"{}",String::from_utf8_lossy(&output.stderr));
    let text=String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("slack failed: attention"),"{text}");
    assert!(text.contains("slack attempts: completed"),"{text}");
    let pending=Uuid::new_v4();
    novu_postgres_queue::enqueue(&db,pending,env,"fixture",&serde_json::json!({})).await.unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '120 seconds' WHERE id=$1", &[&pending]).await.unwrap();
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[
      {"channel":"slack","id":"failed","webhook_file":secret,"delivery_checks":{"resend":false,"attempts":true},"delivery_thresholds":{"queue_max_wait_seconds":60}},
      {"channel":"slack","id":"attempts","webhook_file":secret,"delivery_checks":{"resend":false,"attempts":true},"delivery_thresholds":{"queue_max_wait_seconds":600}}
    ]})).unwrap()).unwrap();
    let output=Command::new(env!("CARGO_BIN_EXE_alert-fanout")).args(["delivery",env,"300","120","60"]).arg(&config).env("DATABASE_URL",&url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env_remove("SWALLOW_ALERT_BINDING_KEY").env_remove("SWALLOW_ALERT_BINDING_KEY_FILE").output().unwrap();
    assert_eq!(output.status.code(),Some(2),"{}",String::from_utf8_lossy(&output.stderr));
    let text=String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("slack failed: attention"),"{text}");
    assert!(text.contains("slack attempts: completed"),"{text}");
    let resend_job=Uuid::new_v4();
    novu_postgres_queue::enqueue(&db,resend_job,env,novu_postgres_queue::resend_queue::JOB_KIND,&serde_json::json!({})).await.unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '120 seconds' WHERE id=$1", &[&resend_job]).await.unwrap();
    db.execute("INSERT INTO novu_rust_resend_projection_heartbeat(environment_id,last_completed_at) VALUES ($1,clock_timestamp())", &[&env]).await.unwrap();
    for heartbeat_test in [false,true] {
        if heartbeat_test {
            db.execute("UPDATE novu_queue_jobs SET status='cancelled' WHERE id=$1", &[&resend_job]).await.unwrap();
            db.execute("UPDATE novu_rust_resend_projection_heartbeat SET last_completed_at=clock_timestamp()-interval '120 seconds' WHERE environment_id=$1", &[&env]).await.unwrap();
        }
        let (fast,slow)=if heartbeat_test { (serde_json::json!({"resend_heartbeat_max_seconds":60}),serde_json::json!({"resend_heartbeat_max_seconds":600})) } else { (serde_json::json!({"resend_grace_seconds":60}),serde_json::json!({"resend_grace_seconds":600})) };
        std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[
          {"channel":"slack","id":"failed","webhook_file":secret,"delivery_checks":{"resend":true},"alert_timing":fast},
          {"channel":"slack","id":"attempts","webhook_file":secret,"delivery_checks":{"resend":true},"alert_timing":slow}
        ]})).unwrap()).unwrap();
        let output=Command::new(env!("CARGO_BIN_EXE_alert-fanout")).args(["delivery",env,"300","300","60"]).arg(&config).env("DATABASE_URL",&url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS").env_remove("SWALLOW_ALERT_BINDING_KEY").env_remove("SWALLOW_ALERT_BINDING_KEY_FILE").output().unwrap();
        assert_eq!(output.status.code(),Some(2),"{}",String::from_utf8_lossy(&output.stderr));
        let text=String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("slack failed: attention"),"{text}");assert!(text.contains("slack attempts: completed"),"{text}");
    }
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[
      {"channel":"slack","id":"failed","webhook_file":secret,"delivery_checks":{"resend":true}},
      {"channel":"slack","id":"attempts","webhook_file":secret,"delivery_thresholds":{"twilio_grace_seconds":60}}
    ]})).unwrap()).unwrap();
    let output=Command::new(env!("CARGO_BIN_EXE_alert-fanout")).args(["delivery",env,"300","300","60"]).arg(&config).env("DATABASE_URL",&url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env("SWALLOW_ALERT_FANOUT_PREFLIGHT_PAIRS","true").env_remove("SWALLOW_ALERT_TWILIO_GRACE_SECONDS").env_remove("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS").output().unwrap();
    assert_eq!(output.status.code(),Some(1));assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("incomplete alert threshold pair"));
    std::fs::remove_dir_all(dir).unwrap();
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn destinations_have_independent_slots_and_failures_do_not_suppress_others() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('fanout-env','org'); INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'fanout-env',rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user; INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('fanout-env','slack','ops-a','rejected'),('fanout-env','slack','ops-b','rejected'),('fanout-env','email','mail-a','rejected'),('fanout-env','email','mail-b','rejected')").await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&db, job, "fanout-env", "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-fanout-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let slack = dir.join("slack-secret");
    let key = dir.join("email-secret");
    let config = dir.join("config.json");
    let binding_key = dir.join("binding-key");
    std::fs::write(
        &binding_key,
        "fixture-binding-key-0123456789abcdef0123456789",
    )
    .unwrap();
    std::fs::write(&slack, "https://hooks.slack.com/services/A/B/C").unwrap();
    std::fs::write(&key, "fixture-secret-key").unwrap();
    let write = |targets: serde_json::Value| {
        std::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({"targets":targets})).unwrap(),
        )
        .unwrap();
    };
    let run = |mode: &str, concurrency: &str| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_alert-fanout"));
        if mode == "delivery" {
            command.args(["delivery", "fanout-env", "300", "120", "60"]);
        } else {
            command.args(["database", "fanout-env", "probe", "60"]);
        }
        command
            .arg(&config)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_FANOUT_CONCURRENCY", concurrency)
            .env("SWALLOW_ALERT_BINDING_KEY_FILE", &binding_key)
            .env_remove("SWALLOW_ALERT_BINDING_KEY")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env(
                "SWALLOW_DB_PROBE_URL",
                "postgres://postgres@127.0.0.1:1/postgres",
            )
            .env("SWALLOW_DB_PROBE_TIMEOUT_SECONDS", "1")
            // Conflicting inherited credentials must not override each target.
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "bad-secret-should-not-appear",
            )
            .env("NOVU_RESEND_API_KEY", "inherited-secret")
            .env("NOVU_SENDGRID_IP_POOL_NAME", "inherited-pool")
            .env_remove("SWALLOW_ALERT_CHECK_SCOPED_ACCESS")
            .env_remove("SWALLOW_ALERT_SCOPED_DELIVERY")
            .env_remove("SWALLOW_ALERT_COOLDOWN_POLICY")
            .output()
            .unwrap()
    };
    let targets = serde_json::json!([
        {"channel":"slack","id":"ops-a","webhook_file":slack},
        {"channel":"email","id":"mail-a","provider":"resend","to":"one@example.test","from":"sender@example.test","api_key_file":key},
        {"channel":"slack","id":"ops-b","webhook_file":slack},
        {"channel":"email","id":"mail-b","provider":"sendgrid","to":"two@example.test","from":"sender@example.test","api_key_file":key,"region":"eu","ip_pool_name":"ops-pool"}
    ]);
    write(targets.clone());
    for (mode, concurrency) in [
        ("delivery", "1"),
        ("database", "1"),
        ("delivery", "4"),
        ("database", "4"),
    ] {
        let out = run(mode, concurrency);
        assert_eq!(
            out.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8(out.stdout).unwrap();
        for id in ["ops-a", "mail-a", "ops-b", "mail-b"] {
            assert!(text.contains(&format!("{id}: attention")), "{text}");
        }
        for secret in [
            "fixture-secret-key",
            "inherited-secret",
            "one@example.test",
            "https://hooks.slack.com",
        ] {
            assert!(!text.contains(secret));
        }
    }
    let bound: i64 = db.query_one("SELECT count(*) FROM public.novu_rust_alert_destination_bindings WHERE environment_id='fanout-env'", &[]).await.unwrap().get(0);
    assert_eq!(bound, 4);
    std::fs::write(&slack, "https://hooks.slack.com/services/A/B/Changed").unwrap();
    for mode in ["delivery", "database"] {
        let out = run(mode, "4");
        assert_eq!(out.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&out.stderr).contains("slack ops-a: failed"));
        assert!(String::from_utf8_lossy(&out.stdout).contains("email mail-b: attention"));
        assert!(!String::from_utf8_lossy(&out.stderr).contains("Changed"));
    }
    std::fs::write(&slack, "https://hooks.slack.com/services/A/B/C").unwrap();
    let mut changed_email = targets.as_array().unwrap().clone();
    changed_email[1]["to"] = serde_json::json!("someone-else@example.test");
    write(serde_json::json!(changed_email));
    let out = run("delivery", "4");
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("email mail-a: failed"));
    assert!(String::from_utf8_lossy(&out.stdout).contains("slack ops-b: attention"));
    write(targets.clone());
    let mut broken = targets.as_array().unwrap().clone();
    broken[1]["api_key_file"] = serde_json::json!(dir.join("missing"));
    write(serde_json::json!(broken));
    let out = run("delivery", "4");
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("email mail-a: failed"));
    assert!(String::from_utf8_lossy(&out.stdout).contains("slack ops-b: attention"));
    write(serde_json::json!([targets[0].clone(), targets[0].clone()]));
    let out = run("delivery", "4");
    assert_eq!(out.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&out.stderr).contains("hooks.slack.com"));
    std::fs::remove_dir_all(dir).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn provider_failures_are_recorded_per_destination() {
    use std::{
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    };
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('fanout-network-env','org'); INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'fanout-network-env',rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user").await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        job,
        "fanout-network-env",
        "fixture",
        &serde_json::json!({}),
    )
    .await
    .unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-fanout-proxy-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let slack = dir.join("slack");
    let key = dir.join("key");
    let config = dir.join("targets.json");
    std::fs::write(&slack, "https://hooks.slack.com/services/A/B/C").unwrap();
    std::fs::write(&key, "fixture-token").unwrap();
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[
        {"channel":"slack","id":"slack-a","webhook_file":slack},
        {"channel":"email","id":"email-a","provider":"resend","to":"one@example.test","from":"sender@example.test","api_key_file":key},
        {"channel":"slack","id":"slack-b","webhook_file":slack},
        {"channel":"email","id":"email-b","provider":"sendgrid","to":"two@example.test","from":"sender@example.test","api_key_file":key}
    ]})).unwrap()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let worker_count = count.clone();
    let proxy_task = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(20);
        while worker_count.load(Ordering::SeqCst) < 4 && Instant::now() < deadline {
            match listener.accept() {
                Ok((socket, _)) => {
                    worker_count.fetch_add(1, Ordering::SeqCst);
                    drop(socket);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => panic!("proxy accept failed: {error}"),
            }
        }
    });
    let out = Command::new(env!("CARGO_BIN_EXE_alert-fanout"))
        .args(["delivery", "fanout-network-env", "300", "120", "60"])
        .arg(&config)
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
        .env("SWALLOW_ALERT_FANOUT_CONCURRENCY", "4")
        .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "3")
        .env("HTTPS_PROXY", &proxy)
        .env("https_proxy", &proxy)
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .env_remove("SWALLOW_ALERT_CHECK_SCOPED_ACCESS")
        .env_remove("SWALLOW_ALERT_SCOPED_DELIVERY")
        .env_remove("SWALLOW_ALERT_COOLDOWN_POLICY")
        .output()
        .unwrap();
    proxy_task.join().unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(count.load(Ordering::SeqCst), 4);
    let rows=db.query("SELECT channel,target_id,state FROM novu_rust_alert_delivery_slots WHERE environment_id='fanout-network-env' ORDER BY channel,target_id",&[]).await.unwrap();
    assert_eq!(rows.len(), 4);
    for row in rows {
        assert_eq!(row.get::<_, String>(2), "unknown");
    }
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!output.contains("fixture-token"));
    assert!(!output.contains("one@example.test"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn configuration_binding_rejects_divergent_email_sender_settings() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('binding-env','org'); INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'binding-env',rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user; INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('binding-env','email','mail-sendgrid','rejected'),('binding-env','email','mail-resend','rejected')").await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&db, job, "binding-env", "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-binding-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let key = dir.join("email-key");
    let binding = dir.join("binding-key");
    let config = dir.join("targets.json");
    std::fs::write(&key, "fixture-secret-original").unwrap();
    std::fs::write(&binding, "binding-key-0123456789abcdef0123456789").unwrap();
    let run = |target: serde_json::Value, mode: &str, profile: &str, key_present: bool| {
        std::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({"targets":[target]})).unwrap(),
        )
        .unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_alert-fanout"));
        if mode == "delivery" {
            cmd.args(["delivery", "binding-env", "300", "120", "60"]);
        } else {
            cmd.args(["database", "binding-env", "probe", "60"]);
        }
        cmd.arg(&config)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_EMAIL_BINDING_PROFILE", profile)
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env(
                "SWALLOW_DB_PROBE_URL",
                "postgres://postgres@127.0.0.1:1/postgres",
            )
            .env("SWALLOW_DB_PROBE_TIMEOUT_SECONDS", "1")
            .env_remove("SWALLOW_ALERT_BINDING_KEY");
        if key_present {
            cmd.env("SWALLOW_ALERT_BINDING_KEY_FILE", &binding);
        } else {
            cmd.env_remove("SWALLOW_ALERT_BINDING_KEY_FILE");
        }
        cmd.output().unwrap()
    };
    for provider in ["sendgrid", "resend"] {
        let mut target = serde_json::json!({"channel":"email","id":format!("mail-{provider}"),"provider":provider,"to":"ops@example.test","from":"sender@example.test","sender_name":"Ops","api_key_file":key});
        if provider == "sendgrid" {
            target["region"] = "global".into();
            target["ip_pool_name"] = "pool-a".into();
        }
        for mode in ["delivery", "database"] {
            let out = run(target.clone(), mode, "configuration", true);
            assert_eq!(
                out.status.code(),
                Some(2),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        for field in ["from", "sender_name", "to"] {
            let mut changed = target.clone();
            changed[field] = "different@example.test".into();
            let out = run(changed, "delivery", "configuration", true);
            assert_eq!(out.status.code(), Some(1));
        }
        if provider == "sendgrid" {
            for (field, value) in [("region", "eu"), ("ip_pool_name", "pool-b")] {
                let mut changed = target.clone();
                changed[field] = value.into();
                assert_eq!(
                    run(changed, "delivery", "configuration", true)
                        .status
                        .code(),
                    Some(1)
                );
            }
        }
        std::fs::write(&key, "fixture-secret-rotated").unwrap();
        let out = run(target.clone(), "delivery", "configuration", true);
        assert_eq!(out.status.code(), Some(1));
        let text = String::from_utf8_lossy(&out.stderr);
        assert!(!text.contains("fixture-secret"));
        assert!(!text.contains("ops@example.test"));
        std::fs::write(&key, "fixture-secret-original").unwrap();
        assert_eq!(
            run(target.clone(), "delivery", "destination", true)
                .status
                .code(),
            Some(1)
        );
        assert_eq!(
            run(target, "delivery", "configuration", false)
                .status
                .code(),
            Some(1)
        );
    }
    let fingerprints:Vec<String>=db.query("SELECT fingerprint FROM public.novu_rust_alert_destination_bindings WHERE environment_id='binding-env'",&[]).await.unwrap().into_iter().map(|row|row.get(0)).collect();
    assert_eq!(fingerprints.len(), 2);
    assert!(
        fingerprints
            .iter()
            .all(|s| s.len() == 64 && !s.contains("fixture-secret"))
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn smtp_without_starttls_is_unknown_and_not_resent() {
    use std::io::{BufRead, BufReader, Write};
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let env = format!("smtp-alert-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user",&[&env]).await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&db, job, &env, "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-smtp-fanout-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let config = dir.join("targets.json");
    let password = dir.join("password");
    std::fs::write(&password, "fixture-secret").unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[{"channel":"email","id":"smtp-ops","provider":"smtp","to":"ops@example.test","from":"sender@example.test","smtp_host":"127.0.0.1","smtp_port":port,"smtp_tls":"starttls","smtp_username":"fixture-user","smtp_password_file":password}]})).unwrap()).unwrap();
    let server = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(v) => break v,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("{e}"),
            }
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(b"220 localhost ESMTP\r\n").unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.to_ascii_uppercase().starts_with("EHLO"));
        stream
            .write_all(b"250-localhost\r\n250 AUTH PLAIN\r\n")
            .unwrap();
        line.clear();
        let n = reader.read_line(&mut line).unwrap_or(0);
        assert!(
            n == 0
                || (!line.to_ascii_uppercase().starts_with("AUTH")
                    && !line.to_ascii_uppercase().starts_with("MAIL"))
        );
        listener
    });
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_alert-fanout"))
            .args(["delivery", &env, "300", "120", "60"])
            .arg(&config)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "5")
            .env_remove("SWALLOW_ALERT_SCOPED_DELIVERY")
            .env_remove("SWALLOW_ALERT_CHECK_SCOPED_ACCESS")
            .env_remove("SWALLOW_ALERT_BINDING_KEY")
            .env_remove("SWALLOW_ALERT_BINDING_KEY_FILE")
            .output()
            .unwrap()
    };
    let first = run();
    assert_eq!(
        first.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let listener = server.join().unwrap();
    let state:String=db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='email' AND target_id='smtp-ops'",&[&env]).await.unwrap().get(0);
    assert_eq!(state, "unknown");
    let second = run();
    assert_eq!(second.status.code(), Some(2));
    assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
    let attempts:i64=db.query_one("SELECT count(*) FROM novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND channel='email' AND target_id='smtp-ops'",&[&env]).await.unwrap().get(0);
    assert_eq!(attempts, 1);
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(!combined.contains("fixture-secret"));
    assert!(!combined.contains("ops@example.test"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL, openssl and python3"]
async fn smtp_private_ca_delivers_an_alert_and_preserves_cooldown() {
    use std::io::{BufRead, BufReader};
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let env = format!("smtp-ca-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user",&[&env]).await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&db, job, &env, "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-smtp-ca-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    let message = dir.join("message");
    let password = dir.join("password");
    let config = dir.join("targets.json");
    std::fs::write(&password, "fixture-password").unwrap();
    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    let mut server = Command::new("python3")
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/smtp_starttls_fixture.py"),
        )
        .arg(&cert)
        .arg(&key)
        .arg(&message)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(server.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let port: u16 = line.trim().parse().unwrap();
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[{"channel":"email","id":"smtp-ca","provider":"smtp","to":"ops@example.test","from":"sender@example.test","smtp_host":"localhost","smtp_port":port,"smtp_tls":"starttls","smtp_username":"fixture-user","smtp_password_file":password,"smtp_ca_file":cert}]})).unwrap()).unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_alert-fanout"))
            .args(["delivery", &env, "300", "120", "60"])
            .arg(&config)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS", "8")
            .env_remove("SWALLOW_ALERT_SCOPED_DELIVERY")
            .env_remove("SWALLOW_ALERT_CHECK_SCOPED_ACCESS")
            .env_remove("SWALLOW_ALERT_BINDING_KEY")
            .env_remove("SWALLOW_ALERT_BINDING_KEY_FILE")
            .output()
            .unwrap()
    };
    let first = run();
    assert_eq!(
        first.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(server.wait().unwrap().success());
    let text = std::fs::read_to_string(&message).unwrap();
    assert!(text.contains("Swallow delivery alert"));
    assert!(!text.contains("fixture-password"));
    let state:String=db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='email' AND target_id='smtp-ca'",&[&env]).await.unwrap().get(0);
    assert_eq!(state, "accepted");
    let second = run();
    assert_eq!(second.status.code(), Some(0));
    let count:i64=db.query_one("SELECT count(*) FROM novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND channel='email' AND target_id='smtp-ca'",&[&env]).await.unwrap().get(0);
    assert_eq!(count, 1);
    std::fs::remove_dir_all(dir).unwrap();
}
