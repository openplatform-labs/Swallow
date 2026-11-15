//! Disposable synchronous PostgreSQL promotion preserving v2 alert state.
use std::{
    process::{Command, Stdio},
    time::Duration,
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
#[tokio::test]
#[ignore = "requires disposable synchronously replicated local PostgreSQL; stops its primary"]
async fn v2_conditions_survive_promotion_and_fence_competing_senders() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let primary = connect(&primary_url).await;
    let env = format!("alert-v2-ha-{}", Uuid::new_v4());
    primary
        .execute(
            "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
            &[&env],
        )
        .await
        .unwrap();
    primary.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user",&[&env]).await.unwrap();
    let reserve =
        "SELECT result,attempt_id FROM public.novu_rust_reserve_alert_v2($1,'slack','ops',3600,$2)";
    let failed: i64 = 1 << 18;
    let twilio: i64 = 1 << 36;
    let first = primary.query_one(reserve, &[&env, &failed]).await.unwrap();
    assert_eq!(first.get::<_, String>(0), "claimed");
    let id: Uuid = first.get(1);
    assert!(
        primary
            .query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','ops',$2,'accepted',NULL)",
                &[&env, &id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let standby = connect(&standby_url).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row=standby.query_one("SELECT pg_is_in_recovery(),EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='ops' AND state='accepted' AND last_condition_mask_v2=$2),EXISTS(SELECT 1 FROM public.novu_rust_alert_v2_targets WHERE environment_id=$1 AND channel='slack' AND target_id='ops')",&[&env,&failed]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        if row.get::<_, bool>(1) && row.get::<_, bool>(2) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "V2 state was not replayed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
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
    let peer = connect(&standby_url).await;
    assert!(
        !promoted
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(
        promoted
            .query_one(reserve, &[&env, &failed])
            .await
            .unwrap()
            .get::<_, String>(0),
        "cooldown"
    );
    assert!(promoted.query_one("SELECT result FROM public.novu_rust_reserve_alert($1,'slack','ops',3600,1::integer)",&[&env]).await.is_err());
    let combined = failed | twilio;
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&env, &combined];
    let (a, b) = tokio::join!(
        promoted.query_one(reserve, params),
        peer.query_one(reserve, params)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    let states = [a.get::<_, String>(0), b.get::<_, String>(0)];
    assert!(states.contains(&"claimed".to_string()) && states.contains(&"inflight".to_string()));
    let claimed: Uuid = if states[0] == "claimed" {
        a.get(1)
    } else {
        b.get(1)
    };
    assert!(
        promoted
            .query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','ops',$2,'accepted',NULL)",
                &[&env, &claimed]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(
        promoted
            .query_one(reserve, &[&env, &combined])
            .await
            .unwrap()
            .get::<_, String>(0),
        "cooldown"
    );
    assert_eq!(promoted.query_one("SELECT count(*) FROM public.novu_rust_alert_delivery_attempts WHERE environment_id=$1",&[&env]).await.unwrap().get::<_,i64>(0),2);
}
