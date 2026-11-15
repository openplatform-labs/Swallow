//! Disposable synchronous PostgreSQL promotion for scoped v3 alert reservations.
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
async fn v3_history_and_inflight_survive_promotion() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let admin = connect(&primary_url).await;
    let env = format!("alert-v3-ha-{}", Uuid::new_v4());
    let role = format!("alert_v3_ha_{}", Uuid::new_v4().simple());
    admin
        .execute(
            "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
            &[&env],
        )
        .await
        .unwrap();
    admin.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint,bigint,bigint),public.novu_rust_observe_alert_conditions_v3(text,text,text,bigint,bigint,bigint,bigint),public.novu_rust_finish_alert(text,text,text,uuid,text,integer) TO {role}")).await.unwrap();
    admin.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    let role_url = |url: &str| {
        let mut u = reqwest::Url::parse(url).unwrap();
        u.set_username(&role).unwrap();
        u.to_string()
    };
    let primary = connect(&role_url(&primary_url)).await;
    let reserve = "SELECT result,attempt_id FROM public.novu_rust_reserve_alert_v3($1,'slack','ops',3600,$2,$3,$4,$5)";
    let first = primary
        .query_one(reserve, &[&env, &i64::MIN, &i64::MIN, &1_i64, &3_i64])
        .await
        .unwrap();
    let id: Uuid = first.get(1);
    assert!(primary
        .query_one(
            "SELECT public.novu_rust_finish_alert($1,'slack','ops',$2,'accepted',NULL)",
            &[&env, &id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0));
    let second = primary
        .query_one(reserve, &[&env, &1_i64, &1_i64, &2_i64, &2_i64])
        .await
        .unwrap();
    assert_eq!(second.get::<_, String>(0), "claimed");
    let second_id: Uuid = second.get(1);
    let standby = connect(&standby_url).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = standby.query_one("SELECT pg_is_in_recovery(),EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_slots s JOIN public.novu_rust_alert_v3_targets t USING(environment_id,channel,target_id) WHERE s.environment_id=$1 AND s.channel='slack' AND s.target_id='ops' AND s.state='dispatching' AND s.attempt_id=$2 AND s.last_condition_mask_v2=$3 AND s.last_condition_mask_v3_high=3)", &[&env,&second_id,&(i64::MIN | 1)]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        if row.get::<_, bool>(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "reservation not replayed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let ctl = std::env::var("TEST_LOCAL_PG_CTL").unwrap();
    let primary_data = std::env::var("TEST_LOCAL_PRIMARY_DATA").unwrap();
    let standby_data = std::env::var("TEST_LOCAL_STANDBY_DATA").unwrap();
    assert!(Command::new(&ctl)
        .args(["-D", &primary_data, "-m", "immediate", "stop"])
        .stdout(Stdio::null())
        .status()
        .unwrap()
        .success());
    assert!(Command::new(&ctl)
        .args(["-D", &standby_data, "promote", "-w"])
        .stdout(Stdio::null())
        .status()
        .unwrap()
        .success());
    let promoted_admin = connect(&standby_url).await;
    let promoted = connect(&role_url(&standby_url)).await;
    let peer = connect(&role_url(&standby_url)).await;
    assert_eq!(
        promoted
            .query_one(reserve, &[&env, &i64::MIN, &i64::MIN, &1_i64, &3_i64])
            .await
            .unwrap()
            .get::<_, String>(0),
        "inflight"
    );
    promoted_admin.execute("UPDATE public.novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id=$1 AND target_id='ops'", &[&env]).await.unwrap();
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
        &[&env, &i64::MIN, &i64::MIN, &1_i64, &3_i64];
    let (a, b) = tokio::join!(
        promoted.query_one(reserve, params),
        peer.query_one(reserve, params)
    );
    assert_eq!(a.unwrap().get::<_, String>(0), "blocked");
    assert_eq!(b.unwrap().get::<_, String>(0), "blocked");
    let row = promoted_admin.query_one("SELECT s.state,a.state,s.last_condition_mask_v2,s.last_condition_mask_v3_high FROM public.novu_rust_alert_delivery_slots s JOIN public.novu_rust_alert_delivery_attempts a ON a.id=s.attempt_id WHERE s.environment_id=$1 AND s.target_id='ops'", &[&env]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(row.get::<_, i64>(2), i64::MIN | 1);
    assert_eq!(row.get::<_, i64>(3), 3);
    assert_eq!(promoted_admin.query_one("SELECT count(*) FROM public.novu_rust_alert_delivery_attempts WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,i64>(0),2);
}
