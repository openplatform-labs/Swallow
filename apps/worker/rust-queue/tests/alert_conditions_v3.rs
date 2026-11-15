use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn v3_preserves_v2_history_fences_old_senders_and_tracks_new_conditions() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let env = format!("alert-v3-{}", Uuid::new_v4());
    let other = format!("other-{}", Uuid::new_v4());
    let role = format!("v3_alert_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint,bigint,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint),public.novu_rust_observe_alert_conditions_v3(text,text,text,bigint,bigint,bigint,bigint),public.novu_rust_finish_alert(text,text,text,uuid,text,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let mut parsed = reqwest::Url::parse(&url).unwrap();
    parsed.set_username(&role).unwrap();
    let role_url = parsed.to_string();
    let (client, driver) = tokio_postgres::connect(&role_url, NoTls).await.unwrap();
    tokio::spawn(driver);
    // Old binaries cannot reserve after the migration, including those with an
    // explicit grant to the earlier signature.
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint) TO {role}")).await.unwrap();
    assert!(client.query_one("SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','old',60,$2,$3)", &[&env,&1_i64,&0_i64]).await.is_err());
    assert!(client.query_one("SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','unknown',60,$2,$3,$4,$5)", &[&env,&1_i64,&0_i64,&0_i64,&0_i64]).await.is_err());
    let first_unknown = client.query_one("SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','unknown',3600,$2,$3,$4,$5)", &[&env,&i64::MIN,&i64::MIN,&1_i64,&3_i64]).await.unwrap();
    let first_unknown_id: Uuid = first_unknown.get(1);
    client.query_one("SELECT public.novu_rust_finish_alert($1,'slack','unknown',$2,'accepted',NULL)",&[&env,&first_unknown_id]).await.unwrap();
    // A new check-failure bit causes another send while the original checks
    // are unobservable. Both prior words must survive the reservation.
    let next_unknown = client.query_one("SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','unknown',3600,$2,$3,$4,$5)", &[&env,&1_i64,&1_i64,&2_i64,&2_i64]).await.unwrap();
    assert_eq!(next_unknown.get::<_,String>(0), "claimed");
    let next_unknown_id: Uuid = next_unknown.get(1);
    client.query_one("SELECT public.novu_rust_finish_alert($1,'slack','unknown',$2,'accepted',NULL)",&[&env,&next_unknown_id]).await.unwrap();
    let stored = db.query_one("SELECT last_condition_mask_v2,last_condition_mask_v3_high FROM public.novu_rust_alert_delivery_slots WHERE environment_id=$1 AND target_id='unknown'", &[&env]).await.unwrap();
    assert_eq!(stored.get::<_,i64>(0), i64::MIN | 1);
    assert_eq!(stored.get::<_,i64>(1), 3);
    let still_unhealthy = client.query_one("SELECT result FROM public.novu_rust_reserve_alert_v3($1,'slack','unknown',3600,$2,$3,$4,$5)", &[&env,&i64::MIN,&i64::MIN,&1_i64,&3_i64]).await.unwrap();
    assert_eq!(still_unhealthy.get::<_,String>(0), "cooldown");
    client.query_one("SELECT public.novu_rust_observe_alert_conditions_v3($1,'slack','unknown',$2,$3,$4,$5)",&[&env,&0_i64,&i64::MIN,&0_i64,&3_i64]).await.unwrap();
    assert_eq!(client.query_one("SELECT result FROM public.novu_rust_reserve_alert_v3($1,'slack','unknown',3600,$2,$3,$4,$5)", &[&env,&i64::MIN,&i64::MIN,&1_i64,&3_i64]).await.unwrap().get::<_,String>(0),"claimed");
    assert!(
        client
            .query_one(
                "SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','ops',60,$2,$2,$3,$3)",
                &[&other, &1_i64, &0_i64]
            )
            .await
            .is_err()
    );
    let initial = client
        .query_one(
            "SELECT * FROM public.novu_rust_reserve_alert_v2($1,'slack','ops',3600,$2)",
            &[&env, &i64::MIN],
        )
        .await
        .unwrap();
    assert_eq!(initial.get::<_, String>(0), "claimed");
    let first: Uuid = initial.get(1);
    assert!(
        client
            .query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','ops',$2,'accepted',NULL)",
                &[&env, &first]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let same = client
        .query_one(
            "SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','ops',3600,$2,$2,$3,$3)",
            &[&env, &i64::MIN, &0_i64],
        )
        .await
        .unwrap();
    assert_eq!(same.get::<_, String>(0), "cooldown");
    assert!(
        client
            .query_one(
                "SELECT * FROM public.novu_rust_reserve_alert_v2($1,'slack','ops',3600,$2)",
                &[&env, &1_i64]
            )
            .await
            .is_err()
    );
    assert!(
        client
            .query_one(
                "SELECT public.novu_rust_observe_alert_conditions_v2($1,'slack','ops',$2,$2)",
                &[&env, &1_i64]
            )
            .await
            .is_err()
    );
    assert!(
        client
            .query_one(
                "SELECT * FROM public.novu_rust_reserve_alert($1,'slack','ops',3600,$2)",
                &[&env, &1_i32]
            )
            .await
            .is_err()
    );
    let new = client
        .query_one(
            "SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','ops',3600,$2,$2,$3,$3)",
            &[&env, &i64::MIN, &1_i64],
        )
        .await
        .unwrap();
    assert_eq!(new.get::<_, String>(0), "claimed");
    let attempt: Uuid = new.get(1);
    assert!(
        client
            .query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','ops',$2,'accepted',NULL)",
                &[&env, &attempt]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(
        client
            .query_one(
                "SELECT result FROM public.novu_rust_reserve_alert_v3($1,'slack','ops',3600,$2,$2,$3,$3)",
                &[&env, &i64::MIN, &1_i64]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        "cooldown"
    );
    client
        .query_one(
            "SELECT public.novu_rust_observe_alert_conditions_v3($1,'slack','ops',$2,$3,$4,$5)",
            &[&env, &0_i64, &0_i64, &0_i64, &3_i64],
        )
        .await
        .unwrap();
    let recurrence = client
        .query_one(
            "SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','ops',3600,$2,$2,$3,$3)",
            &[&env, &i64::MIN, &1_i64],
        )
        .await
        .unwrap();
    assert_eq!(recurrence.get::<_, String>(0), "claimed");
    let adoption = client
        .query_one(
            "SELECT * FROM public.novu_rust_reserve_alert_v2($1,'slack','adopt',3600,$2)",
            &[&env, &1_i64],
        )
        .await
        .unwrap();
    let adoption_id: Uuid = adoption.get(1);
    assert!(
        client
            .query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','adopt',$2,'accepted',NULL)",
                &[&env, &adoption_id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    client
        .query_one(
            "SELECT public.novu_rust_observe_alert_conditions_v3($1,'slack','adopt',$2,$3,$4,$5)",
            &[&env, &0_i64, &1_i64, &0_i64, &0_i64],
        )
        .await
        .unwrap();
    assert!(
        client
            .query_one(
                "SELECT * FROM public.novu_rust_reserve_alert_v2($1,'slack','adopt',3600,$2)",
                &[&env, &1_i64]
            )
            .await
            .is_err()
    );
    assert_eq!(client.query_one("SELECT result FROM public.novu_rust_reserve_alert_v3($1,'slack','adopt',3600,$2,$2,$3,$3)",&[&env,&1_i64,&0_i64]).await.unwrap().get::<_,String>(0),"claimed");
    let (peer, driver) = tokio_postgres::connect(&role_url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&env, &0_i64, &1_i64];
    let (a, b) = tokio::join!(
        client.query_one(
            "SELECT result FROM public.novu_rust_reserve_alert_v3($1,'email','race',60,$2,$2,$3,$3)",
            params
        ),
        peer.query_one(
            "SELECT result FROM public.novu_rust_reserve_alert_v3($1,'email','race',60,$2,$2,$3,$3)",
            params
        )
    );
    let mut outcomes = vec![
        a.unwrap().get::<_, String>(0),
        b.unwrap().get::<_, String>(0),
    ];
    outcomes.sort();
    assert_eq!(outcomes, vec!["claimed", "inflight"]);
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id=$1 AND channel='email' AND target_id='race'",&[&env]).await.unwrap();
    assert_eq!(
        client
            .query_one(
                "SELECT result FROM public.novu_rust_reserve_alert_v3($1,'email','race',60,$2,$2,$3,$3)",
                params
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        "blocked"
    );
    assert!(db.query_opt("SELECT 1 FROM public.novu_rust_alert_delivery_slots s JOIN public.novu_rust_alert_delivery_attempts a ON a.id=s.attempt_id WHERE s.environment_id=$1 AND s.target_id='race' AND s.state='unknown' AND a.state='unknown'",&[&env]).await.unwrap().is_some());
    let legacy = client
        .query_one(
            "SELECT * FROM public.novu_rust_reserve_alert($1,'slack','legacy',60,$2)",
            &[&env, &1_i32],
        )
        .await
        .unwrap();
    let legacy_id: Uuid = legacy.get(1);
    assert!(
        client
            .query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','legacy',$2,'accepted',NULL)",
                &[&env, &legacy_id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        client
            .query_one(
                "SELECT * FROM public.novu_rust_reserve_alert_v3($1,'slack','legacy',60,$2,$2,$3,$3)",
                &[&env, &1_i64, &1_i64]
            )
            .await
            .is_err()
    );
    assert!(client.query_one("SELECT public.novu_rust_observe_alert_conditions_v3($1,'slack','legacy',$2,$3,$4,$5)",&[&env,&0_i64,&1_i64,&0_i64,&0_i64]).await.is_err());
}
