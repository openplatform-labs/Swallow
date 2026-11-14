//! Offline database administrator tool for scoped console credentials.
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::OpenOptions,
    io::{Read, Write},
};
use uuid::Uuid;

#[cfg(unix)]
fn write_secret(path: &str, bytes: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
#[cfg(not(unix))]
fn write_secret(_path: &str, _bytes: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
    Err("secure token file creation is unavailable on this platform".into())
}

#[cfg(unix)]
fn read_secret(path: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    // Safety: geteuid has no preconditions and does not mutate process state.
    let owner = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.uid() != owner
        || metadata.mode() & 0o077 != 0
        || metadata.len() != 64
    {
        return Err("token file must be a private 64-byte regular file owned by the caller".into());
    }
    let mut bytes = Vec::with_capacity(64);
    std::io::Read::by_ref(&mut file)
        .take(65)
        .read_to_end(&mut bytes)?;
    if bytes.len() != 64
        || !bytes
            .iter()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err("invalid token file contents".into());
    }
    Ok(bytes)
}
#[cfg(not(unix))]
fn read_secret(_path: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    Err("secure token file reading is unavailable on this platform".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: console-credential rotate-scoped ENV OLD_ID SECONDS GRACE_SECONDS NEW_FILE none|copy|retry|accepted|both REASON | issue ENV SECONDS NEW_FILE REASON | issue-scoped ENV SECONDS NEW_FILE subscriber|none REASON | issue-permissions ENV SECONDS NEW_FILE none|subscriber|alert|both REASON | rotate ENV OLD_ID SECONDS NEW_FILE REASON | rotate-overlap ENV OLD_ID SECONDS GRACE_SECONDS NEW_FILE REASON | rotation-audit ENV [AFTER_ID] | rotation-audit-v2 ENV [AFTER_ID] | decision-access ENV CREDENTIAL_ID retry|accepted grant|revoke REASON | decision-access-list ENV [AFTER_ID] | decision-audit ENV grant|resolution [AFTER_ID] | subscriber-access ENV CREDENTIAL_ID grant|revoke REASON | subscriber-access-list ENV [AFTER_ID] | subscriber-access-audit ENV [AFTER_ID] | inspect ENV TOKEN_FILE | revoke ENV CREDENTIAL_ID REASON | list ENV [AFTER_ID] | audit ENV [AFTER_ID] | alert-access ENV CREDENTIAL_ID grant|revoke REASON | alert-access-list ENV [AFTER_ID] | alert-access-audit ENV [AFTER_ID]";
    let valid = matches!(args.as_slice(),[action,_,_,_,_,_,_,_] if action=="rotate-scoped")
        || matches!(args.as_slice(),[action,_,_,_,_,_,_] if action=="rotate-overlap")
        || matches!(args.as_slice(),[action,_,_,_,_,_] if action=="issue-scoped" || action=="issue-permissions" || action=="rotate" || action=="decision-access")
        || matches!(args.as_slice(),[action,_,_,_,_] if action=="issue" || action=="alert-access" || action=="subscriber-access")
        || matches!(args.as_slice(),[action,_,_,_] if action=="revoke")
        || matches!(args.as_slice(),[action,_,_,_] if action=="decision-audit")
        || matches!(args.as_slice(),[action,_,_] if action=="decision-audit" || action=="decision-access-list")
        || matches!(args.as_slice(),[action,_] if action=="decision-access-list")
        || matches!(args.as_slice(),[action,_,_] if action=="inspect")
        || matches!(args.as_slice(),[action,_] if action=="list" || action=="audit" || action=="rotation-audit" || action=="rotation-audit-v2" || action=="alert-access-list" || action=="alert-access-audit" || action=="subscriber-access-list" || action=="subscriber-access-audit")
        || matches!(args.as_slice(),[action,_,_] if action=="list" || action=="audit" || action=="rotation-audit" || action=="rotation-audit-v2" || action=="alert-access-list" || action=="alert-access-audit" || action=="subscriber-access-list" || action=="subscriber-access-audit");
    if !valid {
        return Err(usage.into());
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (db, connection) =
        tokio::time::timeout(std::time::Duration::from_secs(30), config.connect(tls)).await??;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    match args[0].as_str() {
        "rotate-scoped" => {
            let old = Uuid::parse_str(&args[2])?;
            let seconds: i64 = args[3].parse()?;
            let grace: i32 = args[4].parse()?;
            if !(1..=2_592_000).contains(&seconds)
                || !(0..=86400).contains(&grace)
                || !["none", "copy", "retry", "accepted", "both"].contains(&args[6].as_str())
            {
                return Err("invalid lifetime, overlap or decision scope".into());
            }
            let mut random = [0u8; 32];
            SystemRandom::new()
                .fill(&mut random)
                .map_err(|_| "secure randomness unavailable")?;
            let token = random
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>();
            write_secret(&args[5], token.as_bytes())
                .map_err(|_| "token file creation failed; inspect the destination safely")?;
            let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
            let result = tokio::time::timeout(std::time::Duration::from_secs(30), db.query_one(
                "SELECT * FROM public.novu_rust_rotate_console_credential_scoped($1,$2,$3,clock_timestamp()+($4::bigint * interval '1 second'),$5,$6,$7)",
                &[&args[1],&old,&hash,&seconds,&grace,&args[6],&args[7]],
            )).await;
            match result {
                Ok(Ok(row)) => println!("{}", json!({"credentialId":row.get::<_,Uuid>(0).to_string(),"tokenFileCreated":true,"oldValidUntil":row.get::<_,Option<String>>(1)})),
                _ => return Err("rotation result unavailable; token file retained; reconcile old and new credentials, grants and rotation audit before retrying".into()),
            }
        }
        "decision-access" => {
            let id = Uuid::parse_str(&args[2])?;
            if !["retry", "accepted"].contains(&args[3].as_str())
                || !["grant", "revoke"].contains(&args[4].as_str())
            {
                return Err("invalid decision access action".into());
            }
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query_one(
                    "SELECT public.novu_rust_manage_console_alert_decision($1,$2,$3,$4,$5)",
                    &[&args[1], &id, &args[3], &args[4], &args[5]],
                ),
            )
            .await;
            match result {
                Ok(Ok(row)) => println!("{}",json!({"changed":row.get::<_,bool>(0)})),
                _ => return Err("decision access outcome unavailable; reconcile grants and audit before retrying".into()),
            }
        }
        "decision-access-list" => {
            let after = args.get(2).map(|s| Uuid::parse_str(s)).transpose()?;
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query(
                    "SELECT * FROM public.novu_rust_list_console_alert_decisions($1,$2)",
                    &[&args[1], &after],
                ),
            )
            .await??;
            let values: Vec<_> = rows.into_iter().map(|r| json!({"credentialId":r.get::<_,Uuid>(0).to_string(),"retry":r.get::<_,bool>(1),"accepted":r.get::<_,bool>(2),"enabled":r.get::<_,bool>(3),"expiresAt":r.get::<_,String>(4)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "decision-audit" => {
            let after = args
                .get(3)
                .map(|s| s.parse::<i64>())
                .transpose()?
                .unwrap_or(0);
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query(
                    "SELECT * FROM public.novu_rust_list_console_decision_audit($1,$2,$3)",
                    &[&args[1], &after, &args[2]],
                ),
            )
            .await??;
            let values: Vec<_> = rows.into_iter().map(|r| json!({"id":r.get::<_,String>(0),"credentialId":r.get::<_,Uuid>(1).to_string(),"details":r.get::<_,serde_json::Value>(2),"createdAt":r.get::<_,String>(3)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "rotate" | "rotate-overlap" => {
            let overlap = args[0] == "rotate-overlap";
            let old = Uuid::parse_str(&args[2])?;
            let seconds: i64 = args[3].parse()?;
            if !(1..=2_592_000).contains(&seconds) {
                return Err("lifetime must be 1 to 2592000 seconds".into());
            }
            let grace: i32 = if overlap { args[4].parse()? } else { 0 };
            if overlap && !(1..=86400).contains(&grace) {
                return Err("overlap must be 1 to 86400 seconds".into());
            }
            let mut random = [0u8; 32];
            SystemRandom::new()
                .fill(&mut random)
                .map_err(|_| "secure randomness unavailable")?;
            let token = random
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>();
            let file = if overlap { &args[5] } else { &args[4] };
            let reason = if overlap { &args[6] } else { &args[5] };
            write_secret(file, token.as_bytes())
                .map_err(|_| "token file creation failed; inspect the destination safely")?;
            let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
            let result = if overlap {
                tokio::time::timeout(std::time::Duration::from_secs(30), db.query_one("SELECT * FROM public.novu_rust_rotate_console_credential_overlap($1,$2,$3,clock_timestamp()+($4::bigint * interval '1 second'),$5,$6)", &[&args[1],&old,&hash,&seconds,&grace,reason])).await
            } else {
                tokio::time::timeout(std::time::Duration::from_secs(30), db.query_one("SELECT public.novu_rust_rotate_console_credential($1,$2,$3,clock_timestamp()+($4::bigint * interval '1 second'),$5)", &[&args[1],&old,&hash,&seconds,reason])).await
            };
            match result {
                Ok(Ok(row)) => println!("{}",json!({"credentialId":row.get::<_,Uuid>(0).to_string(),"tokenFileCreated":true,"oldValidUntil":if overlap {Some(row.get::<_,String>(1))} else {None}})),
                _ => return Err("rotation result unavailable; token file retained; reconcile old and new credentials and rotation audit before retrying".into()),
            }
        }
        "rotation-audit-v2" => {
            let after = args
                .get(2)
                .map(|s| s.parse::<i64>())
                .transpose()?
                .unwrap_or(0);
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query(
                    "SELECT * FROM public.novu_rust_list_console_rotation_audit_v2($1,$2)",
                    &[&args[1], &after],
                ),
            )
            .await??;
            let values: Vec<_> = rows.into_iter().map(|r| json!({"id":r.get::<_,String>(0),"oldCredentialId":r.get::<_,Uuid>(1).to_string(),"newCredentialId":r.get::<_,Uuid>(2).to_string(),"actor":r.get::<_,String>(3),"reason":r.get::<_,String>(4),"createdAt":r.get::<_,String>(5),"oldValidUntil":r.get::<_,Option<String>>(6)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "rotation-audit" => {
            let after = args
                .get(2)
                .map(|s| s.parse::<i64>())
                .transpose()?
                .unwrap_or(0);
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query(
                    "SELECT * FROM public.novu_rust_list_console_rotation_audit($1,$2)",
                    &[&args[1], &after],
                ),
            )
            .await??;
            let values: Vec<_> = rows.into_iter().map(|r| json!({"id":r.get::<_,String>(0),"oldCredentialId":r.get::<_,Uuid>(1).to_string(),"newCredentialId":r.get::<_,Uuid>(2).to_string(),"actor":r.get::<_,String>(3),"reason":r.get::<_,String>(4),"createdAt":r.get::<_,String>(5)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "issue" | "issue-scoped" | "issue-permissions" => {
            let scoped = args[0] != "issue";
            let permissions = args[0] == "issue-permissions";
            let (subscriber_read, alert_read) = if permissions {
                match args[4].as_str() {
                    "none" => (false, false),
                    "subscriber" => (true, false),
                    "alert" => (false, true),
                    "both" => (true, true),
                    _ => return Err("invalid initial permissions".into()),
                }
            } else if scoped {
                match args[4].as_str() {
                    "subscriber" => (true, false),
                    "none" => (false, false),
                    _ => return Err("invalid subscriber scope".into()),
                }
            } else {
                (true, false)
            };
            let reason = if scoped { &args[5] } else { &args[4] };
            let seconds: i64 = args[2].parse()?;
            if !(1..=2_592_000).contains(&seconds) {
                return Err("lifetime must be 1 to 2592000 seconds".into());
            }
            let mut random = [0u8; 32];
            SystemRandom::new()
                .fill(&mut random)
                .map_err(|_| "secure randomness unavailable")?;
            let token = random
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>();
            write_secret(&args[3], token.as_bytes())
                .map_err(|_| "token file creation failed; inspect the destination safely")?;
            let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
            let result = if permissions {
                tokio::time::timeout(std::time::Duration::from_secs(30), db.query_one("SELECT public.novu_rust_issue_console_credential_permissions($1,$2,clock_timestamp()+($3::bigint * interval '1 second'),$4,$5,$6)",&[&args[1],&hash,&seconds,&subscriber_read,&alert_read,reason])).await
            } else if scoped {
                tokio::time::timeout(std::time::Duration::from_secs(30), db.query_one("SELECT public.novu_rust_issue_console_credential_scoped($1,$2,clock_timestamp()+($3::bigint * interval '1 second'),$4,$5)",&[&args[1],&hash,&seconds,&subscriber_read,reason])).await
            } else {
                tokio::time::timeout(std::time::Duration::from_secs(30), db.query_one("SELECT public.novu_rust_issue_console_credential($1,$2,clock_timestamp()+($3::bigint * interval '1 second'),$4)",&[&args[1],&hash,&seconds,reason])).await
            };
            match result {
                Ok(Ok(row)) => println!("{}",json!({"credentialId":row.get::<_,Uuid>(0).to_string(),"tokenFileCreated":true})),
                _ => return Err("issuance result unavailable; token file retained; investigate database and audit before retrying".into()),
            }
        }
        "inspect" => {
            let token =
                read_secret(&args[2]).map_err(|_| "token file is unavailable or invalid")?;
            let hash = format!("{:x}", Sha256::digest(&token));
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query_opt(
                    "SELECT * FROM public.novu_rust_inspect_console_credential($1,$2)",
                    &[&args[1], &hash],
                ),
            )
            .await;
            match result {
                Ok(Ok(Some(row))) => println!(
                    "{}",
                    json!({"found":true,"credentialId":row.get::<_,Uuid>(0).to_string(),"enabled":row.get::<_,bool>(1),"expiresAt":row.get::<_,String>(2),"createdAt":row.get::<_,String>(3)})
                ),
                Ok(Ok(None)) => println!("{}", json!({"found":false})),
                _ => return Err("inspection unavailable; do not infer issuance outcome".into()),
            }
        }
        "subscriber-access" => {
            let id = Uuid::parse_str(&args[2])?;
            if !["grant", "revoke"].contains(&args[3].as_str()) {
                return Err("invalid subscriber access action".into());
            }
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query_one(
                    "SELECT public.novu_rust_manage_console_subscriber_scope($1,$2,$3,$4)",
                    &[&args[1], &id, &args[3], &args[4]],
                ),
            )
            .await;
            match result {
                Ok(Ok(row)) => println!("{}",json!({"changed":row.get::<_,bool>(0)})),
                _ => return Err("subscriber access outcome unavailable; reconcile scope and audit before retrying".into()),
            }
        }
        "subscriber-access-list" => {
            let after = args.get(2).map(|s| Uuid::parse_str(s)).transpose()?;
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query(
                    "SELECT * FROM public.novu_rust_list_console_subscriber_scopes($1,$2)",
                    &[&args[1], &after],
                ),
            )
            .await??;
            let values: Vec<_> = rows.into_iter().map(|r|json!({"credentialId":r.get::<_,Uuid>(0).to_string(),"enabled":r.get::<_,bool>(1),"expiresAt":r.get::<_,String>(2),"subscriberRead":r.get::<_,bool>(3)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "subscriber-access-audit" => {
            let after = args
                .get(2)
                .map(|s| s.parse::<i64>())
                .transpose()?
                .unwrap_or(0);
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query(
                    "SELECT * FROM public.novu_rust_list_console_subscriber_scope_audit($1,$2)",
                    &[&args[1], &after],
                ),
            )
            .await??;
            let values: Vec<_> = rows.into_iter().map(|r|json!({"id":r.get::<_,String>(0),"credentialId":r.get::<_,Uuid>(1).to_string(),"action":r.get::<_,String>(2),"actor":r.get::<_,String>(3),"reason":r.get::<_,String>(4),"createdAt":r.get::<_,String>(5)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "alert-access" => {
            let id = Uuid::parse_str(&args[2])?;
            if !["grant", "revoke"].contains(&args[3].as_str()) {
                return Err("invalid alert access action".into());
            }
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query_one(
                    "SELECT public.novu_rust_manage_console_alert_grant($1,$2,$3,$4)",
                    &[&args[1], &id, &args[3], &args[4]],
                ),
            )
            .await;
            match result {
                Ok(Ok(row)) => println!("{}", json!({"changed":row.get::<_,bool>(0)})),
                _ => return Err(
                    "alert access outcome unavailable; reconcile grant and audit before retrying"
                        .into(),
                ),
            }
        }
        "alert-access-list" => {
            let after = args.get(2).map(|s| Uuid::parse_str(s)).transpose()?;
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query(
                    "SELECT * FROM public.novu_rust_list_console_alert_grants($1,$2)",
                    &[&args[1], &after],
                ),
            )
            .await??;
            let values: Vec<_> = rows.into_iter().map(|r|json!({"credentialId":r.get::<_,Uuid>(0).to_string(),"enabled":r.get::<_,bool>(1),"expiresAt":r.get::<_,String>(2)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "alert-access-audit" => {
            let after = args
                .get(2)
                .map(|s| s.parse::<i64>())
                .transpose()?
                .unwrap_or(0);
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                db.query(
                    "SELECT * FROM public.novu_rust_list_console_alert_grant_audit($1,$2)",
                    &[&args[1], &after],
                ),
            )
            .await??;
            let values: Vec<_> = rows.into_iter().map(|r|json!({"id":r.get::<_,String>(0),"credentialId":r.get::<_,Uuid>(1).to_string(),"action":r.get::<_,String>(2),"actor":r.get::<_,String>(3),"reason":r.get::<_,String>(4),"createdAt":r.get::<_,String>(5)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "revoke" => {
            let id = Uuid::parse_str(&args[2])?;
            let changed: bool = db
                .query_one(
                    "SELECT public.novu_rust_revoke_console_credential($1,$2,$3)",
                    &[&args[1], &id, &args[3]],
                )
                .await?
                .get(0);
            println!("{}", json!({"changed":changed}));
        }
        "list" => {
            let after = args.get(2).map(|s| Uuid::parse_str(s)).transpose()?;
            let rows = db
                .query(
                    "SELECT * FROM public.novu_rust_list_console_credentials($1,$2)",
                    &[&args[1], &after],
                )
                .await?;
            let values:Vec<_>=rows.into_iter().map(|r|json!({"credentialId":r.get::<_,Uuid>(0).to_string(),"enabled":r.get::<_,bool>(1),"expiresAt":r.get::<_,String>(2),"createdAt":r.get::<_,String>(3)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        "audit" => {
            let after = args
                .get(2)
                .map(|s| s.parse::<i64>())
                .transpose()?
                .unwrap_or(0);
            let rows = db
                .query(
                    "SELECT * FROM public.novu_rust_list_console_credential_audit($1,$2)",
                    &[&args[1], &after],
                )
                .await?;
            let values:Vec<_>=rows.into_iter().map(|r|json!({"id":r.get::<_,String>(0),"credentialId":r.get::<_,Uuid>(1).to_string(),"action":r.get::<_,String>(2),"actor":r.get::<_,String>(3),"reason":r.get::<_,String>(4),"expiresAt":r.get::<_,String>(5),"createdAt":r.get::<_,String>(6)})).collect();
            println!("{}", serde_json::to_string(&values)?);
        }
        _ => unreachable!(),
    }
    Ok(())
}
