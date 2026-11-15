//! Restricted DB operator for revoking operator JWT identifiers.
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{error::Error, io::Read, path::Path, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Revocation {
    issuer: String,
    subject: String,
    client_id: String,
    jti: String,
    reason: String,
}

#[cfg(unix)]
fn read_request(path: &Path) -> Result<Revocation> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "request file unavailable")?;
    let metadata = file.metadata().map_err(|_| "request file unavailable")?;
    // geteuid has no preconditions and does not change process state.
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
        || metadata.len() > 16384
    {
        return Err(
            "request file must be a private regular file owned by the caller and at most 16 KiB"
                .into(),
        );
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(16385)
        .read_to_end(&mut bytes)
        .map_err(|_| "request file unavailable")?;
    if bytes.len() > 16384 {
        return Err("invalid request file".into());
    }
    let request: Revocation = serde_json::from_slice(&bytes).map_err(|_| "invalid request file")?;
    if request.issuer.is_empty()
        || request.issuer.len() > 2048
        || [&request.subject, &request.client_id, &request.jti]
            .iter()
            .any(|s| s.is_empty() || s.len() > 1024)
        || request.reason.trim().is_empty()
        || request.reason.trim().len() > 1000
        || [
            &request.issuer,
            &request.subject,
            &request.client_id,
            &request.jti,
            &request.reason,
        ]
        .iter()
        .any(|s| s.contains('\0'))
    {
        return Err("invalid request file".into());
    }
    Ok(request)
}
#[cfg(not(unix))]
fn read_request(_path: &Path) -> Result<Revocation> {
    Err("private file validation is unavailable on this platform".into())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let request = match args.as_slice() {
        [action, path] if action == "revoke" => Some(read_request(Path::new(path))?),
        [action] | [action, _] if action == "audit" => None,
        _ => {
            return Err(
                "usage: operator-token-admin revoke PRIVATE_JSON_FILE | audit [BEFORE_ID]".into(),
            );
        }
    };
    let before = if args[0] == "audit" {
        args.get(1)
            .map(|v| v.parse::<i64>())
            .transpose()
            .map_err(|_| "invalid audit cursor")?
    } else {
        None
    };
    if before.is_some_and(|v| v < 1) {
        return Err("invalid audit cursor".into());
    }
    let url = std::env::var("DATABASE_URL").map_err(|_| "database configuration unavailable")?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&url)
        .map_err(|_| "invalid database configuration")?;
    let (db, driver) = tokio::time::timeout(Duration::from_secs(30), config.connect(tls))
        .await
        .map_err(|_| "database unavailable")?
        .map_err(|_| "database unavailable")?;
    tokio::spawn(async move {
        let _ = driver.await;
    });
    if let Some(request) = request {
        let issuer = Sha256::digest(request.issuer.as_bytes());
        let subject = Sha256::digest(request.subject.as_bytes());
        let client = Sha256::digest(request.client_id.as_bytes());
        let jti = Sha256::digest(request.jti.as_bytes());
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            db.query_one(
                "SELECT public.novu_rust_admin_revoke_operator_token($1,$2,$3,$4,$5)",
                &[
                    &issuer.as_slice(),
                    &subject.as_slice(),
                    &client.as_slice(),
                    &jti.as_slice(),
                    &request.reason,
                ],
            ),
        )
        .await;
        match result {
            Ok(Ok(row)) => println!("{}", json!({"created":row.get::<_,bool>(0)})),
            _ => {
                return Err(
                    "revocation result unavailable; check the denylist and audit before retrying"
                        .into(),
                );
            }
        }
    } else {
        let result=tokio::time::timeout(Duration::from_secs(30),db.query("SELECT id,issuer_hash,subject_hash,client_hash,token_id_hash,actor::text,actor_oid::bigint,reason,created_at::text FROM public.novu_rust_operator_token_revocation_audit_page($1,100)",&[&before])).await;
        let rows = result
            .map_err(|_| "audit unavailable")?
            .map_err(|_| "audit unavailable")?;
        let items: Vec<_>=rows.iter().map(|row| json!({"id":row.get::<_,i64>(0),"issuerHash":hex(&row.get::<_,Vec<u8>>(1)),"subjectHash":hex(&row.get::<_,Vec<u8>>(2)),"clientHash":hex(&row.get::<_,Vec<u8>>(3)),"tokenIdHash":hex(&row.get::<_,Vec<u8>>(4)),"actor":row.get::<_,String>(5),"actorOid":row.get::<_,i64>(6),"reason":row.get::<_,String>(7),"createdAt":row.get::<_,String>(8)})).collect();
        println!(
            "{}",
            json!({"data":items,"nextBefore":if rows.len()==100 { rows.last().map(|r|r.get::<_,i64>(0)) } else { None }})
        );
    }
    Ok(())
}
