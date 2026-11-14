//! Explicit, transactional worker migration history. API migrations are managed separately.
use ring::digest::{SHA256, digest};
use std::{error::Error, fs, path::Path};
use tokio_postgres::Client;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
struct Migration {
    version: i32,
    name: String,
    checksum: Vec<u8>,
    sql: String,
}
fn read_migrations(path: &Path) -> Result<Vec<Migration>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "non-UTF8 migration name")?;
        if !name.ends_with(".sql") {
            continue;
        }
        let bytes = name.as_bytes();
        if bytes.len() < 10
            || !bytes[..4].iter().all(u8::is_ascii_digit)
            || bytes[4] != b'_'
            || !bytes[5..]
                .iter()
                .all(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'.')
            || !entry.file_type()?.is_file()
        {
            return Err("invalid migration file".into());
        }
        let version: i32 = name[..4].parse()?;
        let sql = fs::read_to_string(entry.path())?;
        let checksum = digest(&SHA256, sql.as_bytes()).as_ref().to_vec();
        files.push(Migration {
            version,
            name,
            checksum,
            sql,
        });
    }
    files.sort_by_key(|v| v.version);
    if files.is_empty()
        || files
            .iter()
            .enumerate()
            .any(|(i, m)| m.version != (i + 1) as i32)
    {
        return Err("migration versions must be contiguous and start at 0001".into());
    }
    Ok(files)
}
fn transaction_body(m: &Migration) -> Result<String> {
    let mut result = String::new();
    let mut removed = 0;
    for line in m.sql.lines() {
        let word = line.trim().to_ascii_uppercase();
        if ["BEGIN;", "COMMIT;", "ROLLBACK;"].contains(&word.as_str()) {
            if m.name == "0023_experimental_topic_subscriptions.sql"
                && ((removed == 0 && word == "BEGIN;") || (removed == 1 && word == "COMMIT;"))
            {
                removed += 1;
                continue;
            }
            return Err("explicit transaction control is not supported in a migration".into());
        }
        result.push_str(line);
        result.push('\n');
    }
    if m.name == "0023_experimental_topic_subscriptions.sql" && removed != 2 {
        return Err("unexpected migration 0023 transaction wrapper".into());
    }
    Ok(result)
}
async fn run(db: &mut Client, files: &[Migration], adopt: Option<i32>) -> Result<usize> {
    let tx = db.transaction().await?;
    tx.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock(736915402,871)",
        &[],
    )
    .await?;
    let existing=tx.query_one("SELECT to_regclass('public.novu_rust_worker_migration_history') IS NOT NULL, to_regclass('public.novu_queue_jobs') IS NOT NULL",&[]).await?;
    let tracked: bool = existing.get(0);
    let queue: bool = existing.get(1);
    if adopt.is_some() && (tracked || !queue) {
        return Err("baseline requires an untracked existing worker schema".into());
    }
    if !tracked && queue && adopt.is_none() {
        return Err("existing worker schema has no migration history; review it and explicitly baseline first".into());
    }
    if !tracked {
        tx.batch_execute("CREATE TABLE public.novu_rust_worker_migration_history(version integer PRIMARY KEY,filename text NOT NULL UNIQUE,checksum bytea NOT NULL CHECK(octet_length(checksum)=32),method text NOT NULL CHECK(method IN ('applied','baseline')),applied_by text NOT NULL DEFAULT session_user,applied_at timestamptz NOT NULL DEFAULT clock_timestamp()); REVOKE ALL ON public.novu_rust_worker_migration_history FROM PUBLIC;").await?;
    }
    let rows=tx.query("SELECT version,filename,checksum FROM public.novu_rust_worker_migration_history ORDER BY version",&[]).await?;
    if rows.len() > files.len() {
        return Err("migration history is newer than migration files".into());
    }
    for (row, file) in rows.iter().zip(files) {
        if row.get::<_, i32>(0) != file.version
            || row.get::<_, String>(1) != file.name
            || row.get::<_, Vec<u8>>(2) != file.checksum
        {
            return Err("migration history does not match local files".into());
        }
    }
    if let Some(version) = adopt {
        if version < 1 || version as usize > files.len() {
            return Err("invalid baseline version".into());
        }
        for m in files.iter().take(version as usize) {
            tx.execute("INSERT INTO public.novu_rust_worker_migration_history(version,filename,checksum,method) VALUES ($1,$2,$3,'baseline')",&[&m.version,&m.name,&m.checksum]).await?;
        }
        tx.commit().await?;
        return Ok(version as usize);
    }
    let mut count = 0;
    for m in files.iter().skip(rows.len()) {
        let sql = transaction_body(m)?;
        tx.batch_execute(&sql).await?;
        tx.execute("INSERT INTO public.novu_rust_worker_migration_history(version,filename,checksum,method) VALUES ($1,$2,$3,'applied')",&[&m.version,&m.name,&m.checksum]).await?;
        count += 1;
    }
    tx.commit().await?;
    Ok(count)
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let adopt=match args.as_slice() {
        [command,_] if command=="apply"=>None,
        [command,_,version,confirmation] if command=="baseline" && confirmation=="I-VERIFIED-THE-EXISTING-SCHEMA"=>Some(version.parse()?),
        _=>return Err("usage: migrate-worker apply MIGRATION_DIR | baseline MIGRATION_DIR VERSION I-VERIFIED-THE-EXISTING-SCHEMA".into()),
    };
    let files = read_migrations(Path::new(&args[1]))?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("database connection failed");
        }
    });
    let count = run(&mut db, &files, adopt).await?;
    println!(
        "{} {} worker migrations",
        if adopt.is_some() {
            "baselined"
        } else {
            "applied"
        },
        count
    );
    Ok(())
}
