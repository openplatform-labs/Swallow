//! Read a bounded secret from exactly one configured source.
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

const MAX_SECRET_BYTES: u64 = 16_384;

/// Reads once at startup. No path or secret is included in errors.
pub fn read(inline: Option<&str>, file: Option<&Path>) -> Result<Option<String>, io::Error> {
    if inline.is_some() && file.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "multiple secret sources",
        ));
    }
    let bytes = match (inline, file) {
        (Some(value), None) => value.as_bytes().to_vec(),
        (None, Some(path)) => {
            let mut value = Vec::new();
            File::open(path)
                .map_err(|_| io::Error::other("secret file unavailable"))?
                .take(MAX_SECRET_BYTES + 1)
                .read_to_end(&mut value)
                .map_err(|_| io::Error::other("secret file unavailable"))?;
            value
        }
        (None, None) => return Ok(None),
        _ => unreachable!(),
    };
    validate(bytes).map(Some)
}

fn validate(mut bytes: Vec<u8>) -> Result<String, io::Error> {
    if bytes.len() as u64 > MAX_SECRET_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid secret"));
    }
    if bytes.ends_with(b"\n") {
        bytes.pop();
        if bytes.ends_with(b"\r") {
            bytes.pop();
        }
    }
    let value = String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid secret"))?;
    if value.is_empty() || value.contains(['\r', '\n', '\0']) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid secret"));
    }
    Ok(value)
}

/// Read a regular file through its opened handle; on Unix, nonblocking open
/// prevents a FIFO without a writer from hanging the worker. Symlinks to
/// regular files are allowed for secret mounts and atomic replacement.
fn read_regular_bytes(path: &Path, limit: u64) -> Result<Vec<u8>, io::Error> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| io::Error::other("secret file unavailable"))?;
    if !file
        .metadata()
        .map_err(|_| io::Error::other("secret file unavailable"))?
        .is_file()
    {
        return Err(io::Error::other("invalid secret file type"));
    }
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| io::Error::other("secret file unavailable"))?;
    if bytes.is_empty() || bytes.len() as u64 > limit || bytes.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid secret"));
    }
    Ok(bytes)
}

pub fn read_regular_file(path: &Path) -> Result<String, io::Error> {
    validate(read_regular_bytes(path, MAX_SECRET_BYTES)?)
}

/// Read bounded UTF-8 multiline credentials from a regular file without logging them.
pub fn read_regular_multiline_file(path: &Path, limit: u64) -> Result<String, io::Error> {
    String::from_utf8(read_regular_bytes(path, limit)?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid secret"))
}

pub fn from_env(name: &str, file_name: &str) -> Result<Option<String>, io::Error> {
    let inline = match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid secret"));
        }
    };
    let file = std::env::var_os(file_name);
    read(inline.as_deref(), file.as_deref().map(Path::new))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_sources_without_leaking_secrets() {
        let path = std::env::temp_dir().join(format!("swallow-secret-test-{}", std::process::id()));
        std::fs::write(&path, b"fixture-secret\n").unwrap();
        assert_eq!(
            read(None, Some(&path)).unwrap().as_deref(),
            Some("fixture-secret")
        );
        assert_eq!(
            read(Some("inline"), None).unwrap().as_deref(),
            Some("inline")
        );
        assert!(
            read(Some("sensitive"), Some(&path))
                .unwrap_err()
                .to_string()
                .contains("multiple")
        );
        std::fs::write(&path, vec![b'x'; MAX_SECRET_BYTES as usize + 1]).unwrap();
        assert!(read(None, Some(&path)).is_err());
        assert!(read(Some("bad\nsecret"), None).is_err());
        assert!(read(Some(""), None).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn regular_file_reader_rejects_fifo_and_allows_symlink() {
        let dir = std::env::temp_dir().join(format!("swallow-secret-file-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fifo = dir.join("fifo");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(read_regular_file(&fifo));
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_secs(2))
                .unwrap()
                .is_err()
        );
        std::fs::write(dir.join("key"), "fixture\n").unwrap();
        std::os::unix::fs::symlink("key", dir.join("link")).unwrap();
        assert_eq!(read_regular_file(&dir.join("link")).unwrap(), "fixture");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
