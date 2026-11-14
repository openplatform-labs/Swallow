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
    let mut bytes = match (inline, file) {
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
    Ok(Some(value))
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
}
