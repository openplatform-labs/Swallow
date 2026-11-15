use reqwest::header;
use std::time::{Duration, SystemTime};

// Return a conservative whole-second delay. When available, the server's
// Date header avoids relying on agreement between the server and worker clocks.
pub(crate) fn retry_after_seconds(headers: &header::HeaderMap, now: SystemTime) -> Option<u32> {
    let mut values = headers.get_all(header::RETRY_AFTER).iter();
    let value = match (values.next(), values.next()) {
        (Some(value), None) => value.to_str().ok()?,
        _ => return None,
    };
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        return value.parse::<u32>().ok().filter(|seconds| *seconds <= 86_400);
    }
    let until = httpdate::parse_http_date(value).ok()?;
    let mut dates = headers.get_all(header::DATE).iter();
    let base = match (dates.next(), dates.next()) {
        (None, None) => now,
        (Some(date), None) => httpdate::parse_http_date(date.to_str().ok()?).ok()?,
        _ => return None,
    };
    let delay = until.duration_since(base).unwrap_or(Duration::ZERO);
    let seconds = delay.as_secs().checked_add(u64::from(delay.subsec_nanos() != 0))?;
    u32::try_from(seconds).ok().filter(|seconds| *seconds <= 86_400)
}

