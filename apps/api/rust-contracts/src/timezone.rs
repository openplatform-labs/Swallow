//! Node 22.23.4 / ICU 78.3 time-zone names and ECMA-402 numeric offsets.
use std::sync::OnceLock;

pub fn valid(value: &str) -> bool {
    if let Some(offset) = value
        .strip_prefix('+')
        .or_else(|| value.strip_prefix('-'))
        .or_else(|| value.strip_prefix('−'))
    {
        let bytes = offset.as_bytes();
        let (hour, minute) = match bytes {
            [h1, h2] => ([*h1, *h2], [b'0', b'0']),
            [h1, h2, m1, m2] => ([*h1, *h2], [*m1, *m2]),
            [h1, h2, b':', m1, m2] => ([*h1, *h2], [*m1, *m2]),
            _ => return false,
        };
        if !hour.iter().chain(minute.iter()).all(u8::is_ascii_digit) {
            return false;
        }
        let hours = (hour[0] - b'0') * 10 + hour[1] - b'0';
        let minutes = (minute[0] - b'0') * 10 + minute[1] - b'0';
        return hours <= 23 && minutes <= 59;
    }
    if !value.is_ascii() {
        return false;
    }
    static NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();
    let names = NAMES.get_or_init(|| include_str!("timezones-node22.txt").lines().collect());
    names
        .binary_search(&value.to_ascii_lowercase().as_str())
        .is_ok()
}
