//! Locale grammar used by validator.js 13.15.26's isLocale.
const GRANDFATHERED: &[&str] = &[
    "en-GB-oed",
    "i-ami",
    "i-bnn",
    "i-default",
    "i-enochian",
    "i-hak",
    "i-klingon",
    "i-lux",
    "i-mingo",
    "i-navajo",
    "i-pwn",
    "i-tao",
    "i-tay",
    "i-tsu",
    "sgn-BE-FR",
    "sgn-BE-NL",
    "sgn-CH-DE",
    "art-lojban",
    "cel-gaulish",
    "no-bok",
    "no-nyn",
    "zh-guoyu",
    "zh-hakka",
    "zh-min",
    "zh-min-nan",
    "zh-xiang",
];
fn alpha(s: &[u8]) -> bool {
    s.iter().all(u8::is_ascii_alphabetic)
}
fn alnum(s: &[u8]) -> bool {
    s.iter().all(u8::is_ascii_alphanumeric)
}
fn separator(c: u8) -> bool {
    c == b'-' || c == b'_'
}
fn private(s: &[u8], at: usize) -> bool {
    if s.get(at) != Some(&b'x') {
        return false;
    }
    let mut i = at + 1;
    let mut parts = 0;
    while i < s.len() && s[i] == b'-' {
        i += 1;
        let begin = i;
        while i < s.len() && s[i].is_ascii_alphanumeric() {
            i += 1;
        }
        if !(1..=8).contains(&(i - begin)) {
            return false;
        }
        parts += 1;
    }
    parts > 0 && i == s.len()
}
fn suffix(s: &[u8], at: usize, memo: &mut [Option<bool>]) -> bool {
    if at == s.len() {
        return true;
    }
    if let Some(result) = memo[at] {
        return result;
    }
    let result = (|| {
        if !separator(s[at]) {
            return false;
        }
        let start = at + 1;
        if private(s, start) {
            return true;
        }
        // validator.js includes '-' in the 4-character variant's trailing class.
        if start + 4 <= s.len()
            && s[start].is_ascii_digit()
            && s[start + 1..start + 4]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
            && suffix(s, start + 4, memo)
        {
            return true;
        }
        for n in 5..=8 {
            if start + n <= s.len() && alnum(&s[start..start + n]) && suffix(s, start + n, memo) {
                return true;
            }
        }
        if start >= s.len()
            || !s[start].is_ascii_alphanumeric()
            || s[start].eq_ignore_ascii_case(&b'x')
        {
            return false;
        }
        fn extension(s: &[u8], at: usize, memo: &mut [Option<bool>]) -> bool {
            if at >= s.len() || s[at] != b'-' {
                return false;
            }
            let begin = at + 1;
            for n in 2..=8 {
                if begin + n <= s.len()
                    && alnum(&s[begin..begin + n])
                    && (suffix(s, begin + n, memo) || extension(s, begin + n, memo))
                {
                    return true;
                }
            }
            false
        }
        extension(s, start + 1, memo)
    })();
    memo[at] = Some(result);
    result
}
fn region(s: &[u8], at: usize, memo: &mut [Option<bool>]) -> bool {
    if suffix(s, at, memo) {
        return true;
    }
    if at >= s.len() || !separator(s[at]) {
        return false;
    }
    let i = at + 1;
    (i + 2 <= s.len() && alpha(&s[i..i + 2]) && suffix(s, i + 2, memo))
        || (i + 3 <= s.len()
            && s[i..i + 3].iter().all(u8::is_ascii_digit)
            && suffix(s, i + 3, memo))
}
fn script(s: &[u8], at: usize, memo: &mut [Option<bool>]) -> bool {
    if region(s, at, memo) {
        return true;
    }
    at < s.len()
        && separator(s[at])
        && at + 5 <= s.len()
        && alpha(&s[at + 1..at + 5])
        && region(s, at + 5, memo)
}
fn language(s: &[u8], base: usize, memo: &mut [Option<bool>]) -> bool {
    if script(s, base, memo) {
        return true;
    }
    if base > 3 {
        return false;
    }
    let mut at = base;
    for _ in 0..3 {
        if at + 4 > s.len() || s[at] != b'-' || !alpha(&s[at + 1..at + 4]) {
            break;
        }
        at += 4;
        if script(s, at, memo) {
            return true;
        }
    }
    false
}
pub fn valid(value: &str) -> bool {
    // Keep recursive extension parsing bounded for untrusted API input.
    if value.len() > 1024 {
        return false;
    }
    if GRANDFATHERED.contains(&value) {
        return true;
    }
    let s = value.as_bytes();
    if private(s, 0) {
        return true;
    }
    let mut memo = vec![None; s.len() + 1];
    [2, 3, 5, 6, 7, 8]
        .into_iter()
        .any(|n| n <= s.len() && alpha(&s[..n]) && language(s, n, &mut memo))
}
