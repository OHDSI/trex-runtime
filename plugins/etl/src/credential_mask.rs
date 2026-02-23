/// Mask the password in a libpq-format connection string.
///
/// Handles both key=value format (`password=secret`) and
/// URI format (`postgresql://user:pass@host/db`).
pub fn mask_password(conn_str: &str) -> String {
    if conn_str.starts_with("postgresql://") || conn_str.starts_with("postgres://") {
        mask_uri_password(conn_str)
    } else {
        mask_kv_password(conn_str)
    }
}

fn mask_kv_password(conn_str: &str) -> String {
    let mut result = String::with_capacity(conn_str.len());
    let mut remaining = conn_str;

    while !remaining.is_empty() {
        let trimmed = remaining.trim_start();
        if trimmed.len() < remaining.len() {
            result.push_str(&remaining[..remaining.len() - trimmed.len()]);
            remaining = trimmed;
        }

        if remaining.is_empty() {
            break;
        }

        if let Some(eq_pos) = remaining.find('=') {
            let key = &remaining[..eq_pos];
            let after_eq = &remaining[eq_pos + 1..];

            let (value, rest) = if after_eq.starts_with('\'') {
                if let Some(end_quote) = after_eq[1..].find('\'') {
                    (&after_eq[..end_quote + 2], &after_eq[end_quote + 2..])
                } else {
                    (after_eq, "")
                }
            } else {
                match after_eq.find(' ') {
                    Some(sp) => (&after_eq[..sp], &after_eq[sp..]),
                    None => (after_eq, ""),
                }
            };

            if key.eq_ignore_ascii_case("password") {
                result.push_str(key);
                result.push_str("=***");
            } else {
                result.push_str(key);
                result.push('=');
                result.push_str(value);
            }

            remaining = rest;
        } else {
            result.push_str(remaining);
            break;
        }
    }

    result
}

fn mask_uri_password(conn_str: &str) -> String {
    let scheme_end = conn_str.find("://").unwrap_or(0) + 3;
    let authority = &conn_str[scheme_end..];

    let at_pos = match authority.find('@') {
        Some(pos) => pos,
        None => return conn_str.to_string(),
    };

    let userinfo = &authority[..at_pos];
    let colon_pos = match userinfo.find(':') {
        Some(pos) => pos,
        None => return conn_str.to_string(),
    };

    format!(
        "{}{}:***@{}",
        &conn_str[..scheme_end],
        &userinfo[..colon_pos],
        &authority[at_pos + 1..]
    )
}

/// Extract a named parameter from a libpq-format connection string.
///
/// Only matches whole key names — e.g. searching for "host" will not match
/// "ghosthost=val".
pub fn extract_param<'a>(conn_str: &'a str, key: &str) -> Option<&'a str> {
    let search = format!("{}=", key);
    let mut search_from = 0;

    loop {
        let relative = conn_str[search_from..].find(&search)?;
        let abs_start = search_from + relative;

        // Ensure we matched a whole key: must be at start or preceded by a space
        if abs_start == 0 || conn_str.as_bytes()[abs_start - 1] == b' ' {
            let value_start = abs_start + search.len();
            let rest = &conn_str[value_start..];

            if rest.starts_with('\'') {
                let end = rest[1..].find('\'')?;
                return Some(&rest[1..1 + end]);
            } else {
                let end = rest.find(' ').unwrap_or(rest.len());
                return Some(&rest[..end]);
            }
        }

        // False match inside another key — keep searching
        search_from = abs_start + 1;
    }
}
