//! URL helpers (ARCH-7: moved out of `lib.rs`; re-exported at the crate root
//! so `zimservice::redact_url` stays stable).

/// Replace `user:pass@` in a URL's **authority** with `***@` so credentials
/// never reach logs. Only the userinfo before the host is redacted — an `@` in
/// the path/query/fragment is left alone (and the host is preserved).
/// URLs without userinfo are returned unchanged.
///
/// ```
/// assert_eq!(
///     zimservice::redact_url("http://user:pass@example.com/qb"),
///     "http://***@example.com/qb"
/// );
/// assert_eq!(zimservice::redact_url("https://example.com/"), "https://example.com/");
/// ```
pub fn redact_url(url: &str) -> String {
    if let Some((scheme, rest)) = url.split_once("://") {
        // Restrict the `@` search to the authority (before the first `/`): an
        // `@` later in the path/query is not userinfo. `authority` is a
        // prefix of `rest`, so `at` indexes validly into `rest`.
        let authority = rest.split('/').next().unwrap_or(rest);
        if let Some(at) = authority.rfind('@') {
            return format!("{scheme}://***@{}", &rest[at + 1..]);
        }
    }
    url.to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::redact_url;

    #[test]
    fn redact_url_strips_userinfo() {
        assert_eq!(
            redact_url("http://admin:adminadmin@localhost:8080"),
            "http://***@localhost:8080"
        );
        assert_eq!(
            redact_url("https://u:p@host/x?a=1"),
            "https://***@host/x?a=1"
        );
        // no userinfo → unchanged
        assert_eq!(redact_url("http://localhost:8080"), "http://localhost:8080");
        assert_eq!(redact_url("not-a-url"), "not-a-url");
        assert_eq!(redact_url("http://@host"), "http://***@host");
        // M-redact: an `@` in the path/query is **not** userinfo — the host is
        // preserved and nothing is redacted (previously `rfind('@')` scanned the
        // whole URL and mangled host-less-looking authorities).
        assert_eq!(redact_url("http://host/x?a=@b"), "http://host/x?a=@b");
        assert_eq!(
            redact_url("http://u:p@host/path@x"),
            "http://***@host/path@x"
        );
        assert_eq!(redact_url("weird@thing"), "weird@thing");
    }
}
