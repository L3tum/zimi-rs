//! Embedded Web UI handlers: the three `include_str!` HTML pages plus the
//! embedded JS files (`common.js` + one per page), each carrying the SEC L3
//! security headers.
use axum::http::header;
use axum::response::Response;

/// Baseline CSP for the embedded Web UI pages (SEC L3). All scripts are
/// external embedded files (`script-src 'self'`) — the pages carry no inline
/// `<script>` blocks, so `'unsafe-inline'` and per-script `sha256-` hashes
/// are unnecessary (the `pages_have_no_inline_scripts` test guards that).
/// Inline *styles* keep `'unsafe-inline'` (no script-execution surface).
const WEB_CSP: &str =
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'";

/// Shared security headers for the embedded Web UI (SEC L3): refuse framing
/// (`X-Frame-Options: DENY`) and referrer leakage (`Referrer-Policy:
/// no-referrer`), and the baseline CSP (see [`WEB_CSP`]).
fn web_security_headers() -> Vec<(header::HeaderName, header::HeaderValue)> {
    vec![
        (
            header::CONTENT_SECURITY_POLICY,
            header::HeaderValue::from_static(WEB_CSP),
        ),
        (
            header::X_FRAME_OPTIONS,
            header::HeaderValue::from_static("DENY"),
        ),
        (
            header::REFERRER_POLICY,
            header::HeaderValue::from_static("no-referrer"),
        ),
    ]
}

/// CSP for served ZIM content (SEC-M2): blocks all subresources by default,
/// kills script execution and framing; keeps inline styles + data-URI
/// images/fonts so article rendering survives.
pub const RAW_CONTENT_CSP: &str =
    "default-src 'none'; script-src 'none'; style-src 'unsafe-inline'; img-src data: blob:; font-src data:; frame-ancestors 'none'";

/// MIME types that browsers render as HTML documents (i.e. can execute
/// scripts). Used to decide whether the sandbox CSP + `X-Frame-Options: DENY`
/// must be applied to a `/w` response. The ZIM's central directory is
/// attacker-controlled for downloaded archives, so gating on the broader
/// "browser-rendered-as-HTML" set (not just the `text/html` prefix) closes
/// the `application/xhtml+xml` / `image/svg+xml` sandbox bypass (SEC-M1).
fn is_browser_html(content_type: &str) -> bool {
    let ct = content_type.split(';').next().unwrap_or("").trim();
    ct == "text/html" || ct == "application/xhtml+xml" || ct == "image/svg+xml"
}

/// Sandbox headers for `/w` responses: applied to every MIME type browsers
/// render as an HTML document (see `is_browser_html`); empty for all other
/// content types.
pub fn raw_content_security_headers(content_type: &str) -> Vec<(header::HeaderName, String)> {
    if is_browser_html(content_type) {
        vec![
            (header::CONTENT_SECURITY_POLICY, RAW_CONTENT_CSP.to_string()),
            (header::X_FRAME_OPTIONS, "DENY".to_string()),
        ]
    } else {
        Vec::new()
    }
}

/// Build a `text/html` response for an embedded page carrying the SEC L3
/// security headers.
fn web_html_response(body: &'static str) -> Response {
    let mut resp = Response::new(axum::body::Body::from(body));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/html; charset=utf-8"
            .parse()
            .expect("valid static content-type"),
    );
    for (name, value) in web_security_headers() {
        resp.headers_mut().insert(name, value);
    }
    resp
}

/// Build an `application/javascript` response for an embedded JS file
/// carrying the SEC L3 security headers.
fn web_js_response(body: &'static str) -> Response {
    let mut resp = Response::new(axum::body::Body::from(body));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/javascript"
            .parse()
            .expect("valid static content-type"),
    );
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        "public, max-age=300"
            .parse()
            .expect("valid static cache-control"),
    );
    for (name, value) in web_security_headers() {
        resp.headers_mut().insert(name, value);
    }
    resp
}

pub async fn web_index() -> Response {
    web_html_response(include_str!("../../../web/index.html"))
}

pub async fn web_search() -> Response {
    web_html_response(include_str!("../../../web/search.html"))
}

pub async fn web_settings() -> Response {
    web_html_response(include_str!("../../../web/settings.html"))
}

pub async fn web_common_js() -> Response {
    web_js_response(include_str!("../../../web/common.js"))
}

pub async fn web_index_js() -> Response {
    web_js_response(include_str!("../../../web/index.js"))
}

pub async fn web_search_js() -> Response {
    web_js_response(include_str!("../../../web/search.js"))
}

pub async fn web_settings_js() -> Response {
    web_js_response(include_str!("../../../web/settings.js"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // ── Web security headers (SEC L3) ─────────────────────────────────────────

    fn assert_web_security_headers(resp: &axum::response::Response) {
        let headers = resp.headers();
        assert_eq!(
            headers
                .get(header::CONTENT_SECURITY_POLICY)
                .and_then(|v| v.to_str().ok()),
            Some(WEB_CSP),
            "CSP must be set"
        );
        assert_eq!(
            headers
                .get(header::X_FRAME_OPTIONS)
                .and_then(|v| v.to_str().ok()),
            Some("DENY"),
            "X-Frame-Options must be DENY"
        );
        assert_eq!(
            headers
                .get(header::REFERRER_POLICY)
                .and_then(|v| v.to_str().ok()),
            Some("no-referrer"),
            "Referrer-Policy must be no-referrer"
        );
    }

    #[tokio::test]
    async fn web_index_has_security_headers() {
        let resp = web_index().await;
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/html; charset=utf-8")
        );
        assert_web_security_headers(&resp);
    }

    #[tokio::test]
    async fn web_js_files_have_security_headers() {
        // Every embedded JS file keeps its own content-type + caching, but
        // also gets the shared security headers.
        let resp = web_common_js().await;
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/javascript")
        );
        assert_eq!(
            resp.headers()
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("public, max-age=300")
        );
        assert_web_security_headers(&resp);
        for resp in [
            web_index_js().await,
            web_search_js().await,
            web_settings_js().await,
        ] {
            assert_web_security_headers(&resp);
        }
    }

    // ── CSP inline-script hashes (SEC L-2) ───────────────────────────────

    /// Extract the body of every inline `<script>` block — the exact text
    /// `include_str!` serves. A `<script` tag counts as inline when its
    /// attribute list (up to the closing `>`) contains no `src=` attribute —
    /// so `<script>`, `<script type="module">`, `<script defer>` all count,
    /// while the shared JS tags (`<script src=…>`) are excluded.
    fn inline_scripts(html: &str) -> Vec<&str> {
        let mut out = Vec::new();
        let mut rest = html;
        while let Some(start) = rest.find("<script") {
            let tag_start = start + "<script".len();
            let tag_end = match rest[tag_start..].find('>') {
                Some(e) => tag_start + e,
                None => break, // unterminated tag — nothing to hash
            };
            if attrs_contains_src(&rest[tag_start..tag_end]) {
                // External script — skip past the tag (its body is empty).
                rest = &rest[tag_end + 1..];
                continue;
            }
            let after = &rest[tag_end + 1..];
            match after.find("</script>") {
                Some(end) => {
                    out.push(&after[..end]);
                    rest = &after[end..];
                }
                None => break,
            }
        }
        out
    }

    /// True when the tag's attribute text carries a `src=` attribute —
    /// `src=` at the start of the text, or preceded by a non-word (i.e. not
    /// `[A-Za-z0-9_]`) character (the `\bsrc=` half of the regex the old
    /// node-side gates used).
    fn attrs_contains_src(attrs: &str) -> bool {
        let b = attrs.as_bytes();
        let mut i = 0;
        while i + 4 <= b.len() {
            if &b[i..i + 4] == b"src="
                && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'))
            {
                return true;
            }
            i += 1;
        }
        false
    }

    // SEC L-2: the pages carry no inline <script> blocks — all scripts are
    // external embedded files allowed by `script-src 'self'`. This is the
    // Rust-level guard (runs without node) that keeps the CSP hash-free.
    #[test]
    fn pages_have_no_inline_scripts() {
        let pages = [
            (include_str!("../../../web/index.html"), "/index.js"),
            (include_str!("../../../web/search.html"), "/search.js"),
            (include_str!("../../../web/settings.html"), "/settings.js"),
        ];
        for (page, page_js) in pages {
            assert!(
                inline_scripts(page).is_empty(),
                "page has an inline <script> block — extract it to a web/*.js file"
            );
            assert!(
                page.contains("<script src=\"/common.js\"></script>"),
                "page lost its common.js tag"
            );
            assert!(
                page.contains(&format!("<script src=\"{page_js}\"></script>")),
                "page lost its {page_js} tag"
            );
        }
    }

    #[test]
    fn inline_scripts_matches_attributed_tags() {
        // Any `<script …>` tag whose attribute list has no src= counts —
        // including attributed ones (`type=`, `defer=`); a `src=` excludes it.
        // Quirk shared with the old node regex: `data-src=` contains a
        // `\bsrc=` match (the `-` is a word boundary), so it is excluded too.
        let html = "<script>one</script>\
                    <script type=\"module\">two</script>\
                    <script src=\"/common.js\"></script>\
                    <script data-src=\"/x.js\">three</script>\
                    <script defer>four</script>";
        assert_eq!(inline_scripts(html), vec!["one", "two", "four"]);
    }

    #[test]
    fn web_csp_has_no_unsafe_inline_for_scripts() {
        // The point of fully external scripts: script-src must not fall back
        // to 'unsafe-inline' and needs no sha256- hashes (style-src may keep
        // it — styles can't execute).
        let script_src = WEB_CSP
            .split(';')
            .find(|d| d.trim_start().starts_with("script-src"))
            .expect("script-src directive");
        assert!(!script_src.contains("'unsafe-inline'"));
        assert!(!script_src.contains("sha256-"));
        assert!(script_src.contains("'self'"));
    }

    // ── Raw-content sandbox headers (SEC-M2) ──────────────────────────────────

    #[test]
    fn raw_content_security_headers_html() {
        let h = raw_content_security_headers("text/html; charset=utf-8");
        assert_eq!(h.len(), 2);
        assert_eq!(
            h[0],
            (header::CONTENT_SECURITY_POLICY, RAW_CONTENT_CSP.to_string())
        );
        assert_eq!(h[1], (header::X_FRAME_OPTIONS, "DENY".to_string()));
    }

    #[test]
    fn raw_content_security_headers_non_html() {
        for ct in [
            "text/plain; charset=utf-8",
            "application/octet-stream",
            "image/png",
        ] {
            assert!(
                raw_content_security_headers(ct).is_empty(),
                "{ct} must carry no sandbox headers"
            );
        }
    }

    // SEC-M1: non-`text/html` MIME types that browsers render as HTML must
    // also carry the sandbox CSP + X-Frame-Options.
    #[test]
    fn raw_content_security_headers_xhtml() {
        let h = raw_content_security_headers("application/xhtml+xml");
        assert_eq!(h.len(), 2);
        assert_eq!(
            h[0],
            (header::CONTENT_SECURITY_POLICY, RAW_CONTENT_CSP.to_string())
        );
        assert_eq!(h[1], (header::X_FRAME_OPTIONS, "DENY".to_string()));
    }

    #[test]
    fn raw_content_security_headers_svg() {
        let h = raw_content_security_headers("image/svg+xml");
        assert_eq!(h.len(), 2);
        assert_eq!(
            h[0],
            (header::CONTENT_SECURITY_POLICY, RAW_CONTENT_CSP.to_string())
        );
        assert_eq!(h[1], (header::X_FRAME_OPTIONS, "DENY".to_string()));
    }

    #[test]
    fn raw_content_security_headers_xhtml_with_charset() {
        let h = raw_content_security_headers("application/xhtml+xml; charset=utf-8");
        assert_eq!(h.len(), 2);
    }
}
