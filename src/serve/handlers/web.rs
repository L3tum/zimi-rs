//! Embedded Web UI handlers: the three `include_str!` HTML pages and the
//! shared `common.js`, each carrying the SEC L3 security headers.
use axum::http::header;
use axum::response::Response;

/// Shared security headers for the embedded Web UI (SEC L3): refuse framing
/// (`X-Frame-Options: DENY`) and referrer leakage (`Referrer-Policy:
/// no-referrer`), and a baseline CSP. The pages use inline scripts/styles, so
/// `script-src` / `style-src` require `'unsafe-inline'` (web-check proves the
/// inline scripts exist in all 3 pages).
fn web_security_headers() -> Vec<(header::HeaderName, header::HeaderValue)> {
    vec![
        (
            header::CONTENT_SECURITY_POLICY,
            header::HeaderValue::from_static(
                "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'",
            ),
        ),
        (header::X_FRAME_OPTIONS, header::HeaderValue::from_static("DENY")),
        (header::REFERRER_POLICY, header::HeaderValue::from_static("no-referrer")),
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
/// render as an HTML document (see [`is_browser_html`]); empty for all other
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

pub async fn web_index() -> Response {
    web_html_response(include_str!("../../../web/index.html"))
}

pub async fn web_search() -> Response {
    web_html_response(include_str!("../../../web/search.html"))
}

pub async fn web_settings() -> Response {
    web_html_response(include_str!("../../../web/settings.html"))
}

pub async fn web_common_js() -> axum::response::Response {
    let mut resp = axum::response::Response::new(axum::body::Body::from(include_str!(
        "../../../web/common.js"
    )));
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // ── Web security headers (SEC L3) ─────────────────────────────────────────

    fn assert_web_security_headers(resp: &axum::response::Response) {
        let headers = resp.headers();
        assert_eq!(
            headers.get(header::CONTENT_SECURITY_POLICY).and_then(|v| v.to_str().ok()),
            Some("default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'"),
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
    async fn web_common_js_has_security_headers() {
        let resp = web_common_js().await;
        // The JS file keeps its own content-type + caching, but also gets the
        // shared security headers.
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
