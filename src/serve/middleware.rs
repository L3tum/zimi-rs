//! HTTP middleware: shared-password auth, `access_token` sanitisation, and
//! the global rate-limiting middleware.
//!
//! `auth_required` gates mutating endpoints (and, behind a flag, reads);
//! `is_authorized` matches a Bearer header and/or `access_token` query param
//! in constant time; `sanitize_uri_for_logs` strips the token so it never
//! lands in logs; `rate_limit` enforces the global token-bucket limiter from
//! `crate::access::ratelimit` (exempting the `/health` probe). The policy
//! objects themselves — the limiter, the per-IP auth-failure lockout, and the
//! trusted-proxy CIDR / `X-Forwarded-For` client-IP resolution — live in
//! `crate::access` (M-B); this module only *applies* them as HTTP middleware
//! and request gates.
use axum::extract::{FromRequestParts, State};
use axum::http::{header, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};

use crate::access::cidr::client_ip_from_xff;
use crate::access::ratelimit::ms_to_retry_after_secs;
use crate::settings::{KEY_ACCESS_ADMIN_PASSWORD, KEY_GENERAL_TRUSTED_PROXY_CIDRS};
use crate::AppState;

/// Simple shared-password auth.
///
/// Enforced only when `access.mode == "password"` and a non-empty
/// `access.admin_password` is set. In that mode, **mutating** requests
/// (POST/PUT/PATCH/DELETE) must present the password as either
/// `Authorization: Bearer <password>` or `?access_token=<password>`.
///
/// GET requests and the static web UI stay open so pages can load; this is
/// intentionally a shared-token scheme, not per-user auth (see the access
/// section of the settings UI).
pub async fn auth_middleware(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, Response> {
    let mode = state.settings.access_mode();
    let password = state
        .settings
        .get_typed::<String>(KEY_ACCESS_ADMIN_PASSWORD)
        .unwrap_or_default();

    // BUG-2: `/health` (and `/health/…`) is exempt from the fail-closed 503
    // below — a misconfigured-auth 503 would mask the DB-down signal the
    // probe exists to report (mirrors the rate-limit exemption in
    // `rate_limit` below, via the shared `is_health_path`). Every other
    // route keeps today's behavior exactly.
    let path = request.uri().path();
    let is_health = is_health_path(path);

    if !is_health && mode == crate::settings::ACCESS_MODE_PASSWORD && password.is_empty() {
        // Password mode requested but none configured — fail CLOSED. (The
        // startup check already rejects this in the initial config; this
        // covers a runtime flip in the settings table. Failing open here
        // would silently disable auth for an operator who believes it's on.)
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "authentication is misconfigured",
                "hint": "access.mode is \"password\" but access.admin_password is empty"
            })),
        )
            .into_response());
    }

    if !auth_required(
        request.method().as_str(),
        &mode,
        &password,
        state.settings.require_auth_for_reads(),
    ) {
        return Ok(next.run(request).await);
    }

    // Per-source-IP lockout gate (SEC-M1). `ConnectInfo` is injected into the
    // request extensions by `into_make_service_with_connect_info` (see
    // main.rs); oneshot router tests build the service without it, so the
    // lookup is `None` there and the lockout is a no-op (HTTP-level lockout is
    // covered by WI-50's real-listener test).
    let ip = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip());
    // WI-14: when `general.trusted_proxy_cidrs` is set and the direct
    // connect IP is within a trusted CIDR, resolve the real client IP from
    // `X-Forwarded-For` for the lockout key (trusted-chain algorithm — see
    // `client_ip_from_xff`). The header is read first and the settings
    // lookup (cache read + `String` clone) happens only when the header is
    // present — it is the expensive part, and the header is absent on most
    // requests.
    let ip = ip.map(|ip| {
        let xff = request
            .headers()
            .get("x-forwarded-for")
            .and_then(|h| h.to_str().ok());
        match xff {
            Some(x) => {
                let cidrs_raw = state
                    .settings
                    .get_typed::<String>(KEY_GENERAL_TRUSTED_PROXY_CIDRS)
                    .unwrap_or_default();
                client_ip_from_xff(&cidrs_raw, Some(x), ip)
            }
            // `client_ip_from_xff(_cidrs, None, ip) == ip` for every input:
            // no XFF value means no entries to walk, so the trusted-peer
            // path falls back to the peer IP (and the untrusted-peer path
            // returns it outright — see the `client_ip_from_xff` docs and
            // its `xff_trusted_peer_empty_header_falls_back_to_peer` test).
            // A missing header therefore skips the settings lookup with no
            // behavior change.
            None => ip,
        }
    });
    let locked_remaining = ip.and_then(|ip| state.auth_lockout.lockout_remaining(&ip));
    if let Some(rem) = locked_remaining {
        let secs = rem.as_secs_f64().ceil().clamp(1.0, f64::MAX) as u64;
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            [(axum::http::header::RETRY_AFTER, secs.to_string())],
            Json(serde_json::json!({
                "error": "too many failed authentication attempts",
                "hint": "locked out after repeated failures; try again later"
            })),
        )
            .into_response());
    }

    let authorization = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let query = request.uri().query();
    // SEC-M3: the `?access_token=` query-string form is accepted **only on
    // read-only requests** (GET/HEAD/OPTIONS). Mutating verbs fall through to
    // Bearer-only — a token in the query string leaks via server-side logs,
    // proxies, and `Referer` headers, so it must not be usable to mutate.
    let q = if is_read_verb(request.method().as_str()) {
        query_token(query)
    } else {
        None
    };
    let presented = bearer_token(authorization).or(q.as_deref());
    let ok = match presented.filter(|t| !t.is_empty()) {
        Some(t) => state.settings.auth().verify_bg(t).await,
        None => false,
    };

    if ok {
        if let Some(ip) = &ip {
            state.auth_lockout.record_success(ip);
        }
        // Transparent upgrade: a legacy plaintext value that just verified is
        // re-stored as a salted hash. The middleware reads the password from
        // the cache, so `SettingsAuth::upgrade` updates cache + DB together —
        // without the cache update, every authenticated request would re-run
        // the 100k-iteration verify + UPDATE until restart. (upgrade also
        // invalidates the token cache, so this legacy value is not
        // re-verified as plaintext after the upgrade.)
        if crate::settings::is_legacy_password(&password) {
            let hashed = crate::settings::hash_admin_password(&password);
            state.settings.auth().upgrade(hashed).await;
        }
        return Ok(next.run(request).await);
    }

    if let Some(ip) = &ip {
        state.auth_lockout.record_failure(ip);
    }
    // SEC-M3: the 401 hint only advertises the query-string form on read
    // verbs; mutating verbs must use the Bearer header.
    let hint = if is_read_verb(request.method().as_str()) {
        "send Authorization: Bearer <password> or ?access_token=<password>"
    } else {
        "send Authorization: Bearer <password>"
    };
    Err((
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "error": "authentication required",
            "hint": hint,
        })),
    )
        .into_response())
}

// ─── Pure auth logic (unit-tested) ────────────────────────────────────────────

/// Whether a path is the `/health` probe (or a future `/health/…` subroute).
///
/// Shared by the auth middleware's fail-closed 503 exemption and the
/// rate-limiter's probe exemption so the two can never drift (BUG-2: a
/// misconfigured-auth 503 would mask the DB-down signal the probe exists to
/// report). Matching `/health/…` too is defensive against future subroutes;
/// none exist today.
pub(crate) fn is_health_path(path: &str) -> bool {
    path == "/health" || path.starts_with("/health/")
}

/// Axum middleware: enforces the global rate limit on all routes except
/// `/health` (and any future `/health/…` subroute — load-balancer probes must
/// never be throttled). The health-path predicate is shared with the auth
/// middleware so the two exemptions can't drift.
pub async fn rate_limit(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: axum::extract::Request,
    next: Next,
) -> Response {
    if is_health_path(req.uri().path()) {
        return next.run(req).await;
    }

    let limiter = state.rate_limiter.limiter(&state.settings);
    match limiter.try_acquire() {
        Ok(()) => next.run(req).await,
        Err(retry_after_ms) => {
            let retry_after_secs = ms_to_retry_after_secs(retry_after_ms);
            (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, retry_after_secs.to_string())],
                Json(serde_json::json!({ "error": "rate limit exceeded" })),
            )
                .into_response()
        }
    }
}

/// Whether an HTTP method is read-only. SEC-M3: the `?access_token=`
/// query-string token form is only honored on these verbs; mutating verbs
/// must use the `Authorization: Bearer` header.
fn is_read_verb(method: &str) -> bool {
    matches!(method, "GET" | "HEAD" | "OPTIONS")
}

/// Whether a request must present the shared password.
///
/// Only when `access.mode == "password"` and a non-empty password is
/// configured. By default only **mutating** requests (POST/PUT/PATCH/DELETE)
/// are gated; GET/HEAD/OPTIONS stay open so the web UI can load.
///
/// When `require_reads` is true (the `access.require_auth_for_reads` option,
/// M1), reads are gated too — the operator has accepted that the web UI's
/// plain-anchor article links (`/w/...`) will 401 until they're refetched via
/// the token-attached `apiFetch`. The option's *startup* default is
/// bind-based (M-1): non-loopback binds start with it `true`, loopback keeps
/// it `false`. See the README threat-model section.
fn auth_required(
    method: &str,
    access_mode: &str,
    admin_password: &str,
    require_reads: bool,
) -> bool {
    access_mode == crate::settings::ACCESS_MODE_PASSWORD
        && !admin_password.is_empty()
        && (require_reads || !matches!(method, "GET" | "HEAD" | "OPTIONS"))
}

/// Extract the token from an `Authorization: Bearer <token>` header.
/// The scheme is matched case-insensitively (RFC 7235); the token is taken
/// verbatim after the single separating space (BUG-17), so a password
/// containing leading/trailing whitespace can be presented. RFC 7235 OWS is
/// intentionally **not** stripped (the `query_token` path also does not trim,
/// so both channels agree on exact-token semantics).
fn bearer_token(authorization: Option<&str>) -> Option<&str> {
    let (scheme, token) = authorization?.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then_some(token)
}

/// Strip `access_token` query params from a URI for safe logging.
/// Decode-aware (BUG-13): a pair is dropped when its **decoded** key is
/// `access_token` (catches `?%61ccess_token=…`), and a pair is masked when its
/// **decoded** value embeds an `access_token=` pair (catches `?x=%26access_token=…`).
/// The query is re-encoded only when something was stripped, so clean URIs are
/// logged verbatim.
pub(crate) fn sanitize_uri_for_logs(uri: &axum::http::Uri) -> String {
    let path = uri.path().to_string();
    let Some(query) = uri.query() else {
        return path;
    };
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    let mut stripped = false;
    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        if k == "access_token" {
            // ?%61ccess_token=… (encoded key) or the plain form.
            stripped = true;
            continue;
        }
        if v.contains("access_token=") {
            // ?x=%26access_token=… (token smuggled inside another value).
            stripped = true;
            serializer.append_pair(k.as_ref(), "[redacted]");
            continue;
        }
        serializer.append_pair(k.as_ref(), v.as_ref());
    }
    if !stripped {
        return format!("{}?{}", path, query);
    }
    let new_q = serializer.finish();
    if new_q.is_empty() {
        path
    } else {
        format!("{}?{}", path, new_q)
    }
}

/// Extract and decode the `access_token` query parameter (first occurrence wins).
pub(crate) fn query_token(query: Option<&str>) -> Option<String> {
    query.and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes())
            .find(|(k, v)| k == "access_token" && !v.is_empty())
            .map(|(_, v)| v.into_owned())
    })
}

/// Whether the presented credentials (Bearer header and/or query param) match
/// the expected password. Empty tokens never match.
///
/// Uses `verify_admin_password` so both legacy plaintext and `sha2:`-hashed
/// stored values are accepted; the transparent upgrade happens in
/// `auth_middleware`, not here (this is also called by non-middleware paths
/// that have no settings handle for the upgrade).
///
/// Kept as a pure helper for unit tests (production paths use
/// [`settings_authed`], the cached variant).
#[cfg(test)]
pub(crate) fn is_authorized(
    authorization: Option<&str>,
    query: Option<&str>,
    expected: &str,
) -> bool {
    let q = query_token(query);
    bearer_token(authorization)
        .or(q.as_deref())
        .is_some_and(|t| !t.is_empty() && crate::settings::verify_admin_password(expected, t))
}

/// Single shared auth decision for the settings handlers (M3): whether the
/// request presents a token that verifies against the stored admin password.
///
/// This is the **one** place the 100k-iteration hash may run — at most once
/// per `TOKEN_CACHE_TTL` (60s) per distinct token, via the settings token
/// cache. The cache is invalidated on `reload()` (the realistic password
/// change path — `access.admin_password` is API-immutable) and on
/// `update()`/`SettingsAuth::upgrade()` (defense in depth).
pub(crate) async fn settings_authed(
    state: &AppState,
    method: &str,
    authorization: Option<&str>,
    query: Option<&str>,
) -> bool {
    let mode = state.settings.access_mode();
    if mode != crate::settings::ACCESS_MODE_PASSWORD {
        return false;
    }
    // SEC-M3: query-string tokens only on read verbs; mutating verbs are
    // Bearer-only here too (mirroring the middleware gate).
    let q = if is_read_verb(method) {
        query_token(query)
    } else {
        None
    };
    match bearer_token(authorization)
        .or(q.as_deref())
        .filter(|t| !t.is_empty())
    {
        Some(t) => state.settings.auth().verify_bg(t).await,
        None => false,
    }
}

/// One-time, per-request auth verdict for handlers (ARCH-H2).
///
/// Previously each auth-aware handler hand-assembled the
/// `Authorization`/query-token plumbing and re-invoked `settings_authed` —
/// a drift trap: a new handler that redacts or gates on auth had to replicate
/// the exact call, or it would silently leak topology / weaken a gate. With
/// the extractor, "this handler is auth-aware" is a type-level property and
/// the verdict logic stays centralized in `settings_authed`.
///
/// The extractor only runs when a handler asks for it (axum extractors are
/// per-handler), and in open mode it returns `authenticated = false` without
/// touching the token cache. It complements — does not replace —
/// [`auth_middleware`], which owns the request *gate* (lockout, fail-closed
/// 503, legacy upgrade).
#[derive(Clone, Copy, Default)]
pub struct AuthContext {
    /// True when the request presents a token that verifies against the
    /// stored admin password (always false in open mode).
    pub authenticated: bool,
}

impl FromRequestParts<AppState> for AuthContext {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let authorization = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        let authenticated = settings_authed(
            state,
            parts.method.as_str(),
            authorization,
            parts.uri.query(),
        )
        .await;
        Ok(AuthContext { authenticated })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── sanitize_uri_for_logs ────────────────────────────────────────────

    #[test]
    fn sanitize_strips_access_token_keeps_other_params() {
        let uri: axum::http::Uri = "/search?q=x&access_token=abc".parse().unwrap();
        assert_eq!(sanitize_uri_for_logs(&uri), "/search?q=x");
    }

    #[test]
    fn sanitize_only_param_no_dangling_query() {
        let uri: axum::http::Uri = "/w/foo/bar.zim?access_token=only".parse().unwrap();
        assert_eq!(sanitize_uri_for_logs(&uri), "/w/foo/bar.zim");
    }

    #[test]
    fn sanitize_no_query_unchanged() {
        let uri: axum::http::Uri = "/w/foo/bar.zim".parse().unwrap();
        assert_eq!(sanitize_uri_for_logs(&uri), "/w/foo/bar.zim");
    }

    #[test]
    fn sanitize_no_token_param_unchanged() {
        let uri: axum::http::Uri = "/search?q=hello&limit=10".parse().unwrap();
        assert_eq!(sanitize_uri_for_logs(&uri), "/search?q=hello&limit=10");
    }

    #[test]
    fn sanitize_token_at_end() {
        let uri: axum::http::Uri = "/search?limit=5&access_token=secret".parse().unwrap();
        assert_eq!(sanitize_uri_for_logs(&uri), "/search?limit=5");
    }

    #[test]
    fn sanitize_token_middle_of_query() {
        let uri: axum::http::Uri = "/search?a=1&access_token=x&b=2".parse().unwrap();
        assert_eq!(sanitize_uri_for_logs(&uri), "/search?a=1&b=2");
    }

    #[test]
    fn sanitize_encoded_key_percent61() {
        // BUG-13: `?%61ccess_token=SECRET` decodes to key `access_token` —
        // the old literal-substring check let it through.
        let uri: axum::http::Uri = "/search?%61ccess_token=SECRET".parse().unwrap();
        let out = sanitize_uri_for_logs(&uri);
        assert_eq!(out, "/search");
    }

    #[test]
    fn sanitize_encoded_value_smuggled_token() {
        // BUG-13: `?x=%26access_token=SECRET` hides the token inside another
        // value's decoded form — the value must be masked (a raw-substring
        // split would leave `x=%26` and drop nothing).
        let uri: axum::http::Uri = "/search?x=%26access_token=SECRET".parse().unwrap();
        let out = sanitize_uri_for_logs(&uri);
        assert!(!out.contains("SECRET"));
        assert!(out.contains("x=%5Bredacted%5D"));
    }

    #[test]
    fn sanitize_repeated_access_token_params() {
        let uri: axum::http::Uri = "/search?access_token=a&access_token=b".parse().unwrap();
        assert_eq!(sanitize_uri_for_logs(&uri), "/search");
    }

    // ── auth_required ──────────────────────────────────────────────────────

    #[test]
    fn auth_required_password_mode_mutating() {
        for m in ["POST", "PUT", "PATCH", "DELETE", "post", "delete"] {
            assert!(
                auth_required(m, "password", "secret", false),
                "{m} should require auth"
            );
        }
    }

    #[test]
    fn auth_required_reads_stay_open() {
        for m in ["GET", "HEAD", "OPTIONS"] {
            assert!(
                !auth_required(m, "password", "secret", false),
                "{m} must stay open when require_reads is off"
            );
        }
    }

    #[test]
    fn auth_required_open_mode_never() {
        assert!(!auth_required("POST", "open", "secret", false));
        assert!(!auth_required("PUT", "anything-else", "secret", false));
        // Even with the flag on, open mode never requires auth.
        assert!(!auth_required("GET", "open", "secret", true));
    }

    // ── is_health_path (WP2.3: shared probe exemption) ───────────────────

    #[test]
    fn is_health_path_matches_health_and_subpaths() {
        // Exact probe + any subroute (defensive; none exist today).
        assert!(is_health_path("/health"));
        assert!(is_health_path("/health/"));
        assert!(is_health_path("/health/x"));
        // Must NOT match look-alike prefixes.
        assert!(!is_health_path("/healthx"));
        assert!(!is_health_path("/health2"));
        assert!(!is_health_path("/x/health"));
        assert!(!is_health_path("/"));
    }

    // ── is_read_verb (SEC-M3) ─────────────────────────────────────────────

    #[test]
    fn is_read_verb_matrix() {
        for m in ["GET", "HEAD", "OPTIONS"] {
            assert!(is_read_verb(m), "{m} is read-only");
        }
        for m in ["POST", "PUT", "PATCH", "DELETE"] {
            assert!(!is_read_verb(m), "{m} is mutating");
        }
    }

    #[test]
    fn auth_required_empty_password_never() {
        assert!(!auth_required("POST", "password", "", false));
        assert!(!auth_required("GET", "password", "", true));
    }

    #[test]
    fn auth_required_reads_gated_when_flag_on() {
        // M1: require_reads=true gates read verbs in password mode.
        for m in ["GET", "HEAD", "OPTIONS"] {
            assert!(
                auth_required(m, "password", "secret", true),
                "{m} should require auth when require_reads is on"
            );
        }
    }

    #[test]
    fn auth_required_flag_on_mutating_still_true() {
        // Flag on must not weaken mutating-verb gating.
        for m in ["POST", "PUT", "PATCH", "DELETE"] {
            assert!(
                auth_required(m, "password", "secret", true),
                "{m} should still require auth"
            );
        }
    }

    // ── bearer_token ───────────────────────────────────────────────────────

    #[test]
    fn bearer_basic() {
        assert_eq!(bearer_token(Some("Bearer abc123")), Some("abc123"));
        assert_eq!(bearer_token(Some("bearer abc123")), Some("abc123"));
        assert_eq!(bearer_token(Some("BEARER abc123")), Some("abc123"));
        assert_eq!(bearer_token(Some("Bearer")), None); // no space/token
        assert_eq!(bearer_token(Some("Basic abc123")), None);
        assert_eq!(bearer_token(None), None);
        assert_eq!(bearer_token(Some("")), None);
    }

    #[test]
    fn bearer_token_with_spaces_in_password() {
        // split_once on the first space: scheme "Bearer", rest is the token,
        // taken verbatim (BUG-17: no trim — RFC 7235 OWS is not stripped).
        assert_eq!(bearer_token(Some("Bearer my secret")), Some("my secret"));
        // Two spaces after the scheme → one leading space preserved in the token.
        assert_eq!(
            bearer_token(Some("Bearer  two-spaces")),
            Some(" two-spaces")
        );
    }

    #[test]
    fn bearer_token_leading_trailing_whitespace_preserved() {
        // BUG-17: a password with leading/trailing whitespace can be presented
        // and matched (no trim anywhere in the token path).
        assert_eq!(bearer_token(Some("Bearer  pw ")), Some(" pw "));
        assert!(is_authorized(Some("Bearer  pw"), None, " pw"));
        // A trailing-whitespace password matches only an identically-padded expected.
        assert!(!is_authorized(Some("Bearer  pw "), None, " pw"));
    }

    // ── query_token ────────────────────────────────────────────────────────

    #[test]
    fn query_token_extracts() {
        assert_eq!(query_token(Some("access_token=abc")), Some("abc".into()));
        assert_eq!(
            query_token(Some("a=1&access_token=abc&b=2")),
            Some("abc".into())
        );
        assert_eq!(query_token(Some("q=hello")), None);
        assert_eq!(query_token(None), None);
        assert_eq!(query_token(Some("")), None);
    }

    #[test]
    fn query_token_decodes() {
        assert_eq!(
            query_token(Some("access_token=hello%20world")),
            Some("hello world".into())
        );
        assert_eq!(query_token(Some("access_token=a+b")), Some("a b".into()));
    }

    #[test]
    fn query_token_first_occurrence_wins() {
        assert_eq!(
            query_token(Some("access_token=first&access_token=second")),
            Some("first".into())
        );
    }

    #[test]
    fn query_token_no_prefix_confusion() {
        assert_eq!(query_token(Some("xaccess_token=abc")), None);
        assert_eq!(query_token(Some("access_token")), None);
    }

    // ── is_authorized ──────────────────────────────────────────────────────

    #[test]
    fn authorized_via_bearer() {
        assert!(is_authorized(Some("Bearer s3cret"), None, "s3cret"));
        assert!(!is_authorized(Some("Bearer wrong"), None, "s3cret"));
        assert!(!is_authorized(None, None, "s3cret"));
    }

    #[test]
    fn authorized_via_query() {
        assert!(is_authorized(None, Some("access_token=s3cret"), "s3cret"));
        assert!(is_authorized(
            None,
            Some("access_token=s3%20cret"),
            "s3 cret"
        ));
        assert!(!is_authorized(None, Some("access_token=nope"), "s3cret"));
    }

    #[test]
    fn authorized_bearer_with_spaces_in_password() {
        assert!(is_authorized(Some("Bearer my secret"), None, "my secret"));
    }

    #[test]
    fn authorized_never_matches_empty() {
        // auth_required guards against empty passwords, but is_authorized
        // must not accidentally accept them either.
        assert!(!is_authorized(Some("Bearer "), None, ""));
        assert!(!is_authorized(None, Some("access_token="), ""));
    }
}
