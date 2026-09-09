//! HTTP middleware: shared-password auth and `access_token` sanitisation.
//!
//! `auth_required` gates mutating endpoints (and, behind a flag, reads);
//! `is_authorized` matches a Bearer header and/or `access_token` query param
//! in constant time; `sanitize_uri_for_logs` strips the token so it never
//! lands in logs.
use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use axum::extract::{FromRequestParts, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};

use crate::settings::{KEY_ACCESS_ADMIN_PASSWORD, KEY_GENERAL_TRUSTED_PROXY_CIDRS};
use crate::AppState;

/// Per-source-IP auth-failure lockout (SEC-M1). In-memory, capped, per
/// process. After [`Self::MAX_FAILURES`] failures within [`Self::WINDOW`], the
/// IP is locked out for the remainder of the window. A successful auth clears
/// the IP. The map is bounded to `MAX_TRACKED` distinct IPs; when the
/// cap is hit the entry with the oldest `first_failure` is evicted.
#[derive(Default)]
pub struct LockoutTracker {
    /// (failure count, first_failure instant) per IP.
    pub inner: std::sync::Mutex<HashMap<IpAddr, (u32, Instant)>>,
}

impl LockoutTracker {
    /// Failure count at which an IP is locked out.
    pub const MAX_FAILURES: u32 = 5;
    /// Lockout window; failures are counted from the first failure in it.
    pub const WINDOW: Duration = Duration::from_secs(15 * 60);
    /// Bound on distinct tracked IPs.
    const MAX_TRACKED: usize = 10_000;

    /// True if `ip` is currently locked out.
    pub fn is_locked_out(&self, ip: &IpAddr) -> bool {
        self.lockout_remaining(ip).is_some()
    }

    /// Remaining lockout time for `ip`, if any.
    ///
    /// PERF: no O(n) prune on this path — it runs on **every** auth-gated
    /// request, and a full `retain` over up to `MAX_TRACKED` entries
    /// under the process-wide mutex would serialize all concurrent requests
    /// (worst case: a scanner from many IPs). Expired entries are harmless
    /// here: the per-entry `first.elapsed()` filter below returns `None` for
    /// them, and the map stays bounded by `record_failure`'s cap eviction.
    pub fn lockout_remaining(&self, ip: &IpAddr) -> Option<Duration> {
        let m = self.inner.lock().expect("lockout mutex");
        m.get(ip)
            .filter(|(n, first)| *n >= Self::MAX_FAILURES && first.elapsed() < Self::WINDOW)
            .map(|(_, first)| Self::WINDOW - first.elapsed())
    }

    /// Record a failed auth for `ip`, keeping the first_failure instant of
    /// the current window. An entry whose window has fully elapsed restarts
    /// the count (no stale-window carry-over). Evicts the oldest
    /// first_failure at the cap — the map is bounded without any O(n) prune
    /// on the read path.
    pub fn record_failure(&self, ip: &IpAddr) {
        let mut m = self.inner.lock().expect("lockout mutex");
        let window_active = m
            .get(ip)
            .map(|(_, first)| first.elapsed() < Self::WINDOW)
            .unwrap_or(false);
        if window_active {
            if let Some((n, _)) = m.get_mut(ip) {
                *n += 1;
            }
            return;
        }
        // New IP (or expired window): start a fresh count.
        if m.len() >= Self::MAX_TRACKED {
            // Evict the entry with the oldest first_failure (expired entries
            // are the most likely to be oldest, so the cap self-cleans).
            if let Some(oldest) = m
                .iter()
                .min_by_key(|(_, (_, first))| *first)
                .map(|(k, _)| *k)
            {
                m.remove(&oldest);
            }
        }
        m.insert(*ip, (1, Instant::now()));
    }

    /// Clear any failure count / lockout for `ip` on a successful auth.
    pub fn record_success(&self, ip: &IpAddr) {
        self.inner.lock().expect("lockout mutex").remove(ip);
    }
}

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
    // ratelimit.rs). Every other route keeps today's behavior exactly.
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
    // `client_ip_from_xff`).
    let ip = ip.map(|ip| {
        let cidrs_raw = state
            .settings
            .get_typed::<String>(KEY_GENERAL_TRUSTED_PROXY_CIDRS)
            .unwrap_or_default();
        let xff = request
            .headers()
            .get("x-forwarded-for")
            .and_then(|h| h.to_str().ok());
        client_ip_from_xff(&cidrs_raw, xff, ip)
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
        Some(t) => state.settings.token_verify_cached_bg(t).await,
        None => false,
    };

    if ok {
        if let Some(ip) = &ip {
            state.auth_lockout.record_success(ip);
        }
        // Transparent upgrade: a legacy plaintext value that just verified is
        // re-stored as a salted hash. The middleware reads the password from
        // the cache, so upgrade_password updates cache + DB together — without
        // the cache update, every authenticated request would re-run the
        // 100k-iteration verify + UPDATE until restart. (upgrade_password
        // also invalidates the token cache, so this legacy value is not
        // re-verified as plaintext after the upgrade.)
        if crate::settings::is_legacy_password(&password) {
            let hashed = crate::settings::hash_admin_password(&password);
            state.settings.upgrade_password(hashed).await;
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

/// WI-14: check if `ip` falls within any CIDR in the comma-separated `cidrs`
/// string. Supports both IPv4 and IPv6 CIDRs (e.g. "10.0.0.0/8,192.168.1.0/24").
/// An empty string or unparseable CIDR is treated as "no match".
pub(crate) fn cidrs_contains(cidrs: &str, ip: &std::net::IpAddr) -> bool {
    cidrs.split(',').any(|s| {
        let s = s.trim();
        if s.is_empty() {
            return false;
        }
        // Plain IP address (no prefix) → exact match only.
        if !s.contains('/') {
            return s.parse::<std::net::IpAddr>() == Ok(*ip);
        }
        // CIDR match — shared entry parser (S1) so the request-time check and
        // the startup over-broad guard parse identically.
        let Some((net, prefix)) = parse_cidr_entry(s) else {
            return false;
        };
        match (net, *ip) {
            (std::net::IpAddr::V4(net_v4), std::net::IpAddr::V4(target)) => {
                prefix <= 32 && ip4_in_cidr(net_v4, target, prefix)
            }
            (std::net::IpAddr::V6(net_v6), std::net::IpAddr::V6(target)) => {
                prefix <= 128 && ip6_in_cidr(net_v6, target, prefix)
            }
            _ => false, // version mismatch
        }
    })
}

/// SEC-M3: parse one comma-separated entry of a `general.trusted_proxy_cidrs`
/// value into `(network address, prefix length)`. Shared by the request-time
/// containment check (`cidrs_contains`), the startup over-broad guard
/// (`has_over_broad_cidr`), and the startup /0 refusal
/// (`has_zero_prefix_cidr`) so the three can never drift apart (S1).
///
/// Returns `None` for an empty entry or a plain (prefix-less) IP literal.
pub fn parse_cidr_entry(s: &str) -> Option<(std::net::IpAddr, u32)> {
    let s = s.trim();
    if s.is_empty() || !s.contains('/') {
        return None;
    }
    let (addr_s, prefix_s) = s.rsplit_once('/').unwrap_or_default();
    let addr: std::net::IpAddr = addr_s.trim().parse().ok()?;
    let prefix: u32 = match addr {
        std::net::IpAddr::V4(_) => prefix_s.trim().parse().unwrap_or(32),
        std::net::IpAddr::V6(_) => prefix_s.trim().parse().unwrap_or(128),
    };
    Some((addr, prefix))
}

/// SEC-M3: true when any entry of a `general.trusted_proxy_cidrs` value is an
/// over-broad CIDR — ≥ /8 for IPv4 (a full class-A, 16.7M addresses) or ≥ /56
/// for IPv6 (the traditional allocation unit), including the literals
/// `0.0.0.0/0` and `::/0`. An over-broad list lets a distributed attacker
/// rotate `X-Forwarded-For` values and get a fresh lockout bucket per
/// attempt, defeating the per-IP auth-failure lockout. Checked once at
/// startup (warning); the /0 subclass is **refused** instead — see
/// [`has_zero_prefix_cidr`] (M-3).
pub fn has_over_broad_cidr(cidrs: &str) -> bool {
    if cidrs.trim().is_empty() {
        return false;
    }
    cidrs.split(',').any(|s| match parse_cidr_entry(s) {
        Some((std::net::IpAddr::V4(_), prefix)) => prefix <= 8,
        Some((std::net::IpAddr::V6(_), prefix)) => prefix <= 56,
        None => false,
    })
}

/// M-3: true when any entry of a `general.trusted_proxy_cidrs` value has
/// **prefix length 0** — any `x.x.x.x/0` or `X:X::/0`, including
/// `0.0.0.0/0` and `::/0`. The network address is irrelevant: the
/// request-time containment check (`ip4_in_cidr`/`ip6_in_cidr`) treats any
/// prefix-0 entry as match-all, so even `1.2.3.4/0` trusts *every*
/// `X-Forwarded-For` value unconditionally and a distributed attacker can
/// rotate the header to defeat the per-IP auth-failure lockout entirely.
/// **Refused at startup** (`cmd_serve` bails); the narrower over-broad class
/// still only warns (`has_over_broad_cidr`).
///
/// Shares [`parse_cidr_entry`] with the request-time check so the two can
/// never drift apart (S1).
pub fn has_zero_prefix_cidr(cidrs: &str) -> bool {
    if cidrs.trim().is_empty() {
        return false;
    }
    cidrs
        .split(',')
        .any(|s| matches!(parse_cidr_entry(s), Some((_, 0))))
}

/// WI-14: resolve the lockout client IP from the direct peer address and
/// the `X-Forwarded-For` header under a `general.trusted_proxy_cidrs` list
/// (standard trusted-proxy chain algorithm):
///
/// - peer **not** in the trusted CIDRs → `X-Forwarded-For` is ignored
///   entirely and the peer IP is the key: an untrusted hop can forge the
///   header freely, so only its own TCP address is trustworthy;
/// - peer **in** the trusted CIDRs → walk the XFF entries right-to-left,
///   skipping entries that fall inside the trusted CIDRs (they were
///   appended by trusted proxies); the first (rightmost) entry **not** in
///   the trusted set is the client key;
/// - every XFF entry inside the trusted set (the header is at least as
///   long as the trusted chain) → use the **leftmost** entry: it is the
///   value the first (outermost) trusted proxy saw, i.e. the last one a
///   proxy in our chain could not have appended on the attacker's behalf;
/// - absent or empty (nothing parseable) XFF with a trusted peer → fall
///   back to the peer IP.
///
/// Why rotation inside the trusted range no longer defeats the lockout: a
/// client behind a trusted proxy can only control the entries to its left
/// in the header — the proxy appends (or overwrites) the *real* peer
/// address. When that real peer address falls inside the trusted CIDRs
/// (attacker operating from inside the trusted range), the rightmost
/// attacker-chosen value is skipped as trusted and the key falls to the
/// next entry outside the trusted set, which the trusted proxy recorded
/// beyond the attacker's ability to forge. Rotating XFF values inside the
/// trusted range therefore no longer changes the key. (Residual: a
/// fully-trusted peer inside the CIDR can still influence the leftmost
/// entry — that is inherent to the operator's trust decision to list that
/// CIDR as trusted.)
pub(crate) fn client_ip_from_xff(
    cidrs: &str,
    xff: Option<&str>,
    peer: std::net::IpAddr,
) -> std::net::IpAddr {
    if !cidrs_contains(cidrs, &peer) {
        return peer;
    }
    let entries: Vec<std::net::IpAddr> = xff
        .map(|s| {
            s.split(',')
                .map(|t| t.trim())
                .filter_map(|t| t.parse::<std::net::IpAddr>().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if entries.is_empty() {
        return peer;
    }
    entries
        .iter()
        .rev()
        .find(|c| !cidrs_contains(cidrs, c))
        .copied()
        .unwrap_or_else(|| *entries.first().expect("entries non-empty (checked above)"))
}

fn ip4_in_cidr(net: std::net::Ipv4Addr, target: std::net::Ipv4Addr, prefix: u32) -> bool {
    let net_b = u32::from(net);
    let tgt_b = u32::from(target);
    if prefix == 0 {
        return true;
    }
    let mask = 0xFFFF_FFFF << (32 - prefix);
    (net_b & mask) == (tgt_b & mask)
}

fn ip6_in_cidr(net: std::net::Ipv6Addr, target: std::net::Ipv6Addr, prefix: u32) -> bool {
    let net_b = net.octets();
    let tgt_b = target.octets();
    let full_bytes = (prefix / 8) as usize;
    let rem_bits = prefix % 8;
    if full_bytes > 16 {
        return false;
    }
    if net_b[..full_bytes] != tgt_b[..full_bytes] {
        return false;
    }
    if rem_bits > 0 && full_bytes < 16 {
        let mask = 0xFFu8 << (8 - rem_bits);
        if (net_b[full_bytes] & mask) != (tgt_b[full_bytes] & mask) {
            return false;
        }
    }
    true
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
/// `update()`/`upgrade_password()` (defense in depth).
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
        Some(t) => state.settings.token_verify_cached_bg(t).await,
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
#[allow(clippy::unwrap_used)]
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

    // ── LockoutTracker (SEC-M1) ───────────────────────────────────────────

    fn ip(n: u32) -> IpAddr {
        std::net::Ipv4Addr::new(10, (n >> 16) as u8, (n >> 8) as u8, n as u8).into()
    }

    #[test]
    fn lockout_not_tripped_below_threshold() {
        let t = LockoutTracker::default();
        let a = ip(1);
        for _ in 0..LockoutTracker::MAX_FAILURES - 1 {
            t.record_failure(&a);
            assert!(!t.is_locked_out(&a), "below threshold must not lock");
        }
        t.record_failure(&a);
        assert!(t.is_locked_out(&a), "at threshold must lock");
    }

    #[test]
    fn lockout_success_resets() {
        let t = LockoutTracker::default();
        let a = ip(2);
        for _ in 0..LockoutTracker::MAX_FAILURES {
            t.record_failure(&a);
        }
        assert!(t.is_locked_out(&a));
        t.record_success(&a);
        assert!(!t.is_locked_out(&a), "success must clear the lockout");
        // A single failure after the reset is below threshold again.
        t.record_failure(&a);
        assert!(!t.is_locked_out(&a));
    }

    #[test]
    fn lockout_expires_after_window() {
        let t = LockoutTracker::default();
        let a = ip(3);
        // White-box seed: a past-due window must be treated as not locked.
        {
            let mut m = t.inner.lock().unwrap();
            m.insert(
                a,
                (
                    LockoutTracker::MAX_FAILURES,
                    Instant::now() - LockoutTracker::WINDOW - Duration::from_secs(1),
                ),
            );
        }
        assert!(t.lockout_remaining(&a).is_none());
        assert!(!t.is_locked_out(&a), "expired window must not lock");
    }

    #[test]
    fn lockout_cap_evicts_oldest() {
        let t = LockoutTracker::default();
        // Fill the tracker to the cap with distinct IPs, oldest first.
        for n in 0..LockoutTracker::MAX_TRACKED as u32 {
            t.record_failure(&ip(n));
        }
        assert_eq!(t.inner.lock().unwrap().len(), LockoutTracker::MAX_TRACKED);
        // A brand-new IP must evict the oldest (ip(0)).
        let newest = ip(u32::MAX);
        t.record_failure(&newest);
        let m = t.inner.lock().unwrap();
        assert_eq!(m.len(), LockoutTracker::MAX_TRACKED, "cap must hold");
        assert!(!m.contains_key(&ip(0)), "oldest first_failure evicted");
        assert!(m.contains_key(&newest), "newest entry present");
    }

    // ── WI-14: trusted proxy CIDR + XFF walk ──────────────────────────────

    fn cidrs_test(cidrs: &str, ip: &std::net::IpAddr) -> bool {
        super::cidrs_contains(cidrs, ip)
    }

    fn ipp(s: &str) -> std::net::IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn trusted_proxy_cidr_matching() {
        assert!(cidrs_test("10.0.0.0/8", &ipp("10.1.2.3")));
        assert!(cidrs_test("10.0.0.0/8", &ipp("10.255.255.255")));
        assert!(!cidrs_test("10.0.0.0/8", &ipp("11.0.0.1")));
        assert!(cidrs_test("192.168.1.0/24", &ipp("192.168.1.100")));
        assert!(!cidrs_test("192.168.1.0/24", &ipp("192.168.2.1")));
        // Multiple CIDRs
        assert!(cidrs_test("10.0.0.0/8,172.16.0.0/12", &ipp("172.20.1.1")));
        // Plain IP (no prefix) = exact match
        assert!(cidrs_test("10.0.0.1", &ipp("10.0.0.1")));
        assert!(!cidrs_test("10.0.0.1", &ipp("10.0.0.2")));
        // Empty
        assert!(!cidrs_test("", &ipp("10.0.0.1")));
        assert!(!cidrs_test("  ", &ipp("10.0.0.1")));
        // /0 = everything
        assert!(cidrs_test("0.0.0.0/0", &ipp("1.2.3.4")));
        // IPv6
        assert!(cidrs_test("::1/128", &ipp("::1")));
        assert!(!cidrs_test("::1/128", &ipp("::2")));
    }

    #[test]
    fn zero_prefix_cidr_refused() {
        // M-3: ANY prefix-0 CIDR is refused at startup — the containment
        // check treats any /0 as match-all regardless of the network
        // address, so even `1.2.3.4/0` would trust every X-Forwarded-For
        // value. The over-broad class (still warn-only) and normal proxy
        // ranges must not trigger the refusal.
        assert!(has_zero_prefix_cidr("0.0.0.0/0"));
        assert!(has_zero_prefix_cidr("::/0"));
        assert!(has_zero_prefix_cidr("10.0.0.0/8, 0.0.0.0/0"));
        assert!(has_zero_prefix_cidr("  ::/0  "));
        // Non-zero network address does not matter: any /0 is match-all.
        assert!(has_zero_prefix_cidr("1.2.3.4/0"));
        assert!(has_zero_prefix_cidr("::1/0"));
        assert!(has_zero_prefix_cidr("10.0.0.0/8, 1.2.3.4/0"));
        // Normal (and over-broad-but-not-/0) ranges: not refused.
        assert!(!has_zero_prefix_cidr("10.0.0.0/8"));
        assert!(!has_zero_prefix_cidr("192.168.1.0/24"));
        assert!(!has_zero_prefix_cidr("::1/128"));
        assert!(!has_zero_prefix_cidr("10.0.0.0/8,192.168.1.0/24"));
        // IPv6-mapped-style narrow CIDR: not refused.
        assert!(!has_zero_prefix_cidr("::ffff:a9fe/120"));
        // Empty / unset.
        assert!(!has_zero_prefix_cidr(""));
        assert!(!has_zero_prefix_cidr("  "));
    }

    #[test]
    fn xff_trusted_peer_walks_chain_to_first_untrusted() {
        // Trusted peer + XFF = "8.8.8.8, 203.0.113.7" with 203.0.113.0/24
        // trusted: the rightmost entry (203.0.113.7, appended by a trusted
        // proxy) is skipped, so the client key is 8.8.8.8.
        let cidrs = "203.0.113.0/24";
        assert_eq!(
            client_ip_from_xff(cidrs, Some("8.8.8.8, 203.0.113.7"), ipp("203.0.113.5")),
            ipp("8.8.8.8")
        );
        // Multi-hop chain: 3 proxy hops, skip all trusted, key = leftmost
        // real client.
        let cidrs = "10.0.0.0/8";
        assert_eq!(
            client_ip_from_xff(cidrs, Some("1.2.3.4, 10.0.0.1, 10.0.0.2"), ipp("10.0.0.3")),
            ipp("1.2.3.4")
        );
    }

    #[test]
    fn xff_all_trusted_uses_leftmost() {
        // Every XFF entry is trusted (header at least as long as the
        // trusted chain) → the leftmost entry is the key, not the peer IP.
        let cidrs = "10.0.0.0/8";
        assert_eq!(
            client_ip_from_xff(cidrs, Some("10.0.0.1, 10.0.0.2"), ipp("10.0.0.3")),
            ipp("10.0.0.1")
        );
        // Single all-trusted entry → leftmost == that entry.
        assert_eq!(
            client_ip_from_xff(cidrs, Some("10.0.0.9"), ipp("10.0.0.3")),
            ipp("10.0.0.9")
        );
    }

    #[test]
    fn xff_untrusted_peer_ignores_header() {
        // Peer outside the trusted CIDRs: the attacker can forge XFF
        // freely, so the header is ignored entirely and the key is the
        // peer's own TCP address.
        let cidrs = "10.0.0.0/8";
        assert_eq!(
            client_ip_from_xff(cidrs, Some("8.8.8.8, 1.1.1.1"), ipp("203.0.113.50")),
            ipp("203.0.113.50")
        );
        // No trusted CIDRs configured at all → always the peer IP.
        assert_eq!(
            client_ip_from_xff("", Some("8.8.8.8"), ipp("203.0.113.50")),
            ipp("203.0.113.50")
        );
    }

    #[test]
    fn xff_trusted_peer_empty_header_falls_back_to_peer() {
        // Trusted peer with absent or empty (unparseable) XFF → fall back
        // to the peer IP.
        let cidrs = "10.0.0.0/8";
        assert_eq!(
            client_ip_from_xff(cidrs, None, ipp("10.0.0.2")),
            ipp("10.0.0.2")
        );
        assert_eq!(
            client_ip_from_xff(cidrs, Some("  "), ipp("10.0.0.2")),
            ipp("10.0.0.2")
        );
        assert_eq!(
            client_ip_from_xff(cidrs, Some("not-an-ip"), ipp("10.0.0.2")),
            ipp("10.0.0.2")
        );
    }

    #[test]
    fn xff_attacker_rotation_inside_trusted_range_no_longer_rotates_key() {
        // An attacker operating from *inside* the trusted range (true peer
        // IP in the CIDR, e.g. a compromised internal host) sits behind a
        // trusted proxy that appends the real peer address to XFF. Under
        // the old rightmost-value logic, the attacker rotated the
        // rightmost (attacker-chosen, inside-trusted-range) entry to get a
        // fresh lockout bucket per attempt. Under the trusted-chain
        // algorithm, trusted-range entries are skipped, so the key falls
        // to the next entry outside the trusted set — the value the
        // trusted proxy recorded beyond the attacker's ability to forge —
        // and rotating entries inside the trusted range no longer changes
        // it (same key whenever the rightmost-untrusted entry is stable).
        let cidrs = "10.0.0.0/8";
        // Proxy appends the attacker's real (trusted) IP; the attacker
        // rotates that rightmost trusted-range value.
        let xff_v1 = "198.51.100.9, 10.0.0.50";
        let xff_v2 = "198.51.100.9, 10.0.0.77";
        let peer = ipp("10.0.0.2"); // the trusted proxy
        let key1 = client_ip_from_xff(cidrs, Some(xff_v1), peer);
        let key2 = client_ip_from_xff(cidrs, Some(xff_v2), peer);
        assert_eq!(key1, ipp("198.51.100.9"), "key is rightmost untrusted");
        assert_eq!(
            key1, key2,
            "rotation inside the trusted range must not change the key"
        );
    }
}
