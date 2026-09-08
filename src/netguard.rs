//! Shared SSRF / network guards for outbound HTTP.
//!
//! Used by the download poller (direct `.zim` downloads, OPDS catalog) and by
//! the embedding client (OpenAI-compatible endpoints). Rules:
//!
//! - only `http`/`https` schemes may be fetched (other schemes pass through
//!   host checks — the poller makes no HTTP request for e.g. `magnet:`);
//! - loopback is blocked **unless** `allow_loopback` (the embed endpoint
//!   allows local servers such as Ollama; downloads never do);
//! - link-local (incl. the 169.254.169.254 metadata service, IPv4 and IPv6
//!   `fe80::/10`) and unspecified addresses are **always** blocked;
//! - private ranges (RFC1918, ULA `fc00::/7`) are blocked unless the
//!   caller passes `allow_private = true` (from
//!   `downloads.allow_private_networks`);
//! - redirects run in **two regimes** (SEC-1: a sub-second TTL flip between
//!   a check and the CONNECT must not be able to steer a fetch at a blocked
//!   host):
//!   - *user-influenced URLs* (direct `.zim` downloads, the OPDS catalog)
//!     are fetched with **manual, bounded redirect following**
//!     (`follow_pinned_get`): every hop — including the first — is
//!     validated, re-resolved, and pinned to the exact address(es) that
//!     passed the check, and the request goes out through a client pinned
//!     to that resolution (`Policy::none` clients never follow redirects
//!     themselves). This closes the DNS-rebinding window that per-hop
//!     *re-validation alone* leaves open: a hop to a *different* host would
//!     otherwise be re-resolved by reqwest at CONNECT time, **after** the
//!     policy's check.
//!   - *operator-configured endpoints* (qBittorrent, embedding) use
//!     reqwest's built-in following with the initial host pinned
//!     (`resolve_download_host`) and every hop re-validated
//!     (`redirect_hop_ok`). The residual sub-second rebinding window on
//!     a redirect to a *different* host is accepted there: the operator
//!     picked the endpoint, and the per-hop re-validation bounds the
//!     damage.

use std::net::{IpAddr, ToSocketAddrs};

use crate::error::{Error, Result};

/// True for addresses that must not be fetched under the given policy.
pub(crate) fn is_blocked_ip(ip: IpAddr, allow_private: bool, allow_loopback: bool) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            // 169.254/16 = cloud metadata (e.g. 169.254.169.254). Always blocked.
            if v4.is_link_local() || v4.is_unspecified() {
                return true;
            }
            if v4.is_loopback() {
                return !allow_loopback;
            }
            // IETF documentation / reserved special-purpose ranges (RFC 5735):
            // never legitimately host a mirror; block even under allow_private.
            let o = v4.octets();
            if (o[0] == 192 && o[1] == 0 && o[2] == 2)
                || (o[0] == 198 && o[1] == 51 && o[2] == 100)
                || (o[0] == 203 && o[1] == 0 && o[2] == 113)
            {
                return true;
            }
            // RFC 6598 CGNAT (100.64.0.0/10) — carrier-grade NAT range that
            // can reach internal services; treat like private.
            let is_cgnat = v4.octets()[0] == 100 && (v4.octets()[1] & 0xC0) == 64;
            !allow_private && (v4.is_private() || is_cgnat)
        }
        IpAddr::V6(v6) => {
            // Loopback and unspecified are decided **before** the
            // IPv4-embedded recursion below: `::1` and `::` also fall in the
            // legacy ::/96 block, but must keep their plain V6 loopback /
            // unspecified semantics (respecting `allow_loopback`).
            if v6.is_unspecified() {
                return true;
            }
            if v6.is_loopback() {
                return !allow_loopback;
            }
            // IPv4-embedded IPv6 — recurse on the embedded IPv4 address so
            // the V4 rules apply. Covers both the IPv4-*mapped* form
            // (::ffff:0:0/96, e.g. ::ffff:169.254.169.254) and the legacy
            // IPv4-*compatible* form (::/96, first six segments zero, e.g.
            // ::a9fe:a9fe == 169.254.169.254 cloud metadata) — the latter
            // matched no V6 check and otherwise passed as a public address
            // (SSRF bypass). (`to_ipv4` accepts exactly these two blocks; the
            // `::1`/`::` members of ::/96 are handled above.)
            if let Some(v4) = v6.to_ipv4() {
                return is_blocked_ip(IpAddr::V4(v4), allow_private, allow_loopback);
            }
            // fe80::/10 link-local — the IPv6 metadata / AMT surface.
            if (v6.segments()[0] & 0xffc0) == 0xfe80 {
                return true;
            }
            // Special-purpose ranges that never host a real mirror: the NAT64
            // well-known prefix 64:ff9b::/96 and IPv6 documentation
            // 2001:db8::/32. Blocked even under allow_private.
            let s = v6.segments();
            if (s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 0 && s[3] == 0)
                || (s[0] == 0x2001 && s[1] == 0x0db8)
            {
                return true;
            }
            // Unique-local fc00::/7.
            !allow_private && (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

/// Host check for http/https URLs: reject `localhost` and blocked (private,
/// loopback, link-local/metadata, unspecified) IP literals. Non-http(s)
/// schemes (e.g. `magnet:`) pass through — no HTTP request is made for those,
/// so there is no SSRF surface. `allow_loopback` admits `localhost` and
/// loopback IP literals (embed endpoints only; downloads pass `false`).
pub(crate) fn assert_host_not_blocked(
    url: &str,
    allow_private: bool,
    allow_loopback: bool,
) -> Result<()> {
    let parsed =
        url::Url::parse(url).map_err(|e| Error::InvalidInput(format!("invalid URL: {e}")))?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Ok(());
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| Error::InvalidInput("URL has no host".into()))?;
    if host.eq_ignore_ascii_case("localhost") && !allow_loopback {
        return Err(Error::InvalidInput(
            "URL host is not allowed (localhost)".into(),
        ));
    }
    // If the host is an IP literal, check its range (IPv6 literals come
    // bracketed, e.g. "[::1]").
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = bare.parse::<IpAddr>() {
        if is_blocked_ip(ip, allow_private, allow_loopback) {
            return Err(Error::InvalidInput(format!(
                "URL host {host} is in a blocked range"
            )));
        }
    }
    Ok(())
}

/// SSRF guard for direct downloads: require an http/https URL with a
/// non-blocked host. Downloads never allow loopback.
///
/// Plaintext `http` (not just `https`) is accepted **by design** for LAN
/// mirrors: local/self-hosted mirrors are a documented use case and plain
/// http is the only transport they expose (SEC M3a — a documented decision,
/// no behavioral change). There is **no** integrity mechanism for a direct
/// download (no checksum/signature verification — see `verify_zim`); the only
/// trust boundary is this host/scheme guard plus `allow_private` (SEC M3b).
pub(crate) fn validate_download_url(url: &str, allow_private: bool) -> Result<()> {
    let parsed = url::Url::parse(url)
        .map_err(|e| Error::InvalidInput(format!("invalid download URL: {e}")))?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(Error::InvalidInput(format!(
            "only http/https download URLs are allowed (got '{scheme}')"
        )));
    }
    // SEC M3a: plaintext http is allowed by design (LAN mirrors); make it
    // explicit in the logs (debug level — this runs on every enqueue, so keep
    // it cheap). The URL is logged as-given; callers pass trusted/operator
    // URLs, and the host is re-validated below.
    if scheme == "http" {
        tracing::debug!("direct download over plaintext http (no TLS): {url}");
    }
    assert_host_not_blocked(url, allow_private, false)
}

/// Build an SSRF-guarded `reqwest::Client` builder: validates the endpoint
/// host up front, then attaches a redirect policy that re-validates every
/// redirect hop against the same fixed policy (so a 3xx to a private/metadata
/// IP aborts instead of following). Callers add timeouts etc. and call `.build()`.
///
/// Used for fixed-policy clients (embedding endpoint, qBittorrent API).
/// `allow_loopback` admits `localhost`/loopback literals (local Ollama, local
/// qBittorrent); `allow_private` admits RFC1918/ULA ranges.
///
/// `pin` (host, addr) optionally fixes the resolved address for the
/// *initial* host (DNS-rebinding guard); redirects are still re-resolved and
/// re-validated per hop by the policy.
pub(crate) fn build_guarded_client(
    endpoint: &str,
    allow_private: bool,
    allow_loopback: bool,
    pin: Option<(String, std::net::SocketAddr)>,
) -> Result<reqwest::ClientBuilder> {
    assert_host_not_blocked(endpoint, allow_private, allow_loopback)?;
    let policy = redirect_policy_flags(allow_private, allow_loopback);
    let mut builder = reqwest::Client::builder().redirect(policy);
    if let Some((host, ip)) = pin {
        builder = builder.resolve(&host, ip);
    }
    Ok(builder)
}

/// One redirect-policy builder for both fixed-flag and live-settings callers:
/// `check` re-validates each hop (scheme + host + DNS re-resolution) and any
/// `Err` aborts the request. Both variants re-check the scheme per hop — the
/// stricter of the two legacy behaviors (F4).
pub(crate) fn redirect_policy_with(
    check: impl Fn(&url::Url) -> std::result::Result<(), String> + Send + Sync + 'static,
) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| match check(attempt.url()) {
        Ok(()) => attempt.follow(),
        Err(e) => attempt.error(format!("redirect target rejected: {e}")),
    })
}

/// Sync check usable from reqwest's redirect policy (whose `Policy::custom`
/// closure is sync and cannot await): scheme must be http/https, string host
/// check, and for named hosts a blocking `ToSocketAddrs` re-resolution
/// (redirects are rare; briefly blocking the request thread is acceptable)
/// rejecting if ANY address is blocked. Closes the gap where a 3xx to an
/// *internal hostname* (e.g. `internal.corp`) passed the string check and
/// followed a hop that only DNS would have revealed as private.
pub(crate) fn redirect_hop_ok(
    url: &url::Url,
    allow_private: bool,
    allow_loopback: bool,
) -> std::result::Result<(), String> {
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(format!(
            "only http/https redirect targets allowed (got '{}')",
            url.scheme()
        ));
    }
    assert_host_not_blocked(url.as_str(), allow_private, allow_loopback)
        .map_err(|e| e.to_string())?;
    let host = url.host_str().unwrap_or("");
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if !host.is_empty() && bare.parse::<IpAddr>().is_err() {
        let port = match url.scheme() {
            "https" => url.port_or_known_default().unwrap_or(443),
            _ => url.port_or_known_default().unwrap_or(80),
        };
        let mut addrs = (host, port)
            .to_socket_addrs()
            .map_err(|e| format!("redirect target {host} unresolvable: {e}"))?;
        if let Some(bad) = addrs.find(|a| is_blocked_ip(a.ip(), allow_private, allow_loopback)) {
            return Err(format!(
                "redirect target {host} resolves to blocked address {}",
                bad.ip()
            ));
        }
    }
    Ok(())
}

/// Fixed-flag redirect policy (no live settings): re-validates every hop
/// against `allow_private` / `allow_loopback`.
fn redirect_policy_flags(allow_private: bool, allow_loopback: bool) -> reqwest::redirect::Policy {
    redirect_policy_with(move |url| redirect_hop_ok(url, allow_private, allow_loopback))
}

/// Resolve a URL's hostname to concrete addresses, rejecting if **any**
/// resolved address is in a blocked range (closes the
/// "attacker domain points at an internal IP" bypass). Returns
/// `(host, addr)` to pin via the client's `.resolve()`, or `None` for
/// IP-literal hosts (already range-checked by the URL validators).
pub(crate) async fn resolve_download_host(
    url: &str,
    allow_private: bool,
    allow_loopback: bool,
) -> Result<Option<(String, std::net::SocketAddr)>> {
    let parsed =
        url::Url::parse(url).map_err(|e| Error::InvalidInput(format!("invalid URL: {e}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| Error::InvalidInput("URL has no host".into()))?;
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if bare.parse::<IpAddr>().is_ok() {
        return Ok(None);
    }
    let port = match parsed.scheme() {
        "https" => parsed.port_or_known_default().unwrap_or(443),
        _ => parsed.port_or_known_default().unwrap_or(80),
    };
    let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
    if addrs.is_empty() {
        return Err(Error::InvalidInput(format!(
            "host {host} resolved to no addresses"
        )));
    }
    for a in &addrs {
        if is_blocked_ip(a.ip(), allow_private, allow_loopback) {
            return Err(Error::InvalidInput(format!(
                "host {host} resolves to blocked address {}",
                a.ip()
            )));
        }
    }
    Ok(Some((host.to_string(), addrs[0])))
}

/// Maximum number of redirect *follows* performed by [`follow_pinned_get`]:
/// the initial request is hop 0, the chain follows at most this many 3xx
/// hops, and a (1 + MAX_REDIRECT_HOPS)-th consecutive redirect is an error
/// (bounded: a hostile/looping redirect chain can neither hang the fetch
/// nor pin-escalate us through more than a handful of fresh resolutions).
pub(crate) const MAX_REDIRECT_HOPS: usize = 5;

/// Terminal response of a manually followed redirect chain, plus the
/// pinned client and final URL that produced it. `client` is still valid
/// for follow-up requests to the *same* final URL (e.g. the 416 fresh-start
/// re-issue) — it is pinned to the final hop's validated resolution.
pub(crate) struct PinnedResponse {
    /// The URL that produced `response` (the terminal hop of the chain).
    pub url: String,
    /// The pinned client that issued the terminal request.
    pub client: reqwest::Client,
    /// The non-redirect response (any other status is returned unchanged;
    /// the caller interprets it).
    pub response: reqwest::Response,
}

// `reqwest::Response` is not `Debug` — report the URL only.
impl std::fmt::Debug for PinnedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedResponse")
            .field("url", &self.url)
            .finish()
    }
}

/// One decision step of the manual redirect chain.
#[derive(Debug)]
pub(crate) enum RedirectStep {
    /// Terminal: this response is the final result for the caller (any
    /// non-followable status — including a 3xx without a usable `Location`).
    Final,
    /// Follow: the next hop is this absolute URL (re-validated, re-resolved,
    /// and re-pinned by the loop).
    Follow(String),
}

/// Decide, for a response at `current`, whether the manual chain follows
/// (pure — unit-testable without a server): only 301/302/303/307/308 with a
/// non-empty, joinable `Location` header are followed; a relative
/// `Location` is resolved against `current` (host change included).
/// Anything else is terminal. The hop cap is enforced by the caller.
pub(crate) fn redirect_step(
    current: &str,
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> RedirectStep {
    match status.as_u16() {
        301 | 302 | 303 | 307 | 308 => {
            let loc = match headers
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
            {
                Some(l) if !l.trim().is_empty() => l.trim().to_string(),
                _ => return RedirectStep::Final,
            };
            match url::Url::parse(current)
                .ok()
                .and_then(|base| base.join(&loc).ok())
            {
                Some(next) => RedirectStep::Follow(next.into()),
                // Unparseable base or `Location` → terminal; the caller
                // returns the 3xx response as-is.
                None => RedirectStep::Final,
            }
        }
        _ => RedirectStep::Final,
    }
}

/// The per-hop pin for the manual chain: IP-literal hosts need no pin
/// (already range-checked by `validate_download_url`, no DNS → no
/// rebinding window); a caller-provided `host_pins` entry maps that host to
/// a fixed address with **no** DNS lookup (mirrors
/// `ClientBuilder::resolve` semantics — a pinned host is never looked up
/// again); any other named host goes through [`resolve_download_host`
/// ] (all addresses resolved, rejected if ANY is blocked).
///
/// Production callers pass an empty `host_pins` — every pin there is a
/// `resolve_download_host` output. Tests use it to map a fake hostname
/// onto a local mock server without real DNS; a provided pin is trusted by
/// the caller (production pins have already passed `is_blocked_ip`).
async fn pin_for_hop(
    url: &str,
    allow_private: bool,
    host_pins: &[(String, std::net::SocketAddr)],
) -> Result<Option<(String, std::net::SocketAddr)>> {
    let parsed =
        url::Url::parse(url).map_err(|e| Error::InvalidInput(format!("invalid URL: {e}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| Error::InvalidInput("URL has no host".into()))?;
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if bare.parse::<IpAddr>().is_ok() {
        return Ok(None);
    }
    if let Some((_, ip)) = host_pins.iter().find(|(h, _)| h.eq_ignore_ascii_case(host)) {
        return Ok(Some((host.to_string(), *ip)));
    }
    // Downloads never allow loopback (validate_download_url semantics).
    resolve_download_host(url, allow_private, false).await
}

/// Perform a GET through a **manual, bounded redirect chain** with
/// per-hop resolve + pin (SEC-1). This is the fetch path for user-influenced
/// URLs (direct `.zim` downloads, OPDS catalog).
///
/// Each hop — including the initial one:
/// 1. `validate_download_url` (scheme + host checks, loopback never allowed);
/// 2. resolve the target host ([`pin_for_hop`] → [`resolve_download_host`]:
///    ALL addresses, rejected if ANY is blocked under the live
///    `allow_private` flag);
/// 3. build a client pinned to exactly that resolution via
///    `ClientBuilder::resolve` (`build_client`), so reqwest connects to the
///    pinned IP — there is **no** rebinding window between check and
///    connect;
/// 4. execute the request and inspect the status: 301/302/303/307/308 with
///    a non-empty `Location` is followed (at most [`MAX_REDIRECT_HOPS`] times
///    — a further redirect is an error, no hang); anything else is
///    terminal and returned to the caller unchanged in [`PinnedResponse`].
///
/// `allow_private` is a live provider (typically over the settings cache)
/// invoked per hop — netguard stays free of a settings dependency (ARCH-3).
/// `host_pins` and `build_client` are documented on their parameters; `build_client`
/// receives the hop's pin (`None` for IP-literal hosts) and is expected to
/// use `Policy::none` (see `build_download_client`).
pub(crate) async fn follow_pinned_get(
    start_url: &str,
    allow_private: std::sync::Arc<dyn Fn() -> bool + Send + Sync>,
    host_pins: &[(String, std::net::SocketAddr)],
    build_client: &(dyn Fn(Option<(String, std::net::SocketAddr)>) -> Result<reqwest::Client>
          + Sync),
    request: &(dyn Fn(&reqwest::Client, &str) -> reqwest::RequestBuilder + Sync),
) -> Result<PinnedResponse> {
    let mut url = start_url.to_string();
    let mut followed = 0usize;
    loop {
        let allow_private = allow_private();
        validate_download_url(&url, allow_private)?;
        let pin = pin_for_hop(&url, allow_private, host_pins).await?;
        let client = build_client(pin)?;
        let response = request(&client, &url).send().await.map_err(Error::Http)?;
        match redirect_step(&url, response.status(), response.headers()) {
            RedirectStep::Final => {
                return Ok(PinnedResponse {
                    url,
                    client,
                    response,
                })
            }
            RedirectStep::Follow(next) => {
                if followed >= MAX_REDIRECT_HOPS {
                    return Err(Error::InvalidInput(format!(
                        "too many redirects: {url} still redirecting after {MAX_REDIRECT_HOPS} hops"
                    )));
                }
                followed += 1;
                tracing::debug!("manual redirect {followed}/{MAX_REDIRECT_HOPS}: {url} → {next}");
                url = next;
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn ssrf_blocks_private_and_local_hosts() {
        for bad in [
            "http://127.0.0.1/x.zim",
            "http://localhost/x.zim",
            "http://LOCALHOST/x.zim",
            "http://169.254.169.254/latest/meta-data",
            "http://10.0.0.5/x.zim",
            "http://172.16.0.1/x.zim",
            "http://192.168.1.1/x.zim",
            "http://0.0.0.0/x.zim",
            "http://[::1]/x.zim",
            "http://[fc00::1]/x.zim",
            // M-v6: IPv6 link-local (fe80::/10) must be blocked too.
            "http://[fe80::1]/x.zim",
        ] {
            // Direct downloads must reject it by default...
            assert!(
                validate_download_url(bad, false).is_err(),
                "must block: {bad}"
            );
            // ...and the torrent-URL variant blocks the same http(s) hosts.
            assert!(
                assert_host_not_blocked(bad, false, false).is_err(),
                "must block: {bad}"
            );
        }
    }

    #[test]
    fn ssrf_allow_private_op_in() {
        // Opt-in for LAN mirrors: private ranges pass, but loopback,
        // link-local/metadata, and localhost stay blocked.
        for ok in [
            "http://10.0.0.5/x.zim",
            "http://172.16.0.1/x.zim",
            "http://192.168.1.1/x.zim",
            "http://[fc00::1]/x.zim",
        ] {
            assert!(
                validate_download_url(ok, true).is_ok(),
                "must allow with opt-in: {ok}"
            );
        }
        for still in [
            "http://127.0.0.1/x.zim",
            "http://localhost/x.zim",
            "http://169.254.169.254/latest/meta-data",
            "http://0.0.0.0/x.zim",
            "http://[::1]/x.zim",
            "http://[fe80::1]/x.zim",
        ] {
            assert!(
                validate_download_url(still, true).is_err(),
                "must block even with opt-in: {still}"
            );
        }
    }

    #[test]
    fn ssrf_allows_public_and_magnet() {
        assert!(validate_download_url(
            "https://example.com/wikimedia/zim/wikipedia/wikipedia_en.zim",
            false
        )
        .is_ok());
        assert!(validate_download_url("http://93.184.215.14/x.zim", false).is_ok());
        // Magnet links aren't fetched by the poller — allowed for torrents.
        assert!(
            assert_host_not_blocked("magnet:?xt=urn:btih:abcdef1234567890", false, false).is_ok()
        );
    }

    #[test]
    fn ssrf_requires_http_for_direct() {
        // Direct downloads must be http/https (magnet/ftp rejected).
        assert!(validate_download_url("magnet:?xt=urn:btih:abcdef", false).is_err());
        assert!(validate_download_url("ftp://example.com/x.zim", false).is_err());
    }

    #[test]
    fn allow_loopback_admits_localhost_only_for_embed_style_checks() {
        // Downloads never pass allow_loopback:
        assert!(validate_download_url("http://localhost:11434/api/embed", false).is_err());
        assert!(validate_download_url("http://127.0.0.1:11434/api/embed", false).is_err());
        // Embed-style checks (allow_loopback = true) admit loopback...
        assert!(assert_host_not_blocked("http://localhost:11434/api/embed", false, true).is_ok());
        assert!(assert_host_not_blocked("http://127.0.0.1:11434/api/embed", false, true).is_ok());
        assert!(assert_host_not_blocked("http://[::1]:11434/api/embed", false, true).is_ok());
        // ...but link-local/metadata and unspecified stay blocked even then.
        assert!(assert_host_not_blocked("http://169.254.169.254/x", false, true).is_err());
        assert!(assert_host_not_blocked("http://[fe80::1]/x", false, true).is_err());
        assert!(assert_host_not_blocked("http://0.0.0.0/x", false, true).is_err());
        // allow_private still controls private ranges under allow_loopback.
        assert!(assert_host_not_blocked("http://10.0.0.5/x", false, true).is_err());
        assert!(assert_host_not_blocked("http://10.0.0.5/x", true, true).is_ok());
    }

    #[test]
    fn redirect_hop_ok_rejects_blocked_and_non_http() {
        use url::Url;
        // Cloud metadata (link-local) is always blocked, even with loopback+private.
        let meta = Url::parse("http://169.254.169.254/latest/meta-data").unwrap();
        assert!(redirect_hop_ok(&meta, true, true).is_err());

        // Non-http scheme is rejected by the policy (stricter of the two legacy
        // behaviors — F4).
        let ftp = Url::parse("ftp://example.com/x").unwrap();
        assert!(redirect_hop_ok(&ftp, true, true).is_err());

        // A public host passes when resolvable; a loopback IP literal is
        // admitted only under allow_loopback.
        let lo = Url::parse("http://127.0.0.1:11434/").unwrap();
        assert!(redirect_hop_ok(&lo, false, false).is_err());
        assert!(redirect_hop_ok(&lo, false, true).is_ok());

        // A private IP literal: gated by allow_private.
        let priv4 = Url::parse("http://10.0.0.5/x").unwrap();
        assert!(redirect_hop_ok(&priv4, false, true).is_err());
        assert!(redirect_hop_ok(&priv4, true, true).is_ok());
    }

    /// SEC-1: the pure redirect-step decision — no server, no DNS.
    #[test]
    fn redirect_step_decides_follow_or_final() {
        use axum::http::{HeaderMap, HeaderValue};

        let loc = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(reqwest::header::LOCATION, HeaderValue::try_from(v).unwrap());
            h
        };
        let empty = HeaderMap::new();
        let base = "http://a.example:8080/dir/file";

        // Only 301/302/303/307/308 with a non-empty Location are followed.
        for code in [301, 302, 303, 307, 308] {
            let st = axum::http::StatusCode::from_u16(code).unwrap();
            assert!(
                matches!(
                    redirect_step(base, st, &loc("http://b.example/x")),
                    RedirectStep::Follow(_)
                ),
                "{code} with Location must be followed"
            );
        }

        // Every other status is terminal.
        for code in [200, 204, 206, 300, 304, 404, 416, 500] {
            let st = axum::http::StatusCode::from_u16(code).unwrap();
            assert!(
                matches!(
                    redirect_step(base, st, &loc("http://b.example/x")),
                    RedirectStep::Final
                ),
                "{code} must be terminal even with a Location"
            );
            assert!(
                matches!(redirect_step(base, st, &empty), RedirectStep::Final),
                "{code} without headers must be terminal"
            );
        }

        // 3xx without a usable Location is terminal.
        let st302 = axum::http::StatusCode::from_u16(302).unwrap();
        assert!(matches!(
            redirect_step(base, st302, &empty),
            RedirectStep::Final
        ));
        let blank = loc("   ");
        assert!(matches!(
            redirect_step(base, st302, &blank),
            RedirectStep::Final
        ));

        // Absolute Location is kept as-is; relative Location joins against
        // the current URL (query/fragment of the base must not leak in).
        match redirect_step(base, st302, &loc("http://b.example:9090/y?z=1")) {
            RedirectStep::Follow(u) => assert_eq!(u, "http://b.example:9090/y?z=1"),
            other => panic!("absolute Location must be followed: {other:?}"),
        }
        match redirect_step(base, st302, &loc("other.zim")) {
            RedirectStep::Follow(u) => assert_eq!(u, "http://a.example:8080/dir/other.zim"),
            other => panic!("relative Location must join: {other:?}"),
        }
        match redirect_step(base, st302, &loc("/root.zim")) {
            RedirectStep::Follow(u) => assert_eq!(u, "http://a.example:8080/root.zim"),
            other => panic!("root-relative Location must join: {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_download_host_loopback_flag_gates() {
        // localhost resolves to a loopback address: admitted under
        // allow_loopback=true, rejected under false. Do NOT pin the exact IP
        // (localhost resolution order is environment-dependent).
        let url = "http://localhost:11434/v1";
        let ok = resolve_download_host(url, false, true)
            .await
            .expect("loopback admitted under allow_loopback");
        assert!(ok.is_some(), "named host must pin an address");
        let (host, ip) = ok.unwrap();
        assert_eq!(host, "localhost");
        assert!(
            ip.ip().is_loopback(),
            "localhost must resolve to a loopback IP"
        );

        let bad = resolve_download_host(url, false, false).await;
        assert!(
            bad.is_err(),
            "loopback must be rejected under allow_loopback=false"
        );
    }

    #[test]
    fn redirect_policy_with_rejects_blocked_hop() {
        // Exercise the merged policy builder through its check closure: an
        // ftp:// target is rejected, a loopback target gated by the flags.
        let ftp = url::Url::parse("ftp://example.com/x").unwrap();
        assert!(redirect_hop_ok(&ftp, true, true).is_err());
    }

    #[test]
    fn redirect_policy_provider_gates_private_hop() {
        // ARCH-3: `redirect_policy_flags` wraps exactly this provider→hop
        // closure (around `redirect_hop_ok`) in a reqwest policy; drive the
        // closure directly with a live provider so a mid-process settings flip
        // is picked up per hop.
        let provider = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let check = {
            let p = provider.clone();
            move |url: &url::Url| {
                redirect_hop_ok(url, p.load(std::sync::atomic::Ordering::SeqCst), false)
            }
        };
        let private_hop = url::Url::parse("http://10.0.0.5/x").unwrap();
        assert!(
            check(&private_hop).is_err(),
            "private-IP hop must be rejected while the provider says false"
        );
        provider.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            check(&private_hop).is_ok(),
            "private-IP hop must be allowed once the provider flips true"
        );
    }

    #[test]
    fn is_blocked_ip_table() {
        // Loopback: blocked unless allow_loopback.
        let lo4 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        assert!(is_blocked_ip(lo4, false, false));
        assert!(!is_blocked_ip(lo4, false, true));
        let lo6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        assert!(is_blocked_ip(lo6, false, false));
        assert!(!is_blocked_ip(lo6, false, true));

        // Link-local (cloud metadata, v4 + v6) always blocked.
        let meta = IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254));
        assert!(is_blocked_ip(meta, false, true));
        assert!(is_blocked_ip(meta, true, true));
        let fe80 = IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1));
        assert!(is_blocked_ip(fe80, false, true));
        assert!(is_blocked_ip(fe80, true, true));

        // Unspecified always blocked.
        assert!(is_blocked_ip(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            false,
            true
        ));
        assert!(is_blocked_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), true, true));

        // Private v4: blocked when allow_private=false, allowed when true.
        let ten = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));
        assert!(is_blocked_ip(ten, false, false));
        assert!(!is_blocked_ip(ten, true, false));

        // ULA fc00::/7: same as private.
        let v6_ul = IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1));
        assert!(is_blocked_ip(v6_ul, false, false));
        assert!(!is_blocked_ip(v6_ul, true, false));

        // Public IPs: always allowed. (Use a genuinely global-unicast v6 —
        // 2001:db8::/32 is the IETF *documentation* range, now always blocked.)
        let pub4 = IpAddr::V4(Ipv4Addr::new(93, 184, 215, 14));
        assert!(!is_blocked_ip(pub4, false, false));
        assert!(!is_blocked_ip(pub4, true, true));
        let pub6 = IpAddr::V6(Ipv6Addr::new(0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888));
        assert!(!is_blocked_ip(pub6, false, false));

        // IETF documentation / special-purpose ranges: always blocked, even
        // under allow_private (RFC 5735 v4 + NAT64 well-known + v6 doc).
        for doc4 in [
            Ipv4Addr::new(192, 0, 2, 1),
            Ipv4Addr::new(198, 51, 100, 1),
            Ipv4Addr::new(203, 0, 113, 1),
        ] {
            let ip = IpAddr::V4(doc4);
            assert!(
                is_blocked_ip(ip, true, true),
                "{doc4} must always be blocked"
            );
        }
        let nat64 = IpAddr::V6(Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0, 1));
        assert!(
            is_blocked_ip(nat64, true, true),
            "NAT64 64:ff9b::/96 must always be blocked"
        );
        let v6_doc = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
        assert!(
            is_blocked_ip(v6_doc, true, true),
            "2001:db8::/32 must always be blocked"
        );

        // IPv4-mapped IPv6 (::ffff:x.x.x.x) must recurse into the V4 rules —
        // otherwise `::ffff:169.254.169.254` slips past every V6 check.
        // Link-local / metadata wins regardless of flags.
        // ::ffff:169.254.169.254
        let m6_meta = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xa9fe, 0xa9fe));
        assert!(is_blocked_ip(m6_meta, false, true));
        assert!(is_blocked_ip(m6_meta, true, true));
        // Loopback: ::ffff:127.0.0.1 — blocked unless allow_loopback.
        let m6_lo = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x7f00, 0x0001));
        assert!(is_blocked_ip(m6_lo, false, false));
        assert!(!is_blocked_ip(m6_lo, false, true));
        // Private: ::ffff:10.0.0.5 — blocked unless allow_private.
        let m6_priv = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0a00, 0x0005));
        assert!(is_blocked_ip(m6_priv, false, false));
        assert!(!is_blocked_ip(m6_priv, true, false));

        // RFC 6598 CGNAT (100.64.0.0/10): blocked when !allow_private.
        let cgnat_lo = IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1));
        assert!(is_blocked_ip(cgnat_lo, false, false));
        assert!(!is_blocked_ip(cgnat_lo, true, false));
        let cgnat_hi = IpAddr::V4(Ipv4Addr::new(100, 127, 255, 255));
        assert!(is_blocked_ip(cgnat_hi, false, false));
        assert!(!is_blocked_ip(cgnat_hi, true, false));
        // Just outside the /10: 100.63.x and 100.128.x are public/allowed.
        let not_cgnat_1 = IpAddr::V4(Ipv4Addr::new(100, 63, 0, 1));
        assert!(!is_blocked_ip(not_cgnat_1, false, false));
        let not_cgnat_2 = IpAddr::V4(Ipv4Addr::new(100, 128, 0, 1));
        assert!(!is_blocked_ip(not_cgnat_2, false, false));
    }

    #[test]
    fn is_blocked_ip_v4_compatible_ipv6() {
        // Legacy IPv4-*compatible* IPv6 (::/96, first five segments zero)
        // must recurse into the V4 rules exactly like the ::ffff:0:0/96
        // mapped form. Previously only the mapped form was checked, so
        // `::a9fe:a9fe` (== 169.254.169.254 cloud metadata) slipped past
        // every V6 check and passed as a public address (SSRF bypass).
        // ::a9fe:a9fe == 169.254.169.254 — link-local/metadata, always
        // blocked regardless of the flags.
        let compat_meta = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0xa9fe, 0xa9fe));
        assert!(is_blocked_ip(compat_meta, false, true));
        assert!(is_blocked_ip(compat_meta, true, true));
        // ::7f00:1 == 127.0.0.1 — loopback gated by allow_loopback.
        let compat_lo = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0x7f00, 0x0001));
        assert!(is_blocked_ip(compat_lo, false, false));
        assert!(!is_blocked_ip(compat_lo, false, true));
        // ::a:0:1 == 10.0.0.1 — private, gated by allow_private.
        let compat_priv = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0x0a00, 0x0001));
        assert!(is_blocked_ip(compat_priv, false, false));
        assert!(!is_blocked_ip(compat_priv, true, false));
        // The ::ffff:0:0/96 *mapped* form still recurses (no regression):
        // ::ffff:a9fe:a9fe == 169.254.169.254.
        let mapped_meta = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xa9fe, 0xa9fe));
        assert!(is_blocked_ip(mapped_meta, true, true));
        // ::1 technically falls in the legacy ::/96 block, but its plain
        // V6 loopback semantics take precedence (checked before the
        // recursion) — behavior unchanged, respects allow_loopback.
        let lo6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        assert!(is_blocked_ip(lo6, false, false));
        assert!(!is_blocked_ip(lo6, false, true));
        // Genuinely public IPv6 (2001:4860::8888) is still allowed — note
        // 2001:db8::/32 is IETF documentation and is always blocked.
        let pub6 = IpAddr::V6(Ipv6Addr::new(0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888));
        assert!(!is_blocked_ip(pub6, false, false));

        // URL level: a compatible-form metadata IP literal is rejected by
        // the download guard (this is the bypass `validate_download_url`
        // must now close; `resolve_download_host` re-checks resolved
        // addresses through the same `is_blocked_ip`).
        assert!(validate_download_url("http://[::a9fe:a9fe]/latest/meta-data", true).is_err());
        assert!(validate_download_url("http://[::ffff:a9fe:a9fe]/latest/meta-data", true).is_err());
    }
}
