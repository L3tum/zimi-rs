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
//! - every redirect hop is re-validated (see `redirect_policy`), and named
//!   hosts are resolved up front and pinned (see `resolve_download_host`)
//!   to close the DNS-rebinding window.

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
            // IPv4-mapped IPv6 (e.g. ::ffff:169.254.169.254) — recurse on
            // the embedded IPv4 address so the V4 rules apply.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_blocked_ip(IpAddr::V4(v4), allow_private, allow_loopback);
            }
            // fe80::/10 link-local — the IPv6 metadata / AMT surface.
            if (v6.segments()[0] & 0xffc0) == 0xfe80 || v6.is_unspecified() {
                return true;
            }
            if v6.is_loopback() {
                return !allow_loopback;
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

/// reqwest redirect policy that re-validates **every** hop, gating private
/// ranges by a caller-supplied provider (typically
/// `downloads.allow_private_networks`). The provider keeps netguard free of a
/// settings dependency (ARCH-3); it is invoked per hop so a mid-process
/// settings change is picked up on the next redirect.
pub(crate) fn redirect_policy(
    allow_private: std::sync::Arc<dyn Fn() -> bool + Send + Sync>,
) -> reqwest::redirect::Policy {
    redirect_policy_with(move |url| {
        let allow_private = allow_private();
        // Downloads never allow loopback (validate_download_url semantics).
        redirect_hop_ok(url, allow_private, false)
    })
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
        // ARCH-3: `redirect_policy` wraps exactly this provider→hop closure in
        // a reqwest policy; drive the closure directly with a live provider so
        // a mid-process settings flip is picked up per hop.
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
}
