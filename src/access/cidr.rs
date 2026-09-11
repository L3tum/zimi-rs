//! Trusted-proxy CIDR validation + `X-Forwarded-For` client-IP resolution
//! (WI-14 / SEC-M3). M-B: these are access-control policy objects — the
//! request-time containment/`XFF` resolution is *applied* by
//! `serve::middleware`'s auth gate, and the over-broad / `/0` guards are
//! checked at startup (`startup::serve_policy_checks`), so they live in
//! `access` rather than inside `serve`.

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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
