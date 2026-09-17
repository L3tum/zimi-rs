//! Per-source-IP auth-failure lockout policy (SEC-M1). M-B: the tracker is an
//! access-control policy object held live by `AppState` at the serve layer's
//! level; `serve::middleware` *applies* it as the per-request auth gate.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Per-source-IP auth-failure lockout (SEC-M1). In-memory, capped, per
/// process. After [`Self::MAX_FAILURES`] failures within [`Self::WINDOW`], the
/// IP is locked out for the remainder of the window. A successful auth clears
/// the IP. The map is bounded to `MAX_TRACKED` distinct IPs; when the
/// cap is hit the entry with the oldest `first_failure` is evicted.
#[derive(Default)]
pub struct LockoutTracker {
    /// (failure count, first_failure instant) per IP. `pub(crate)`: only
    /// in-crate tests read it directly; production goes through the
    /// capped-eviction API below.
    pub(crate) inner: std::sync::Mutex<HashMap<IpAddr, (u32, Instant)>>,
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
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
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
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
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
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn record_success(&self, ip: &IpAddr) {
        self.inner.lock().expect("lockout mutex").remove(ip);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
}
