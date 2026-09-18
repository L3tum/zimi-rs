//! Memoized liveness probes for `/health` (ARCH-7: moved out of `lib.rs`;
//! both slots — qbit and db — keep the same 2 s TTL. The db probe was
//! de-memoized by PONY-N3, but the 2026-09 PERF review overrides that:
//! `/health` is rate-limit-exempt and monitor-polled, and its per-call pool
//! checkouts are an explicit checkout site behind the `/diagnostic`
//! checkout-wait metric (site `"health:db_probe"`), so they accumulate.)
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{db, torrent};

/// Liveness probes for `/health`. The qbit and db probes are each cached
/// for a short TTL so a burst of health checks does not ping qBittorrent or
/// the Postgres pool unthrottled (the db probe's decision and staleness
/// bound are documented on [`HealthProbes::probe_db`]). The cache is
/// checked and updated without holding the lock across an `.await` (two
/// short lock phases), so the handler future stays `Send`.
#[derive(Clone)]
pub struct HealthProbes {
    inner: Arc<Mutex<ProbeState>>,
}

#[derive(Default)]
struct ProbeState {
    qbit: Option<(bool, Instant)>,
    db: Option<(bool, Instant)>,
}

impl HealthProbes {
    /// How long a probe result (qbit or db) is trusted before re-probing.
    const TTL: Duration = Duration::from_secs(2);

    /// Return the cached value if it is younger than the TTL (as of `now`),
    /// else `None`. A timestamp in the future (clock skew) is treated as stale.
    fn fresh(slot: &Option<(bool, Instant)>, now: Instant) -> Option<bool> {
        slot.as_ref()
            .filter(|(_, t)| {
                now.checked_duration_since(*t)
                    .is_some_and(|age| age < Self::TTL)
            })
            .map(|(v, _)| *v)
    }

    /// Real qBittorrent liveness probe (one lightweight `app/version` call),
    /// memoized. `false` when qB is not configured.
    ///
    /// `version()` is the lightest endpoint: a single `GET /api/v2/app/version`
    /// returns a short version string, so a burst of health checks does not
    /// pull the (potentially large) full torrent list. The 2 s memoization TTL
    /// bounds probe cost either way.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub async fn probe_qbit(&self, qbit: &Option<Arc<torrent::QbitClient>>) -> bool {
        {
            let g = self.inner.lock().expect("health-probes lock poisoned");
            if let Some(v) = Self::fresh(&g.qbit, Instant::now()) {
                return v;
            }
        }
        let ok = match qbit {
            Some(q) => q.version().await.is_ok(),
            None => false,
        };
        let mut g = self.inner.lock().expect("health-probes lock poisoned");
        g.qbit = Some((ok, Instant::now()));
        ok
    }

    /// Real Postgres liveness probe (`SELECT 1`), memoized at the same
    /// 2 s TTL as the qbit probe.
    ///
    /// The 2026-09 PERF review overrides PONY-N3's de-memoization: `/health`
    /// is rate-limit-exempt and polled by monitors, so a per-call pool
    /// checkout accumulates with the poll frequency — and this is one of
    /// the explicit checkout sites behind the `/diagnostic` checkout-wait
    /// metric (site `"health:db_probe"`), which is what made the cost
    /// measurable. Staleness bound: a DB outage (or its recovery) is now
    /// reflected in `/health` within at most the TTL (≤ 2 s).
    ///
    /// `acquire_timed` (not `acquire`): the `"health:db_probe"` site label
    /// keeps this probe's checkout waits attributable per-site, separate
    /// from search/suggest checkouts (the `/diagnostic` metric depends on
    /// the label).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub async fn probe_db(&self, db: &db::Pool) -> bool {
        {
            let g = self.inner.lock().expect("health-probes lock poisoned");
            if let Some(v) = Self::fresh(&g.db, Instant::now()) {
                return v;
            }
        }
        let ok = match db::pool::acquire_timed(db, "health:db_probe").await {
            Ok(mut pg) => {
                // `SELECT 1` liveness probe — the cheapest possible statement,
                // run on a raw pooled connection (db::raw helpers are for query
                // builders, not a one-word probe).
                // RAW-OK: raw one-word probe; no db::raw helper exists for it.
                sqlx::query("SELECT 1").execute(&mut *pg).await.is_ok()
            }
            Err(_) => false,
        };
        let mut g = self.inner.lock().expect("health-probes lock poisoned");
        g.db = Some((ok, Instant::now()));
        ok
    }
}

impl Default for HealthProbes {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProbeState::default())),
        }
    }
}

/// Per-branch degradation tracker: counts consecutive failures per search
/// branch so `/health` and search responses can surface which capabilities
/// are silently degraded.
///
/// Uses `std::sync::Mutex` — the tracker methods are synchronous (no `.await`),
/// so the guard is never held across an await point.
#[derive(Clone, Default)]
pub struct DegradationTracker {
    inner: Arc<std::sync::Mutex<std::collections::HashMap<&'static str, (u32, Instant)>>>,
}

impl DegradationTracker {
    /// Record a failure for the given branch, incrementing its consecutive
    /// failure count and updating the last-failure timestamp.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn record_failure(&self, branch: &'static str) {
        let mut g = self
            .inner
            .lock()
            .expect("degradation tracker lock poisoned");
        let entry = g.entry(branch).or_insert((0, Instant::now()));
        entry.0 += 1;
        entry.1 = Instant::now();
    }

    /// Record a success for the given branch, resetting its consecutive
    /// failure count to zero.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn record_success(&self, branch: &'static str) {
        let mut g = self
            .inner
            .lock()
            .expect("degradation tracker lock poisoned");
        if let Some(entry) = g.get_mut(branch) {
            entry.0 = 0;
        }
    }

    /// Return a snapshot of all branches with ≥ 3 consecutive failures,
    /// as `(branch_name, failure_count)` pairs.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn degraded_snapshot(&self) -> Vec<(String, u32)> {
        let g = self
            .inner
            .lock()
            .expect("degradation tracker lock poisoned");
        g.iter()
            .filter(|(_, (count, _))| *count >= 3)
            .map(|(name, (count, _))| (name.to_string(), *count))
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::HealthProbes;
    use std::time::{Duration, Instant};

    /// TTL logic for the memoized liveness probes, tested against an injected
    /// `now` so fresh/stale/skew behaviour is deterministic. (The TTL governs
    /// both the qbit and db slots; `fresh`'s signature is unchanged.)
    #[test]
    fn health_probes_fresh_ttl_logic() {
        let now = Instant::now();

        // Recorded 1s ago (TTL is 2s) → fresh, returns cached value.
        let fresh_slot = Some((true, now - Duration::from_secs(1)));
        assert_eq!(HealthProbes::fresh(&fresh_slot, now), Some(true));
        let fresh_false = Some((false, now - Duration::from_millis(100)));
        assert_eq!(HealthProbes::fresh(&fresh_false, now), Some(false));

        // Recorded 3s ago → stale, forces a re-probe.
        let stale_slot = Some((true, now - Duration::from_secs(3)));
        assert_eq!(HealthProbes::fresh(&stale_slot, now), None);

        // No probe recorded yet → stale.
        let empty: Option<(bool, Instant)> = None;
        assert_eq!(HealthProbes::fresh(&empty, now), None);

        // Clock skew: timestamp in the future → treat as stale (never panic).
        let future_slot = Some((true, now + Duration::from_secs(5)));
        assert_eq!(HealthProbes::fresh(&future_slot, now), None);
    }

    // ─── probe_db memoization (2026-09 PERF review: re-memoized at 2 s) ───

    use crate::testing::dead_pool;

    #[tokio::test]
    async fn probe_db_cache_hit_returns_stored_value_without_pool() {
        // Pre-seed the db slot with a fresh `true`: against the dead pool
        // any actual probe fails, so `true` can only come from the cache —
        // this proves the hit path does not touch the pool.
        let probes = HealthProbes::default();
        {
            let mut g = probes.inner.lock().unwrap();
            g.db = Some((true, Instant::now()));
        }
        assert!(probes.probe_db(&dead_pool()).await);
    }

    #[tokio::test]
    async fn probe_db_failure_does_not_flap_within_ttl() {
        let probes = HealthProbes::default();
        let pool = dead_pool();
        // First call actually probes: the dead pool's acquire fails → false.
        assert!(!probes.probe_db(&pool).await);
        let t1 = probes.inner.lock().unwrap().db.as_ref().unwrap().1;
        // Second call within the TTL must serve the cache: same value, and
        // the slot timestamp untouched (a re-probe would rewrite it).
        assert!(!probes.probe_db(&pool).await);
        let t2 = probes.inner.lock().unwrap().db.as_ref().unwrap().1;
        assert_eq!(t1, t2, "a cache hit must not rewrite the slot timestamp");
    }

    #[tokio::test]
    async fn probe_db_reprobes_after_ttl_expiry() {
        let probes = HealthProbes::default();
        // Stale `true` (older than the TTL): a fresh read would return it,
        // so `false` proves the expiry forced a re-probe (which the dead
        // pool fails) and the slot was rewritten.
        let stale_at = Instant::now() - (HealthProbes::TTL + Duration::from_secs(1));
        {
            let mut g = probes.inner.lock().unwrap();
            g.db = Some((true, stale_at));
        }
        assert!(!probes.probe_db(&dead_pool()).await);
        let (v, t) = probes.inner.lock().unwrap().db.unwrap();
        assert!(!v);
        assert!(
            t >= stale_at,
            "the re-probe must rewrite the slot timestamp"
        );
    }

    // ─── DegradationTracker unit tests ───

    use super::DegradationTracker;

    #[test]
    fn degradation_tracker_record_and_snapshot() {
        let tracker = DegradationTracker::default();
        // No failures → no degraded branches.
        assert!(tracker.degraded_snapshot().is_empty());

        // 2 failures → still below threshold (3).
        tracker.record_failure("fts");
        tracker.record_failure("fts");
        assert!(tracker.degraded_snapshot().is_empty());

        // 3rd failure → appears in snapshot.
        tracker.record_failure("fts");
        let snap = tracker.degraded_snapshot();
        assert_eq!(snap, vec![("fts".to_string(), 3)]);

        // Success resets the counter → no longer degraded.
        tracker.record_success("fts");
        assert!(tracker.degraded_snapshot().is_empty());
    }

    #[test]
    fn degradation_tracker_multiple_branches() {
        let tracker = DegradationTracker::default();
        tracker.record_failure("fts");
        tracker.record_failure("fts");
        tracker.record_failure("fts");
        tracker.record_failure("vector_embed");
        tracker.record_failure("vector_embed");
        // Only fts is degraded (3+ failures).
        let snap = tracker.degraded_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].0, "fts");
        assert_eq!(snap[0].1, 3);
    }

    #[test]
    fn degradation_tracker_clone_shares_state() {
        let tracker = DegradationTracker::default();
        let clone = tracker.clone();
        clone.record_failure("trgm_prefix");
        clone.record_failure("trgm_prefix");
        clone.record_failure("trgm_prefix");
        let snap = tracker.degraded_snapshot();
        assert_eq!(snap, vec![("trgm_prefix".to_string(), 3)]);
    }
}
