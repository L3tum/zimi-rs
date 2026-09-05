//! Memoized liveness probes for `/health` (ARCH-7: moved out of `lib.rs`;
//! PONY-N3: the db probe is de-memoized, only the qbit slot keeps the TTL).
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{db, torrent};

/// Liveness probes for `/health`. The qbit probe is cached for a short TTL so
/// a burst of health checks does not ping qBittorrent unthrottled (the db
/// probe is deliberately not memoized — see [`HealthProbes::probe_db`]). The
/// cache is checked and updated without holding the lock across an `.await`
/// (two short lock phases), so the handler future stays `Send`.
#[derive(Clone)]
pub struct HealthProbes {
    inner: Arc<Mutex<ProbeState>>,
}

#[derive(Default)]
struct ProbeState {
    qbit: Option<(bool, Instant)>,
}

impl HealthProbes {
    /// How long a qbit probe result is trusted before re-probing.
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

    /// Real Postgres liveness probe (`SELECT 1`). Deliberately **not**
    /// memoized (PONY-N3): the qbit probe stays 2 s TTL because
    /// `app/version` is a network round-trip; `SELECT 1` is the cheapest
    /// possible probe and `/health` is rate-limit-exempt (ratelimit.rs:190).
    pub async fn probe_db(&self, db: &db::Pool) -> bool {
        match db.get().await {
            Ok(pg) => pg.query_one("SELECT 1", &[]).await.is_ok(),
            Err(_) => false,
        }
    }

    /// Real qBittorrent liveness probe (one lightweight `app/version` call),
    /// memoized. `false` when qB is not configured.
    ///
    /// `version()` is the lightest endpoint: a single `GET /api/v2/app/version`
    /// returns a short version string, so a burst of health checks does not
    /// pull the (potentially large) full torrent list. The 2 s memoization TTL
    /// bounds probe cost either way.
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::HealthProbes;
    use std::time::{Duration, Instant};

    /// TTL logic for the memoized liveness probe, tested against an injected
    /// `now` so fresh/stale/skew behaviour is deterministic. (PONY-N3: the TTL
    /// now governs only the qbit slot; `fresh`'s signature is unchanged.)
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
