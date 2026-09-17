//! Global token-bucket rate limiting policy for the HTTP API (M-B: the
//! policy object lives in `access`; the axum `rate_limit` middleware that
//! *applies* it lives in `serve::middleware`).
//!
//! tower-http (0.7, the latest) still ships no rate-limiting middleware, so
//! this is a small self-contained limiter: at most `burst` requests per
//! rolling second, refilling at `rps`. Exceeding the limit yields
//! `429 Too Many Requests` with a `Retry-After` header.
//!
//! The limit is service-wide (not per-client): this deployment model is a
//! single instance fronting a small number of clients, and tracking
//! per-IP state is overkill.
//!
//! The bucket itself is a `Mutex`-guarded value rather than a lock-free CAS
//! loop: the critical section is a few integer ops (no I/O, no awaits) and
//! the CAS version's publish-before-CAS ordering subtleties were not
//! measurable at this scale, so the simpler, obviously-correct form wins.

use std::sync::{Arc, Mutex};

use crate::settings::{KEY_ACCESS_RATE_LIMIT_BURST, KEY_ACCESS_RATE_LIMIT_RPS};

/// Token-bucket rate limiter allowing `burst` requests per rolling second,
/// refilling at `rps` requests/second.
pub struct RateLimiter {
    inner: Mutex<Bucket>,
    /// burst × SCALE.
    capacity: u64,
    /// rps × SCALE (scaled tokens per nanosecond, applied via `NS_PER_SEC`).
    refill_per_sec: u64,
}

/// One token is `SCALE` units, so fractional refill accumulates as
/// integer arithmetic without floats.
struct Bucket {
    /// Scaled token balance (≤ capacity).
    tokens: u64,
    /// Last refill timestamp, nanoseconds since the Unix epoch.
    last_refill: u64,
}

const SCALE: u128 = 1_000_000;
const NS_PER_SEC: u128 = 1_000_000_000;

/// Sane ceilings for settings-derived rate-limit values. A mis-set or
/// attacker-set `access.rate_limit_rps`/`access.rate_limit_burst` is clamped
/// here so the scaled arithmetic below (×SCALE = ×1e6) can never overflow
/// `u64` (panic in debug, wrap in release) or be used to effectively disable
/// the limiter.
const RPS_CEILING: u64 = 1_000_000;
const BURST_CEILING: u64 = 1_000_000;

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl RateLimiter {
    /// Create a limiter starting at full capacity (`burst` tokens, refilling
    /// at `rps`). Both values are clamped to sane ceilings before scaling.
    pub fn new(rps: u64, burst: u64) -> Self {
        let (capacity, refill_per_sec) = scaled_limits(rps, burst);
        Self {
            inner: Mutex::new(Bucket {
                tokens: capacity,
                last_refill: now_ns(),
            }),
            capacity,
            refill_per_sec,
        }
    }

    /// Rebuild with a new (rps, burst) while carrying the old token balance
    /// (capped at the new capacity) and the old refill timestamp (BUG-18: a
    /// runtime rps/burst change must not reset the bucket to full capacity —
    /// the old code's `RateLimiter::new` granted a free burst on every resize).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    fn resized(old: &Self, rps: u64, burst: u64) -> Self {
        let (capacity, refill_per_sec) = scaled_limits(rps, burst);
        // Carry the balance: the old tokens may exceed the new (smaller)
        // capacity — cap them; the refill timestamp is carried verbatim so no
        // elapsed window is lost or double-charged.
        let b = old.inner.lock().expect("rate limiter bucket poisoned");
        Self {
            inner: Mutex::new(Bucket {
                tokens: b.tokens.min(capacity),
                last_refill: b.last_refill,
            }),
            capacity,
            refill_per_sec,
        }
    }

    /// Try to consume one token. Returns `Ok(())` when the request is
    /// allowed, or `Err` with a suggested `Retry-After` in milliseconds.
    ///
    /// Each admitted request consumes exactly one whole token under the
    /// bucket lock, so concurrent requests cannot over-admit: admissions are
    /// bounded by the available tokens regardless of interleaving (pinned by
    /// `concurrency_admits_exactly_burst`). The refill is credited from the
    /// elapsed wall-clock window and capped at `capacity`; `last_refill` is
    /// advanced on every call (allowed or not) with the credit already in
    /// `tokens`, so no window is lost or double-charged.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn try_acquire(&self) -> Result<(), u64> {
        const NEED: u64 = SCALE as u64;
        let mut b = self.inner.lock().expect("rate limiter bucket poisoned");
        let now = now_ns();
        let elapsed_ns = now.saturating_sub(b.last_refill) as u128;
        let refill = elapsed_ns.saturating_mul(self.refill_per_sec as u128) / NS_PER_SEC;
        b.tokens = (b.tokens as u128 + refill).min(self.capacity as u128) as u64;
        b.last_refill = now;

        if b.tokens < NEED {
            // Not enough: report how long until one token is available.
            let deficit = (NEED - b.tokens) as u128;
            // B2: divide by the refill rate FIRST, then convert ns → ms. The
            // old left-associative `* NS / 1000 / refill` rounded away up to
            // 999 ns × (refill/1000) of wait whenever refill_per_sec > 1000,
            // slightly under-reporting `Retry-After` at high rps. (The +1
            // keeps the value ≥ 1 ms so `ms_to_retry_after_secs` never
            // suggests an instant retry.)
            let wait_ns = deficit * NS_PER_SEC / self.refill_per_sec as u128;
            return Err((wait_ns / 1000 + 1).min(60_000) as u64);
        }
        b.tokens -= NEED;
        Ok(())
    }

    /// Test-only accessor for the scaled token capacity.
    #[cfg(test)]
    pub(crate) fn capacity_for_test(&self) -> u64 {
        self.capacity
    }

    /// Test-only accessor for the current scaled token balance.
    #[cfg(test)]
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    fn tokens_for_test(&self) -> u64 {
        self.inner
            .lock()
            .expect("rate limiter bucket poisoned")
            .tokens
    }

    /// Test-only: pretend `ns` wall-clock time passed without moving
    /// `last_refill`'s reference point (replaces the old atomic stores).
    #[cfg(test)]
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    fn shift_refill_ns(&self, ns: u64) {
        let mut b = self.inner.lock().expect("rate limiter bucket poisoned");
        b.last_refill = b.last_refill.saturating_sub(ns);
    }
}

/// Clamp the (settings-derived) limits to sane ceilings and scale them.
///
/// A mis-set or attacker-set `access.rate_limit_rps`/`access.rate_limit_burst`
/// is clamped here so the scaled arithmetic (×SCALE = ×1e6) can never
/// overflow `u64` (panic in debug, wrap in release) or be used to effectively
/// disable the limiter. After clamping, ×SCALE is ≤ 1e12, far inside u64.
fn scaled_limits(rps: u64, burst: u64) -> (u64, u64) {
    // Clamp the (settings-derived) values to sane ceilings first.
    let rps = rps.clamp(1, RPS_CEILING);
    let burst = burst.clamp(1, BURST_CEILING);
    let capacity = burst
        .checked_mul(SCALE as u64)
        .unwrap_or(BURST_CEILING * (SCALE as u64));
    let refill_per_sec = rps
        .checked_mul(SCALE as u64)
        .unwrap_or(RPS_CEILING * (SCALE as u64));
    (capacity, refill_per_sec)
}

/// Handle that rebuilds the underlying [`RateLimiter`] when the rate-limit
/// settings change at runtime (same live-reload pattern as the embedding
/// client). Checking costs one settings-cache read and a mutex lock per
/// request; the limiter itself is only rebuilt on setting changes.
/// `(rps, burst)` settings fingerprint paired with the limiter built for it.
/// Struct (not the old nested `(u64, (u64, u64), Arc<…>)` tuple — the 2026-09
/// review flagged the nested tuple): the settings generation at (re)build
/// time, the limits it was built for, and the limiter itself.
struct LimiterSlot {
    gen: u64,
    rps: u64,
    burst: u64,
    limiter: Arc<RateLimiter>,
}

/// Handle to the process-wide limiter: lazily builds it on first use and
/// rebuilds it when the `access.rate_limit_*` settings change at runtime.
pub struct RateLimiterHandle {
    inner: std::sync::Mutex<Option<LimiterSlot>>,
}

impl RateLimiterHandle {
    /// Create an empty handle; the underlying limiter is built on the first
    /// `limiter()` call.
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(None),
        }
    }

    /// Get the current limiter, rebuilding it if the rate-limit settings
    /// changed at runtime (same live-reload pattern as the embedding client).
    ///
    /// W6.3 generation-gated fast path: the slot records the settings
    /// [`generation`](crate::settings::SettingsCache::generation) it was built
    /// from. When that generation is unchanged, the two `access.rate_limit_*`
    /// keys are unchanged too (they only change via a settings mutation, which
    /// bumps the generation), so we return the cached limiter with **no**
    /// `get_typed` read — skipping the per-request serde deserialization.
    /// A generation mismatch (or the first call) falls through to the
    /// existing two-read + compare/`resized()` path, recording the new
    /// generation. A gen bump that leaves the limits unchanged keeps the bucket
    /// and just refreshes the recorded generation.
    /// Acquire the current limiter. The `std::sync::Mutex` is held for < 1 µs
    /// (fast path: generation compare + Arc clone; slow path: two `get_typed`
    /// reads, no awaits) and is not a contention point at the project's QPS.
    /// Replacing with `ArcSwap`/CAS would not be measurable at this scale.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn limiter(&self, settings: &crate::settings::SettingsCache) -> Arc<RateLimiter> {
        let mut guard = self.inner.lock().expect("rate limiter mutex poisoned");
        let cur_gen = settings.generation();
        match guard.as_ref() {
            Some(slot) if slot.gen == cur_gen => {
                // Fast path: generation unchanged → limits unchanged → no read.
                slot.limiter.clone()
            }
            _ => {
                let rps = settings
                    .get_typed::<u64>(KEY_ACCESS_RATE_LIMIT_RPS)
                    .unwrap_or(100);
                let burst = settings
                    .get_typed::<u64>(KEY_ACCESS_RATE_LIMIT_BURST)
                    .unwrap_or(200);
                let limiter = match guard.as_ref() {
                    Some(slot) if slot.rps == rps && slot.burst == burst => {
                        // Limits unchanged (gen bumped, values same): keep the
                        // bucket, refresh the recorded generation below.
                        slot.limiter.clone()
                    }
                    Some(slot) => Arc::new(RateLimiter::resized(&slot.limiter, rps, burst)),
                    None => Arc::new(RateLimiter::new(rps, burst)),
                };
                *guard = Some(LimiterSlot {
                    gen: cur_gen,
                    rps,
                    burst,
                    limiter: limiter.clone(),
                });
                limiter
            }
        }
    }
}

impl Default for RateLimiterHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a millisecond wait into a valid `Retry-After` delta-seconds
/// value: round up (never tell a client to retry too early), minimum 1s.
pub(crate) fn ms_to_retry_after_secs(ms: u64) -> u64 {
    ms.div_ceil(1000).max(1)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_is_delta_seconds() {
        assert_eq!(ms_to_retry_after_secs(0), 1);
        assert_eq!(ms_to_retry_after_secs(1), 1);
        assert_eq!(ms_to_retry_after_secs(999), 1);
        assert_eq!(ms_to_retry_after_secs(1000), 1);
        assert_eq!(ms_to_retry_after_secs(1001), 2);
        assert_eq!(ms_to_retry_after_secs(1500), 2);
        assert_eq!(ms_to_retry_after_secs(10_500), 11);
    }

    #[test]
    fn allows_burst_then_throttles() {
        let limiter = RateLimiter::new(100, 5);
        // The full burst is allowed immediately.
        for _ in 0..5 {
            assert!(limiter.try_acquire().is_ok());
        }
        // The next request is throttled, with a sane retry hint.
        let err = limiter.try_acquire().expect_err("should be throttled");
        assert!((1..=60_000).contains(&err));
    }

    #[test]
    fn refills_after_time_passes() {
        let limiter = RateLimiter::new(1000, 1);
        assert!(limiter.try_acquire().is_ok());
        assert!(limiter.try_acquire().is_err());

        // Simulate 200ms passing at 1000 rps → 2 tokens refilled.
        limiter.shift_refill_ns(200_000_000);
        assert!(limiter.try_acquire().is_ok());
    }

    #[test]
    fn burst_never_exceeds_capacity() {
        let limiter = RateLimiter::new(10, 3);
        // Drain completely.
        while limiter.try_acquire().is_ok() {}
        // Simulate a very long time — tokens must cap at burst, not grow unbounded.
        limiter.shift_refill_ns(10_000_000_000);
        let mut allowed = 0;
        while limiter.try_acquire().is_ok() {
            allowed += 1;
        }
        assert_eq!(allowed, 3);
    }

    /// Concurrency regression: 50 real threads racing for a burst-5 budget
    /// must admit *exactly* 5 — no over-admission (the old non-atomic
    /// read-compute-store admitted all 50, because every requester read the
    /// same token count and stored a decrement without comparing) and no lost
    /// updates (which would under-admit). Each admission consumes exactly one
    /// whole token under the bucket `Mutex`, and tokens cannot go negative, so
    /// the count is deterministic at `burst` regardless of interleaving. rps=1
    /// keeps the refill during the race window well under one token, so exactly
    /// the initial burst is claimable.
    #[test]
    fn concurrency_admits_exactly_burst() {
        let n = 50;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(n));
        let admitted = std::thread::scope(|s| {
            // Construct the limiter immediately before spawning so the 1-token
            // refill window (rps=1 → 1 token/s) cannot add tokens between
            // creation and the race.
            let limiter = std::sync::Arc::new(RateLimiter::new(1, 5));
            let mut handles = Vec::new();
            for _ in 0..n {
                let l = std::sync::Arc::clone(&limiter);
                let b = std::sync::Arc::clone(&barrier);
                handles.push(s.spawn(move || {
                    // Fire all threads at the limiter in the same instant so
                    // stale-read races are exercised (not just serialized
                    // calls).
                    b.wait();
                    l.try_acquire().is_ok()
                }));
            }
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|ok| *ok)
                .count()
        });
        assert_eq!(
            admitted, 5,
            "expected exactly the burst of 5, got {admitted}"
        );
    }

    /// A huge settings value (u64::MAX) must clamp to the ceiling, not
    /// overflow-panic (debug) or wrap (release) the ×SCALE multiplication.
    #[test]
    fn huge_limits_are_clamped_not_wrapped() {
        // Would overflow `u64` (×1e6) if unclamped; must not panic.
        let limiter = RateLimiter::new(u64::MAX, u64::MAX);
        assert_eq!(limiter.capacity_for_test(), BURST_CEILING * (SCALE as u64));
        // A normal value scales through untouched.
        let normal = RateLimiter::new(100, 200);
        assert_eq!(normal.capacity_for_test(), 200 * SCALE as u64);
        // And an unreasonably high rps clamps to the ceiling, not zero.
        let hot = RateLimiter::new(u64::MAX, 1);
        assert!(hot.try_acquire().is_ok());
    }

    /// Build a `SettingsCache` backed by a pool that never connects (we only
    /// read in-memory defaults), seeded with the given rate-limit values.
    fn settings_with_limits(rps: u64, burst: u64) -> crate::settings::SettingsCache {
        let pool = crate::testing::dead_pool();
        let mut map = std::collections::HashMap::new();
        map.insert(
            KEY_ACCESS_RATE_LIMIT_RPS.to_string(),
            serde_json::json!(rps),
        );
        map.insert(
            KEY_ACCESS_RATE_LIMIT_BURST.to_string(),
            serde_json::json!(burst),
        );
        crate::settings::SettingsCache::new_with_map(pool, map, std::collections::HashMap::new())
    }

    #[test]
    fn handle_reuses_limiter_until_settings_change() {
        let handle = RateLimiterHandle::new();
        let s1 = settings_with_limits(100, 200);
        let a = handle.limiter(&s1);
        // Same settings → same limiter instance (no rebuild).
        let b = handle.limiter(&s1);
        assert!(Arc::ptr_eq(&a, &b));

        // Changed settings → a fresh limiter instance. A real settings mutation
        // both changes the limits and bumps the generation — model that by
        // bumping `s2`'s generation (the W6.3 fast path is keyed on it).
        let s2 = settings_with_limits(500, 900);
        s2.inner.bump_generation();
        let c = handle.limiter(&s2);
        assert!(!Arc::ptr_eq(&a, &c));

        // And it's stable again under the new settings.
        let d = handle.limiter(&s2);
        assert!(Arc::ptr_eq(&c, &d));
    }

    /// W6.3: a matching generation returns the cached limiter without reading
    /// the (different) limits from the new settings object — same `Arc`.
    #[test]
    fn limiter_fast_path_skips_settings_reads() {
        let handle = RateLimiterHandle::new();
        let a = settings_with_limits(100, 200); // generation 0
        let b = settings_with_limits(999, 999); // generation 0, different limits
        let l1 = handle.limiter(&a);
        // `b` shares `a`'s generation, so the fast path returns the cached
        // limiter without ever reading `b`'s (different) `access.rate_limit_*`.
        let l2 = handle.limiter(&b);
        assert!(
            Arc::ptr_eq(&l1, &l2),
            "generation match must skip the settings read and reuse the limiter"
        );
    }

    /// W6.3: a generation bump (with changed limits) forces a rebuild — a new
    /// `Arc`, not the cached one.
    #[test]
    fn limiter_resized_when_generation_changes() {
        let handle = RateLimiterHandle::new();
        let a = settings_with_limits(100, 200); // generation 0
        let l1 = handle.limiter(&a);
        // A mutation: the limits change AND the generation is bumped.
        let b = settings_with_limits(50, 100); // generation 0
        b.inner.bump_generation(); // generation 1
        let l2 = handle.limiter(&b);
        assert!(
            !Arc::ptr_eq(&l1, &l2),
            "a generation change must rebuild (resize) the limiter"
        );
    }

    /// BUG-18: a resize from a drained bucket must not grant a free burst.
    #[test]
    fn resize_preserves_drained_bucket() {
        let old = RateLimiter::new(1, 1);
        // Drain the single token.
        assert!(old.try_acquire().is_ok());
        assert!(old.try_acquire().is_err());
        // Resize to a much larger capacity — the balance must stay at 0,
        // not reset to the new capacity.
        let new = RateLimiter::resized(&old, 100, 100);
        // No free burst: the next acquire must still fail (or only succeed
        // after enough time for refill, which we haven't waited for).
        assert!(
            new.try_acquire().is_err(),
            "resized limiter must not grant a free burst on a drained bucket"
        );
    }

    /// BUG-18: resizing to a smaller burst caps the token balance.
    #[test]
    fn resize_shrinks_capacity() {
        let old = RateLimiter::new(100, 10);
        // Full bucket at burst=10.
        assert_eq!(old.tokens_for_test(), 10 * SCALE as u64);
        // Resize to burst=1: tokens must be capped to 1.
        let new = RateLimiter::resized(&old, 100, 1);
        assert_eq!(new.tokens_for_test(), SCALE as u64);
        // At most 1 immediate admission.
        assert!(new.try_acquire().is_ok());
        assert!(new.try_acquire().is_err());
    }
}
