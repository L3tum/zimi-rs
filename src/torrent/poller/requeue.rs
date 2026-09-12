//! Bounded-retry requeue guard (pure state machine, no DB/IO).
//!
//! A download row in `error` state whose message matches
//! [`REQUEUE_ERROR_PATTERN`] is re-queued for a **bounded** retry: after
//! `REQUEUE_GIVE_UP_AFTER` consecutive identical non-auth errors the guard
//! gives up and leaves the row in `error` for manual intervention (401/403
//! session-expiry messages are always exempt — a re-login is the fix, not a
//! human). Give-up is *sticky*: the row's guard entry is retained so
//! the next pass keeps seeing the give-up state instead of resetting to a
//! fresh failure. Entries are evicted **lazily**: only when the map hits
//! `MAX_LAST_ERROR_TRACKED` are entries not observed as error rows within
//! the last `GUARD_STALE_AFTER_PASSES` requeue passes swept. The 23505
//! collision pre-filter ([`filter_requeue_name_collisions`]) lives here too,
//! so the requeue UPDATE never trips the partial unique
//! `uq_downloads_active_name`.

use std::collections::{HashMap, HashSet};

/// Error messages that indicate a transient/connection-level failure worth
/// retrying. A row in `error` state whose `error` matches this pattern (case
/// insensitive, Postgres `~*`) is re-queued for a bounded retry.
///
/// Kept as a named constant so the regex (which is subtle — a plain substring
/// check would be wrong) can be unit-tested independently of the DB.
pub(crate) const REQUEUE_ERROR_PATTERN: &str = "connect|timeout|refused|unreachable|connection";

/// After this many consecutive bounded-retry cycles with the SAME
/// error message, the row is left `error` (manual intervention) instead of
/// being re-queued again. 401/403 session-expiry messages are exempt — a
/// re-login is the fix, not a human.
const REQUEUE_GIVE_UP_AFTER: u32 = 3;
/// Hard cap on the requeue guard map (one entry per row id cycling transient
/// errors). Entries are RETAINED on give-up (the give-up state must survive to
/// the next pass) and are only reclaimed by the lazy sweep once the row stops
/// appearing as an `error` row; the cap (with its pre-admission sweep of stale
/// entries) only stops pathological pile-up.
const MAX_LAST_ERROR_TRACKED: usize = 10_000;
/// Guard lazy-eviction window, in requeue passes: when the map hits
/// `MAX_LAST_ERROR_TRACKED`, entries not observed as error rows for this
/// many passes are swept before a new entry is admitted.
const GUARD_STALE_AFTER_PASSES: u64 = 100;

/// Whether an error message is an auth/session-expiry (HTTP 401/403).
/// Checks the token with word-boundary semantics (non-alphanumeric
/// boundaries) — a plain substring check would misclassify port numbers
/// or IDs ("connection refused: port 40152") as auth and exempt them
/// from the requeue give-up guard.
fn is_auth_error(msg: &str) -> bool {
    ["401", "403"].iter().any(|code| {
        msg.split(|c: char| !c.is_alphanumeric())
            .any(|tok| tok == *code)
    })
}

/// Requeue guard (pure): given the row's previous guard entry
/// (`Some((previous_message, consecutive_count))`) and the row's current
/// `error` message, whether the bounded retry loop should stop.
///
/// `true` only when the same non-auth message was already recorded
/// `REQUEUE_GIVE_UP_AFTER - 1` times in a row (making this occurrence the
/// `REQUEUE_GIVE_UP_AFTER`-th consecutive identical error). A different
/// message resets the count (returning `false`), and 401/403 (session
/// expiry) is always exempt — the next re-login is the fix, not manual
/// intervention.
fn should_give_up(prev: Option<(&str, u32)>, msg: &str, is_auth: bool) -> bool {
    if is_auth {
        return false;
    }
    matches!(prev, Some((pm, n)) if pm == msg && n >= REQUEUE_GIVE_UP_AFTER - 1)
}

/// One entry of the requeue guard (keyed by row id in
/// [`crate::torrent::poller::DownloadPoller::last_error`]): the last transient
/// error message observed for the row, how many times in a row, and the
/// requeue pass it was last observed on (the lazy-sweep timestamp: the sweep
/// is the only removal path — give-up retains the entry).
#[derive(Debug, Clone)]
pub(crate) struct GuardEntry {
    pub(crate) prev_msg: String,
    pub(crate) count: u32,
    /// Requeue pass (monotonic counter, see
    /// [`crate::torrent::poller::DownloadPoller::requeue_passes`])
    /// on which the row was last observed as an error row.
    pub(crate) last_seen: u64,
}

/// Guard lazy eviction (pure): remove entries not observed as error
/// rows within the last `GUARD_STALE_AFTER_PASSES` requeue passes and
/// return how many were evicted. Called only when the map hits
/// `MAX_LAST_ERROR_TRACKED`, so a recently-observed entry is never swept.
fn sweep_stale_guard_entries(guard: &mut HashMap<i32, GuardEntry>, pass: u64) -> usize {
    let horizon = pass.saturating_sub(GUARD_STALE_AFTER_PASSES);
    let stale: Vec<i32> = guard
        .iter()
        .filter(|(_, e)| e.last_seen < horizon)
        .map(|(&id, _)| id)
        .collect();
    for id in &stale {
        guard.remove(id);
    }
    stale.len()
}

/// Outcome of one stale `error` row passing through the requeue guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequeueDecision {
    /// Re-queue the row (it goes into the `id = ANY(…)` requeue batch).
    Requeue,
    /// Give up: leave the row in `error` for manual intervention (not re-queued).
    GiveUp,
}

/// Bounded-retry requeue guard, one row at a time (pure — no DB, no lock, so the
/// give-up contract is unit-testable without Postgres).
///
/// Decides whether the row is re-queued this pass and updates `guard` in
/// place:
///
/// - **Give up** (`should_give_up`): the row is *not* re-queued and its guard
///   entry is **retained** — *not* removed — with its `last_seen` refreshed to
///   `pass`. Retention is the whole point: a give-up row stays `error`
///   with the same message, so the *next* pass must still see the give-up state
///   (count already at the cap) instead of treating it as a fresh failure
///   (`prev = None` → count resets to 1) and re-queueing it — that one-shot
///   removal was the bug that made "bounded retry" unbounded. Once the row
///   leaves `error` by other means (cancel / complete) it stops being observed
///   as an error row, its `last_seen` freezes, and the lazy
///   [`sweep_stale_guard_entries`] reclaims the entry after
///   `GUARD_STALE_AFTER_PASSES` unobserved passes.
/// - **Re-queue**: record/refresh the row's entry, incrementing the
///   consecutive-identical-message count, and re-queue it. A brand-new entry is
///   admitted only while under `MAX_LAST_ERROR_TRACKED` (sweeping stale entries
///   first; failing open — re-queue without a guard entry — if nothing was
///   stale).
pub(crate) fn requeue_guard_step(
    guard: &mut HashMap<i32, GuardEntry>,
    id: i32,
    msg: &str,
    pass: u64,
) -> RequeueDecision {
    let is_auth = is_auth_error(msg);
    let prev = guard.get(&id).map(|e| (e.prev_msg.as_str(), e.count));
    if should_give_up(prev, msg, is_auth) {
        tracing::warn!(
            download_id = id,
            "requeue guard: same error {msg:?} {REQUEUE_GIVE_UP_AFTER}x in a row — leaving row in error state"
        );
        // Retain the entry (never remove) so the give-up state survives
        // to the next pass; refresh `last_seen` so the lazy sweep reclaims it
        // once the row leaves `error` by other means. On give-up the entry is
        // always present (`should_give_up` needs a prior count ≥ 2).
        if let Some(existing) = guard.get_mut(&id) {
            existing.last_seen = pass;
        }
        return RequeueDecision::GiveUp;
    }
    let count = prev.filter(|(m, _)| *m == msg).map_or(1, |(_, n)| n + 1);
    let entry = GuardEntry {
        prev_msg: msg.to_string(),
        count,
        last_seen: pass,
    };
    if let Some(existing) = guard.get_mut(&id) {
        *existing = entry;
    } else if guard.len() >= MAX_LAST_ERROR_TRACKED {
        // Cap reached: sweep entries whose rows stopped appearing as error
        // rows (left `error` by other means) more than
        // `GUARD_STALE_AFTER_PASSES` passes ago, then admit the new entry if
        // the sweep freed a slot. If nothing was stale, fail open: the row
        // retries without a guard entry (and without give-up protection).
        sweep_stale_guard_entries(guard, pass);
        if guard.len() < MAX_LAST_ERROR_TRACKED {
            guard.insert(id, entry);
        }
    } else {
        guard.insert(id, entry);
    }
    if count >= 2 {
        tracing::warn!(
            download_id = id,
            attempt = count,
            "requeue guard: same error {count}x, will stop after {REQUEUE_GIVE_UP_AFTER}"
        );
    }
    RequeueDecision::Requeue
}

/// 23505 name-collision pre-filter, pure: drop stale `error` rows that would trip the
/// partial unique `uq_downloads_active_name` if re-queued in this batch —
/// either because their `name` is already held by an ACTIVE row, or because
/// another stale `error` row in the same batch shares it.
///
/// The partial unique `uq_downloads_active_name` (name UNIQUE where status IN
/// queued/downloading) covers active-vs-active only — an `error` row may share
/// a `name` with an active one. Re-queueing such a row (`status='error'` →
/// `queued`) would trip 23505, which the guarded UPDATE surfaces as an error
/// the tick treats as fatal — aborting the whole tick (and, with `updated_at`
/// never advancing, re-picking the same row every tick until the active row
/// goes terminal). Skipping the colliding rows here (they stay in `error` and
/// retry on a later pass once the colliding active row leaves the active
/// states) keeps the requeue UPDATE from ever seeing a collision. Returns the
/// `(id, name, error)` rows that may be safely re-queued.
pub(crate) fn filter_requeue_name_collisions(
    stale: &[(i32, String, String)],
    active_names: &HashSet<String>,
) -> Vec<(i32, String, String)> {
    // Two stale `error` rows sharing a name (re-adding a previously-failed
    // name is legal — only active-vs-active is constrained) would both flip
    // to `queued` in one batched UPDATE and still trip the partial unique.
    // Keep only the newest (highest-id) candidate per name; the older
    // duplicates stay in `error` and can be retried later.
    let mut best_id: HashMap<String, i32> = HashMap::new();
    for (id, name, _) in stale {
        if active_names.contains(name) {
            continue;
        }
        match best_id.get_mut(name) {
            Some(prev) => *prev = (*prev).max(*id),
            None => {
                best_id.insert(name.clone(), *id);
            }
        }
    }
    stale
        .iter()
        .filter(|(id, name, _)| best_id.get(name) == Some(id))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pure requeue guard gives up only on the
    /// `REQUEUE_GIVE_UP_AFTER`-th consecutive IDENTICAL non-auth error; a
    /// different message resets the count, and 401/403 is always exempt.
    #[test]
    fn requeue_guard_gives_up_on_third_identical_error() {
        const MSG: &str = "connection refused";
        const AUTH: &str = "connection failed: HTTP 401";
        // 1st and 2nd identical errors → keep retrying; 3rd → give up.
        assert!(!super::should_give_up(None, MSG, false));
        assert!(!super::should_give_up(Some((MSG, 1)), MSG, false));
        assert!(super::should_give_up(Some((MSG, 2)), MSG, false));
        // A different message resets the consecutive count.
        assert!(!super::should_give_up(Some(("timeout", 2)), MSG, false));
        // 401/403 (session expiry) is always exempt — the re-login path fixes it.
        assert!(!super::should_give_up(Some((AUTH, 2)), AUTH, true));
        assert!(!super::should_give_up(
            Some(("HTTP 403", 2)),
            "HTTP 403",
            true
        ));
        assert!(!super::should_give_up(None, AUTH, true));
    }

    /// Lazy eviction: the sweep removes only entries not observed
    /// as error rows within the last `GUARD_STALE_AFTER_PASSES` requeue
    /// passes (rows that left `error` by other means — e.g. a manual
    /// re-queue — otherwise leak until the cap); the horizon saturates to
    /// 0 before the window has ever been filled, sweeping nothing.
    #[test]
    fn guard_sweep_evicts_only_unobserved_entries() {
        let entry = |last_seen: u64| GuardEntry {
            prev_msg: "m".into(),
            count: 1,
            last_seen,
        };
        let mut guard: std::collections::HashMap<i32, GuardEntry> =
            std::collections::HashMap::new();
        guard.insert(1, entry(399)); // 101 passes stale → swept
        guard.insert(2, entry(400)); // exactly at the horizon → kept
        guard.insert(3, entry(450)); // recent → kept
        let n = sweep_stale_guard_entries(&mut guard, 500);
        assert_eq!(n, 1, "only the 101-pass-stale entry is swept");
        assert!(!guard.contains_key(&1));
        assert!(guard.contains_key(&2));
        assert!(guard.contains_key(&3));

        // Fewer total passes than the window: nothing is swept.
        let mut fresh: std::collections::HashMap<i32, GuardEntry> =
            std::collections::HashMap::new();
        fresh.insert(9, entry(1));
        assert_eq!(sweep_stale_guard_entries(&mut fresh, 50), 0);
        assert!(fresh.contains_key(&9));
    }

    /// Auth detection must use word boundaries: port numbers/IDs that
    /// merely CONTAIN "401"/"403" ("port 40152") are not session
    /// expiry and must NOT be exempt from the give-up guard.
    #[test]
    fn is_auth_error_requires_word_boundaries() {
        assert!(super::is_auth_error("connection failed: HTTP 401"));
        assert!(super::is_auth_error("request failed: HTTP 403"));
        assert!(!super::is_auth_error("connection refused: port 40152"));
        assert!(!super::is_auth_error("download failed"));
    }

    /// Give-up is STICKY, not one-shot: the moment a row
    /// gives up, its guard entry is retained (not removed), so the very
    /// next pass — the row still `error` with the same message — keeps
    /// giving up instead of treating it as a fresh failure (count resets
    /// to 1) and re-queueing it forever. While the row stays in `error` it
    /// keeps being observed, so `last_seen` is refreshed each pass and it
    /// is never swept. Once the row leaves `error` by other means
    /// (cancel/complete) it stops being observed, `last_seen` freezes, and
    /// the lazy stale sweep reclaims the entry after
    /// `GUARD_STALE_AFTER_PASSES` unobserved passes.
    #[test]
    fn requeue_guard_give_up_is_sticky_not_one_shot() {
        let mut guard: std::collections::HashMap<i32, GuardEntry> =
            std::collections::HashMap::new();
        const ID: i32 = 42;
        const MSG: &str = "connection refused";
        // First two identical errors → keep retrying (count climbs 1→2).
        assert_eq!(
            requeue_guard_step(&mut guard, ID, MSG, 1),
            RequeueDecision::Requeue
        );
        assert_eq!(
            requeue_guard_step(&mut guard, ID, MSG, 2),
            RequeueDecision::Requeue
        );
        // Third identical error → give up, and the entry must be RETAINED.
        assert_eq!(
            requeue_guard_step(&mut guard, ID, MSG, 3),
            RequeueDecision::GiveUp
        );
        assert!(
            guard.contains_key(&ID),
            "give-up must retain the guard entry (the one-shot-removal bug)"
        );
        assert_eq!(guard[&ID].last_seen, 3);
        // The next pass (row still error, same message) must still give up
        // rather than reset the count to 1 and re-queue it, and keep the
        // entry fresh while the row stays in `error`.
        assert_eq!(
            requeue_guard_step(&mut guard, ID, MSG, 4),
            RequeueDecision::GiveUp
        );
        assert_eq!(guard[&ID].last_seen, 4);
        assert_eq!(
            requeue_guard_step(&mut guard, ID, MSG, 50),
            RequeueDecision::GiveUp
        );
        assert_eq!(guard[&ID].last_seen, 50);

        // The row leaves `error` (cancel/complete) at pass 50: it stops
        // being observed, `last_seen` freezes at 50, and the lazy sweep
        // reclaims the entry once it is > `GUARD_STALE_AFTER_PASSES` (100)
        // passes old.
        let horizon = 50 + super::GUARD_STALE_AFTER_PASSES;
        assert!(
            sweep_stale_guard_entries(&mut guard, horizon) == 0,
            "at the sweep horizon the entry is still kept"
        );
        assert!(guard.contains_key(&ID));
        assert!(
            sweep_stale_guard_entries(&mut guard, horizon + 1) == 1,
            "just past the horizon the entry is swept"
        );
        assert!(
            !guard.contains_key(&ID),
            "stale give-up entry must be reclaimed"
        );
    }

    /// 23505 name-collision pre-filter: a stale `error` row whose `name` is already
    /// held by an ACTIVE (`queued`/`downloading`) row must be skipped, and
    /// two stale `error` rows sharing a name must collapse to the newest —
    /// re-queueing both would trip the partial unique `uq_downloads_active_name`
    /// and abort the whole tick — while a non-colliding `error` row is
    /// still re-queued. The filter is pure (no DB), so it is testable
    /// without Postgres: seed an in-memory active name set and stale error
    /// rows, and check the colliding ids are dropped and the rest kept.
    #[test]
    fn requeue_skips_error_rows_whose_name_is_held_by_an_active_row() {
        // Seed: one active row `dupe`, plus two stale error rows — one
        // sharing that name (collides), one not.
        let active: std::collections::HashSet<String> = ["dupe".to_string()].into_iter().collect();
        let stale = vec![
            (1, "dupe".to_string(), "connection refused".to_string()),
            (
                2,
                "other".to_string(),
                "timeout while connecting".to_string(),
            ),
        ];
        let kept = filter_requeue_name_collisions(&stale, &active);
        assert_eq!(
            kept.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            vec![2],
            "only the non-colliding error row is requeued"
        );
        // No active rows → nothing collides, everything is requeued.
        let none: std::collections::HashSet<String> = Default::default();
        assert_eq!(
            filter_requeue_name_collisions(&stale, &none)
                .iter()
                .map(|(id, _, _)| *id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        // Two stale error rows sharing a name (re-adding a previously-
        // failed name) → only the newest (highest id) is requeued; the
        // older duplicate stays in `error` for a later pass.
        let stale_dupes = vec![
            (5, "dupe".to_string(), "older failure".to_string()),
            (9, "dupe".to_string(), "newer failure".to_string()),
        ];
        assert_eq!(
            filter_requeue_name_collisions(&stale_dupes, &none)
                .iter()
                .map(|(id, _, _)| *id)
                .collect::<Vec<_>>(),
            vec![9],
            "only the newest same-name error row is requeued"
        );
    }
}
