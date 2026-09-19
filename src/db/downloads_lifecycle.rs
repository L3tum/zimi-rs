//! Download **lifecycle** state machine — the single home for every status
//! (and hash / error / seed-stats) transition on the `downloads` table.
//!
//! The poller (`torrent::poller`) drives this machine. Centralizing the
//! WHERE-guarded `UPDATE`s here (ARCH M-2/M-3) makes the transitions a
//! reviewable, *enumerable* artifact: every place a row's status, hash, error,
//! or seed stats changes lives in this module, and every status value is
//! rendered from the `DownloadStatus` enum (never a raw literal). The intended
//! graph is `DownloadStatus::can_transition_to`; the per-statement `WHERE
//! status …` guards are the enforcement, and the read-side `IN (…)` filters
//! render their sets through `in_list`.
//!
//! **Scope note:** read-only row fetches (in-flight / queued / reconcile
//! selects, the skip-tick count, the orphan-`.part` path set) and the per-tick
//! stats `unnest` batch stay in the poller — they read or refresh columns
//! without changing the row's *status*, so they are not state transitions.
//!
//! Query layer: raw SQL via [`crate::db::raw`]. `updated_at = now()` stays
//! a *server-side* timestamp (the SQL `now()`, never a Rust-side clock), so
//! the SQL semantics match the old raw statements exactly.
use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::{Error, Result};
use sqlx::postgres::PgConnection;

// ── Status enum ────────────────────────────────────────────────────────────

/// The download lifecycle — single source of truth for every value stored in
/// `downloads.status` (ARCH-H1).
///
/// Previously the six status strings lived as ~55 hardcoded literals across
/// the poller submodules, `opds.rs`, and the downloads handler; a lifecycle
/// change touched 4–5 files of raw SQL and a wrong literal silently no-oped
/// (an `UPDATE … WHERE status = '…'` that matches nothing is a success from
/// the driver's point of view). All production SQL now renders status values
/// through [`DownloadStatus::as_str`] / [`in_list`], so a typo is a compile
/// error, and the intended state machine is pinned by
/// `transition_table_matches_production_guards`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DownloadStatus {
    /// Waiting to start (nothing in progress yet).
    Queued,
    /// In progress: a torrent or direct file the poller is tracking.
    Downloading,
    /// Downloaded, verified, and installed into the ZIM directory.
    Complete,
    /// Installed and still sharing with the swarm (`torrent.keep_completed`
    /// on, seed ratio capped); settles back to `Complete` when seeding ends.
    Seeding,
    /// Failed; the row's `error` column carries the last message.
    Error,
    /// Cancelled by the user (the row is kept for history).
    Cancelled,
}

impl DownloadStatus {
    /// Every status, in lifecycle order.
    pub const ALL: [DownloadStatus; 6] = [
        DownloadStatus::Queued,
        DownloadStatus::Downloading,
        DownloadStatus::Complete,
        DownloadStatus::Seeding,
        DownloadStatus::Error,
        DownloadStatus::Cancelled,
    ];

    /// The value stored in `downloads.status`.
    pub const fn as_str(self) -> &'static str {
        match self {
            DownloadStatus::Queued => "queued",
            DownloadStatus::Downloading => "downloading",
            DownloadStatus::Complete => "complete",
            DownloadStatus::Seeding => "seeding",
            DownloadStatus::Error => "error",
            DownloadStatus::Cancelled => "cancelled",
        }
    }

    /// Parse a stored value. `None` for unknown values (defensive — the DB
    /// contents are trusted, but callers must not panic on a legacy row).
    #[cfg(test)]
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|st| st.as_str() == s)
    }

    /// The intended state machine: may a row in `self` transition to
    /// `target`? This is the documented spec that the production SQL guards
    /// must agree with (pinned by `transition_table_matches_production_guards`;
    /// the poller's per-row `WHERE status …` guards are the enforcement).
    ///
    /// Notable non-obvious edges:
    /// - `Downloading → Queued` exists only for startup reconcile of
    ///   interrupted *direct* downloads (`.part` resume, PERF-11) and the
    ///   bounded 10-minute error retry.
    /// - `Error → Queued` is the bounded error retry (tick);
    ///   a repeated give-up error stays `error` for manual intervention.
    /// - `Seeding → Complete` is settlement (torrent gone or fatal in qB).
    /// - Nothing leaves `Cancelled` except manual DB surgery; cancelled rows
    ///   are terminal from the poller's point of view.
    pub fn can_transition_to(self, target: Self) -> bool {
        use DownloadStatus::*;
        matches!(
            (self, target),
            (Queued, Downloading | Error | Cancelled)
                | (Downloading, Complete | Seeding | Error | Cancelled | Queued)
                | (Error, Queued)
                | (Seeding, Complete)
        )
    }
}

/// Render `statuses` as a SQL `IN` list: `('queued', 'downloading')`.
///
/// Used for the read-side filters (rows to inspect / count), where the set is
/// "statuses of interest" rather than a transition guard; transition guards
/// keep their per-site exact membership but render their values through this
/// helper as well, so no status literal ever appears in the source.
pub fn in_list(statuses: impl IntoIterator<Item = DownloadStatus>) -> String {
    format!(
        "({})",
        statuses
            .into_iter()
            .map(|s| format!("'{}'", s.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

// ── Status reads ───────────────────────────────────────────────────────────
// The cancel-check core shared by every in-flight path. A row's status is
// re-read before each mutation so a cancel always wins over a racing
// finalize.

/// Read a row's status, fail-open: a transient DB blip must not abort an
/// in-flight download (the next periodic check re-runs). Returns `Some(status)`.
pub async fn status_of(client: &mut PgConnection, id: i32) -> Option<String> {
    let status: Option<String> = match raw::fetch_scalar_optional(
        client,
        "SELECT status FROM downloads WHERE id = $1",
        |q| q.bind(id),
    )
    .await
    {
        Ok(status) => status,
        Err(e) => {
            tracing::warn!("status check for download {id} failed: {e}");
            return None;
        }
    };
    status
}

/// Read a row's status, hard-failing on any query error (incl. a missing
/// row — the caller is deciding cancel/finalize and a vanished row must not
/// silently pass) — for the site where a DB blip must not mask a cancel.
pub async fn status_checked(pool: &Pool, id: i32) -> Result<String> {
    let status: Option<String> =
        raw::fetch_scalar_optional(pool, "SELECT status FROM downloads WHERE id = $1", |q| {
            q.bind(id)
        })
        .await?;
    status.ok_or_else(|| Error::NotFound(format!("download {id} does not exist")))
}

/// Re-read a row's status against `expected`; return `Some(status)` when it
/// has changed (the caller must bail before any further mutation), `None`
/// while it still is — including when the check query itself fails (fail-open).
pub async fn status_if_changed(pool: &Pool, id: i32, expected: &str) -> Option<String> {
    match status_checked(pool, id).await {
        Ok(status) => (status != expected).then_some(status),
        Err(_) => None,
    }
}

/// Pure cancel-check filter: `None` while still downloading (proceed),
/// `Some(s)` the moment it leaves that state (bail). Named so the unit test
/// pins the production filter itself rather than a copy.
fn no_longer_downloading(s: &str) -> Option<String> {
    (s != DownloadStatus::Downloading.as_str()).then_some(s.to_string())
}

/// Re-read a row's status; return `Some(status)` when it is no longer
/// `downloading` (the caller must bail), `None` while it still is (fail-open
/// on a query blip). Takes an already-checked-out `&mut PgConnection` so the
/// caller can pair the read with a follow-up write on the same connection.
pub async fn status_no_longer_downloading(client: &mut PgConnection, id: i32) -> Option<String> {
    status_of(client, id)
        .await
        .and_then(|s| no_longer_downloading(&s))
}

// ── Transitions ────────────────────────────────────────────────────────────

/// `queued → downloading` (direct `.zim` claim). The `AND status = 'queued'`
/// guard makes the claim atomic: the first claimant wins, so a concurrent tick
/// cannot double-spawn the same download. Returns the row count (0 = someone
/// else already claimed it).
pub async fn claim_direct(pool: &Pool, id: i32, part: &str) -> Result<u64> {
    raw::execute(
        pool,
        "UPDATE downloads SET status = $1, file_path = $2, updated_at = now() \
         WHERE id = $3 AND status = $4",
        |q| {
            q.bind(DownloadStatus::Downloading.as_str())
                .bind(part)
                .bind(id)
                .bind(DownloadStatus::Queued.as_str())
        },
    )
    .await
}

/// `* → error` (terminal-state guard). A failure is recorded without
/// clobbering a terminal state — a cancel racing the failure path must win.
/// Best-effort: a pool blip is logged and swallowed (the row is retried next
/// tick / stays in its current state).
pub async fn mark_error(pool: &Pool, id: i32, msg: &str) {
    let _ = raw::execute(
        pool,
        "UPDATE downloads SET status = $1, error = $2, updated_at = now() \
         WHERE id = $3 AND status NOT IN ($4, $5, $6, $7)",
        |q| {
            q.bind(DownloadStatus::Error.as_str())
                .bind(msg)
                .bind(id)
                .bind(DownloadStatus::Cancelled.as_str())
                .bind(DownloadStatus::Complete.as_str())
                .bind(DownloadStatus::Seeding.as_str())
                .bind(DownloadStatus::Error.as_str())
        },
    )
    .await;
}

/// `queued | downloading → error` (fatal torrent state / torrent vanished
/// after the grace period / qB add failure / qB not configured / install
/// failure). The `AND status IN ('queued', 'downloading')` guard — exactly
/// the statuses [`fatal_guard_statuses`] names — means a cancel (or a
/// settlement to `complete`/`seeding`) landing between the row fetch and
/// this update still wins: a racing failure mark never undoes a terminal
/// state (the worst case was a multi-GB verify+install whose failure would
/// flip a `cancelled` row back to `error`). Returns the row count (0 = the
/// row changed state underneath us — or the write was swallowed below; the
/// caller logs and takes no further compensating action).
/// Propagates a pool-checkout failure (aborts the tick, as before) but
/// swallows the write failure.
pub async fn mark_fatal_error(pool: &Pool, id: i32, msg: &str) -> Result<u64> {
    let sql = mark_fatal_error_sql();
    execute_best_effort(pool, &sql, |q| {
        q.bind(DownloadStatus::Error.as_str()).bind(msg).bind(id)
    })
    .await
}

/// The guard set for [`mark_fatal_error`]: the only statuses a fatal mark is
/// legitimate from — `queued` (the enqueue path) and `downloading` (the
/// in-flight path). Every other status, in particular terminal `cancelled`,
/// is excluded so a cancel always wins over a racing failure mark.
pub fn fatal_guard_statuses() -> [DownloadStatus; 2] {
    [DownloadStatus::Queued, DownloadStatus::Downloading]
}

/// The rendered SQL for [`mark_fatal_error`] (a `fn`, not a `const`, so the
/// guard set renders through [`in_list`]). Pinned by
/// `transition_table_matches_production_guards`.
fn mark_fatal_error_sql() -> String {
    format!(
        "UPDATE downloads SET status = $1, error = $2, updated_at = now() \
         WHERE id = $3 AND status IN {}",
        in_list(fatal_guard_statuses())
    )
}

/// `error → queued` (bounded stale-error retry). The `AND status = 'error'`
/// guard means a row that moved on between the select and this update (e.g.
/// manually cancelled) is never clobbered. Returns the row count requeued.
pub async fn requeue_stale_errors(pool: &Pool, ids: &[i32]) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    // The id list is bound as one `int[]` (`id = ANY($2)`) — equivalent to
    // the `id IN (…)` list, dynamic length.
    raw::execute(
        pool,
        "UPDATE downloads SET status = $1, error = NULL, updated_at = now() \
         WHERE id = ANY($2) AND status = $3",
        |q| {
            q.bind(DownloadStatus::Queued.as_str())
                .bind(ids)
                .bind(DownloadStatus::Error.as_str())
        },
    )
    .await
}

/// `downloading → complete` (direct-download finalize). The `AND status =
/// 'downloading'` guard means a cancel landing between the pre-rename check and
/// this update still wins — never clobber a terminal state. `sha256` is the
/// MEASURED digest of the installed bytes (see the finalize path): when the
/// row carried a catalog claim, the bytes already matched it (a mismatch
/// never reaches this point), so storing the measured value both records the
/// provenance and — for claim-less rows — makes the settled row carry the
/// digest the queue guard's version-key compares against. Returns the row
/// count (0 = cancelled mid-finalize; the caller discards the staged file).
pub async fn finalize_direct(pool: &Pool, id: i32, dst: &str, sha256: Option<&str>) -> Result<u64> {
    raw::execute(
        pool,
        "UPDATE downloads SET status = $1, progress = 1.0, file_path = $2, sha256 = $3, \
         updated_at = now() WHERE id = $4 AND status = $5",
        |q| {
            q.bind(DownloadStatus::Complete.as_str())
                .bind(dst)
                .bind(sha256)
                .bind(id)
                .bind(DownloadStatus::Downloading.as_str())
        },
    )
    .await
}

/// Read a row's catalog digest claim (the `sha256` value recorded when the
/// OPDS queue enqueued it; `None` when the source declared nothing). For
/// rows that never settled from this path it is the raw claim; after a
/// direct finalize it is the measured digest (which equals the claim when
/// one was present).
pub async fn claim_digest(pool: &Pool, id: i32) -> Result<Option<String>> {
    // The column is nullable (no claim) — decode as `Option<String>` so a
    // NULL decodes to `None` instead of a decode error (a claim-less row is
    // the current Kiwix catalog shape, not an edge case).
    let claim: Option<Option<String>> =
        raw::fetch_scalar_optional(pool, "SELECT sha256 FROM downloads WHERE id = $1", |q| {
            q.bind(id)
        })
        .await?;
    Ok(claim.flatten())
}

/// The previously observed digest for the SAME identity (SEC drift
/// detection): the most recent settled (`complete`/`seeding`) row for the
/// same source URL, excluding this row, that recorded a digest. `None`
/// means this URL has no integrity history (first observation — or a NULL
/// legacy record — drift cannot be detected), so the install path must not
/// flag it. Distinct URLs are distinct identities and never compare.
pub async fn prior_observed_digest_by_url(
    pool: &Pool,
    id: i32,
    url: &str,
) -> Result<Option<String>> {
    raw::fetch_scalar_optional(
        pool,
        // `id DESC` tie-breaks equal `updated_at` (same-millisecond settles
        // — a re-queued version of the same identity always gets a larger
        // id, so it is the newer observation).
        "SELECT sha256 FROM downloads \
         WHERE url = $1 AND id <> $2 AND sha256 IS NOT NULL \
         AND status IN ($3, $4) ORDER BY updated_at DESC, id DESC LIMIT 1",
        |q| {
            q.bind(url)
                .bind(id)
                .bind(DownloadStatus::Complete.as_str())
                .bind(DownloadStatus::Seeding.as_str())
        },
    )
    .await
}

/// The previously observed digest for the SAME identity on the torrent
/// path: keyed by torrent info-hash (the torrent's content identity) with
/// the same semantics as [`prior_observed_digest_by_url`].
pub async fn prior_observed_digest_by_hash(
    pool: &Pool,
    id: i32,
    hash: &str,
) -> Result<Option<String>> {
    raw::fetch_scalar_optional(
        pool,
        // `id DESC` tie-break — see `prior_observed_digest_by_url`.
        "SELECT sha256 FROM downloads \
         WHERE hash = $1 AND id <> $2 AND sha256 IS NOT NULL \
         AND status IN ($3, $4) ORDER BY updated_at DESC, id DESC LIMIT 1",
        |q| {
            q.bind(hash)
                .bind(id)
                .bind(DownloadStatus::Complete.as_str())
                .bind(DownloadStatus::Seeding.as_str())
        },
    )
    .await
}

/// The drift decision itself (SEC, pure — both install paths call this so
/// the rule lives in one place): a later download of the SAME identity
/// yields drift iff a previous observed digest exists AND differs from the
/// current one. `None` previous observation (first observation for the
/// identity, or a NULL legacy record) is NEVER drift — there is nothing to
/// compare against. Comparison is case-insensitive (the CHECK constraints
/// pin lowercase, but a hand-edited row must not flip the flag).
pub fn drift_decision(prev: Option<&str>, current: &str) -> bool {
    matches!(prev, Some(p) if !p.eq_ignore_ascii_case(current))
}

/// Record the observed SHA-256 of the installed bytes on a SETTLED torrent
/// row (the provenance record + drift baseline; `mark_complete` does not
/// write digests, so this runs after it). Status-guarded on the settled
/// statuses so a racing settle/cancel wins over a late write. Returns the
/// row count (0 = the row left the settled states — a no-op, harmless).
pub async fn record_observed_digest(pool: &Pool, id: i32, sha256: &str) -> Result<u64> {
    raw::execute(
        pool,
        "UPDATE downloads SET sha256 = $1, updated_at = now() \
         WHERE id = $2 AND status IN ($3, $4)",
        |q| {
            q.bind(sha256)
                .bind(id)
                .bind(DownloadStatus::Complete.as_str())
                .bind(DownloadStatus::Seeding.as_str())
        },
    )
    .await
}

/// `downloading | complete → (complete | seeding)` (the torrent-completion
/// settlement). The guard is the shared [`completion_guard_statuses`] const:
/// `downloading` (fresh download) and `complete` (startup recovery of a
/// `complete` row whose file was deleted and is being reinstalled) are both
/// covered; `cancelled` is excluded so a cancel always wins. Seeding rows keep
/// their seed stats, everything else clears them. Returns the row count
/// (0 = a cancel raced the install; the caller discards).
///
/// `new_status` is the settled status (`seeding` when `keep_completed` + qB
/// present, else `complete`); the seed stats (`ratio`, `up_speed_bps`,
/// `num_seeds`) are `Some` only for the seeding arm. The old SQL's
/// `CASE WHEN $3 = 'seeding' …` arms are the same branch in Rust here.
pub async fn mark_complete(
    pool: &Pool,
    id: i32,
    dst: &str,
    new_status: DownloadStatus,
    ratio: Option<f32>,
    up_speed_bps: Option<i64>,
    num_seeds: Option<i64>,
) -> Result<u64> {
    let [guard1, guard2] = completion_guard_statuses();
    let updated = match new_status {
        DownloadStatus::Seeding => {
            raw::execute(
                pool,
                "UPDATE downloads SET status = $1, progress = 1.0, file_path = $2, \
             ratio = $3, up_speed_bps = $4, num_seeds = $5, error = NULL, updated_at = now() \
             WHERE id = $6 AND status IN ($7, $8)",
                |q| {
                    q.bind(DownloadStatus::Seeding.as_str())
                        .bind(dst)
                        .bind(ratio)
                        .bind(up_speed_bps)
                        .bind(num_seeds)
                        .bind(id)
                        .bind(guard1.as_str())
                        .bind(guard2.as_str())
                },
            )
            .await?
        }
        _ => {
            raw::execute(
                pool,
                "UPDATE downloads SET status = $1, progress = 1.0, file_path = $2, \
             ratio = NULL, up_speed_bps = NULL, num_seeds = NULL, error = NULL, \
             updated_at = now() WHERE id = $3 AND status IN ($4, $5)",
                |q| {
                    q.bind(DownloadStatus::Complete.as_str())
                        .bind(dst)
                        .bind(id)
                        .bind(guard1.as_str())
                        .bind(guard2.as_str())
                },
            )
            .await?
        }
    };
    Ok(updated)
}

/// The guard set for [`mark_complete`]: the statuses its final `UPDATE` is
/// allowed to transition from. `cancelled` (and any other terminal state) is
/// deliberately excluded so a cancel always wins over a finishing install;
/// `complete` is included so the startup-recovery path (reinstalling a
/// `complete` row's deleted file) doesn't take the `updated == 0` discard
/// branch and delete the file it just reinstalled.
pub fn completion_guard_statuses() -> [DownloadStatus; 2] {
    [DownloadStatus::Downloading, DownloadStatus::Complete]
}

/// `queued → downloading` (a torrent row was added to qBittorrent this
/// tick). The `AND status = 'queued'` guard mirrors [`claim_direct`]'s
/// atomic-claim pattern: a cancel landing between the `queued` select and
/// the qB `add_torrent` round-trip must win — an `cancelled` row is never
/// silently flipped back to `downloading` and driven to completion.
/// Returns the row count (0 = the row changed state mid-flight; the caller
/// must remove the just-added torrent from qB and skip the rest of the row's
/// processing — no error mark, the row is cancelled or otherwise terminal).
/// Propagates a pool-checkout failure but swallows the write failure.
pub async fn mark_downloading(pool: &Pool, id: i32) -> Result<u64> {
    let sql = mark_downloading_sql();
    execute_best_effort(pool, &sql, |q| {
        q.bind(DownloadStatus::Downloading.as_str()).bind(id)
    })
    .await
}

/// The rendered SQL for [`mark_downloading`] (a `fn`, not a `const`, so the
/// guard renders through [`DownloadStatus::as_str`]). Pinned by
/// `transition_table_matches_production_guards`.
fn mark_downloading_sql() -> String {
    format!(
        "UPDATE downloads SET status = $1, updated_at = now() \
         WHERE id = $2 AND status = '{}'",
        DownloadStatus::Queued.as_str()
    )
}

/// Bind a row's torrent `hash` (rebind on a hash/name match). Propagates a
/// pool-checkout failure but swallows the write failure.
pub async fn bind_hash(pool: &Pool, id: i32, hash: &str) -> Result<()> {
    execute_best_effort(
        pool,
        "UPDATE downloads SET hash = $1, updated_at = now() WHERE id = $2",
        |q| q.bind(hash).bind(id),
    )
    .await
    .map(|_| ())
}

/// Refresh a `downloading` row's `updated_at` to reflect torrent
/// *visibility*, not just stat changes: the missing-torrent grace clock is
/// measured from `updated_at`, but the row is only written when its stats
/// actually change — so a stalled torrent (no progress/speed change for
/// minutes) would sit with a stale `updated_at` and get errored the instant
/// it leaves qBittorrent (grace already exhausted), rather than aging out
/// after being genuinely *invisible* for the full grace window. Status-
/// guarded like its siblings so a racing cancel/settlement wins; returns the
/// row count (0 = the row left `downloading` in the meantime — harmless).
pub async fn touch_downloading(pool: &Pool, id: i32) -> Result<u64> {
    execute_best_effort(
        pool,
        "UPDATE downloads SET updated_at = now() WHERE id = $1 AND status = $2",
        |q| q.bind(id).bind(DownloadStatus::Downloading.as_str()),
    )
    .await
}

/// Clear a row's stale `hash` binding (cancel cleanup, once the qB removal is
/// confirmed).
pub async fn clear_hash(pool: &Pool, id: i32) {
    let _ = raw::execute(
        pool,
        "UPDATE downloads SET hash = NULL, updated_at = now() WHERE id = $1",
        |q| q.bind(id),
    )
    .await;
}

/// Drain the `hash` bindings on all `cancelled` rows (no qB configured): once
/// the hash is NULL the rows early-exit and stop re-running the per-tick
/// bookkeeping queries (BUG-21). Returns the row count cleared.
pub async fn drain_cancelled_hashes(pool: &Pool) -> Result<u64> {
    raw::execute(
        pool,
        "UPDATE downloads SET hash = NULL WHERE status = $1 AND hash IS NOT NULL",
        |q| q.bind(DownloadStatus::Cancelled.as_str()),
    )
    .await
}

/// `seeding → complete` (settle a seeding row: the torrent left qB after the
/// grace period, or it reports a fatal state — the ZIM is already installed,
/// so a fatal torrent is noted but not a failure). `error` carries the qB
/// error note for the fatal arm. Propagates a pool-checkout failure but
/// swallows the write failure.
pub async fn settle_seeding(pool: &Pool, id: i32, error: Option<&str>) -> Result<()> {
    match error {
        Some(msg) => execute_best_effort(
            pool,
            "UPDATE downloads SET status = $1, error = $2, updated_at = now() \
             WHERE id = $3 AND status = $4",
            |q| {
                q.bind(DownloadStatus::Complete.as_str())
                    .bind(msg)
                    .bind(id)
                    .bind(DownloadStatus::Seeding.as_str())
            },
        )
        .await
        .map(|_| ()),
        None => execute_best_effort(
            pool,
            "UPDATE downloads SET status = $1, updated_at = now() WHERE id = $2 AND status = $3",
            |q| {
                q.bind(DownloadStatus::Complete.as_str())
                    .bind(id)
                    .bind(DownloadStatus::Seeding.as_str())
            },
        )
        .await
        .map(|_| ()),
    }
}

/// Refresh a seeding row's per-torrent seed stats (ratio / up_speed / seeds /
/// speed). Guarded on `status = 'seeding'` so a settle racing the refresh
/// wins. Propagates a pool-checkout failure but swallows the write failure.
pub async fn refresh_seeding_stats(
    pool: &Pool,
    id: i32,
    ratio: f32,
    up_speed_bps: i64,
    num_seeds: i64,
    speed_bps: i64,
) -> Result<()> {
    execute_best_effort(
        pool,
        "UPDATE downloads SET ratio = $1, up_speed_bps = $2, num_seeds = $3, \
         speed_bps = $4, updated_at = now() WHERE id = $5 AND status = $6",
        |q| {
            q.bind(ratio)
                .bind(up_speed_bps)
                .bind(num_seeds)
                .bind(speed_bps)
                .bind(id)
                .bind(DownloadStatus::Seeding.as_str())
        },
    )
    .await
    .map(|_| ())
}

/// Refresh an in-progress row's `progress` / `speed_bps` (the 2 s stream tick
/// in the direct-download path). Propagates a pool-checkout failure (the
/// caller aborts the stream, as before) but swallows the write failure
/// (a stats blip must not discard a healthy in-flight download). Unguarded on
/// status by design — the in-loop cancel check handles the terminal-state
/// race; this only touches stat columns.
pub async fn refresh_progress(pool: &Pool, id: i32, progress: f32, speed_bps: i64) -> Result<()> {
    execute_best_effort(
        pool,
        "UPDATE downloads SET progress = $1, speed_bps = $2, updated_at = now() WHERE id = $3",
        |q| q.bind(progress).bind(speed_bps).bind(id),
    )
    .await
    .map(|_| ())
}

/// `downloading → queued` (startup reconcile of interrupted *direct* `.zim`
/// downloads: clear `file_path`/`error` so the next tick re-claims them and
/// resumes from a surviving `.part`, PERF-11). Returns the row count retried.
/// The `.zim` predicate stays a shared SQL fragment
/// ([`crate::db::downloads::ZIM_URL_PREDICATE`], also spliced into the
/// poller's raw SQL).
pub async fn retry_interrupted_directs(pool: &Pool) -> Result<u64> {
    let pred = crate::db::downloads::ZIM_URL_PREDICATE;
    let sql = format!(
        "UPDATE downloads SET status = $1, file_path = NULL, error = NULL, updated_at = now() \
         WHERE status = $2 AND ({pred}) LIKE '%.zim'"
    );
    raw::execute(pool, &sql, |q| {
        q.bind(DownloadStatus::Queued.as_str())
            .bind(DownloadStatus::Downloading.as_str())
    })
    .await
}

/// Best-effort over a raw [`raw::execute`] result: propagate a pool-acquire
/// failure (aborts the tick, as before) but swallow write failures (a
/// swallowed failure reports a row count of 0 — indistinguishable from a
/// no-match, and the row is retried next tick anyway). Returns the row
/// count. sqlx folds acquire and execution into one `sqlx::Error`; acquire
/// failures surface as `PoolTimedOut` / `PoolClosed`.
async fn execute_best_effort<'q, B>(pool: &Pool, sql: &'q str, bind: B) -> Result<u64>
where
    B: FnOnce(raw::PgQuery<'q>) -> raw::PgQuery<'q>,
{
    match raw::execute(pool, sql, bind).await {
        Ok(n) => Ok(n),
        Err(Error::Database(e))
            if matches!(&e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed) =>
        {
            Err(Error::Database(e))
        }
        Err(_) => Ok(0),
    }
}

/// Adopt a category torrent that has no tracking row (e.g. after a DB reset or
/// added manually in the qB UI). The `WHERE NOT EXISTS (… hash = $3)` guard
/// makes the adoption idempotent against a concurrent insert. Propagates a
/// pool-checkout failure but swallows the write failure.
pub async fn adopt_torrent(
    pool: &Pool,
    name: &str,
    hash: &str,
    status: DownloadStatus,
    progress: f32,
) -> Result<()> {
    execute_best_effort(
        pool,
        "INSERT INTO downloads (name, url, hash, status, progress, updated_at) \
         SELECT $1, $2, $3, $4, $5, now() \
         WHERE NOT EXISTS (SELECT 1 FROM downloads WHERE hash = $3)",
        |q| {
            q.bind(name)
                .bind(format!("qbt://{hash}"))
                .bind(hash)
                .bind(status.as_str())
                .bind(progress)
        },
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use DownloadStatus::*;

    #[test]
    fn download_status_as_str_round_trips() {
        for st in DownloadStatus::ALL {
            assert_eq!(DownloadStatus::parse(st.as_str()), Some(st));
        }
        assert_eq!(DownloadStatus::parse("bogus"), None);
    }

    #[test]
    fn in_list_renders_sql_list() {
        assert_eq!(in_list([Queued, Downloading]), "('queued', 'downloading')");
        assert_eq!(in_list([Cancelled]), "('cancelled')");
    }

    /// ARCH-H1: the intended state machine. These are the transitions the
    /// poller's per-row `WHERE status …` guards enforce; any change to the
    /// lifecycle must update this table AND the corresponding SQL guards.
    #[test]
    fn transition_table_is_intended_state_machine() {
        // Forward happy path.
        assert!(Queued.can_transition_to(Downloading));
        assert!(Downloading.can_transition_to(Complete));
        assert!(Downloading.can_transition_to(Seeding));
        assert!(Seeding.can_transition_to(Complete));

        // Cancellation wins from the active states.
        assert!(Queued.can_transition_to(Cancelled));
        assert!(Downloading.can_transition_to(Cancelled));

        // Erroring from the active states.
        assert!(Queued.can_transition_to(Error));
        assert!(Downloading.can_transition_to(Error));

        // The bounded retry edges.
        assert!(Error.can_transition_to(Queued));
        // Interrupted direct download restart (startup reconcile, PERF-11).
        assert!(Downloading.can_transition_to(Queued));

        // Terminal / invalid transitions must be rejected.
        assert!(!Complete.can_transition_to(Queued));
        assert!(!Complete.can_transition_to(Downloading));
        assert!(!Cancelled.can_transition_to(Queued));
        assert!(!Cancelled.can_transition_to(Downloading));
        assert!(!Cancelled.can_transition_to(Error));
        assert!(!Seeding.can_transition_to(Queued));
        assert!(!Seeding.can_transition_to(Downloading));
        assert!(!Error.can_transition_to(Complete));
        // Nobody transitions into Cancelled except the active states.
        assert!(!Complete.can_transition_to(Cancelled));
        assert!(!Error.can_transition_to(Cancelled));
        assert!(!Seeding.can_transition_to(Cancelled));
    }

    /// ARCH-H1 enforcement seam: the intended transition table and the
    /// production guards on the two formerly-unguarded UPDATEs agree. The
    /// string assertions pin the rendered guard SQL — a dropped/added guard
    /// status would become a silent no-op in production, exactly the bug
    /// class the lifecycle module was introduced to catch; the
    /// `can_transition_to` assertions pin `cancelled` as terminal.
    #[test]
    fn transition_table_matches_production_guards() {
        // Cancelled is terminal: the table rejects every transition out of
        // it (in particular cancelled→downloading and cancelled→error).
        for target in DownloadStatus::ALL {
            assert!(
                !Cancelled.can_transition_to(target),
                "cancelled → {target:?} must be rejected by the table"
            );
        }

        // mark_fatal_error: the guard covers exactly the statuses the table
        // allows to transition to `error`.
        let fatal = mark_fatal_error_sql();
        for st in DownloadStatus::ALL {
            assert_eq!(
                fatal.contains(&format!("'{}'", st.as_str())),
                st.can_transition_to(Error),
                "fatal-error guard drifted at {st:?}: {fatal}"
            );
        }

        // mark_downloading: the guard covers exactly the statuses the table
        // allows to transition to `downloading` — `queued` only.
        let downloading = mark_downloading_sql();
        for st in DownloadStatus::ALL {
            assert_eq!(
                downloading.contains(&format!("status = '{}'", st.as_str())),
                st.can_transition_to(Downloading),
                "downloading guard drifted at {st:?}: {downloading}"
            );
        }
    }

    /// The completion guard (B1) must cover BOTH `downloading` (the
    /// fresh-download path) and `complete` (the startup-recovery path that
    /// reinstalls a `complete` row's deleted file), while excluding
    /// `cancelled` so a cancel always wins.
    #[test]
    fn completion_guard_covers_downloading_and_complete() {
        let g = completion_guard_statuses();
        assert!(
            g.contains(&DownloadStatus::Downloading),
            "fresh path must be covered"
        );
        assert!(
            g.contains(&DownloadStatus::Complete),
            "recovery must be covered"
        );
        assert!(
            !g.contains(&DownloadStatus::Cancelled),
            "cancel must keep winning"
        );
        assert!(
            !g.contains(&DownloadStatus::Seeding),
            "seeding is settled by its own arms"
        );
        assert_eq!(g.len(), 2, "exactly the two statuses, nothing else");
    }

    /// Pins the production cancel-check filter (`no_longer_downloading`), the
    /// core of `status_no_longer_downloading`: it returns `None` while the row
    /// is still downloading (proceed) and `Some` the moment it leaves that
    /// state (bail). The test calls the production helper itself rather than a
    /// copy, so it goes red if the filter ever drifts; the DB round-trip is
    /// covered by the poller's DB-gated tests.
    #[test]
    fn status_no_longer_downloading_predicate() {
        assert_eq!(
            no_longer_downloading("downloading"),
            None,
            "still downloading → proceed"
        );
        assert_eq!(
            no_longer_downloading("cancelled"),
            Some("cancelled".into()),
            "cancelled → bail"
        );
        assert_eq!(
            no_longer_downloading("complete"),
            Some("complete".into()),
            "complete → bail"
        );
    }

    /// SEC drift decision truth table: drift requires BOTH a previous
    /// observed digest AND a difference from the current one. `None`
    /// previous (first observation / NULL legacy) is never drift; an equal
    /// digest (any case) is never drift; a different one is.
    #[test]
    fn drift_decision_truth_table() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        // First observation (no history) — never drift.
        assert!(
            !drift_decision(None, &a),
            "no previous observation → no drift"
        );
        // Same digest — no drift (case-insensitive: a hand-edited uppercase
        // row must not flip the flag).
        assert!(!drift_decision(Some(&a), &a), "equal digest → no drift");
        assert!(
            !drift_decision(Some(&a.to_uppercase()), &a),
            "case-insensitive equality → no drift"
        );
        // Different digest — drift.
        assert!(drift_decision(Some(&b), &a), "different digest → drift");
        assert!(
            drift_decision(Some(&b.to_uppercase()), &a),
            "different digest (uppercase stored) → drift"
        );
    }
}
