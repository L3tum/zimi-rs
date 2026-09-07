//! Download **lifecycle** state machine — the single home for every status
//! (and hash / error / seed-stats) transition on the `downloads` table.
//!
//! The poller (`crate::torrent::poller`) drives this machine. Centralizing the
//! WHERE-guarded `UPDATE`s here (ARCH M-2/M-3) makes the transitions a
//! reviewable, *enumerable* artifact: every place a row's status, hash, error,
//! or seed stats changes lives in this module, and every status value is
//! rendered from the `DownloadStatus` enum (never a raw literal). The intended
//! graph is [`DownloadStatus::can_transition_to`]; the per-statement `WHERE
//! status …` guards are the enforcement, and the read-side `IN (…)` filters
//! render their sets through [`crate::torrent::in_list`].
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
use crate::torrent::DownloadStatus;
use sqlx::postgres::PgConnection;

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

/// `downloading → error` (fatal torrent state / torrent vanished after the
/// grace period / qB add failure). Unguarded on status by design: the caller
/// has already established the row is a live in-flight row this tick.
/// Propagates a pool-checkout failure (aborts the tick, as before) but
/// swallows the write failure.
pub async fn mark_fatal_error(pool: &Pool, id: i32, msg: &str) -> Result<()> {
    execute_best_effort(
        pool,
        "UPDATE downloads SET status = $1, error = $2, updated_at = now() WHERE id = $3",
        |q| q.bind(DownloadStatus::Error.as_str()).bind(msg).bind(id),
    )
    .await
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
/// this update still wins — never clobber a terminal state. Returns the row
/// count (0 = cancelled mid-finalize; the caller discards the staged file).
pub async fn finalize_direct(pool: &Pool, id: i32, dst: &str) -> Result<u64> {
    raw::execute(
        pool,
        "UPDATE downloads SET status = $1, progress = 1.0, file_path = $2, updated_at = now() \
         WHERE id = $3 AND status = $4",
        |q| {
            q.bind(DownloadStatus::Complete.as_str())
                .bind(dst)
                .bind(id)
                .bind(DownloadStatus::Downloading.as_str())
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

/// `queued → downloading` (a torrent row was added to qBittorrent this tick).
/// Unguarded on status by design: the row was claimed `queued` at the top of
/// the enqueue loop and the per-row budget gate prevents double-spawn.
/// Propagates a pool-checkout failure but swallows the write failure.
pub async fn mark_downloading(pool: &Pool, id: i32) -> Result<()> {
    execute_best_effort(
        pool,
        "UPDATE downloads SET status = $1, updated_at = now() WHERE id = $2",
        |q| q.bind(DownloadStatus::Downloading.as_str()).bind(id),
    )
    .await
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
        Some(msg) => {
            execute_best_effort(
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
        }
        None => execute_best_effort(
            pool,
            "UPDATE downloads SET status = $1, updated_at = now() WHERE id = $2 AND status = $3",
            |q| {
                q.bind(DownloadStatus::Complete.as_str())
                    .bind(id)
                    .bind(DownloadStatus::Seeding.as_str())
            },
        )
        .await,
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
/// failure (aborts the tick, as before) but swallow write failures. sqlx
/// folds acquire and execution into one `sqlx::Error`; acquire failures
/// surface as `PoolTimedOut` / `PoolClosed`.
async fn execute_best_effort<'q, B>(pool: &Pool, sql: &'q str, bind: B) -> Result<()>
where
    B: FnOnce(raw::PgQuery<'q>) -> raw::PgQuery<'q>,
{
    match raw::execute(pool, sql, bind).await {
        Ok(_) => Ok(()),
        Err(Error::Database(e))
            if matches!(&e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed) =>
        {
            Err(Error::Database(e))
        }
        Err(_) => Ok(()),
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
}
