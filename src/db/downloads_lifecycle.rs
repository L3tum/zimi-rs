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
//! Query layer: the SeaORM query builder (`update_many` + `col_expr` /
//! bound filters) over the shared pool via [`crate::db::sea_orm_db`].
//! `updated_at = now()` stays a *server-side* timestamp via
//! `Expr::cust("now()")`, so the SQL semantics match the old raw statements
//! exactly. Statements stay in [`crate::db::raw`] because the SeaORM query
//! builder cannot express them: the single-column status reads (the hot
//! cancel-check path on a checked-out connection) and [`adopt_torrent`]
//! (`INSERT … SELECT … WHERE NOT EXISTS`).
use crate::db::entities::downloads::{ActiveModel, Column, Entity};
use crate::db::pool::Pool;
use crate::db::{raw, sea_orm_db};
use crate::error::{Error, Result};
use crate::torrent::DownloadStatus;
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveValue, ColumnTrait, EntityTrait, QueryFilter};
use sqlx::postgres::PgConnection;

// ── Status reads ───────────────────────────────────────────────────────────
// The cancel-check core shared by every in-flight path. A row's status is
// re-read before each mutation so a cancel always wins over a racing
// finalize.

/// Read a row's status, fail-open: a transient DB blip must not abort an
/// in-flight download (the next periodic check re-runs). Returns `Some(status)`.
pub async fn status_of(client: &mut PgConnection, id: i32) -> Option<String> {
    let status: Option<String> =
        match raw::fetch_scalar_optional(
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
    let status: Option<String> = raw::fetch_scalar_optional(
        pool,
        "SELECT status FROM downloads WHERE id = $1",
        |q| q.bind(id),
    )
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
    let db = sea_orm_db(pool);
    let updated = Entity::update_many()
        .col_expr(Column::Status, Expr::val(DownloadStatus::Downloading.as_str()).into())
        .col_expr(Column::FilePath, Expr::val(part).into())
        .col_expr(Column::UpdatedAt, Expr::cust("now()"))
        .filter(Column::Id.eq(id))
        .filter(Column::Status.eq(DownloadStatus::Queued.as_str()))
        .exec(&db)
        .await
        .map_err(Error::from)?
        .rows_affected;
    Ok(updated)
}

/// `* → error` (terminal-state guard). A failure is recorded without
/// clobbering a terminal state — a cancel racing the failure path must win.
/// Best-effort: a pool blip is logged and swallowed (the row is retried next
/// tick / stays in its current state).
pub async fn mark_error(pool: &Pool, id: i32, msg: &str) {
    let db = sea_orm_db(pool);
    let _ = Entity::update_many()
        .col_expr(Column::Status, Expr::val(DownloadStatus::Error.as_str()).into())
        .col_expr(Column::Error, Expr::val(msg).into())
        .col_expr(Column::UpdatedAt, Expr::cust("now()"))
        .filter(Column::Id.eq(id))
        .filter(Column::Status.is_not_in([
            DownloadStatus::Cancelled.as_str(),
            DownloadStatus::Complete.as_str(),
            DownloadStatus::Seeding.as_str(),
            DownloadStatus::Error.as_str(),
        ]))
        .exec(&db)
        .await;
}

/// `downloading → error` (fatal torrent state / torrent vanished after the
/// grace period / qB add failure). Unguarded on status by design: the caller
/// has already established the row is a live in-flight row this tick.
/// Propagates a pool-checkout failure (aborts the tick, as before) but
/// swallows the write failure.
pub async fn mark_fatal_error(pool: &Pool, id: i32, msg: &str) -> Result<()> {
    let db = sea_orm_db(pool);
    best_effort(
        Entity::update_many()
            .col_expr(Column::Status, Expr::val(DownloadStatus::Error.as_str()).into())
            .col_expr(Column::Error, Expr::val(msg).into())
            .col_expr(Column::UpdatedAt, Expr::cust("now()"))
            .filter(Column::Id.eq(id))
            .exec(&db)
            .await,
    )
}

/// `error → queued` (bounded stale-error retry). The `AND status = 'error'`
/// guard means a row that moved on between the select and this update (e.g.
/// manually cancelled) is never clobbered. Returns the row count requeued.
pub async fn requeue_stale_errors(pool: &Pool, ids: &[i32]) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let db = sea_orm_db(pool);
    let updated = Entity::update_many()
        .col_expr(Column::Status, Expr::val(DownloadStatus::Queued.as_str()).into())
        .col_expr(Column::Error, Expr::val(None::<String>).into())
        .col_expr(Column::UpdatedAt, Expr::cust("now()"))
        .filter(Column::Id.is_in(ids.to_vec()))
        .filter(Column::Status.eq(DownloadStatus::Error.as_str()))
        .exec(&db)
        .await
        .map_err(Error::from)?
        .rows_affected;
    Ok(updated)
}

/// `downloading → complete` (direct-download finalize). The `AND status =
/// 'downloading'` guard means a cancel landing between the pre-rename check and
/// this update still wins — never clobber a terminal state. Returns the row
/// count (0 = cancelled mid-finalize; the caller discards the staged file).
pub async fn finalize_direct(pool: &Pool, id: i32, dst: &str) -> Result<u64> {
    let db = sea_orm_db(pool);
    let updated = Entity::update_many()
        .col_expr(Column::Status, Expr::val(DownloadStatus::Complete.as_str()).into())
        .col_expr(Column::Progress, Expr::val(1.0f32).into())
        .col_expr(Column::FilePath, Expr::val(dst).into())
        .col_expr(Column::UpdatedAt, Expr::cust("now()"))
        .filter(Column::Id.eq(id))
        .filter(Column::Status.eq(DownloadStatus::Downloading.as_str()))
        .exec(&db)
        .await
        .map_err(Error::from)?
        .rows_affected;
    Ok(updated)
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
    let db = sea_orm_db(pool);
    let model = match new_status {
        DownloadStatus::Seeding => ActiveModel {
            status: ActiveValue::Set(DownloadStatus::Seeding.as_str().to_owned()),
            progress: ActiveValue::Set(1.0f32),
            file_path: ActiveValue::Set(Some(dst.to_owned())),
            ratio: ActiveValue::Set(ratio),
            up_speed_bps: ActiveValue::Set(up_speed_bps),
            num_seeds: ActiveValue::Set(num_seeds),
            error: ActiveValue::Set(None),
            ..Default::default()
        },
        _ => ActiveModel {
            status: ActiveValue::Set(DownloadStatus::Complete.as_str().to_owned()),
            progress: ActiveValue::Set(1.0f32),
            file_path: ActiveValue::Set(Some(dst.to_owned())),
            ratio: ActiveValue::Set(None),
            up_speed_bps: ActiveValue::Set(None),
            num_seeds: ActiveValue::Set(None),
            error: ActiveValue::Set(None),
            ..Default::default()
        },
    };
    let updated = Entity::update_many()
        .set(model)
        .col_expr(Column::UpdatedAt, Expr::cust("now()"))
        .filter(Column::Id.eq(id))
        .filter(Column::Status.is_in(
            completion_guard_statuses()
                .into_iter()
                .map(|s| s.as_str().to_owned()),
        ))
        .exec(&db)
        .await
        .map_err(Error::from)?
        .rows_affected;
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

/// Best-effort over a SeaORM update result: propagate a pool-acquire
/// failure (aborts the tick, as before) but swallow write failures. sqlx
/// folds acquire and execution into one `sqlx::Error`, and sea-orm maps a
/// pool failure to [`sea_orm::DbErr::ConnectionAcquire`] — that variant
/// distinguishes the two.
fn best_effort(res: std::result::Result<sea_orm::UpdateResult, sea_orm::DbErr>) -> Result<()> {
    match res {
        Ok(_) => Ok(()),
        Err(e) if matches!(e, sea_orm::DbErr::ConnectionAcquire(_)) => Err(Error::SeaOrm(e)),
        Err(_) => Ok(()),
    }
}

/// `queued → downloading` (a torrent row was added to qBittorrent this tick).
/// Unguarded on status by design: the row was claimed `queued` at the top of
/// the enqueue loop and the per-row budget gate prevents double-spawn.
/// Propagates a pool-checkout failure but swallows the write failure.
pub async fn mark_downloading(pool: &Pool, id: i32) -> Result<()> {
    let db = sea_orm_db(pool);
    best_effort(
        Entity::update_many()
            .col_expr(Column::Status, Expr::val(DownloadStatus::Downloading.as_str()).into())
            .col_expr(Column::UpdatedAt, Expr::cust("now()"))
            .filter(Column::Id.eq(id))
            .exec(&db)
            .await,
    )
}

/// Bind a row's torrent `hash` (rebind on a hash/name match). Propagates a
/// pool-checkout failure but swallows the write failure.
pub async fn bind_hash(pool: &Pool, id: i32, hash: &str) -> Result<()> {
    let db = sea_orm_db(pool);
    best_effort(
        Entity::update_many()
            .col_expr(Column::Hash, Expr::val(hash).into())
            .col_expr(Column::UpdatedAt, Expr::cust("now()"))
            .filter(Column::Id.eq(id))
            .exec(&db)
            .await,
    )
}

/// Clear a row's stale `hash` binding (cancel cleanup, once the qB removal is
/// confirmed).
pub async fn clear_hash(pool: &Pool, id: i32) {
    let db = sea_orm_db(pool);
    let _ = Entity::update_many()
        .col_expr(Column::Hash, Expr::val(None::<String>).into())
        .col_expr(Column::UpdatedAt, Expr::cust("now()"))
        .filter(Column::Id.eq(id))
        .exec(&db)
        .await;
}

/// Drain the `hash` bindings on all `cancelled` rows (no qB configured): once
/// the hash is NULL the rows early-exit and stop re-running the per-tick
/// bookkeeping queries (BUG-21). Returns the row count cleared.
pub async fn drain_cancelled_hashes(pool: &Pool) -> Result<u64> {
    let db = sea_orm_db(pool);
    let updated = Entity::update_many()
        .col_expr(Column::Hash, Expr::val(None::<String>).into())
        .filter(Column::Status.eq(DownloadStatus::Cancelled.as_str()))
        .filter(Column::Hash.is_not_null())
        .exec(&db)
        .await
        .map_err(Error::from)?
        .rows_affected;
    Ok(updated)
}

/// `seeding → complete` (settle a seeding row: the torrent left qB after the
/// grace period, or it reports a fatal state — the ZIM is already installed,
/// so a fatal torrent is noted but not a failure). `error` carries the qB
/// error note for the fatal arm. Propagates a pool-checkout failure but
/// swallows the write failure.
pub async fn settle_seeding(pool: &Pool, id: i32, error: Option<&str>) -> Result<()> {
    let db = sea_orm_db(pool);
    let mut stmt = Entity::update_many()
        .col_expr(Column::Status, Expr::val(DownloadStatus::Complete.as_str()).into())
        .col_expr(Column::UpdatedAt, Expr::cust("now()"))
        .filter(Column::Id.eq(id))
        .filter(Column::Status.eq(DownloadStatus::Seeding.as_str()));
    if let Some(msg) = error {
        stmt = stmt.col_expr(Column::Error, Expr::val(msg).into());
    }
    best_effort(stmt.exec(&db).await)
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
    let db = sea_orm_db(pool);
    best_effort(
        Entity::update_many()
            .col_expr(Column::Ratio, Expr::val(ratio).into())
            .col_expr(Column::UpSpeedBps, Expr::val(up_speed_bps).into())
            .col_expr(Column::NumSeeds, Expr::val(num_seeds).into())
            .col_expr(Column::SpeedBps, Expr::val(speed_bps).into())
            .col_expr(Column::UpdatedAt, Expr::cust("now()"))
            .filter(Column::Id.eq(id))
            .filter(Column::Status.eq(DownloadStatus::Seeding.as_str()))
            .exec(&db)
            .await,
    )
}

/// Refresh an in-progress row's `progress` / `speed_bps` (the 2 s stream tick
/// in the direct-download path). Propagates a pool-checkout failure (the
/// caller aborts the stream, as before) but swallows the write failure
/// (a stats blip must not discard a healthy in-flight download). Unguarded on
/// status by design — the in-loop cancel check handles the terminal-state
/// race; this only touches stat columns.
pub async fn refresh_progress(pool: &Pool, id: i32, progress: f32, speed_bps: i64) -> Result<()> {
    let db = sea_orm_db(pool);
    best_effort(
        Entity::update_many()
            .col_expr(Column::Progress, Expr::val(progress).into())
            .col_expr(Column::SpeedBps, Expr::val(speed_bps).into())
            .col_expr(Column::UpdatedAt, Expr::cust("now()"))
            .filter(Column::Id.eq(id))
            .exec(&db)
            .await,
    )
}

/// `downloading → queued` (startup reconcile of interrupted *direct* `.zim`
/// downloads: clear `file_path`/`error` so the next tick re-claims them and
/// resumes from a surviving `.part`, PERF-11). Returns the row count retried.
/// The `.zim` predicate stays a shared SQL fragment ([`crate::db::downloads::ZIM_URL_PREDICATE`],
/// also spliced into the poller's raw SQL) via `Expr::cust`.
pub async fn retry_interrupted_directs(pool: &Pool) -> Result<u64> {
    let db = sea_orm_db(pool);
    let pred = crate::db::downloads::ZIM_URL_PREDICATE;
    let updated = Entity::update_many()
        .col_expr(Column::Status, Expr::val(DownloadStatus::Queued.as_str()).into())
        .col_expr(Column::FilePath, Expr::val(None::<String>).into())
        .col_expr(Column::Error, Expr::val(None::<String>).into())
        .col_expr(Column::UpdatedAt, Expr::cust("now()"))
        .filter(Column::Status.eq(DownloadStatus::Downloading.as_str()))
        .filter(Expr::cust(format!("({pred}) LIKE '%.zim'")))
        .exec(&db)
        .await
        .map_err(Error::from)?
        .rows_affected;
    Ok(updated)
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
