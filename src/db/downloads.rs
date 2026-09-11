//! Download-queue data access (ARCH M1 repository extraction).
//!
//! The `downloads` table's SQL lives here — mirroring `db/qid.rs` and
//! `db/random_article.rs` — so the HTTP handlers (`serve/handlers/downloads.rs`)
//! stay thin: they validate the request, call these functions, and map the
//! domain outcomes to HTTP responses. Business rules (one live download per
//! URL/name; not-found vs not-cancellable) are expressed as outcomes, not as
//! `Error` variants, so the presentation layer owns the 404/409/200 mapping.
//!
//! Query layer: raw SQL via [`crate::db::raw`]. `updated_at = now()` stays
//! a *server-side* timestamp (the SQL `now()`, never a Rust-side clock), so
//! the SQL semantics match the old raw statements exactly.
use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::{Error, Result};

/// Normalised form of a download `url` for ZIM detection: the path with any
/// `?query` / `#fragment` stripped, lowercased and trimmed. Reused by the
/// `LIKE '%.zim'` predicates (in-flight select, queued select, interrupted-
/// direct retry) so they can't drift. Lives in the db layer because it is a
/// predicate on the `downloads.url` column; the poller re-exports it.
///
/// Equivalence with the Rust `is_direct_zim_url` is verified by the
/// `zim_url_predicate_rust_matches_sql_semantics` test (WI-9).
pub const ZIM_URL_PREDICATE: &str = "lower(trim(split_part(split_part(url, '?', 1), '#', 1)))";

/// One row of `downloads` as the domain layer needs it — pre-DTO and
/// pre-redaction. The handler maps this to the wire `Download` DTO and applies
/// the SEC-L1 topology redaction (a presentation concern).
pub struct DownloadRow {
    /// Primary key.
    pub id: i32,
    /// Display name for the download.
    pub name: String,
    /// Source URL (ZIM archive or direct file).
    pub url: String,
    /// Lifecycle status value (`queued` / `downloading` / …; see [`crate::db::downloads_lifecycle::DownloadStatus`]).
    pub status: String,
    /// Download progress in `0.0..=1.0` (qBittorrent convention).
    pub progress: f32,
    /// Current download speed in bytes/s, when reported.
    pub speed_bps: Option<i64>,
    /// Estimated seconds to completion, when reported.
    pub eta_secs: Option<i64>,
    /// Downloaded/uploaded byte ratio, when reported.
    pub ratio: Option<f32>,
    /// Current upload speed in bytes/s, when reported.
    pub up_speed_bps: Option<i64>,
    /// Seed count, when reported.
    pub num_seeds: Option<i64>,
    /// Last error message, when the row is in the error state.
    pub error: Option<String>,
    /// Server-side (`now()`) timestamp of row creation.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// One full `downloads` row — every column, with the schema types (`i32` id;
/// `Option<String>` hash/file_path/error; `f32` progress; `i64`
/// speed_bps/eta_secs/up_speed_bps/num_seeds; `Option<f32>` ratio;
/// `DateTime<Utc>` created_at/updated_at). Used by the poller's in-flight
/// select, which needs every column; the partial [`DownloadRow`] above is
/// the handler-facing shape.
pub struct DownloadRecord {
    /// Primary key.
    pub id: i32,
    /// Display name for the download.
    pub name: String,
    /// Source URL (ZIM archive or direct file).
    pub url: String,
    /// Torrent hash, once known to the qBittorrent backend.
    pub hash: Option<String>,
    /// Lifecycle status value (`queued` / `downloading` / …; see [`crate::db::downloads_lifecycle::DownloadStatus`]).
    pub status: String,
    /// Download progress in `0.0..=1.0` (qBittorrent convention).
    pub progress: f32,
    /// Current download speed in bytes/s, when reported.
    pub speed_bps: Option<i64>,
    /// Estimated seconds to completion, when reported.
    pub eta_secs: Option<i64>,
    /// Local destination path of the downloaded file (direct downloads).
    pub file_path: Option<String>,
    /// Last error message, when the row is in the error state.
    pub error: Option<String>,
    /// Server-side (`now()`) timestamp of row creation.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Server-side timestamp of the most recent write.
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Downloaded/uploaded byte ratio, when reported.
    pub ratio: Option<f32>,
    /// Current upload speed in bytes/s, when reported.
    pub up_speed_bps: Option<i64>,
    /// Seed count, when reported.
    pub num_seeds: Option<i64>,
}

/// Decode tuple for [`fetch_downloads_by_statuses`] — one field per
/// [`DownloadRecord`] column, in SELECT order.
type DownloadRecordCols = (
    i32,
    String,
    String,
    Option<String>,
    String,
    f32,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
    Option<f32>,
    Option<i64>,
    Option<i64>,
);

/// Fetch `downloads` rows in the two given statuses (explicit column list,
/// decoded as [`DownloadRecord`]).
pub async fn fetch_downloads_by_statuses(
    pool: &Pool,
    status1: &str,
    status2: &str,
) -> Result<Vec<DownloadRecord>> {
    let rows: Vec<DownloadRecordCols> = raw::fetch_all(
        pool,
        "SELECT id, name, url, hash, status, progress, speed_bps, eta_secs, file_path, error, \
         created_at, updated_at, ratio, up_speed_bps, num_seeds \
         FROM downloads WHERE status IN ($1, $2)",
        |q| q.bind(status1).bind(status2),
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                name,
                url,
                hash,
                status,
                progress,
                speed_bps,
                eta_secs,
                file_path,
                error,
                created_at,
                updated_at,
                ratio,
                up_speed_bps,
                num_seeds,
            )| {
                DownloadRecord {
                    id,
                    name,
                    url,
                    hash,
                    status,
                    progress,
                    speed_bps,
                    eta_secs,
                    file_path,
                    error,
                    created_at,
                    updated_at,
                    ratio,
                    up_speed_bps,
                    num_seeds,
                }
            },
        )
        .collect())
}

/// Decode tuple for [`list_downloads`] — one field per [`DownloadRow`]
/// column, in SELECT order.
type DownloadRowCols = (
    i32,
    String,
    String,
    String,
    f32,
    Option<i64>,
    Option<i64>,
    Option<String>,
    chrono::DateTime<chrono::Utc>,
    Option<f32>,
    Option<i64>,
    Option<i64>,
);

/// Most recent downloads (newest first), capped at 500.
pub async fn list_downloads(pool: &Pool) -> Result<Vec<DownloadRow>> {
    let rows: Vec<DownloadRowCols> = raw::fetch_all(
        pool,
        "SELECT id, name, url, status, progress, speed_bps, eta_secs, error, created_at, \
         ratio, up_speed_bps, num_seeds \
         FROM downloads ORDER BY created_at DESC LIMIT 500",
        |q| q,
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                name,
                url,
                status,
                progress,
                speed_bps,
                eta_secs,
                error,
                created_at,
                ratio,
                up_speed_bps,
                num_seeds,
            )| {
                DownloadRow {
                    id,
                    name,
                    url,
                    status,
                    progress,
                    speed_bps,
                    eta_secs,
                    error,
                    created_at,
                    ratio,
                    up_speed_bps,
                    num_seeds,
                }
            },
        )
        .collect())
}

/// The id of another *live* row (`queued`/`downloading`/`complete`/`seeding`)
/// that owns the same `name`, excluding `exclude_id` — or `None`. The
/// install-collision guard for the completion path: enqueue dedup covers live
/// rows only, so an older terminal row with the same name can already own
/// `zim_dir/{name}.zim`; a fresh install on a new row would overwrite it (and
/// the cancel-race discard could delete it). `exclude_id` tolerates the row
/// being completed itself.
pub async fn find_same_name_live_row(
    pool: &Pool,
    name: &str,
    exclude_id: i32,
) -> Result<Option<i32>> {
    raw::fetch_scalar_optional(
        pool,
        "SELECT id FROM downloads \
         WHERE name = $1 AND id != $2 AND status IN ($3, $4, $5, $6) LIMIT 1",
        |q| {
            q.bind(name)
                .bind(exclude_id)
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Queued.as_str())
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Downloading.as_str())
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Complete.as_str())
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Seeding.as_str())
        },
    )
    .await
}

/// Outcome of [`insert_download`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The new row's id.
    Inserted(i32),
    /// A live download already exists for this URL or name.
    Duplicate,
}

/// Insert a `queued` download, enforcing the one-live-download-per-URL/name
/// rule (BUG-3: the `.part` file is named `{name}.part`, so same-name active
/// rows would share it).
///
/// The `url = $2 OR name = $1` pre-check is only fast feedback — the 003/011
/// partial unique indexes are the authority, and the `23505` (unique
/// violation) path below closes the concurrent-POST race.
// LINT-3 (2026-09 sweep): `INSERT … RETURNING id` always yields a row — panic = DB contract violation.
#[allow(clippy::expect_used)]
pub async fn insert_download(pool: &Pool, name: &str, url: &str) -> Result<InsertOutcome> {
    // The `(url = $1 OR name = $2)` pre-check is only fast feedback — the
    // 003/011 partial unique indexes are the authority, and the `23505`
    // (unique violation) path below closes the concurrent-POST race.
    let dup: Option<i32> = raw::fetch_scalar_optional(
        pool,
        "SELECT id FROM downloads \
         WHERE (url = $1 OR name = $2) AND status IN ($3, $4) LIMIT 1",
        |q| {
            q.bind(url)
                .bind(name)
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Queued.as_str())
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Downloading.as_str())
        },
    )
    .await?;
    if dup.is_some() {
        return Ok(InsertOutcome::Duplicate);
    }
    // Only the three caller-provided columns are set; `created_at` /
    // `updated_at` keep the table's `now()` defaults (same column list as
    // the old raw INSERT).
    let id: Option<i32> = match raw::fetch_scalar_optional(
        pool,
        "INSERT INTO downloads (name, url, status) VALUES ($1, $2, $3) RETURNING id",
        |q| {
            q.bind(name)
                .bind(url)
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Queued.as_str())
        },
    )
    .await
    {
        Ok(id) => id,
        // BUG-9: a concurrent POST hitting the partial unique index (23505)
        // is a duplicate, not a DB fault — same mapping as `collections`.
        Err(Error::Database(e)) if crate::db::collections::is_unique_violation(&e) => {
            return Ok(InsertOutcome::Duplicate)
        }
        Err(e) => return Err(e),
    };
    Ok(InsertOutcome::Inserted(
        id.expect("INSERT … RETURNING id always yields a row"),
    ))
}

/// Outcome of [`cancel_download`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The row was cancelled; its id.
    Cancelled(i32),
    /// No such row.
    NotFound,
    /// The row exists but is not cancellable in its current state.
    NotCancellable {
        /// The row's current status (why it can't be cancelled).
        status: String,
    },
}

/// Cancel a `queued`/`downloading` download.
///
/// Distinguishes "not found" from "exists but not cancellable" (BUG-16c) so the
/// handler can map to 404 vs 409. Only `queued`/`downloading` rows are
/// cancellable; anything else (complete/cancelled/…) is left alone.
pub async fn cancel_download(pool: &Pool, id: i32) -> Result<CancelOutcome> {
    let updated = raw::execute(
        pool,
        "UPDATE downloads SET status = $1, updated_at = now() \
         WHERE id = $2 AND status IN ($3, $4)",
        |q| {
            q.bind(crate::db::downloads_lifecycle::DownloadStatus::Cancelled.as_str())
                .bind(id)
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Queued.as_str())
                .bind(crate::db::downloads_lifecycle::DownloadStatus::Downloading.as_str())
        },
    )
    .await?;
    if updated > 0 {
        return Ok(CancelOutcome::Cancelled(id));
    }
    // Distinguish not-found from not-cancellable (BUG-16c).
    let status: Option<String> =
        raw::fetch_scalar_optional(pool, "SELECT status FROM downloads WHERE id = $1", |q| {
            q.bind(id)
        })
        .await?;
    Ok(match status {
        Some(status) => CancelOutcome::NotCancellable { status },
        None => CancelOutcome::NotFound,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::testing::test_pool;

    /// TEST #7 (Tests finding): the `list_downloads` 500-row cap, its newest-
    /// first ordering, and the row→struct column mapping are exercised
    /// end-to-end against a live Postgres.
    ///
    /// Seeds 505 rows under a `__it_listdl__` name prefix with **far-future**
    /// `created_at` values (year 3000) so this block is unambiguously the 505
    /// newest rows in the table even on the shared dev DB — the `LIMIT 500` cap
    /// therefore cuts exactly our 5 oldest rows, and every returned row is one
    /// of ours with deterministic, column-distinct values.
    #[tokio::test]
    async fn list_downloads_caps_at_500_newest_first_and_maps_columns() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");

        // Sweep entry: this test owns the `__it_listdl__` prefix. A shared dev
        // DB may hold stale rows from a prior interrupted run — clear them first.
        let mut c = pool.acquire().await.unwrap();
        crate::db::raw::execute(
            &mut *c,
            "DELETE FROM downloads WHERE name LIKE '__it_listdl__%'",
            |q| q,
        )
        .await
        .unwrap();

        // 505 rows, one INSERT via generate_series (no 505 round trips).
        //
        // i (0..505): row index. i=0 is newest, i=504 is oldest. Our rows are
        // 9999-01-01 minus i*10s, so the descending created_at order is exactly
        // ascending i: the 500 returned rows are i=0..=499; the 5 excluded rows
        // are i=500..=504.
        //
        // Every mapped column carries a value derived from i so a wrong column
        // mapping, or a shifted column order in the SELECT, is caught.
        crate::db::raw::execute(
            &mut *c,
            r#"
            INSERT INTO downloads
                (name, url, hash, status, progress, speed_bps, eta_secs,
                 file_path, error, created_at, ratio, up_speed_bps, num_seeds)
            SELECT
                '__it_listdl__' || lpad(i::text, 4, '0'),
                'http://it.local/__it_listdl__' || lpad(i::text, 4, '0') || '.zim',
                'hash-' || lpad(i::text, 4, '0'),
                'status-' || lpad(i::text, 4, '0'),
                (i % 101) / 100.0,
                (i * 7)::bigint,
                (i * 13)::bigint,
                '/tmp/__it_listdl__' || lpad(i::text, 4, '0') || '.zim',
                'err-' || lpad(i::text, 4, '0'),
                ('3000-01-01T00:00:00Z'::timestamptz) - make_interval(secs => i * 10),
                ((i % 250) + 1) / 100.0,
                (i * 3)::bigint,
                (i * 11)::bigint
            FROM generate_series(0, 504) AS i
            "#,
            |q| q,
        )
        .await
        .unwrap();

        let rows = list_downloads(&pool).await.unwrap();

        // The cap: exactly 500 rows regardless of the 505 seeded.
        assert_eq!(
            rows.len(),
            500,
            "expected the 500-row cap, got {}",
            rows.len()
        );

        // Deterministic per-row expectations for row index i (i=0 newest).
        fn expected(
            i: i64,
        ) -> (
            String,
            String,
            String,
            String,
            f32,
            i64,
            i64,
            i64,
            String,
            f32,
            i64,
        ) {
            let p = format!("{i:04}");
            let f32 = |v: i64| (v as f32) / 100.0;
            (
                format!("__it_listdl__{p}"),
                format!("http://it.local/__it_listdl__{p}.zim"),
                format!("status-{p}"),
                format!("err-{p}"),
                f32(i % 101),
                i * 7,
                i * 13,
                i * 3,
                format!("/tmp/__it_listdl__{p}.zim"),
                f32((i % 250) + 1),
                i * 11,
            )
        }

        // Verify one returned row against every mapped column (a single row
        // checks all 11 mapped columns; the spot-rows below check ordering +
        // the excluded boundary).
        fn assert_row(i: i64, r: &DownloadRow) {
            let (name, url, status, err, prog, speed, eta, up, _file, ratio, seeds) = expected(i);
            assert_eq!(r.name, name, "name at i={i}");
            assert_eq!(r.url, url, "url at i={i}");
            assert_eq!(r.status, status, "status at i={i}");
            assert_eq!(r.error.as_deref(), Some(err.as_str()), "error at i={i}");
            assert!((r.progress - prog).abs() < 1e-6, "progress at i={i}");
            assert_eq!(r.speed_bps, Some(speed), "speed_bps at i={i}");
            assert_eq!(r.eta_secs, Some(eta), "eta_secs at i={i}");
            assert_eq!(r.up_speed_bps, Some(up), "up_speed_bps at i={i}");
            assert!(
                (r.ratio.unwrap_or(0.0) - ratio).abs() < 1e-6,
                "ratio at i={i}"
            );
            assert_eq!(r.num_seeds, Some(seeds), "num_seeds at i={i}");
        }

        // Newest-first ordering: index 0 is our newest (i=0), index 499 is the
        // oldest of the 500 returned (i=499). The 5 excluded rows are i=500..=504.
        assert_row(0, &rows[0]);
        assert_row(250, &rows[250]);
        assert_row(499, &rows[499]);

        // Every row is one of ours (shared dev DB must have leaked none in).
        for (idx, r) in rows.iter().enumerate() {
            assert!(
                r.name.starts_with("__it_listdl__"),
                "row {idx} is not a test row: {:?}",
                r.name
            );
        }

        // The 5 excluded (oldest) rows are exactly i=500..=504. Verify by name
        // that none of them appear, and that the boundary row i=499 (the oldest
        // returned) does.
        for i in 500..=504i64 {
            let (name, ..) = expected(i);
            assert!(
                !rows.iter().any(|r| r.name == name),
                "excluded row i={i} must not be returned"
            );
        }
        assert!(
            rows.iter().any(|r| r.name == "__it_listdl__0499"),
            "boundary row i=499 must be returned"
        );

        // Sweep exit.
        crate::db::raw::execute(
            &mut *c,
            "DELETE FROM downloads WHERE name LIKE '__it_listdl__%'",
            |q| q,
        )
        .await
        .unwrap();
    }
}
