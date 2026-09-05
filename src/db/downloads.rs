//! Download-queue data access (ARCH M1 repository extraction).
//!
//! The `downloads` table's SQL lives here — mirroring `db/qid.rs` and
//! `db/random_article.rs` — so the HTTP handlers (`serve/handlers/downloads.rs`)
//! stay thin: they validate the request, call these functions, and map the
//! domain outcomes to HTTP responses. Business rules (one live download per
//! URL/name; not-found vs not-cancellable) are expressed as outcomes, not as
//! `Error` variants, so the presentation layer owns the 404/409/200 mapping.
use crate::db::pool::Pool;
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
    pub id: i32,
    pub name: String,
    pub url: String,
    pub status: String,
    pub progress: f32,
    pub speed_bps: Option<i64>,
    pub eta_secs: Option<i64>,
    pub ratio: Option<f32>,
    pub up_speed_bps: Option<i64>,
    pub num_seeds: Option<i64>,
    pub error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Most recent downloads (newest first), capped at 500.
pub async fn list_downloads(pool: &Pool) -> Result<Vec<DownloadRow>> {
    let client = pool.get().await.map_err(Error::Pool)?;
    let rows = client
        .query(
            "SELECT id, name, url, status, progress, speed_bps, eta_secs, error, created_at, ratio, up_speed_bps, num_seeds FROM downloads ORDER BY created_at DESC LIMIT 500",
            &[],
        )
        .await
        .map_err(Error::Database)?;
    Ok(rows
        .iter()
        .map(|r| DownloadRow {
            id: r.get(0),
            name: r.get(1),
            url: r.get(2),
            status: r.get(3),
            progress: r.get(4),
            speed_bps: r.get(5),
            eta_secs: r.get(6),
            error: r.get(7),
            created_at: r.get(8),
            ratio: r.get(9),
            up_speed_bps: r.get(10),
            num_seeds: r.get(11),
        })
        .collect())
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
pub async fn insert_download(pool: &Pool, name: &str, url: &str) -> Result<InsertOutcome> {
    let client = pool.get().await.map_err(Error::Pool)?;
    let dup = client
        .query_opt(
            &format!(
                "SELECT 1 FROM downloads WHERE (url = $2 OR name = $1) AND status IN ({act})",
                act = crate::torrent::in_list([
                    crate::torrent::DownloadStatus::Queued,
                    crate::torrent::DownloadStatus::Downloading
                ])
            ),
            &[&name, &url],
        )
        .await
        .map_err(Error::Database)?;
    if dup.is_some() {
        return Ok(InsertOutcome::Duplicate);
    }
    match client
        .query_one(
            "INSERT INTO downloads (name, url, status) VALUES ($1, $2, $3) RETURNING id",
            &[
                &name,
                &url,
                &crate::torrent::DownloadStatus::Queued.as_str(),
            ],
        )
        .await
    {
        Ok(row) => Ok(InsertOutcome::Inserted(row.get(0))),
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
            Ok(InsertOutcome::Duplicate)
        }
        Err(e) => Err(Error::Database(e)),
    }
}

/// Outcome of [`cancel_download`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The row was cancelled; its id.
    Cancelled(i32),
    /// No such row.
    NotFound,
    /// The row exists but is not cancellable in its current state.
    NotCancellable { status: String },
}

/// Cancel a `queued`/`downloading` download.
///
/// Distinguishes "not found" from "exists but not cancellable" (BUG-16c) so the
/// handler can map to 404 vs 409. Only `queued`/`downloading` rows are
/// cancellable; anything else (complete/cancelled/…) is left alone.
pub async fn cancel_download(pool: &Pool, id: i32) -> Result<CancelOutcome> {
    let client = pool.get().await.map_err(Error::Pool)?;
    let updated = client
        .execute(
            &format!(
                "UPDATE downloads SET status = {cancelled}, updated_at = now() WHERE id = $1 AND status IN ({act})",
                cancelled = crate::torrent::DownloadStatus::Cancelled.as_str(),
                act = crate::torrent::in_list([
                    crate::torrent::DownloadStatus::Queued,
                    crate::torrent::DownloadStatus::Downloading
                ])
            ),
            &[&id],
        )
        .await
        .map_err(Error::Database)?;
    if updated > 0 {
        return Ok(CancelOutcome::Cancelled(id));
    }
    let status: Option<String> = client
        .query_opt("SELECT status FROM downloads WHERE id = $1", &[&id])
        .await
        .map_err(Error::Database)?
        .map(|r| r.get(0));
    Ok(match status {
        Some(status) => CancelOutcome::NotCancellable { status },
        None => CancelOutcome::NotFound,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::torrent::poller::test_pool;

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
        let c = pool.get().await.unwrap();
        c.execute(
            "DELETE FROM downloads WHERE name LIKE '__it_listdl__%'",
            &[],
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
        c.execute(
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
            &[],
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
        c.execute(
            "DELETE FROM downloads WHERE name LIKE '__it_listdl__%'",
            &[],
        )
        .await
        .unwrap();
    }
}
