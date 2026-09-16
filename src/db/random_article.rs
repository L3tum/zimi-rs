//! O(1) random article selection via index seek (no `ORDER BY random()`).
//!
//! `fetch_random_article` bounds the id range, picks a local target, then does
//! a single forward index seek with a backward fallback — both scoped to the
//! requested ZIM so a scoped call never returns another ZIM's article.
use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::{Error, Result};
use std::time::{SystemTime, UNIX_EPOCH};

/// MIN/MAX article id bounds, optionally scoped to one ZIM by name. Two
/// named consts (global / scoped) so the SQL is pinned in one place and the
/// query call selects by `zim_filter` — no inline literal duplication.
const BOUNDS_SQL_GLOBAL: &str = "SELECT MIN(id), MAX(id) FROM articles";
const BOUNDS_SQL_OPT: &str =
    "SELECT MIN(a.id), MAX(a.id) FROM articles a JOIN zims z ON z.id = a.zim_id WHERE z.name = $1";

/// Article-row SELECT for the merged seek-or-fallback statement. Each branch
/// is parenthesized — Postgres requires parens for a per-branch
/// `ORDER BY … LIMIT 1` inside a `UNION` (bare `select_no_parens` excludes
/// ORDER BY/LIMIT) — and branch order makes the forward result row 0
/// (preferred); `query_opt` returns that first row.
/// The forward branch serves `id >= $target`; the backward branch (only
/// reached when the target falls in an id gap) serves the largest `id <= $target`.
/// Two variants (global / ZIM-scoped) differ only in the trailing
/// `AND z.name = $2` — kept as consts so the SQL is byte-stable.
const SEEK_SQL_GLOBAL: &str = "(SELECT a.id, a.zim_id, a.path, a.title, a.snippet, z.name \
     FROM articles a JOIN zims z ON z.id = a.zim_id \
     WHERE a.id >= $1 ORDER BY a.id ASC LIMIT 1) \
     UNION ALL (SELECT a.id, a.zim_id, a.path, a.title, a.snippet, z.name \
     FROM articles a JOIN zims z ON z.id = a.zim_id \
     WHERE a.id <= $1 ORDER BY a.id DESC LIMIT 1)";
const SEEK_SQL_OPT: &str = "(SELECT a.id, a.zim_id, a.path, a.title, a.snippet, z.name \
     FROM articles a JOIN zims z ON z.id = a.zim_id \
     WHERE a.id >= $1 AND z.name = $2 ORDER BY a.id ASC LIMIT 1) \
     UNION ALL (SELECT a.id, a.zim_id, a.path, a.title, a.snippet, z.name \
     FROM articles a JOIN zims z ON z.id = a.zim_id \
     WHERE a.id <= $1 AND z.name = $2 ORDER BY a.id DESC LIMIT 1)";

/// A single article row returned by `fetch_random_article`.
pub struct RandomArticle {
    /// Global `articles.id` (unique across ZIMs).
    pub id: i64,
    /// `zims.id` of the owning archive (join key).
    pub zim_id: i32,
    /// ZIM entry path of the article.
    pub path: String,
    /// Article title.
    pub title: String,
    /// Short extract from the article body.
    pub snippet: String,
    /// Name of the owning ZIM archive (resolved in SQL).
    pub zim: String,
}

/// Uniform i64 in [lo, hi] (caller guarantees hi >= lo). Bounds are cast
/// through u64 (bit pattern) so the full i64::MIN..=i64::MAX range — width
/// 2^64 — is handled without sign-extension overflow.
///
/// ```
/// for seed in 0..100 {
///     let v = zimservice::db::random_article::random_id_in_range(0, 999, seed);
///     assert!((0..=999).contains(&v));
/// }
/// // Degenerate: lo == hi always returns lo.
/// assert_eq!(
///     zimservice::db::random_article::random_id_in_range(7, 7, 42),
///     7
/// );
/// ```
pub fn random_id_in_range(lo: i64, hi: i64, seed: u64) -> i64 {
    // Value distance in u64 wrapping space (two's complement makes this
    // exact), widened to u128 so the full-range width 2^64 fits.
    let width = (hi as u64).wrapping_sub(lo as u64) as u128 + 1;
    let offset = seed as u128 % width;
    (lo as u64).wrapping_add(offset as u64) as i64
}

/// Read (min_id, max_id) for a scope from the database.
/// An empty table/ZIM yields `None`.
async fn fetch_bounds(pool: &Pool, zim_filter: Option<&str>) -> Result<Option<(i64, i64)>> {
    let row = match zim_filter {
        Some(zim) => {
            raw::fetch_optional::<(Option<i64>, Option<i64>), _, _>(pool, BOUNDS_SQL_OPT, |q| {
                q.bind(zim)
            })
            .await
        }
        None => {
            raw::fetch_optional::<(Option<i64>, Option<i64>), _, _>(pool, BOUNDS_SQL_GLOBAL, |q| q)
                .await
        }
    };
    match row {
        Ok(Some((Some(min_id), Some(max_id)))) => Ok(Some((min_id, max_id))),
        Ok(_) => Ok(None),
        Err(e) => Err(e),
    }
}

/// One merged seek-or-fallback round trip: `fetch_optional` on
/// [`SEEK_SQL_GLOBAL`] / [`SEEK_SQL_OPT`], returning the forward row (branch 0)
/// when it exists, else the backward row. Both branches carry the ZIM scope
/// when one is given.
async fn seek(pool: &Pool, target: i64, zim_filter: Option<&str>) -> Result<Option<RandomArticle>> {
    let row: Option<(i64, i32, String, String, String, String)> = match zim_filter {
        Some(zim) => raw::fetch_optional(pool, SEEK_SQL_OPT, |q| q.bind(target).bind(zim)).await,
        None => raw::fetch_optional(pool, SEEK_SQL_GLOBAL, |q| q.bind(target)).await,
    }?;
    Ok(
        row.map(|(id, zim_id, path, title, snippet, zim)| RandomArticle {
            id,
            zim_id,
            path,
            title,
            snippet,
            zim,
        }),
    )
}

/// Fetch a random article using an O(1) index-seek strategy.
///
/// 1. Get MIN/MAX article id (optionally filtered by ZIM name).
/// 2. Pick a random target in [min_id, max_id] in Rust.
/// 3. Seek to the smallest live id >= target (single index seek).
/// 4. If that yields nothing (e.g. gap at the top of the range), fall back to
///    the largest id <= target.
///
/// Both seeks carry the ZIM filter when one is given, so a scoped request can
/// never return an article from another ZIM. This also avoids the O(n log n)
/// `ORDER BY random() LIMIT 1` full-table scan.
pub async fn fetch_random_article(pool: &Pool, zim_filter: Option<&str>) -> Result<RandomArticle> {
    // 1. Bounds: one index MIN/MAX probe, scoped when a ZIM filter is given.
    let (min_id, max_id) = match fetch_bounds(pool, zim_filter).await? {
        Some(b) => b,
        None => return Err(Error::NotFound("no articles found".into())),
    };

    // 2. Pick a random target in [min_id, max_id] locally — no SQL round trip.
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ (std::process::id() as u64);
    let target = random_id_in_range(min_id, max_id, seed);

    // 3. One merged seek: smallest live id >= target, else (stale-bounds gap)
    //    the largest id <= target — same ZIM scope. With fresh bounds the
    //    forward branch always hits, so this is a single round trip.
    let article = match seek(pool, target, zim_filter).await? {
        Some(article) => article,
        None => {
            // Only `None` is possible when an id was deleted between the
            // bounds query and the seek. Re-measure the bounds once and
            // retry; the backward branch of the merged statement still
            // covers a partial gap.
            let (min_id, max_id) = match fetch_bounds(pool, zim_filter).await? {
                Some(b) => b,
                None => return Err(Error::NotFound("no articles found".into())),
            };
            let target = random_id_in_range(min_id, max_id, seed);
            seek(pool, target, zim_filter)
                .await?
                .ok_or_else(|| Error::NotFound("no articles found".into()))?
        }
    };

    Ok(article)
}

/// SQL for `fetch_article_snippet`: indexed metadata of one article, looked
/// up by (ZIM name, article path). The `zims` join keeps the lookup in one
/// statement (same shape as the scoped `SEEK_SQL` variants); column order
/// matches the returned tuple.
const SNIPPET_SQL: &str = "SELECT a.snippet, a.title, a.content_preview \
     FROM articles a JOIN zims z ON z.id = a.zim_id \
     WHERE z.name = $1 AND a.path = $2";

/// Fetch the indexed `(snippet, title, content_preview)` of one article by
/// ZIM name and article path (`None` when the article has no `articles`
/// row).
pub async fn fetch_article_snippet(
    pool: &Pool,
    zim: &str,
    path: &str,
) -> Result<Option<(String, String, Option<String>)>> {
    raw::fetch_optional(pool, SNIPPET_SQL, |q| q.bind(zim).bind(path)).await
}

#[cfg(test)]
mod tests {
    use super::random_id_in_range;

    #[test]
    fn single_element_range_always_returns_it() {
        for seed in [0u64, 1, 42, u64::MAX] {
            assert_eq!(random_id_in_range(7, 7, seed), 7);
            assert_eq!(random_id_in_range(i64::MIN, i64::MIN, seed), i64::MIN);
            assert_eq!(random_id_in_range(i64::MAX, i64::MAX, seed), i64::MAX);
        }
    }

    #[test]
    fn results_stay_in_range() {
        let mut seen = std::collections::HashSet::new();
        for seed in 0u64..1000 {
            let v = random_id_in_range(0, 999, seed);
            assert!((0..=999).contains(&v), "out of range: {v}");
            seen.insert(v);
        }
        assert!(seen.len() >= 10, "expected spread, got {}", seen.len());
    }

    #[test]
    fn full_i64_range_does_not_panic() {
        // Casts bounds individually to u128 — `hi - lo` in i64 would overflow.
        for seed in [0u64, 1, 999, u64::MAX] {
            let v = random_id_in_range(i64::MIN, i64::MAX, seed);
            let _ = v; // any value is valid; not panicking is the assertion
        }
    }

    #[test]
    fn seed_zero_and_max_are_distinct() {
        assert_ne!(
            random_id_in_range(0, 10_000, 0),
            random_id_in_range(0, 10_000, u64::MAX)
        );
    }
}
