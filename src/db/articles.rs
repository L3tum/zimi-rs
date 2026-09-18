//! Explicit-ID single-article lookups (the non-random half of article
//! fetching). The 2026-09 review split this out of `random_article.rs`,
//! whose doc/identity is O(1) **random** selection via index seek: a
//! named fetch of one article's indexed metadata by (ZIM name, path) is
//! not a random draw and did not belong in that module.
//!
//! Module split:
//! - [`crate::db::random_article`] — random article selection
//!   (`fetch_random_article`).
//! - `crate::db::articles` (this module) — explicit-ID lookups of a single
//!   article's indexed metadata.
use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::Result;

/// SQL for `fetch_article_snippet`: indexed metadata of one article, looked
/// up by (ZIM name, article path). The `zims` join keeps the lookup in one
/// statement (same shape as the scoped seek-SQL variants in the
/// `random_article` module); column order matches the returned tuple.
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
