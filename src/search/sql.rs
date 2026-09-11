//! Pure SQL-construction helpers for the search engine (query building,
//! fragment/ARM assembly, column fragments). No DB access — this module
//! builds SQL text only.
//!
//! **Why raw SQL:** every query here contains a Postgres operator the
//! `db::raw` helper shapes cannot take, so all five stay on the raw-SQL
//! path ([`crate::db::raw`] / `run_sql_on`):
//! - `fts_sql`: `websearch_to_tsquery`, the `@@` match operator, `ts_rank_cd`,
//!   and `ts_headline` (full-text search).
//! - `trgm_prefix_sql` / `trgm_contains_sql`: the pg_trgm `similarity()` score
//!   (the `LIKE` filter alone would be helper-expressible, but the SELECT
//!   score is not).
//! - `trgm_similarity_sql`: the pg_trgm `%` "is similar" operator and
//!   `similarity()`.
//! - `vector_sql`: the pgvector `<=>` cosine-distance operator, used in both
//!   the score and the ANN `ORDER BY` (an index-only ANN ordering the
//!   helpers cannot emit).
use super::trgm_arms_enabled;

/// Hard ceiling on `limit` regardless of `search.max_limit` (keeps the i32
/// fetch budget bounded).
pub(super) const SEARCH_HARD_LIMIT: usize = 500;

/// Hard ceiling on `offset`: deeper pages are clamped here.
pub(super) const SEARCH_HARD_OFFSET: usize = 5000;

/// Per-branch SQL fetch cap: max offset + max limit (5500).
///
/// Every search branch fetches up to this many rows so the post-dedup merged
/// pool can cover any legal page (`offset + limit` ≤ this cap). Without it
/// each branch fetched a flat `limit * 2` (~6×limit pooled max), so pages
/// beyond ~3 pages were unsatisfiable even with thousands of matches.
pub(super) const SEARCH_FETCH_HARD_CAP: usize = SEARCH_HARD_OFFSET + SEARCH_HARD_LIMIT;

/// Per-branch fetch count for a request: `min(limit + offset,
/// SEARCH_FETCH_HARD_CAP)` (offset-aware — see the cap's doc for why the old
/// flat `limit * 2` under-fetched deep pages).
pub(super) fn branch_fetch_limit(limit: usize, offset: usize) -> i32 {
    limit.saturating_add(offset).min(SEARCH_FETCH_HARD_CAP) as i32
}

/// Escape LIKE wildcards so a user-supplied query matches literally (pair with
/// `ESCAPE '\'` in the SQL).
pub(super) fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Build the three trigram arms for a search (PERF-2). `run` gates all three
/// on the search mode (fts/vector/semantic skip trigrams entirely); the
/// contains/similarity arms are additionally gated by
/// [`trgm_arms_enabled`], so a short query yields prefix-only. Pure so the
/// exact arm set can be unit-tested without a pool.
pub(super) fn build_trgm_arms(
    run: bool,
    query_lower: &str,
    zim: Option<&str>,
    lang: Option<&str>,
    limit: i32,
    weight: f64,
    threshold: f64,
) -> (Option<SqlQuery>, Option<SqlQuery>, Option<SqlQuery>) {
    if !run {
        return (None, None, None);
    }
    let arms_ok = trgm_arms_enabled(query_lower);
    (
        Some(trgm_prefix_sql(query_lower, zim, lang, limit, weight)),
        arms_ok.then(|| trgm_contains_sql(query_lower, zim, lang, limit, weight)),
        arms_ok.then(|| trgm_similarity_sql(query_lower, threshold, zim, lang, limit, weight)),
    )
}

/// A pure SQL builder result: the query string and its parameter values.
/// `pub` so the integration suite can exercise `run_sql_on`'s soft-fail
/// contract (broken query → empty, no error) against a live pool.
#[derive(Debug, Clone, Default)]
pub struct SqlQuery {
    /// The SQL text, with `$N` placeholders in Postgres positional style.
    pub sql: String,
    /// Parameter values, one per `$N` placeholder (1-based).
    pub params: Vec<String>,
}

impl SqlQuery {
    /// Highest `$N` placeholder index appearing in the SQL (0 = none).
    pub(super) fn placeholder_count(&self) -> usize {
        let mut max = 0;
        let bytes = self.sql.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'$' {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > start {
                    if let Ok(n) = self.sql[start..j].parse::<usize>() {
                        max = max.max(n);
                    }
                }
                i = j;
            } else {
                i += 1;
            }
        }
        max
    }

    /// Assert that the placeholder count matches the params length.
    #[cfg(test)]
    pub(super) fn assert_placeholders_match(&self) {
        assert_eq!(
            self.placeholder_count(),
            self.params.len(),
            "SQL has {} placeholders but {} params:\n{}",
            self.placeholder_count(),
            self.params.len(),
            self.sql
        );
    }
}

/// Shared SQL tail for the search builders: the ZIM/language filter block,
/// then `ORDER BY` + `LIMIT`.
///
/// `order_by` is `"ORDER BY score DESC, a.id"` for the FTS/trgm builders
/// (the `a.id` tiebreaker makes equal-score page boundaries deterministic).
/// `vector_sql` must pass the ANN ordering (`ORDER BY a.embedding <=>
/// $1::vector`) instead: the score expression is not index-usable (hnsw/
/// ivfflat), so a uniform `ORDER BY score` tail — or ANY secondary ORDER BY
/// key, which would also defeat the index's order guarantee — would silently
/// degrade the vector query to a full scan + sort. Vector determinism is
/// restored by the outer re-sort `vector_sql` wraps around this tail.
fn push_filters(
    sq: &mut SqlQuery,
    zim: Option<&str>,
    lang: Option<&str>,
    order_by: &str,
    limit: i32,
) {
    if let Some(z) = zim {
        sq.params.push(z.to_string());
        sq.sql
            .push_str(&format!(" AND z.name = ${}", sq.params.len()));
    }
    if let Some(l) = lang {
        sq.params.push(l.to_string());
        sq.sql
            .push_str(&format!(" AND a.language = ${}", sq.params.len()));
    }
    sq.params.push(limit.to_string());
    sq.sql
        .push_str(&format!(" {order_by} LIMIT ${}::int8", sq.params.len()));
}

// The `, a.id` tiebreaker (`articles.id`, BIGINT identity PK) is mandatory
// for page-boundary determinism: without it, equal-score rows have no stable
// DB order and can reorder between requests, skipping/duplicating rows at
// `offset` boundaries. (`a` is the articles alias in every branch.)
const ORDER_BY_SCORE_DESC: &str = "ORDER BY score DESC, a.id";
const ORDER_BY_ANN: &str = "ORDER BY a.embedding <=> $1::vector";

/// Shared SELECT column list for the five search builders (S2: one constant
/// instead of five verbatim copies). `left(a.content_preview, 600)`: the
/// preview is only needed for merged-winner rows (the embed claim uses 500
/// chars), and pulling the full ~2000-char preview into 4–5 branches ×
/// limit*fetch rows is the ~10MB worst-case over-fetch — clamped at fetch.
const SELECT_ARTICLE_COLS: &str = "a.id, a.zim_id, a.path, a.title";
const SELECT_ARTICLE_TAIL: &str =
    ", left(a.content_preview, 600) as content_preview, a.language, z.name as zim_name";

/// Shared similarity score expression for the three trgm builders (identical
/// in all three — one place instead of three verbatim copies).
const SIMILARITY_SCORE: &str = "GREATEST(similarity(a.title_lower, $1), 0.0)";

/// Skeleton for the four single-source score builders (trgm prefix/contains/
/// similarity + vector): the shared
/// `SELECT <cols>, a.snippet<tail>, <weight> * <score> as score FROM articles
/// a JOIN zims z ON z.id = a.zim_id WHERE <where>` prefix, then the shared
/// [`push_filters`] tail. The FTS builder is left standalone — its `tsq` CTE +
/// `CROSS JOIN` + `ts_headline` don't fit this shape.
fn score_query(
    score_expr: &str,
    where_clause: &str,
    params: Vec<String>,
    order_by: &str,
    filters: (Option<&str>, Option<&str>),
    limit: i32,
    weight: f64,
) -> SqlQuery {
    let (zim, lang) = filters;
    let sql = format!(
        "SELECT {SELECT_ARTICLE_COLS}, a.snippet{SELECT_ARTICLE_TAIL}, {weight} * {score_expr} as score FROM articles a JOIN zims z ON z.id = a.zim_id WHERE {where_clause}"
    );
    let mut sq = SqlQuery { sql, params };
    push_filters(&mut sq, zim, lang, order_by, limit);
    sq
}

/// Build the FTS search query. `$1` is always the query string.
///
/// Raw SQL: `websearch_to_tsquery` / `@@` / `ts_rank_cd` / `ts_headline`
/// have no `db::raw` helper equivalents.
pub(super) fn fts_sql(
    query: &str,
    highlight: bool,
    zim: Option<&str>,
    lang: Option<&str>,
    limit: i32,
    weight: f64,
) -> SqlQuery {
    // M2: build `websearch_to_tsquery('simple', $1)` once in a single-row CTE
    // for the headline and the `ts_rank_cd` score (down from two call sites),
    // while the WHERE `@@` predicate stays inline. The inline form is
    // deliberate: Postgres cannot use the GIN index when the index condition
    // references a column of another relation (lateral index conditions are
    // unsupported), so hoisting the WHERE into the CTE would degrade it to a
    // seq scan. Results are identical: same WHERE predicate, same `ts_rank_cd`
    // score, same `ts_headline` output. (The trgm arms'
    // `similarity(a.title_lower, $1)` is per-row, not a per-query constant, so
    // it cannot be hoisted at all.)
    let snippet_col = if highlight {
        "coalesce(ts_headline('simple', a.content_preview, tsq.q, 'StartSel=<b>', 'StopSel=</b>', 'MaxFragments=1', 'MaxWords=30', 'MinWords=10'), a.snippet)"
    } else {
        "a.snippet"
    };

    let mut sql = String::from("WITH tsq AS (SELECT websearch_to_tsquery('simple', $1) AS q) ");
    sql.push_str(&format!(
        "SELECT {SELECT_ARTICLE_COLS}, {snippet_col}{SELECT_ARTICLE_TAIL},"
    ));
    sql.push_str(&format!(
        " {} * ts_rank_cd(a.search_vector, tsq.q) as score",
        weight
    ));
    sql.push_str(" FROM articles a JOIN zims z ON z.id = a.zim_id CROSS JOIN tsq");
    sql.push_str(" WHERE a.search_vector @@ websearch_to_tsquery('simple', $1)");

    let mut sq = SqlQuery {
        sql,
        params: vec![query.to_string()],
    };
    push_filters(&mut sq, zim, lang, ORDER_BY_SCORE_DESC, limit);
    sq
}

/// Build a trigram prefix-match query (btree index on `title_lower`).
///
/// Raw SQL: the score is pg_trgm `similarity()`, which the `db::raw`
/// helpers cannot express (the `LIKE` filter alone would be).
pub(super) fn trgm_prefix_sql(
    query_lower: &str,
    zim: Option<&str>,
    lang: Option<&str>,
    limit: i32,
    weight: f64,
) -> SqlQuery {
    let pattern = format!("{}%", escape_like(query_lower));
    score_query(
        SIMILARITY_SCORE,
        "a.title_lower LIKE $2 ESCAPE '\\'",
        vec![query_lower.to_string(), pattern],
        ORDER_BY_SCORE_DESC,
        (zim, lang),
        limit,
        weight,
    )
}

/// Build a trigram contains-match query (GIN trgm index on `title_lower`).
///
/// Raw SQL: the score is pg_trgm `similarity()` (see `trgm_prefix_sql`).
///
/// `pub` (not `pub(super)`): the query-plan regression gate in
/// `tests/integration/trgm_plan.rs` EXPLAINs this exact arm SQL against a
/// 100k-row temp DB — it must build the arm the same way `run` does, not
/// re-transcribe the SQL by hand (drift would make the gate meaningless).
pub fn trgm_contains_sql(
    query_lower: &str,
    zim: Option<&str>,
    lang: Option<&str>,
    limit: i32,
    weight: f64,
) -> SqlQuery {
    let pattern = format!("%{}%", escape_like(query_lower));
    score_query(
        SIMILARITY_SCORE,
        "a.title_lower LIKE $2 ESCAPE '\\'",
        vec![query_lower.to_string(), pattern],
        ORDER_BY_SCORE_DESC,
        (zim, lang),
        limit,
        weight,
    )
}

/// Build a trigram similarity-threshold query (GiST trgm index on `title_lower`).
///
/// Raw SQL: the pg_trgm `%` operator and `similarity()` have no `db::raw`
/// helper equivalents.
///
/// BUG-1: the old `WHERE similarity(a.title_lower, $1) > $2` bound the
/// threshold as `text` (`SqlQuery.params` is `Vec<String>`); `real > text`
/// has no operator, so every prepare errored and `run_sql_on` soft-failed
/// the whole arm into silence. The predicate is now the index-usable `%`
/// operator (pg_trgm "is similar", `similarity >= the
/// pg_trgm.similarity_threshold` GUC — the only trgm predicate a GiST index
/// can serve) conjoined with the settings threshold, cast explicitly because
/// `similarity()` returns `real` and the param binds as `text`.
/// Equivalent to the originally-intended `similarity > threshold`: the
/// settings floor is 0.3, the GUC default, so `similarity > t` already
/// implies `similarity >= 0.3`; the `%` conjunct is a superset pre-scan
/// that lets the planner use the GiST index.
///
/// ⚠️ GUC coupling: the `%` prefilter is gated by the SERVER
/// `pg_trgm.similarity_threshold` GUC (default 0.3), not by the settings
/// `threshold` param. The equivalence holds only while that GUC is ≤ our
/// `search.trgm_threshold` (which is floored at 0.3, so it always holds at
/// the GUC default): `similarity > t (≥ 0.3)` ⇒ `similarity ≥ 0.3` ⇒ passes
/// `%`. If an operator raises the server GUC above our threshold, the `%`
/// conjunct **silently excludes** rows the `similarity > t` half would have
/// kept. Don't raise that GUC — or drop the `%` prefilter from this
/// predicate.
///
/// `pub` (not `pub(super)`): the query-plan regression gate in
/// `tests/integration/trgm_plan.rs` EXPLAINs this exact arm SQL against a
/// 100k-row temp DB (see [`trgm_contains_sql`] for why it must stay in
/// lockstep with `run`).
pub fn trgm_similarity_sql(
    query_lower: &str,
    threshold: f64,
    zim: Option<&str>,
    lang: Option<&str>,
    limit: i32,
    weight: f64,
) -> SqlQuery {
    score_query(
        SIMILARITY_SCORE,
        "a.title_lower % $1 AND similarity(a.title_lower, $1) > $2::float8",
        vec![query_lower.to_string(), threshold.to_string()],
        ORDER_BY_SCORE_DESC,
        (zim, lang),
        limit,
        weight,
    )
}

/// Build the vector (semantic) search query. `$1` is the pgvector literal.
///
/// Raw SQL: the pgvector `<=>` cosine-distance operator (score + ANN
/// `ORDER BY`) has no `db::raw` helper equivalent.
pub(super) fn vector_sql(
    vec_str: &str,
    zim: Option<&str>,
    lang: Option<&str>,
    limit: i32,
    weight: f64,
) -> SqlQuery {
    // Inner query: ANN ordering ONLY (NOT `ORDER_BY_SCORE_DESC`). The score
    // expression is not index-usable, so the inner tail must stay
    // `ORDER BY a.embedding <=> $1::vector LIMIT k` or the query degrades to
    // a full scan + sort. A secondary key (like the score branches' `a.id`
    // tiebreaker) cannot be added INSIDE the inner query either: the ANN
    // index only guarantees distance order, so `ORDER BY dist, id` would
    // force a Sort node over the entire (filtered) table instead of a
    // top-k index seek.
    let mut inner = score_query(
        "GREATEST(1.0 - (a.embedding <=> $1::vector), 0.0)",
        "a.embedding IS NOT NULL",
        vec![vec_str.to_string()],
        ORDER_BY_ANN,
        (zim, lang),
        limit,
        weight,
    );
    // Outer deterministic re-sort over the fetched top-k rows: exact
    // float-distance ties (hence equal scores) are possible between
    // distinct articles, and without the `s.id` tiebreaker those rows could
    // reorder between requests, skipping/duplicating rows at page
    // boundaries. Sorting ≤ k already-fetched rows is negligible next to
    // the ANN seek.
    let inner_sql = inner.sql;
    let limit_idx = inner.params.len() + 1;
    inner.params.push(limit.to_string());
    inner.sql = format!(
        "SELECT * FROM ({inner_sql}) s ORDER BY s.score DESC, s.id LIMIT ${limit_idx}::int8"
    );
    inner
}

/// Vector (ANN) fetch limit for the branch: the offset-aware `fetch`
/// (`min(limit + offset, SEARCH_FETCH_HARD_CAP)`, see
/// [`branch_fetch_limit`]) over-fetched 2× (4× when a post-filter is
/// active — ANN top-k is not filter-aware, so a plain fetch can return
/// fewer rows than requested after the WHERE), then clamped back to
/// [`SEARCH_FETCH_HARD_CAP`].
///
/// The clamp bounds the worst-case ANN top-k at 5500 instead of
/// 5500×4 = 22000 for a filtered deepest-page request. It never drops
/// below the plain fetch (multiplier ≥ 2, cap ≥ fetch), so an unfiltered
/// vector-only search can still satisfy every legal page; a FILTERED
/// vector-ONLY search on extreme pages may under-return if the ANN scan
/// discards enough post-filter rows — accepted: 22000-row ANN top-k is
/// not a sane cost to avoid that corner, and hybrid-mode requests have the
/// FTS/trgm branches (each capped at 5500) to fill the pool.
/// `merge_results` still applies offset/limit, so this is
/// correctness-neutral either way.
pub(super) fn vector_fetch_limit(fetch: usize, filtered: bool) -> i32 {
    fetch
        .saturating_mul(if filtered { 4 } else { 2 })
        .min(SEARCH_FETCH_HARD_CAP) as i32
}
