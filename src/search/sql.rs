//! Pure SQL-construction helpers for the search engine (query building,
//! fragment/ARM assembly, column fragments). No DB access — this module
//! builds SQL text only.
//!
//! **Why raw SQL and not SeaORM QueryBuilder:** every builder here contains
//! a Postgres operator the SeaORM QueryBuilder cannot express, so all five
//! stay on the raw-SQL escape hatch ([`crate::db::raw`] / `run_sql_on`):
//! - `fts_sql`: `websearch_to_tsquery`, the `@@` match operator, `ts_rank_cd`,
//!   and `ts_headline` (full-text search).
//! - `trgm_prefix_sql` / `trgm_contains_sql`: the pg_trgm `similarity()` score
//!   (the `LIKE` filter alone would be builder-expressible, but the SELECT
//!   score is not).
//! - `trgm_similarity_sql`: the pg_trgm `%` "is similar" operator and
//!   `similarity()`.
//! - `vector_sql`: the pgvector `<=>` cosine-distance operator, used in both
//!   the score and the ANN `ORDER BY` (an index-only ANN ordering the
//!   builder cannot emit).
use super::trgm_arms_enabled;

/// Hard ceiling on `limit` regardless of `search.max_limit`, applied before the
/// `limit * 2` casts to keep the i32 fetch budget bounded.
pub(super) const SEARCH_HARD_LIMIT: usize = 500;

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
    pub sql: String,
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
/// `order_by` is `"ORDER BY score DESC"` for the FTS/trgm builders. `vector_sql`
/// must pass the ANN ordering ("ORDER BY a.embedding <=> $1::vector") instead:
/// the score expression is not index-usable (hnsw/ivfflat), so a uniform
/// `ORDER BY score` tail would silently degrade the vector query to a full
/// scan + sort.
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

const ORDER_BY_SCORE_DESC: &str = "ORDER BY score DESC";
const ORDER_BY_ANN: &str = "ORDER BY a.embedding <=> $1::vector";

/// Shared SELECT column list for the five search builders (S2: one constant
/// instead of five verbatim copies). `left(a.content_preview, 600)`: the
/// preview is only needed for merged-winner rows (the embed claim uses 500
/// chars), and pulling the full ~2000-char preview into 4–5 branches ×
/// limit*2 rows is the ~10MB worst-case over-fetch — clamped at fetch.
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
/// have no SeaORM QueryBuilder equivalents.
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
/// Raw SQL: the score is pg_trgm `similarity()`, which the SeaORM
/// QueryBuilder cannot express (the `LIKE` filter alone would be).
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
pub(super) fn trgm_contains_sql(
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
/// Raw SQL: the pg_trgm `%` operator and `similarity()` have no SeaORM
/// QueryBuilder equivalents.
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
pub(super) fn trgm_similarity_sql(
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
/// `ORDER BY`) has no SeaORM QueryBuilder equivalent.
pub(super) fn vector_sql(
    vec_str: &str,
    zim: Option<&str>,
    lang: Option<&str>,
    limit: i32,
    weight: f64,
) -> SqlQuery {
    // ANN ordering (NOT `ORDER_BY_SCORE_DESC`): the score expression is not
    // index-usable, so `push_filters` must keep `ORDER BY a.embedding <=>
    // $1::vector` or the query degrades to a full scan + sort.
    score_query(
        "GREATEST(1.0 - (a.embedding <=> $1::vector), 0.0)",
        "a.embedding IS NOT NULL",
        vec![vec_str.to_string()],
        ORDER_BY_ANN,
        (zim, lang),
        limit,
        weight,
    )
}

/// ANN fetch limit: over-fetch 2× more when a post-filter is active.
/// ANN top-k is not filter-aware — with `zim_filter`/`lang_filter` active a
/// plain `limit*2` fetch can return fewer than `limit` rows after the WHERE.
/// `merge_results` still applies offset/limit, so this is correctness-neutral.
pub(super) fn vector_fetch_limit(limit: usize, filtered: bool) -> i32 {
    (limit * (if filtered { 4 } else { 2 })) as i32
}
