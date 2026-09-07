//! Multi-engine article search (FTS + trigram + pgvector) with score merge.
//!
//! `SearchEngine` runs the configured branches, dedups by `articles.id`
//! keeping the highest score, and interleaves results by score descending;
//! the SQL builders are pure and unit-tested (placeholder shapes, not
//! interpolated).
mod sql;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use sqlx::Executor;

use crate::db::pool::Pool;
use crate::embed::{format_vector, EmbedClient, EmbedConfig};
use crate::error::{Error, Result};
use crate::settings::{SearchParamsSnapshot, SettingsCache};

pub use self::sql::SqlQuery;
use self::sql::{
    build_trgm_arms, fts_sql, trgm_contains_sql, trgm_prefix_sql, trgm_similarity_sql,
    vector_fetch_limit, vector_sql, SEARCH_HARD_LIMIT,
};

/// PERF-2: the trigram *contains*/*similarity* arms need at least 3 chars to
/// be index-useful (Postgres trigrams are built from 3-char windows), so gate
/// them off for short queries and keep only the btree-backed prefix arm
/// (`LIKE 'q%'`). Counts **chars**, so a multi-byte "日" is 1 (not 3).
pub(crate) fn trgm_arms_enabled(query: &str) -> bool {
    query.chars().count() >= 3
}

/// One search-branch row, in the fixed column order every branch SELECT
/// emits (id, zim_id, path, title, snippet, content_preview, language,
/// zim_name, score).
type SearchRow = (
    i64,
    i32,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    f64,
);

/// Merge the three suggest branches into the final list, preserving the old
/// single-query `ORDER BY CASE WHEN prefix THEN 0 ELSE 1 END, similarity
/// DESC` semantics: prefix matches first, then the other branches, similarity
/// (score) descending within each group. Dedup by id — the prefix group wins
/// a duplicate (in the old SQL such a row was classified as prefix by the
/// CASE). Truncated to `limit` (the old single-query `LIMIT` — neither
/// consumer caps the result itself).
fn merge_suggest(
    prefix: Vec<SearchResult>,
    others: Vec<SearchResult>,
    limit: usize,
) -> Vec<SearchResult> {
    let by_score_desc = |a: &SearchResult, b: &SearchResult| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    };
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut p = prefix;
    p.retain(|r| seen.insert(r.id));
    p.sort_by(by_score_desc);
    let mut o = others;
    o.retain(|r| seen.insert(r.id));
    o.sort_by(by_score_desc);
    let mut out = p;
    out.extend(o);
    out.truncate(limit);
    out
}

/// Search engine that wraps Postgres FTS + trgm + optional vector (semantic) queries.
#[derive(Clone)]
pub struct SearchEngine {
    pool: Pool,
    settings: SettingsCache,
    /// Lazily-built embedding client, keyed by a settings fingerprint so it
    /// is rebuilt when the endpoint/key/model/dimension change at runtime.
    embed_client: Arc<Mutex<Option<(String, EmbedClient)>>>,
    /// Whether the `pg_trgm` extension is available, resolved once per engine
    /// and cached (`WI-37`). Uses a TTL-based cache (WI-5): when the cached
    /// value is `false` (trgm unavailable), a background task re-probes every
    /// 60 s so the arms recover when the extension is installed at runtime.
    /// `Arc` so the `Clone` derive shares the cache across engine copies.
    trgm_ready: Arc<std::sync::Mutex<Option<(bool, std::time::Instant)>>>,
    /// Per-branch degradation tracker (WI-5): records failures so `/health`
    /// and search responses can surface silent capability loss.
    degradation: crate::health::DegradationTracker,
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct SearchResult {
    pub id: i64,
    pub zim_id: i32,
    pub zim_name: String,
    pub path: String,
    pub title: String,
    pub snippet: String,
    pub content_preview: Option<String>,
    pub score: f64,
    pub language: String,
}

/// Optional filters and pagination for [`SearchEngine::search`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchParams<'a> {
    pub zim: Option<&'a str>,
    pub language: Option<&'a str>,
    pub mode: Option<&'a str>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub highlight: bool,
}

impl SearchEngine {
    pub fn new(
        pool: Pool,
        settings: SettingsCache,
        degradation: crate::health::DegradationTracker,
    ) -> Self {
        Self {
            pool,
            settings,
            embed_client: Arc::new(Mutex::new(None)),
            trgm_ready: Arc::new(std::sync::Mutex::new(None)),
            degradation,
        }
    }

    /// TTL for the trgm probe cache: 60 s when `false` (stale → re-probe),
    /// effectively infinite when `true` (no reason to re-probe a positive).
    const TRGM_TTL: std::time::Duration = std::time::Duration::from_secs(60);

    /// Whether the `pg_trgm` extension is available, resolved with a
    /// TTL-based cache (WI-5). A `false` result is re-probed after 60 s so
    /// the trgm arms recover when the extension is installed at runtime. A
    /// `true` result is cached permanently (no reason to re-probe).
    ///
    /// Acquires a pool connection for the probe — use [`SearchEngine::ensure_trgm_on`] from
    /// a code path that already holds a connection (the `search()`/`suggest()`
    /// arms): a second nested checkout stalls the full `acquire_timeout` when
    /// `DB_POOL_SIZE=1`.
    pub async fn ensure_trgm(&self) -> bool {
        if let Some(val) = self.trgm_cached_if_fresh() {
            return val;
        }
        // Slow path: probe the DB on a checked-out connection.
        let ok = match self.pool.acquire().await {
            Ok(mut client) => self.trgm_probe(&mut *client).await,
            Err(_) => false,
        };
        self.trgm_store(ok)
    }

    /// [`SearchEngine::ensure_trgm`] for callers that already hold a pooled connection
    /// (the `search()`/`suggest()` arms): probes **through** the passed
    /// executor instead of taking a second checkout — at `DB_POOL_SIZE=1`
    /// a nested `pool.acquire()` would stall the full `acquire_timeout`
    /// (10 s) before soft-disabling the trgm arms.
    pub async fn ensure_trgm_on<'e, E>(&self, executor: E) -> bool
    where
        E: Executor<'e, Database = sqlx::Postgres>,
    {
        if let Some(val) = self.trgm_cached_if_fresh() {
            return val;
        }
        self.trgm_store(self.trgm_probe(executor).await)
    }

    /// The `pg_trgm` catalog probe on any executor. Raw SQL (catalog probe —
    /// `pg_extension` is a catalog table with no `db::raw` helper; plain
    /// SELECT, no special operators).
    async fn trgm_probe<'e, E>(&self, executor: E) -> bool
    where
        E: Executor<'e, Database = sqlx::Postgres>,
    {
        sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'") // RAW-OK: catalog probe of pg_extension — no db::raw helper exists for catalog tables (sanctioned class per the db::raw doc)
            .fetch_optional(executor)
            .await
            .is_ok()
    }

    /// Fast path: the cached trgm value, if still fresh (see the `trgm_ready`
    /// field for the TTL semantics).
    fn trgm_cached_if_fresh(&self) -> Option<bool> {
        let g = self.trgm_ready.lock().expect("trgm_ready lock poisoned");
        if let Some((val, ts)) = *g {
            if val || self.trgm_ready_fresh(ts) {
                return Some(val);
            }
        }
        None
    }

    /// Record a probe outcome (a failure feeds the degradation tracker) and
    /// refresh the cache with it.
    fn trgm_store(&self, ok: bool) -> bool {
        if !ok {
            self.degradation.record_failure("pg_trgm_probe");
        }
        let mut g = self.trgm_ready.lock().expect("trgm_ready lock poisoned");
        *g = Some((ok, std::time::Instant::now()));
        ok
    }

    /// Whether a cached trgm timestamp is still within the TTL.
    fn trgm_ready_fresh(&self, ts: std::time::Instant) -> bool {
        std::time::Instant::now()
            .checked_duration_since(ts)
            .is_some_and(|age| age < Self::TRGM_TTL)
    }

    /// Public accessor for the background re-probe task (WI-5).
    pub fn trgm_is_degraded(&self) -> bool {
        let g = self.trgm_ready.lock().expect("trgm_ready lock poisoned");
        matches!(*g, Some((false, _)))
    }

    /// Settings fingerprint for the embed client cache key. Read from the
    /// per-request [`SearchParamsSnapshot`] (WI-35) so `search()` reuses the
    /// single-pass read instead of re-hitting the cache per field.
    fn embed_fingerprint(&self, snap: &SearchParamsSnapshot) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            snap.embed_endpoint,
            snap.embed_api_key,
            snap.embed_model,
            snap.embed_dimension,
            snap.embed_batch_size
        )
    }

    /// Embedding client for the current settings, or `None` when embedding is
    /// disabled/unconfigured. Rebuilds the client only when the relevant
    /// settings change. Callers pass the precomputed `fingerprint`
    /// (`embed_fingerprint`) so `search()` computes it once per request
    /// instead of per cache operation.
    ///
    /// Async: on a fingerprint miss the endpoint's resolved address is
    /// looked up (DNS-rebinding pin) **outside** the cache lock — a DNS call
    /// must not serialize concurrent `search()` calls — then the guard is
    /// re-acquired and the fingerprint re-checked (another task may have
    /// filled the cache during the await).
    ///
    /// Cache write policy (re-checked after the await): the cache is written
    /// only when it is EMPTY or holds the SAME fingerprint as ours (a
    /// same-fingerprint entry is kept as-is — an equivalent client). A cache
    /// holding a *different* fingerprint is left untouched: that writer
    /// started after a settings change and is newer, so it wins; the next
    /// request carrying our fingerprint rebuilds. Our own freshly-built
    /// client serves this request regardless of the cache state.
    async fn embed_client_with(&self, fingerprint: &str) -> Option<EmbedClient> {
        {
            let guard = self
                .embed_client
                .lock()
                .expect("embed client mutex poisoned");
            if guard
                .as_ref()
                .map(|(fp, _)| fp.as_str() == fingerprint)
                .unwrap_or(false)
            {
                return guard.as_ref().map(|(_, c)| c.clone());
            }
        }
        let config = EmbedConfig::from_settings(&self.settings)?;
        let pin = match crate::netguard::resolve_download_host(&config.endpoint, false, true).await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("vector search disabled: {e}");
                return None;
            }
        };
        let client = match EmbedClient::new(config, pin) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("vector search disabled: {e}");
                return None;
            }
        };
        let mut guard = self
            .embed_client
            .lock()
            .expect("embed client mutex poisoned");
        // Re-check: another task may have filled the cache during the await.
        // Write policy (see the method doc): insert into an empty cache;
        // keep an existing same-fingerprint entry; NEVER clobber a different
        // fingerprint — a concurrent newer writer wins, and the next request
        // with our fingerprint rebuilds. Our own client serves this request
        // regardless of what the cache ends up holding.
        match guard.as_ref() {
            None => {
                *guard = Some((fingerprint.to_string(), client));
                guard.as_ref().map(|(_, c)| c.clone())
            }
            Some((fp, _)) if fp == fingerprint => guard.as_ref().map(|(_, c)| c.clone()),
            Some(_) => Some(client),
        }
    }

    /// Perform a search using full-text (FTS) and/or trigram (fuzzy/prefix) engines.
    ///
    /// `mode` selects which engine(s) run:
    ///   - `"fts"` → full-text search only
    ///   - `"trgm"` / `"fuzzy"` / `"prefix"` → trigram/prefix only
    ///   - `"vector"` / `"semantic"` → semantic (embedding) search only
    ///   - `"hybrid"` or `None` → all enabled engines, merged and de-duplicated
    ///
    /// `highlight` wraps matched terms in the contextual snippet using Postgres
    /// `ts_headline` over `content_preview` (full content is not stored in DB).
    pub async fn search(&self, query: &str, p: &SearchParams<'_>) -> Result<Vec<SearchResult>> {
        let SearchParams {
            zim: zim_filter,
            language: lang_filter,
            mode,
            limit,
            offset,
            highlight,
        } = *p;
        let start = Instant::now();
        // WI-35: read every search-path setting in one `cache.read()` pass so
        // this request never observes a torn mix across a concurrent mutation.
        let snap = self.settings.search_params_snapshot();
        let limit = limit
            .unwrap_or(snap.default_limit)
            .min(snap.max_limit)
            .min(SEARCH_HARD_LIMIT);
        let offset = offset.unwrap_or(0).min(SEARCH_HARD_LIMIT * 10);

        let run_fts = !matches!(
            mode,
            Some("trgm") | Some("fuzzy") | Some("prefix") | Some("vector") | Some("semantic")
        );
        let run_trgm = !matches!(mode, Some("fts") | Some("vector") | Some("semantic"));
        // Vector branch: runs for explicit vector/semantic modes and for
        // hybrid/None modes — but always only when embedding is enabled. An
        // explicit `mode=vector` with embedding disabled degrades to no
        // results (as documented) instead of calling the embedding API.
        let run_vector = snap.embedding_enabled
            && (matches!(mode, Some("vector") | Some("semantic"))
                || !matches!(
                    mode,
                    Some("fts") | Some("trgm") | Some("fuzzy") | Some("prefix")
                ));

        let fts_weight = snap.fts_weight;
        let trgm_weight = snap.trgm_weight;
        let vector_weight = snap.vector_weight;
        let limit_i32 = (limit * 2) as i32;

        // ── Build all branch SQL up front (pure, index-friendly shapes) ──
        let fts_sq: Option<SqlQuery> = if run_fts {
            Some(fts_sql(
                query,
                highlight,
                zim_filter,
                lang_filter,
                limit_i32,
                fts_weight,
            ))
        } else {
            None
        };
        // PERF-2: lift the shared lowered query + threshold so the trigram
        // arms can be gated on length (short queries → prefix arm only).
        let query_lower = query.to_lowercase();
        let trgm_threshold = snap.trgm_threshold;
        let (sq_prefix, sq_contains, sq_similarity) = build_trgm_arms(
            run_trgm,
            &query_lower,
            zim_filter,
            lang_filter,
            limit_i32,
            trgm_weight,
            trgm_threshold,
        );
        for sq in [&fts_sq, &sq_prefix, &sq_contains, &sq_similarity]
            .into_iter()
            .flatten()
        {
            debug_assert!(sq.placeholder_count() == sq.params.len());
        }

        // ── Run the slow embed HTTP concurrently with every DB branch ──
        // Total latency is max(embed, fts, trgm×3) + the vector ANN seek, not
        // the sum. The pooled connection is acquired INSIDE the DB arm and
        // dropped at the end of that arm (the arm returns only the rows), so a
        // slow/failing embed API (up to its timeout, default 60 s) cannot hold
        // a pool connection hostage: `tokio::join!` retains each arm's output
        // until BOTH arms finish, so returning the client out of the join
        // would keep it checked out for the entire embed round-trip. The
        // vector ANN seek re-acquires a fresh connection afterward.
        // H3: each phase holds exactly ONE pooled connection, and only for
        // the fast sequential queries (a few ms each). A tokio-postgres
        // client is single-in-flight-command, so the four DB arms run
        // sequentially on it instead of each grabbing its own connection
        // (4–5 of the pool's 20 per request).
        // PERF-10 decision (2026-08-30): keep the arms sequential on ONE
        // connection. A tokio-postgres client is single-in-flight-command;
        // parallel arms would need 4–5 of the pool's 20 connections per request,
        // which is not justified by the few-ms of sequential DB time (the slow
        // arm is the embed HTTP, already concurrent via `join!`). Revisit
        // trigger: sustained search QPS with visible pool saturation (non-trivial
        // p95 pool-checkout wait) — then move the arms to separate connections
        // (or pipelined queries in one transaction).
        let fingerprint = self.embed_fingerprint(&snap);
        let embed_arm = VectorEmbedArm {
            engine: self,
            fingerprint,
            query: query.to_string(),
            enabled: run_vector,
        };
        let (query_vec, db_arm) = tokio::join!(embed_arm.run(), async {
            let mut client = match self.pool.acquire().await {
                Ok(c) => c,
                Err(e) => return Err(e.into()),
            };
            // Q0: full-text search
            let fts = if let Some(sq) = &fts_sq {
                run_sql_on(&mut *client, sq, "FTS", &self.degradation, "fts").await
            } else {
                Vec::new()
            };
            // WI-37: resolve the `pg_trgm` extension once (cached). A
            // missing extension (or a pool blip) soft-disables all three
            // trgm arms below; FTS always runs regardless. Probed on the
            // arm's own connection (no second pool checkout — see
            // `ensure_trgm_on`).
            let trgm_ok = if run_trgm {
                self.ensure_trgm_on(&mut *client).await
            } else {
                false
            };
            // Q1: prefix match — uses btree index on title_lower
            let prefix = if let Some(sq) = sq_prefix.as_ref().filter(|_| trgm_ok) {
                run_sql_on(
                    &mut *client,
                    sq,
                    "trgm prefix query",
                    &self.degradation,
                    "trgm_prefix",
                )
                .await
            } else {
                Vec::new()
            };
            // Q2: contains match — uses GIN trgm index
            let contains = if let Some(sq) = sq_contains.as_ref().filter(|_| trgm_ok) {
                run_sql_on(
                    &mut *client,
                    sq,
                    "trgm contains query",
                    &self.degradation,
                    "trgm_contains",
                )
                .await
            } else {
                Vec::new()
            };
            // Q3: similarity threshold — uses GiST trgm index
            let similarity = if let Some(sq) = sq_similarity.as_ref().filter(|_| trgm_ok) {
                run_sql_on(
                    &mut *client,
                    sq,
                    "trgm similarity query",
                    &self.degradation,
                    "trgm_similarity",
                )
                .await
            } else {
                Vec::new()
            };
            // Drop the pooled connection at the end of the arm so it is
            // returned to the pool the moment the fast queries finish —
            // NOT held across the slow embed round-trip (see the comment
            // above the join). `tokio::join!` retains this arm's output
            // until the embed arm completes, so leaving `client` in the
            // return value would keep it checked out for the whole embed.
            drop(client);
            Ok::<_, Error>((fts, prefix, contains, similarity))
        },);
        let (fts_rows, prefix_rows, contains_rows, similarity_rows) = db_arm?;

        // ── Merge trgm branches: dedup by article id, first-wins by arm
        // order (Q1 prefix matches are generally the most relevant) — see
        // `dedup_trgm_by_first` for the asymmetry vs the final merge. ──
        let trgm_results: Vec<SearchRow> = dedup_trgm_by_first(
            prefix_rows
                .into_iter()
                .chain(contains_rows)
                .chain(similarity_rows),
        );

        // ── Vector query (semantic) — runs after its embedding lands; the ANN
        // seek is fast, so it adds little to the concurrent phase above. A
        // failure degrades this branch only (no early return): FTS + trgm
        // results are always kept. ──
        let vector_results: Vec<SearchRow> = if let Some(vec_str) = query_vec {
            let filtered = zim_filter.is_some() || lang_filter.is_some();
            let sq = vector_sql(
                &vec_str,
                zim_filter,
                lang_filter,
                vector_fetch_limit(limit, filtered),
                vector_weight,
            );
            debug_assert!(sq.placeholder_count() == sq.params.len());
            // Re-acquire a (fresh, fast) connection for the ANN seek — the one
            // from the concurrent phase above was already returned to the pool
            // at the end of that arm. A pool-get failure degrades this branch
            // only (no early return), matching the embed-failure path above.
            match self.pool.acquire().await {
                Ok(mut client) => {
                    run_sql_on(
                        &mut *client,
                        &sq,
                        "vector search",
                        &self.degradation,
                        "vector_ann",
                    )
                    .await
                }
                Err(e) => {
                    tracing::warn!("vector search disabled for this query: {e}");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        // ── Merge: dedup by `articles.id` (rows from the FTS/trgm/vector arms
        // are joined on id; equal-score ties keep the first arm's row) ──
        let fts_mapped: Vec<SearchResult> = fts_rows.iter().map(row_to_result).collect();
        let trgm_mapped: Vec<SearchResult> = trgm_results.iter().map(row_to_result).collect();
        let vector_mapped: Vec<SearchResult> = vector_results.iter().map(row_to_result).collect();
        let merged = merge_results(fts_mapped, trgm_mapped, vector_mapped, limit, offset);

        tracing::debug!(
            "search done: {} results in {:?}",
            merged.len(),
            start.elapsed()
        );

        Ok(merged)
    }

    /// Title autocomplete/suggest.
    ///
    /// The old single OR query (prefix OR contains OR similarity) was not
    /// index-friendly as a whole, so it runs as the same 3-query split as
    /// `search` (weight 1.0 keeps the raw-similarity score) and merges in
    /// Rust with the old ORDER BY semantics. Branch failures degrade to an
    /// empty contribution (a dead branch can't 500 the whole suggestion).
    pub async fn suggest(
        &self,
        query: &str,
        zim_filter: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<SearchResult>> {
        let limit = limit.unwrap_or(10).min(20);
        let query_lower = query.to_lowercase();
        // N3: read the trgm threshold from the same single-pass search
        // snapshot `search()` uses, so the two code paths can't disagree
        // across a concurrent settings write (the snapshot already applies the
        // 0.3 floor via `floor_trgm_threshold`).
        let threshold = self.settings.search_params_snapshot().trgm_threshold;
        let lim = limit as i32;

        // No language filter for suggest; weight 1.0 = raw similarity score.
        // PERF-2: short queries skip the trigram contains/similarity arms
        // (prefix arm only — btree-backed `LIKE 'q%'`).
        let arms_ok = trgm_arms_enabled(&query_lower);
        let sq_prefix = trgm_prefix_sql(&query_lower, zim_filter, None, lim, 1.0);
        let sq_contains =
            arms_ok.then(|| trgm_contains_sql(&query_lower, zim_filter, None, lim, 1.0));
        let sq_similarity = arms_ok
            .then(|| trgm_similarity_sql(&query_lower, threshold, zim_filter, None, lim, 1.0));
        debug_assert!(sq_prefix.placeholder_count() == sq_prefix.params.len());
        for sq in [&sq_contains, &sq_similarity].into_iter().flatten() {
            debug_assert!(sq.placeholder_count() == sq.params.len());
        }

        // H3: one pooled connection for all three arms (sequential on the
        // single-in-flight client).
        let mut client = match self.pool.acquire().await {
            Ok(c) => c,
            Err(e) => return Err(e.into()),
        };
        // WI-37: resolve the `pg_trgm` extension once (cached). Suggest is a
        // pure-trgm op, so a missing extension soft-disables every arm (the
        // whole result is empty rather than a partial). Probed on the arm's
        // own connection (no second pool checkout — see `ensure_trgm_on`).
        let trgm_ok = self.ensure_trgm_on(&mut *client).await;
        let r_prefix = if trgm_ok {
            run_sql_on(
                &mut *client,
                &sq_prefix,
                "suggest prefix query",
                &self.degradation,
                "trgm_prefix",
            )
            .await
        } else {
            Vec::new()
        };
        let r_contains = match &sq_contains {
            Some(sq) if trgm_ok => {
                run_sql_on(
                    &mut *client,
                    sq,
                    "suggest contains query",
                    &self.degradation,
                    "trgm_contains",
                )
                .await
            }
            _ => Vec::new(),
        };
        let r_similarity = match &sq_similarity {
            Some(sq) if trgm_ok => {
                run_sql_on(
                    &mut *client,
                    sq,
                    "suggest similarity query",
                    &self.degradation,
                    "trgm_similarity",
                )
                .await
            }
            _ => Vec::new(),
        };

        let prefix: Vec<SearchResult> = r_prefix.iter().map(row_to_result).collect();
        let others: Vec<SearchResult> = r_contains
            .iter()
            .chain(r_similarity.iter())
            .map(row_to_result)
            .collect();

        Ok(merge_suggest(prefix, others, limit))
    }
}

/// The vector (semantic) arm of [`SearchEngine::search`]: the embed HTTP
/// call plus its `vector_embed` degradation recording, extracted from the
/// join so `search()` reads uniformly as build branch queries → run branches
/// (concurrently where designed) → merge.
///
/// `run()` resolves the engine's cached embed client for this request's
/// `fingerprint` (via `embed_client_with`, which rebuilds it only on a
/// settings change), embeds `query`, records `vector_embed`
/// success/failure on the engine's degradation tracker (WI-5), and returns
/// the `format_vector`-formatted vector string. Returns `None` when the
/// branch is disabled (`enabled == false`) or unconfigured, or on a failed
/// call (warned, degrades this branch only). An `Ok` response with **zero**
/// embeddings (the provider answered `{"data": []}`) is likewise recorded as
/// a `vector_embed` failure and returns `None` — see
/// [`vector_from_response`].
struct VectorEmbedArm<'a> {
    /// The engine whose cached embed client, settings (client-rebuild path),
    /// and degradation tracker the arm touches — nothing else.
    engine: &'a SearchEngine,
    /// Settings fingerprint computed once per request (`embed_fingerprint`).
    fingerprint: String,
    /// The query to embed (moved into the join — runs concurrently with the
    /// DB arm).
    query: String,
    /// Whether the vector branch runs at all (the mode + embedding-enabled
    /// gate computed in `search()`); `false` skips the embed HTTP entirely.
    enabled: bool,
}

impl<'a> VectorEmbedArm<'a> {
    /// Run the embed arm: cached client → embed → `format_vector`, with the
    /// `vector_embed` success/failure degradation recording (the logic
    /// previously inlined in the `tokio::join!` inside `search()`).
    async fn run(self) -> Option<String> {
        if self.enabled {
            match self.engine.embed_client_with(&self.fingerprint).await {
                Some(ec) => match ec.embed(&[self.query]).await {
                    Ok(vecs) => vector_from_response(&self.engine.degradation, vecs),
                    Err(e) => {
                        self.engine.degradation.record_failure("vector_embed");
                        tracing::warn!("vector search disabled for this query: {e}");
                        None
                    }
                },
                None => None,
            }
        } else {
            None
        }
    }
}

/// Decide the vector-branch outcome from a *successful* embed response,
/// recording `vector_embed` on the degradation tracker (WI-5).
///
/// A response with **zero** embeddings is a FAILURE, not a success:
/// `EmbedClient::embed` returns `Ok(vec![])` for a `{"data": []}` provider
/// response (the index check is vacuous on an empty slice), and recording a
/// success would leave the tracker reporting HEALTHY while the vector branch
/// is silently off for the query — the same 0≠n reason the embed pipeline's
/// `store_guard` rejects count mismatches. Extracted from
/// [`VectorEmbedArm::run`] so the decision + degradation recording is
/// unit-testable without HTTP.
fn vector_from_response(
    degradation: &crate::health::DegradationTracker,
    vecs: Vec<Vec<f32>>,
) -> Option<String> {
    if vecs.is_empty() {
        degradation.record_failure("vector_embed");
        tracing::warn!(
            "vector search disabled for this query: embed provider returned an empty embedding (no vectors in the response)"
        );
        return None;
    }
    degradation.record_success("vector_embed");
    vecs.first().map(|v| format_vector(v))
}

/// Pre-merge dedup for the trgm arms: **first-wins by arm order** (prefix →
/// contains → similarity), so a duplicate article id keeps the row from the
/// EARLIEST arm — typically the prefix arm's row, even if a later arm scored
/// it higher. This asymmetry against [`merge_results`], whose final dedup
/// keeps the HIGHEST score, is deliberate (prefix matches are generally the
/// most relevant — the old single-query semantics). A change to
/// highest-score-wins here must be deliberate; the behavior is pinned by
/// `trgm_pre_merge_dedup_first_wins_by_arm_order`.
fn dedup_trgm_by_first(rows: impl Iterator<Item = SearchRow>) -> Vec<SearchRow> {
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut out: Vec<SearchRow> = Vec::new();
    for row in rows {
        if seen.insert(row.0) {
            out.push(row);
        }
    }
    out
}

/// Query on a *shared* pooled connection, warn-and-empty on failure (H3):
/// the same per-branch degradation as the old per-branch pool-get, but
/// without taking a second pool connection — one failed branch degrades to
/// an empty contribution rather than failing the whole search. `pub` for the
/// soft-fail integration test.
///
/// The executor bound is a plain `Executor<'e>` (not HRTB `for<'c>`): the
/// sqlx build in use only implements `Executor` for `&'e mut PgConnection` /
/// `&'e Pool` for the specific borrow lifetime, so an HRTB bound cannot be
/// satisfied by a pooled-connection deref (`&mut *conn`).
pub async fn run_sql_on<'e, E>(
    client: E,
    sq: &SqlQuery,
    what: &str,
    degradation: &crate::health::DegradationTracker,
    branch: &'static str,
) -> Vec<SearchRow>
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    let mut query = sqlx::query_as::<_, SearchRow>(&sq.sql); // RAW-OK: runtime-built FTS/vector hybrid branch query (dynamic SQL + dynamic `$n` binds) — unexpressible via the db::raw helpers
    for p in &sq.params {
        query = query.bind(p);
    }
    match query.fetch_all(client).await {
        Ok(rows) => {
            // BUG-B3: record success even on zero rows — a branch that
            // *recovers* with a legitimately empty result set must clear
            // its degradation state; the old `!rows.is_empty()` guard kept
            // a recovered zero-row branch stuck in "degraded" forever.
            degradation.record_success(branch);
            rows
        }
        Err(e) => {
            degradation.record_failure(branch);
            tracing::warn!("{what} query failed: {e}");
            Vec::new()
        }
    }
}

fn row_to_result(row: &SearchRow) -> SearchResult {
    SearchResult {
        id: row.0,
        zim_id: row.1,
        path: row.2.clone(),
        title: row.3.clone(),
        snippet: row.4.clone(),
        content_preview: row.5.clone(),
        language: row.6.clone(),
        zim_name: row.7.clone(),
        score: row.8,
    }
}

/// Merge full-text, trigram and vector results into a single de-duplicated,
/// score-ordered list. Dedup by `articles.id` (rows from the FTS/trgm/vector
/// arms are joined on id; equal-score ties keep the first arm's row).
/// Applies `offset` then `limit`.
fn merge_results(
    fts: Vec<SearchResult>,
    trgm: Vec<SearchResult>,
    vector: Vec<SearchResult>,
    limit: usize,
    offset: usize,
) -> Vec<SearchResult> {
    let by_score_desc = |a: &SearchResult, b: &SearchResult| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    };
    let mut all = fts;
    all.extend(trgm);
    all.extend(vector);
    all.sort_by(by_score_desc);

    let mut merged: Vec<SearchResult> = Vec::new();
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();

    for next in all {
        if merged.len() >= limit + offset {
            break;
        }
        // `insert` returns true only when the key was newly added → skip dupes.
        // `id` is the `articles` PK, globally unique across all three engines,
        // so it is the canonical dedup key (and avoids cloning `path`).
        if seen.insert(next.id) {
            merged.push(next);
        }
    }

    merged.into_iter().skip(offset).take(limit).collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::sql::escape_like;
    use super::*;

    use crate::settings::KEY_SEARCH_DEFAULT_LIMIT;

    fn mk(id: i64, zim_id: i32, path: &str, score: f64) -> SearchResult {
        SearchResult {
            id,
            zim_id,
            path: path.to_string(),
            title: path.to_string(),
            snippet: String::new(),
            content_preview: None,
            score,
            language: "en".to_string(),
            zim_name: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn search_reads_settings_from_one_snapshot() {
        use crate::settings::{default_settings, SettingsCache};
        let pool = crate::testing::dead_pool();
        let mut map = default_settings();
        map.insert(KEY_SEARCH_DEFAULT_LIMIT.into(), serde_json::json!(7));
        let settings =
            SettingsCache::new_with_map(pool.clone(), map, std::collections::HashMap::new());
        let engine =
            SearchEngine::new(pool, settings, crate::health::DegradationTracker::default());
        // The search path reads all settings through one snapshot; a mutated
        // key flows into it. (A dead pool soft-fails `search()` to Err, so we
        // assert the snapshot directly rather than the result rows.)
        let snap = engine.settings.search_params_snapshot();
        assert_eq!(
            snap.default_limit, 7,
            "mutated default_limit flows into the snapshot"
        );
        let res = engine
            .search(
                "hello",
                &SearchParams {
                    zim: None,
                    language: None,
                    mode: Some("fts"),
                    limit: None,
                    offset: None,
                    highlight: true,
                },
            )
            .await;
        assert!(res.is_err(), "dead pool → search() soft-fails to Err");
    }

    #[tokio::test]
    async fn ensure_trgm_soft_fails_and_caches_on_dead_pool() {
        use crate::settings::{default_settings, SettingsCache};
        let pool = crate::testing::dead_pool();
        let settings = SettingsCache::new_with_map(
            pool.clone(),
            default_settings(),
            std::collections::HashMap::new(),
        );
        let engine =
            SearchEngine::new(pool, settings, crate::health::DegradationTracker::default());
        // Dead pool → the extension probe fails → `false`, and the cache is
        // set so a second call returns `false` without a second pool attempt.
        assert!(!engine.ensure_trgm().await);
        assert!(
            engine.trgm_ready.lock().unwrap().is_some(),
            "trgm cache must be set after the first (failed) probe"
        );
        assert!(
            !engine.ensure_trgm().await,
            "second call reuses the cached `false`"
        );
    }

    #[tokio::test]
    async fn ensure_trgm_on_probes_through_provided_executor() {
        use crate::settings::{default_settings, SettingsCache};
        // The search/suggest arms hold the ONLY pool connection they have
        // (`DB_POOL_SIZE` may be 1): `ensure_trgm_on` must probe through the
        // passed executor and never take a nested checkout (a nested
        // `pool.acquire()` would stall the full acquire_timeout at size 1).
        // The dead pool itself is used as the executor: `&PgPool` implements
        // `Executor`, so this exercises the held-connection code path with
        // no extra connection held at all.
        let pool = crate::testing::dead_pool();
        let settings = SettingsCache::new_with_map(
            pool.clone(),
            default_settings(),
            std::collections::HashMap::new(),
        );
        let engine = SearchEngine::new(
            pool.clone(),
            settings,
            crate::health::DegradationTracker::default(),
        );
        // Dead pool → probe fails → `false`; cache set, second call cached.
        assert!(!engine.ensure_trgm_on(&pool).await);
        assert!(
            engine.trgm_ready.lock().unwrap().is_some(),
            "trgm cache must be set after the first (failed) probe"
        );
        assert!(!engine.ensure_trgm_on(&pool).await);
    }

    #[test]
    fn dedups_by_zim_path_keeps_highest_score() {
        // Same article from both engines; the higher score should win.
        let out = merge_results(
            vec![mk(1, 1, "A/Paris", 0.9)],
            vec![mk(1, 1, "A/Paris", 0.4)],
            vec![],
            10,
            0,
        );
        assert_eq!(out.len(), 1);
        assert!((out[0].score - 0.9).abs() < f64::EPSILON);

        // Reversed: trgm has the higher score this time.
        let out = merge_results(
            vec![mk(1, 1, "A/Paris", 0.4)],
            vec![mk(1, 1, "A/Paris", 0.9)],
            vec![],
            10,
            0,
        );
        assert_eq!(out.len(), 1);
        assert!((out[0].score - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn dedups_across_three_engines() {
        // Same article from all three engines; highest score wins, one entry kept.
        let out = merge_results(
            vec![mk(1, 1, "A/Paris", 0.5)],
            vec![mk(1, 1, "A/Paris", 0.9)],
            vec![mk(1, 1, "A/Paris", 0.7)],
            10,
            0,
        );
        assert_eq!(out.len(), 1);
        assert!((out[0].score - 0.9).abs() < f64::EPSILON);

        // Vector is the highest this time.
        let out = merge_results(
            vec![mk(1, 1, "A/Paris", 0.5)],
            vec![mk(1, 1, "A/Paris", 0.6)],
            vec![mk(1, 1, "A/Paris", 0.95)],
            10,
            0,
        );
        assert_eq!(out.len(), 1);
        assert!((out[0].score - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn interleaves_by_score_desc() {
        let fts = vec![mk(1, 1, "a", 1.0), mk(2, 1, "c", 0.3)];
        let trgm = vec![mk(3, 2, "b", 0.7)];
        let vec_ = vec![mk(4, 3, "d", 0.85)];
        let out = merge_results(fts, trgm, vec_, 10, 0);
        let paths: Vec<&str> = out.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["a", "d", "b", "c"]);
    }

    #[test]
    fn applies_offset_and_limit() {
        let fts = vec![
            mk(1, 1, "a", 4.0),
            mk(2, 1, "b", 3.0),
            mk(3, 1, "c", 2.0),
            mk(4, 1, "d", 1.0),
        ];
        let out = merge_results(fts, vec![], vec![], 2, 1);
        let paths: Vec<&str> = out.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["b", "c"]);
    }

    #[test]
    fn offset_past_end_returns_empty() {
        let out = merge_results(vec![mk(1, 1, "a", 1.0)], vec![], vec![], 10, 5);
        assert!(out.is_empty());
    }

    #[test]
    fn trgm_pre_merge_dedup_first_wins_by_arm_order() {
        // Pin the PRE-merge trgm dedup: rows arrive in arm order prefix →
        // contains → similarity, and a duplicate id keeps the EARLIEST
        // arm's row even though a later arm scored it higher. (merge_results
        // dedups by highest score instead — see
        // dedups_by_zim_path_keeps_highest_score.)
        let row = |id: i64, score: f64| {
            (
                id,
                1,
                "A/Paris".to_string(),
                "Paris".to_string(),
                String::new(),
                None,
                "en".to_string(),
                "zim".to_string(),
                score,
            )
        };
        let out = dedup_trgm_by_first(vec![row(7, 0.4), row(7, 0.9), row(8, 0.5)].into_iter());
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, 7);
        assert!(
            (out[0].8 - 0.4).abs() < f64::EPSILON,
            "prefix arm's (lower) row wins, not the similarity arm's 0.9"
        );
        assert_eq!(out[1].0, 8, "a distinct id survives");
    }

    #[test]
    fn vector_from_response_empty_records_failure() {
        let tracker = crate::health::DegradationTracker::default();
        // Three empty responses → the branch is degraded and reported.
        for _ in 0..3 {
            assert!(
                vector_from_response(&tracker, Vec::new()).is_none(),
                "zero-embedding response must yield no vector"
            );
        }
        let degraded = tracker.degraded_snapshot();
        assert!(
            degraded
                .iter()
                .any(|(name, count)| name == "vector_embed" && *count == 3),
            "empty embed response must be recorded as a vector_embed failure: {degraded:?}"
        );
    }

    #[test]
    fn vector_from_response_non_empty_records_success() {
        let tracker = crate::health::DegradationTracker::default();
        // A fresh failure streak is cleared by a real (non-empty) response.
        tracker.record_failure("vector_embed");
        tracker.record_failure("vector_embed");
        tracker.record_failure("vector_embed");
        assert!(!tracker.degraded_snapshot().is_empty());
        let out = vector_from_response(&tracker, vec![vec![0.1, 0.2]]);
        assert_eq!(out.as_deref(), Some("[0.1000000,0.2000000]"));
        assert!(
            tracker.degraded_snapshot().is_empty(),
            "non-empty response must record success and clear the failure count"
        );
    }

    #[test]
    fn cross_zim_same_path_dedup_by_id() {
        // Two genuinely distinct articles (different `articles` PKs) that share
        // a path across ZIMs both survive — `id` is the dedup key, not path.
        let out = merge_results(
            vec![mk(10, 1, "A/Paris", 0.9)],
            vec![mk(20, 2, "A/Paris", 0.8)],
            vec![],
            10,
            0,
        );
        assert_eq!(out.len(), 2);

        // The same `id` (a true duplicate from two engines) dedups to one.
        let out = merge_results(
            vec![mk(10, 1, "A/Paris", 0.9)],
            vec![mk(10, 2, "A/Paris", 0.8)],
            vec![],
            10,
            0,
        );
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn empty_inputs_return_empty() {
        assert!(merge_results(vec![], vec![], vec![], 10, 0).is_empty());
    }

    #[test]
    fn escape_like_table() {
        let cases = [
            ("", ""),
            ("hello", "hello"),
            ("100%", "100\\%"),
            ("under_score", "under\\_score"),
            ("back\\slash", "back\\\\slash"),
            ("a%b_c\\", "a\\%b\\_c\\\\"),
            ("%_%\\", "\\%\\_\\%\\\\"),
            ("café", "café"),
            ("日本語", "日本語"),
        ];
        for (input, expected) in cases {
            assert_eq!(escape_like(input), expected, "input: {input:?}");
        }
    }

    // ── PERF-2 short-query trigram gate ──────────────────────────────────

    #[test]
    fn trgm_arms_enabled_matrix() {
        // <3 chars → contains/similarity arms off; >=3 → on.
        assert!(!trgm_arms_enabled(""));
        assert!(!trgm_arms_enabled("a"));
        assert!(!trgm_arms_enabled("ab"));
        assert!(trgm_arms_enabled("abc"));
        assert!(trgm_arms_enabled("abcd"));
        // Multi-byte counts by **chars**: "日" is 1 char (not 3 bytes).
        assert!(!trgm_arms_enabled("日"));
        assert!(!trgm_arms_enabled("日a"));
        assert!(trgm_arms_enabled("日ab"));
    }

    #[test]
    fn build_trgm_arms_short_query_prefix_only() {
        // 2-char query: prefix arm present, contains/similarity omitted.
        let (p, c, s) = build_trgm_arms(true, "ab", None, None, 20, 1.0, 0.3);
        assert!(p.is_some());
        assert!(c.is_none(), "2-char query must skip the contains arm");
        assert!(s.is_none(), "2-char query must skip the similarity arm");
        // The prefix arm's placeholder count must match its param count.
        let p = p.unwrap();
        assert_eq!(p.placeholder_count(), p.params.len());
    }

    #[test]
    fn build_trgm_arms_three_char_all_arms() {
        // 3-char query: all three arms present.
        let (p, c, s) = build_trgm_arms(true, "abc", None, None, 20, 1.0, 0.3);
        assert!(p.is_some());
        assert!(c.is_some(), "3-char query keeps the contains arm");
        assert!(s.is_some(), "3-char query keeps the similarity arm");
        for sq in [p.unwrap(), c.unwrap(), s.unwrap()] {
            assert_eq!(sq.placeholder_count(), sq.params.len());
        }
    }

    #[test]
    fn build_trgm_arms_disabled_mode_no_arms() {
        // run=false (fts/vector/semantic mode) → no trigram arms at all.
        let (p, c, s) = build_trgm_arms(false, "abc", None, None, 20, 1.0, 0.3);
        assert!(p.is_none());
        assert!(c.is_none());
        assert!(s.is_none());
    }

    // ── SQL builder placeholder-count tests ──────────────────────────────

    type FtsCase<'a> = (&'a str, bool, Option<&'a str>, Option<&'a str>, i32);

    #[test]
    fn fts_sql_placeholders() {
        let cases: Vec<FtsCase<'_>> = vec![
            ("query", false, None, None, 400),
            ("query", true, None, None, 400),
            ("query", false, Some("zim"), None, 400),
            ("query", false, None, Some("en"), 400),
            ("query", true, Some("zim"), Some("en"), 400),
        ];
        for (q, hl, zim, lang, lim) in cases {
            let sq = fts_sql(q, hl, zim, lang, lim, 1.0);
            sq.assert_placeholders_match();
            // $1 is always the query
            assert!(sq.sql.contains("$1"));
        }
    }

    #[test]
    fn trgm_prefix_sql_placeholders() {
        let cases: Vec<(Option<&str>, Option<&str>)> = vec![
            (None, None),
            (Some("zim"), None),
            (None, Some("en")),
            (Some("zim"), Some("en")),
        ];
        for (zim, lang) in cases {
            let sq = trgm_prefix_sql("alpine", zim, lang, 400, 1.0);
            sq.assert_placeholders_match();
            // Pattern is always the 2nd param (or 1st if no query, but query is always $1)
            assert!(
                sq.params[1].ends_with('%'),
                "prefix pattern should end with %: {}",
                sq.params[1]
            );
        }
    }

    #[test]
    fn trgm_contains_sql_placeholders() {
        let cases: Vec<(Option<&str>, Option<&str>)> = vec![
            (None, None),
            (Some("zim"), None),
            (None, Some("en")),
            (Some("zim"), Some("en")),
        ];
        for (zim, lang) in cases {
            let sq = trgm_contains_sql("alpine", zim, lang, 400, 1.0);
            sq.assert_placeholders_match();
            assert!(sq.params[1].starts_with('%') && sq.params[1].ends_with('%'));
        }
    }

    #[test]
    fn trgm_similarity_sql_placeholders() {
        let cases: Vec<(Option<&str>, Option<&str>)> = vec![
            (None, None),
            (Some("zim"), None),
            (None, Some("en")),
            (Some("zim"), Some("en")),
        ];
        for (zim, lang) in cases {
            let sq = trgm_similarity_sql("alpine", 0.3, zim, lang, 400, 1.0);
            sq.assert_placeholders_match();
            // BUG-1: executable, index-usable predicate (see the builder doc).
            assert!(sq.sql.contains("a.title_lower % $1"));
            assert!(sq
                .sql
                .contains("similarity(a.title_lower, $1) > $2::float8"));
        }
    }

    #[test]
    fn trgm_similarity_sql_live_predicate() {
        // BUG-1 regression pin (plan test `trgm_similarity_sql_typed_f32`,
        // adapted: `SqlQuery.params` stays the pub `Vec<String>` contract, so
        // the "typed bind" is the explicit `::float8` cast on the threshold).
        let sq = trgm_similarity_sql("alp", 0.5, None, Some("en"), 20, 0.5);
        assert_eq!(sq.params.len(), 4); // query, threshold, lang, limit
        assert_eq!(sq.params[0], "alp");
        assert_eq!(sq.params[1], "0.5");
        assert!(
            sq.sql.contains("a.title_lower % $1"),
            "must use the index-usable % operator: {}",
            sq.sql
        );
        assert!(!sq.sql.contains("::text"));
        sq.assert_placeholders_match();
    }

    #[test]
    fn vector_sql_placeholders() {
        let cases: Vec<(Option<&str>, Option<&str>)> = vec![
            (None, None),
            (Some("zim"), None),
            (None, Some("en")),
            (Some("zim"), Some("en")),
        ];
        for (zim, lang) in cases {
            let sq = vector_sql("[0.1,0.2]", zim, lang, 400, 1.0);
            sq.assert_placeholders_match();
            assert!(sq.sql.contains("$1::vector"));
        }
    }

    #[test]
    fn vector_fetch_limit_scales_with_filters() {
        // Unfiltered: the classic limit*2 over-fetch.
        assert_eq!(vector_fetch_limit(10, false), 20);
        assert_eq!(vector_fetch_limit(0, false), 0);
        // Filtered: ANN top-k is not filter-aware, so over-fetch 2× more.
        assert_eq!(vector_fetch_limit(10, true), 40);
        assert_eq!(vector_fetch_limit(0, true), 0);
        // Bounded by the caller: search clamps limit to SEARCH_HARD_LIMIT
        // before this, so no i32 overflow here.
        assert_eq!(vector_fetch_limit(SEARCH_HARD_LIMIT, true), 2000);
    }

    #[test]
    fn merge_suggest_prefix_first_then_similarity_desc_truncated() {
        // mk() shares id 0, but we need distinct ids — build directly.
        let s = |path: &str, score: f64, id: i64| SearchResult {
            id,
            zim_id: 1,
            path: path.to_string(),
            title: path.to_string(),
            snippet: String::new(),
            content_preview: None,
            score,
            language: "en".to_string(),
            zim_name: "test".to_string(),
        };
        let out = merge_suggest(
            vec![s("A/apple pie", 0.5, 1), s("A/apples", 0.9, 2)],
            vec![
                s("A/green apple", 0.8, 3),
                s("A/apples", 0.99, 2), // dupe of prefix id 2 — prefix keeps the slot
            ],
            10,
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].path, "A/apples"); // prefix, 0.9 (not the 0.99 dupe)
        assert_eq!(out[1].path, "A/apple pie"); // prefix, 0.5
        assert_eq!(out[2].path, "A/green apple"); // other group after all prefix

        // Truncation: limit 2 drops the last row (≤ limit guarantee).
        let out = merge_suggest(
            vec![s("A/apple pie", 0.5, 1), s("A/apples", 0.9, 2)],
            vec![s("A/green apple", 0.8, 3)],
            2,
        );
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn sql_builder_escape_interaction() {
        // Verify that special LIKE characters are escaped in trgm patterns
        let sq = trgm_prefix_sql("100%_test", None, None, 10, 1.0);
        assert_eq!(
            sq.params[1], "100\\%\\_test%",
            "pattern should escape % and _"
        );

        let sq = trgm_contains_sql("back\\slash", None, None, 10, 1.0);
        assert_eq!(
            sq.params[1], "%back\\\\slash%",
            "pattern should escape backslash"
        );

        // Unicode passes through unchanged
        let sq = trgm_prefix_sql("日本語", None, None, 10, 1.0);
        assert_eq!(sq.params[1], "日本語%");
    }
}
