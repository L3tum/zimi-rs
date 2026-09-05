//! Optional embedding pipeline for semantic search.
//!
//! Connects to an OpenAI-compatible /v1/embeddings endpoint to generate
//! vector embeddings for article snippets, stored in pgvector.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::db::pool::Pool;
use crate::error::{Error, Result};
use crate::settings::{
    SettingsCache, EMBED_DEFAULT_DIMENSION, EMBED_DEFAULT_HNSW_THRESHOLD,
    EMBED_DEFAULT_IVFFLAT_THRESHOLD, EMBED_DEFAULT_MODEL, KEY_EMBEDDING_API_KEY,
    KEY_EMBEDDING_BATCH_SIZE, KEY_EMBEDDING_DIMENSION, KEY_EMBEDDING_ENDPOINT,
    KEY_EMBEDDING_HNSW_THRESHOLD, KEY_EMBEDDING_IVFFLAT_THRESHOLD, KEY_EMBEDDING_MAX_CONCURRENCY,
    KEY_EMBEDDING_MODEL, KEY_EMBEDDING_TIMEOUT_SECS,
};

/// Minimum number of embedded vectors before `run_pipeline` attempts a
/// vector-index build on completion (H2). Kept at 1 to preserve the original
/// "build after embedding" behavior; the 10k early-build in `auto_embed_loop`
/// covers big libraries so `run_pipeline` doesn't wait on a full run.
pub const VECTOR_INDEX_MIN_ROWS: i64 = 1;

/// Configuration for the embedding client.
#[derive(Debug, Clone)]
pub struct EmbedConfig {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub dimension: u32,
    pub batch_size: usize,
    pub max_concurrency: usize,
    pub timeout_secs: u64,
}

impl EmbedConfig {
    pub fn from_settings(settings: &SettingsCache) -> Option<Self> {
        let endpoint = settings.get_typed::<String>(KEY_EMBEDDING_ENDPOINT)?;
        if endpoint.is_empty() {
            return None;
        }
        Some(Self {
            endpoint,
            api_key: settings
                .get_typed(KEY_EMBEDDING_API_KEY)
                .unwrap_or_default(),
            model: settings
                .get_typed(KEY_EMBEDDING_MODEL)
                .unwrap_or_else(|| EMBED_DEFAULT_MODEL.into()),
            dimension: settings
                .get_typed(KEY_EMBEDDING_DIMENSION)
                .unwrap_or(EMBED_DEFAULT_DIMENSION),
            batch_size: settings
                .get_typed(KEY_EMBEDDING_BATCH_SIZE)
                .unwrap_or(64)
                .max(1),
            max_concurrency: settings
                .get_typed(KEY_EMBEDDING_MAX_CONCURRENCY)
                .unwrap_or(4)
                .max(1),
            timeout_secs: settings
                .get_typed(KEY_EMBEDDING_TIMEOUT_SECS)
                .unwrap_or(60)
                .max(1),
        })
    }
}

/// OpenAI-compatible embeddings client.
#[derive(Clone)]
pub struct EmbedClient {
    http: reqwest::Client,
    config: EmbedConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmbedRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedData>,
}

#[derive(Debug, Deserialize)]
struct EmbedData {
    index: usize,
    embedding: Vec<f32>,
}

impl EmbedClient {
    /// `pin` (host, addr) fixes the resolved address of the endpoint's
    /// initial host (DNS-rebinding guard); redirect hops are re-resolved and
    /// re-validated by the client's policy.
    pub fn new(config: EmbedConfig, pin: Option<(String, std::net::SocketAddr)>) -> Result<Self> {
        // SSRF guard: block metadata IPs and private ranges (loopback is
        // admitted for local Ollama). Every redirect hop is re-validated.
        let http = crate::netguard::build_guarded_client(
            &config.endpoint,
            /* allow_private */ false,
            /* allow_loopback */ true,
            pin,
        )?
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .map_err(|e| Error::Embedding(format!("failed to build HTTP client: {e}")))?;
        Ok(Self { http, config })
    }

    /// Generate embeddings for a batch of texts.
    pub async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut all_embeddings = Vec::new();

        for chunk in texts.chunks(self.config.batch_size) {
            let mut req = self
                .http
                .post(format!(
                    "{}/embeddings",
                    self.config.endpoint.trim_end_matches('/')
                ))
                .json(&EmbedRequest {
                    model: self.config.model.clone(),
                    input: chunk.to_vec(),
                });

            if !self.config.api_key.is_empty() {
                req = req.bearer_auth(&self.config.api_key);
            }

            let resp: EmbedResponse = req
                .send()
                .await
                .map_err(Error::Http)?
                .json()
                .await
                .map_err(Error::Http)?;

            // Sort by index to maintain order
            let mut data = resp.data;
            data.sort_by_key(|d| d.index);
            // Reject duplicate/gap/out-of-range indices before extending — a
            // misaligned batch would attach the wrong vector to an article.
            check_embed_indices(&data)?;
            all_embeddings.extend(data.into_iter().map(|d| d.embedding));
        }

        Ok(all_embeddings)
    }
}

/// Format an embedding as a Postgres vector literal: `[0.1,0.2,...]`.
pub fn format_vector(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 12 + 2);
    s.push('[');
    for (i, f) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{f:.7}");
    }
    s.push(']');
    s
}

/// Reconcile the `articles.embedding` column dimension with the configured
/// model dimension. pgvector columns have a fixed dimension (`vector(N)`),
/// so switching models of a different size requires an ALTER.
///
/// The alter is only performed when no vectors are stored yet — otherwise the
/// data belongs to a different model and would be silently destroyed, so we
/// warn instead.
pub async fn ensure_vector_dimension(pool: &Pool, dimension: u32) -> Result<()> {
    let pg = pool.get().await.map_err(Error::Pool)?;
    let row = pg
        .query_one(
            "SELECT atttypmod FROM pg_attribute
             WHERE attrelid = 'articles'::regclass AND attname = 'embedding'",
            &[],
        )
        .await
        .map_err(Error::Database)?;
    let typmod: i32 = row.get(0);
    // pgvector stores the dimension as typmod - VARHDRSZ (4).
    let current = typmod.saturating_sub(4) as u32;
    if current == dimension {
        return Ok(());
    }

    let stored: i64 = pg
        .query_one(
            "SELECT COUNT(*) FROM articles WHERE embedding IS NOT NULL",
            &[],
        )
        .await
        .map_err(Error::Database)?
        .get(0);

    if stored > 0 {
        tracing::warn!(
            "articles.embedding is vector({current}) but configured dimension is {dimension}; \
             {stored} existing vectors kept — run `zimservice embed` after clearing them if you want to switch models"
        );
        return Ok(());
    }

    tracing::info!("altering articles.embedding from vector({current}) to vector({dimension})");
    let sql = format!("ALTER TABLE articles ALTER COLUMN embedding TYPE vector({dimension})");
    pg.batch_execute(&sql).await.map_err(Error::Database)?;
    Ok(())
}

/// List ZIMs that are indexed, embed-enabled, and have at least one article
/// without an embedding yet. Used by the background auto-embed loop.
pub async fn list_embeddable_zims(pool: &Pool) -> Result<Vec<String>> {
    let pg = pool.get().await.map_err(Error::Pool)?;
    let rows = pg
        .query(
            "SELECT z.name FROM zims z
             WHERE z.embed_enabled = TRUE
               AND z.index_status = 'ready'
               AND EXISTS (
                   SELECT 1 FROM articles a
                   WHERE a.zim_id = z.id AND a.embedding IS NULL
               )
             ORDER BY z.name",
            &[],
        )
        .await
        .map_err(Error::Database)?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Background task: periodically embed any indexed articles that lack
/// vectors, whenever embedding is enabled. Runs the full pipeline per ZIM,
/// which is itself resumable (skips rows that already have vectors).
pub async fn auto_embed_loop(state: Arc<crate::AppState>) {
    // Track the in-flight index build so we don't spawn overlapping builds,
    // and enforce a 10-minute backoff between attempts (A9).
    let mut in_flight: Option<tokio::task::JoinHandle<()>> = None;
    let mut last_attempt: Option<std::time::Instant> = None;

    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        if !state.settings.embedding_enabled() {
            continue;
        }
        // Reap a completed in-flight build handle so the slot is free again.
        if let Some(ref h) = in_flight {
            if h.is_finished() {
                in_flight = None;
            }
        }
        // P3: if there is nothing to embed, skip the `vector_index_state` DB
        // check entirely for this tick (and the pipeline below) — no embeddable
        // ZIM means no new vectors, so there is no point querying the index.
        let Ok(zims) = list_embeddable_zims(&state.db).await else {
            continue;
        };
        if zims.is_empty() {
            continue;
        }
        // Early vector-index build (non-blocking): once ≥ 10k vectors
        // exist and no valid index is present, build it CONCURRENTLY in the
        // background so search stays live. Backoff: at most one build per 10 min.
        {
            // M1: the expensive `COUNT(*) ... WHERE embedding IS NOT NULL`
            // (vector_index_state) only runs on ticks where the index-build
            // decision can actually change. The O(1) pre-filter proves the exact
            // count is unnecessary — and would decide "don't build" — whenever a
            // valid index already exists, or the whole `articles` table is below
            // the build threshold (so the embedded subset is too). This drops the
            // recurring 60 s full-table count from every tick except the rare
            // window where ≥ 10k rows exist with no index yet.
            if index_build_worth_probing(&state.db).await {
                let (count, exists) = vector_index_state(&state.db).await.unwrap_or((0, true));
                let now = std::time::Instant::now();
                if should_spawn_build(last_attempt, now, exists, count) && in_flight.is_none() {
                    let db = state.db.clone();
                    let settings = state.settings.clone();
                    last_attempt = Some(now);
                    in_flight = Some(tokio::spawn(async move {
                        maybe_build_vector_index(&db, &settings, MIN_INDEX_BUILD_ROWS).await;
                    }));
                }
            }
        }
        for name in zims {
            tracing::info!("auto-embedding ZIM '{name}'");
            if let Err(e) = run_pipeline(state.db.clone(), state.settings.clone(), &name).await {
                tracing::error!("auto-embed failed for '{name}': {e}");
            }
        }
    }
}

/// Minimum number of embedded vectors before the background auto-embed loop
/// considers building the partial vector index. Must stay in sync with the
/// `min_rows` passed to [`maybe_build_vector_index`] from [`auto_embed_loop`]
/// and the pre-filter in [`index_build_worth_probing`].
const MIN_INDEX_BUILD_ROWS: i64 = 10_000;

/// Pure gate for whether the vector-index build should be spawned.
/// - `exists`: a valid index already exists → never spawn.
/// - `count` below threshold → never spawn.
/// - `prev` is `Some` and < 10 min elapsed → backoff, do not spawn.
pub(crate) fn should_spawn_build(
    prev: Option<std::time::Instant>,
    now: std::time::Instant,
    exists: bool,
    count: i64,
) -> bool {
    if exists {
        return false;
    }
    if count < MIN_INDEX_BUILD_ROWS {
        return false;
    }
    if let Some(p) = prev {
        if now.duration_since(p) < Duration::from_secs(600) {
            return false;
        }
    }
    true
}

/// Run the embedding pipeline for a ZIM.
///
/// Reads articles without embeddings, batches them, calls the API,
/// and stores the vectors. Supports resumption.
/// Guard: the embedding API must return exactly one vector per input text.
/// A mismatch would mis-zip the vectors at store time, so the caller skips
/// the batch (its rows stay `embedding IS NULL` and are retried next cycle).
///
/// Extracted as a pure fn so the mismatch branch is directly unit-testable
/// without a live endpoint or DB (TEST#5). Returns `Err` carrying the same
/// diagnostic the pipeline logs.
pub(crate) fn store_guard(len_ids: usize, len_vecs: usize) -> Result<()> {
    if len_vecs != len_ids {
        return Err(Error::Embedding(format!(
            "embedding API returned {len_vecs} vectors for {len_ids} texts"
        )));
    }
    Ok(())
}

/// Verify a **sorted** batch of embed results has exactly the contiguous
/// index set `0..data.len()` — i.e. `data[i].index == i` for all `i`.
///
/// The API is expected to echo back one entry per input text, in order. A
/// well-formed response (even when the provider returns entries out of
/// order) sorts to `0,1,2,…`. A duplicate (`[0,1,1]`), a gap (`[0,2]`), or a
/// shifted range (`[1,2,3]`) means the provider dropped or duplicated an
/// entry: extending `all_embeddings` from such a batch would silently attach
/// the wrong vector to the wrong article. Rejecting the whole batch is safer
/// than writing a misaligned vector.
///
/// Module-private (not `pub(crate)`) because it takes the private
/// [`EmbedData`] type; a `pub(crate)` signature would trip `private_interfaces`.
fn check_embed_indices(sorted: &[EmbedData]) -> Result<()> {
    for (i, d) in sorted.iter().enumerate() {
        if d.index != i {
            return Err(Error::Embedding(format!(
                "embedding API returned misaligned indices: position {i} has index {} (expected exactly 0..{})",
                d.index,
                sorted.len()
            )));
        }
    }
    Ok(())
}

// ─── W6.5: bounded re-claim of permanently-failing (poison) rows ─────────────
//
// A row whose embed batch keeps failing (e.g. the endpoint always 500s) stays
// `embedding IS NULL`, so the 10-minute claim window re-claims it forever. This
// in-process counter drops a row from claims after `POISON_FAIL_MAX` failures.
// Documented limit: it resets on restart (≤ `POISON_FAIL_MAX` wasted cycles,
// self-healing). A durable alternative (an `embed_fail_count` column = a new
// migration) was deliberately not adopted — migration 013 stays free.
const POISON_FAIL_MAX: u32 = 3;
const POISON_FAIL_MAP_CAP: usize = 100_000;

static EMBED_FAILS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<i64, u32>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// W6.5: from a freshly-claimed id set, drop the poison rows (those that have
/// failed `POISON_FAIL_MAX`+ times). Returns the surviving ids, or `None` when
/// *every* claimed id is poison (the caller drops the permit and stops).
/// Also enforces the fail-open cap: if the map grew past `POISON_FAIL_MAP_CAP`
/// (a leak), it is logged + cleared.
fn filter_poisoned_ids(ids: &[i64]) -> Option<Vec<i64>> {
    let mut map = EMBED_FAILS.lock().expect("embed-fails lock poisoned");
    if map.len() > POISON_FAIL_MAP_CAP {
        tracing::warn!(
            "EMBED_FAILS map exceeded {POISON_FAIL_MAP_CAP} entries — clearing (fail-open)"
        );
        map.clear();
    }
    let kept: Vec<i64> = ids
        .iter()
        .filter(|id| map.get(id).copied().unwrap_or(0) < POISON_FAIL_MAX)
        .copied()
        .collect();
    if kept.is_empty() {
        None
    } else {
        Some(kept)
    }
}

/// W6.5: record a failed embed batch — bump each row's poison counter.
fn record_batch_failure(ids: &[i64]) {
    let mut map = EMBED_FAILS.lock().expect("embed-fails lock poisoned");
    for id in ids {
        *map.entry(*id).or_insert(0) += 1;
    }
}

/// W6.5: an embed batch succeeded — clear its rows' failure counters.
fn record_batch_success(ids: &[i64]) {
    let mut map = EMBED_FAILS.lock().expect("embed-fails lock poisoned");
    for id in ids {
        map.remove(id);
    }
}

/// Test-only reset of the in-process poison counter (the first in-module
/// global state; kept behind `#[cfg(test)]` so it never ships).
#[cfg(test)]
fn reset_embed_fails() {
    EMBED_FAILS
        .lock()
        .expect("embed-fails lock poisoned")
        .clear();
}

pub async fn run_pipeline(pool: Pool, settings: SettingsCache, zim_name: &str) -> Result<()> {
    let Some(config) = EmbedConfig::from_settings(&settings) else {
        return Err(Error::Embedding("embedding not configured".into()));
    };

    let model = config.model.clone();
    let dimension = config.dimension;
    let max_concurrency = config.max_concurrency.max(1);
    // Batch size from settings (default 64), not hardcoded — captured before
    // `config` moves into the client.
    let batch_size: i64 = config.batch_size.max(1) as i64;

    // Pin the endpoint's resolved address (DNS-rebinding guard). A
    // DNS-blocked/unresolvable endpoint is a hard failure for the embed job.
    let pin = crate::netguard::resolve_download_host(&config.endpoint, false, true).await?;
    let client = EmbedClient::new(config, pin)?;

    // Reconcile the column dimension with the configured model dimension.
    ensure_vector_dimension(&pool, dimension).await?;

    // Bound how many (HTTP + write) batches are in flight at once.
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrency));
    let mut tasks: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::new();

    loop {
        // Backpressure: block until fewer than max_concurrency batches are
        // in flight, so the API is not hammered past the configured cap.
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Embedding("embed pipeline cancelled".into()))?;

        // Claim the next batch atomically: stamping `embed_at` prevents two
        // claims from picking up the same rows (double-embedding). A batch
        // whose writer crashed is retried once older than the staleness
        // window; successfully embedded rows have embedding IS NOT NULL and
        // are never re-claimed.
        let mut rows = {
            let pg = pool.get().await.map_err(Error::Pool)?;
            pg.query(
                "UPDATE articles SET embed_at = now() \
                 WHERE id IN (SELECT id FROM articles \
                     WHERE zim_id = (SELECT id FROM zims WHERE name = $1) \
                       AND embedding IS NULL \
                       AND (embed_at IS NULL OR embed_at < now() - interval '10 minutes') \
                     LIMIT $2) \
                 RETURNING id, coalesce(snippet, '') || ' ' || coalesce(left(content_preview, 500), '')",
                &[&zim_name, &batch_size],
            )
            .await
            .map_err(Error::Database)?
        };

        if rows.is_empty() {
            drop(permit);
            break;
        }

        // W6.5: drop poison rows (failed `POISON_FAIL_MAX`+ times) so they stop
        // being re-claimed forever. If every claimed row is poison there is
        // nothing left to embed — drop the permit and stop.
        {
            let claimed: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
            let kept = match filter_poisoned_ids(&claimed) {
                Some(k) => k,
                None => {
                    drop(permit);
                    break;
                }
            };
            let kept_set: std::collections::HashSet<i64> = kept.iter().copied().collect();
            rows.retain(|r| kept_set.contains(&r.get::<_, i64>(0)));
        }

        let ids: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
        let texts: Vec<String> = rows.iter().map(|r| r.get::<_, String>(1)).collect();

        // In-flight work = HTTP embed + bulk UPDATE, held under the permit
        // (released when the task ends).
        let c = client.clone();
        let p = pool.clone();
        let m = model.clone();
        let z = zim_name.to_string();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let embeddings = match c.embed(&texts).await {
                Ok(e) => e,
                Err(e) => {
                    // W6.5: this batch failed (e.g. the endpoint 500s) — bump
                    // each row's poison counter; a row that fails
                    // `POISON_FAIL_MAX`+ times is dropped from future claims.
                    record_batch_failure(&ids);
                    tracing::error!("embed batch failed (zim: {z}): {e}");
                    return Err(e);
                }
            };

            // Guard: the API must return exactly one vector per input text.
            // A mismatch would mis-zip the vectors below — skip this batch
            // (its rows stay NULL and are retried next cycle).
            if let Err(e) = store_guard(ids.len(), embeddings.len()) {
                tracing::error!("{e} (zim: {z}); skipping this batch, rows retry next cycle");
                return Ok(());
            }

            // Store in Postgres — one bulk UPDATE per batch.
            let n = ids.len();
            let vec_strs: Vec<String> = embeddings.iter().map(|e| format_vector(e)).collect();
            let values: Vec<String> = (0..n)
                .map(|i| format!("({}, ${})", ids[i], i + 1))
                .collect();
            let model_idx = n + 1;
            let sql = format!(
                "UPDATE articles a SET embedding = v.vec::vector, embed_model = ${model_idx}, embed_at = now() \
                 FROM (VALUES {}) AS v(id, vec) WHERE a.id = v.id",
                values.join(", ")
            );
            let refs: Vec<&(dyn postgres_types::ToSql + Sync)> = vec_strs
                .iter()
                .map(|s| s as &(dyn postgres_types::ToSql + Sync))
                .chain(std::iter::once(
                    &m as &(dyn postgres_types::ToSql + Sync),
                ))
                .collect();
            let pg = p.get().await.map_err(Error::Pool)?;
            match pg.execute(&sql, &refs).await.map_err(Error::Database) {
                Ok(_) => {
                    // W6.5: success — clear these rows' poison counters.
                    record_batch_success(&ids);
                    tracing::debug!("embedded batch of {} (zim: {})", n, z);
                    Ok(())
                }
                Err(e) => {
                    // W6.5: the write failed — bump the poison counter so a
                    // persistently-failing row is eventually dropped.
                    record_batch_failure(&ids);
                    tracing::error!("embed batch write failed (zim: {z}): {e}");
                    Err(e)
                }
            }
        }));
    }

    // Drain the in-flight tasks; surface the first error, if any.
    let mut first_err: Option<Error> = None;
    for t in tasks {
        let res = match t.await {
            Ok(r) => r,
            Err(join_err) => Err(Error::Embedding(format!("embed task panicked: {join_err}"))),
        };
        if let Err(e) = res {
            if first_err.is_none() {
                first_err = Some(e);
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }

    // Build the vector index (CONCURRENTLY, so search stays live during
    // the build). `VECTOR_INDEX_MIN_ROWS = 1` preserves the original "build
    // after embedding" behavior; the 10k early-build in `auto_embed_loop`
    // covers the common case without waiting for a full pipeline run.
    maybe_build_vector_index(&pool, &settings, VECTOR_INDEX_MIN_ROWS).await;

    Ok(())
}

/// Global vector-index state: (embedded row count, index present in
/// `pg_indexes` AND `indisvalid`). An in-progress `CONCURRENTLY` build shows
/// up in `pg_indexes` but has `indisvalid = false`, so we treat it as absent
/// until it's fully usable.
///
/// Exposed (not just `pub(crate)`) so the integration suite can assert that
/// a **partial** `WHERE embedding IS NOT NULL` index (built by
/// `maybe_build_vector_index` / migration 010) still satisfies the shape-
/// agnostic existence check (H2).
pub async fn vector_index_state(pool: &Pool) -> Result<(i64, bool)> {
    let pg = pool.get().await.map_err(Error::Pool)?;
    let count: i64 = pg
        .query_one(
            "SELECT COUNT(*) FROM articles WHERE embedding IS NOT NULL",
            &[],
        )
        .await
        .map_err(Error::Database)?
        .get(0);
    let exists: bool = pg
        .query_one(
            "SELECT COUNT(*) > 0 FROM pg_indexes i JOIN pg_index pi ON i.indexrelid = pi.indexrelid \
             WHERE i.indexname = 'idx_articles_embedding' AND pi.indisvalid",
            &[],
        )
        .await
        .map_err(Error::Database)?
        .get(0);
    Ok((count, exists))
}

/// Cheap O(1) pre-filter for the auto-embed vector-index build gate: returns
/// `true` only when an exact [`vector_index_state`] count is worth running.
///
/// The recurring 60 s [`auto_embed_loop`] tick historically ran a full
/// `COUNT(*) ... WHERE embedding IS NOT NULL` every tick, but that count only
/// matters once ≥ [`MIN_INDEX_BUILD_ROWS`] vectors exist **and** no index is
/// present yet. Two O(1) catalog/stat probes capture exactly that:
///
/// - a valid `idx_articles_embedding` already exists → the decision is
///   permanently "no" (`should_spawn_build` returns false when `exists`), so
///   the count is skipped;
/// - the whole `articles` table is below the build threshold → the embedded
///   subset is too, so the count can't reach it and is skipped.
///
/// `n_live_tup` is a stats estimate, and the embedded rows are a subset of
/// the live rows, so an estimate at or above the threshold can only trigger
/// the exact count (which then makes the real decision). A stale-low
/// estimate (the table genuinely crossed the threshold but the stats have not
/// refreshed yet) can defer that exact count by ≤ one stats-refresh interval.
/// That is an accepted perf-gate delay, not a correctness risk: the
/// post-batch build path (`maybe_build_vector_index` with
/// `VECTOR_INDEX_MIN_ROWS` after each embed batch, exact count) still builds
/// promptly, and the build itself is backoff-gated and idempotent — the
/// stale-low tick can never block or mis-size a build. On any probe error the
/// filter fails closed (skip the count); the loop retries next tick.
async fn index_build_worth_probing(pool: &Pool) -> bool {
    let pg = match pool.get().await {
        Ok(pg) => pg,
        Err(_) => return false,
    };
    // (1) A valid index already exists → nothing to build, skip the count.
    let exists: bool = pg
        .query_one(
            "SELECT COUNT(*) > 0 FROM pg_indexes i JOIN pg_index pi ON i.indexrelid = pi.indexrelid \
             WHERE i.indexname = 'idx_articles_embedding' AND pi.indisvalid",
            &[],
        )
        .await
        .ok()
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(true);
    if exists {
        return false;
    }
    // (2) Whole table below the build threshold → embedded subset is too.
    let live: i64 = pg
        .query_one(
            "SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE relname = 'articles'",
            &[],
        )
        .await
        .ok()
        .map(|r| r.get::<_, i64>(0))
        .unwrap_or(0);
    live >= MIN_INDEX_BUILD_ROWS
}

/// Decide whether a vector index is needed (≥ `min_rows` embedded vectors,
/// no index yet per `pg_indexes`, below the IVFFlat ceiling) and, if so,
/// build it with `CREATE INDEX CONCURRENTLY` so search stays live during
/// the build. Returns `true` if a build completed. Tolerates a pre-existing
/// or in-progress index (no-op via `IF NOT EXISTS`).
pub async fn maybe_build_vector_index(
    pool: &Pool,
    settings: &SettingsCache,
    min_rows: i64,
) -> bool {
    let (count, exists) = match vector_index_state(pool).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("vector index state check failed: {e}");
            return false;
        }
    };
    if count < min_rows || exists {
        return false;
    }
    let ivfflat_threshold = settings
        .get_typed(KEY_EMBEDDING_IVFFLAT_THRESHOLD)
        .unwrap_or(EMBED_DEFAULT_IVFFLAT_THRESHOLD);
    if count >= ivfflat_threshold {
        tracing::warn!(
            "{count} vectors exceeds IVFFlat threshold — skipping vector index (will use seq scan)"
        );
        return false;
    }
    let hnsw_threshold = settings
        .get_typed(KEY_EMBEDDING_HNSW_THRESHOLD)
        .unwrap_or(EMBED_DEFAULT_HNSW_THRESHOLD);
    let sql = if count < hnsw_threshold {
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_embedding ON articles USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL".to_string()
    } else {
        let lists = ((count as f64).sqrt() as i32).max(100);
        format!(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_embedding ON articles USING ivfflat (embedding vector_cosine_ops) WITH (lists = {lists}) WHERE embedding IS NOT NULL"
        )
    };
    tracing::info!("building vector index CONCURRENTLY for {count} vectors");
    // CONCURRENTLY must not run inside an explicit transaction; a fresh
    // pooled connection is in autocommit, so this is safe.
    let pg = match pool.get().await {
        Ok(pg) => pg,
        Err(e) => {
            tracing::warn!("pool get for index build failed: {e}");
            return false;
        }
    };
    match pg.batch_execute(&sql).await {
        Ok(()) => true,
        Err(e) => {
            // Tolerated: a concurrent build may already be in progress, or
            // the index appeared between the check and the build.
            tracing::warn!("vector index CONCURRENTLY build failed: {e}");
            false
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::testing::dead_pool;

    #[test]
    fn format_vector_produces_pgvector_literal() {
        assert_eq!(
            format_vector(&[0.5, -1.0, 2.25]),
            "[0.5000000,-1.0000000,2.2500000]"
        );
    }

    #[test]
    fn format_vector_empty() {
        assert_eq!(format_vector(&[]), "[]");
    }

    #[test]
    fn format_vector_single() {
        assert_eq!(format_vector(&[0.0]), "[0.0000000]");
    }

    #[test]
    fn embed_config_empty_endpoint_returns_none() {
        let values = vec![(KEY_EMBEDDING_ENDPOINT.into(), serde_json::json!(""))]
            .into_iter()
            .collect();
        let settings = crate::settings::SettingsCache::new_with_map(
            dead_pool(),
            values,
            std::collections::HashMap::new(),
        );
        assert!(EmbedConfig::from_settings(&settings).is_none());
    }

    #[test]
    fn embed_config_missing_endpoint_returns_none() {
        let values: std::collections::HashMap<_, _> = vec![].into_iter().collect();
        let settings = crate::settings::SettingsCache::new_with_map(
            dead_pool(),
            values,
            std::collections::HashMap::new(),
        );
        assert!(EmbedConfig::from_settings(&settings).is_none());
    }

    #[test]
    fn embed_config_defaults_applied() {
        let values = vec![(
            KEY_EMBEDDING_ENDPOINT.into(),
            serde_json::json!("http://localhost:11434/api/embed"),
        )]
        .into_iter()
        .collect();
        let settings = crate::settings::SettingsCache::new_with_map(
            dead_pool(),
            values,
            std::collections::HashMap::new(),
        );
        let cfg = EmbedConfig::from_settings(&settings).unwrap();
        assert_eq!(cfg.endpoint, "http://localhost:11434/api/embed");
        assert_eq!(cfg.api_key, "");
        assert_eq!(cfg.model, EMBED_DEFAULT_MODEL);
        assert_eq!(cfg.dimension, EMBED_DEFAULT_DIMENSION);
        assert_eq!(cfg.batch_size, 64);
        assert_eq!(cfg.max_concurrency, 4);
        assert_eq!(cfg.timeout_secs, 60);
    }

    #[test]
    fn embed_config_explicit_overrides() {
        let values = vec![
            (KEY_EMBEDDING_ENDPOINT.into(), serde_json::json!("http://x")),
            (KEY_EMBEDDING_API_KEY.into(), serde_json::json!("sk-123")),
            (KEY_EMBEDDING_MODEL.into(), serde_json::json!("bge-m3")),
            (KEY_EMBEDDING_DIMENSION.into(), serde_json::json!(1024)),
            (KEY_EMBEDDING_BATCH_SIZE.into(), serde_json::json!(32)),
            (KEY_EMBEDDING_MAX_CONCURRENCY.into(), serde_json::json!(8)),
            (KEY_EMBEDDING_TIMEOUT_SECS.into(), serde_json::json!(120)),
        ]
        .into_iter()
        .collect();
        let settings = crate::settings::SettingsCache::new_with_map(
            dead_pool(),
            values,
            std::collections::HashMap::new(),
        );
        let cfg = EmbedConfig::from_settings(&settings).unwrap();
        assert_eq!(cfg.endpoint, "http://x");
        assert_eq!(cfg.api_key, "sk-123");
        assert_eq!(cfg.model, "bge-m3");
        assert_eq!(cfg.dimension, 1024);
        assert_eq!(cfg.batch_size, 32);
        assert_eq!(cfg.max_concurrency, 8);
        assert_eq!(cfg.timeout_secs, 120);
    }

    // ── store_guard (TEST#5) ─────────────────────────────────────────────────

    #[test]
    fn store_guard_match_ok() {
        assert!(store_guard(0, 0).is_ok());
        assert!(store_guard(1, 1).is_ok());
        assert!(store_guard(64, 64).is_ok());
    }

    #[test]
    fn store_guard_mismatch_err() {
        // Fewer vectors than texts (the exact mismatch the pipeline must skip
        // rather than mis-zip).
        let err = store_guard(3, 2).unwrap_err();
        assert!(matches!(err, Error::Embedding(_)));
        assert!(err.to_string().contains("2 vectors for 3 texts"));
        // The reverse mismatch (too many vectors) is also rejected.
        assert!(store_guard(2, 3).is_err());
    }

    // ── check_embed_indices ─────────────────────────────────────────────────

    fn embed_data(indices: &[usize]) -> Vec<EmbedData> {
        indices
            .iter()
            .map(|i| EmbedData {
                index: *i,
                embedding: vec![0.0],
            })
            .collect()
    }

    #[test]
    fn check_embed_indices_sorted_ok() {
        // A well-formed batch returned out of order sorts to a contiguous
        // 0..n and passes.
        let mut v = embed_data(&[2, 0, 1]);
        v.sort_by_key(|d| d.index);
        assert!(check_embed_indices(&v).is_ok());
    }

    #[test]
    fn check_embed_indices_duplicate_err() {
        // [0,1,1]: index 2 missing, 1 duplicated.
        assert!(check_embed_indices(&embed_data(&[0, 1, 1])).is_err());
    }

    #[test]
    fn check_embed_indices_gap_err() {
        // [0,2]: index 1 missing.
        assert!(check_embed_indices(&embed_data(&[0, 2])).is_err());
    }

    #[test]
    fn check_embed_indices_shifted_err() {
        // [1,2,3]: starts at 1, not 0 — position 0 has index 1.
        assert!(check_embed_indices(&embed_data(&[1, 2, 3])).is_err());
    }

    #[test]
    fn check_embed_indices_empty_ok() {
        assert!(check_embed_indices(&[]).is_ok());
    }

    // ─── W6.5 poison counter ────────────────────────────────────────────────
    // These tests share the in-process `EMBED_FAILS` static, and each starts by
    // calling `reset_embed_fails()` (a whole-map clear). Under the default
    // parallel `cargo test` a sibling test's reset would clobber another
    // test's mid-run state (flaky failures), so they serialize on a dedicated
    // test lock. They are fast and purely in-memory, so serializing only these
    // leaves the rest of the suite parallel.
    static EMBED_TESTS_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

    #[test]
    fn record_batch_failure_and_success() {
        let _g = EMBED_TESTS_LOCK.lock().expect("embed tests lock poisoned");
        reset_embed_fails();
        // Failure bumps the counter (per row).
        record_batch_failure(&[1, 2, 2]);
        {
            let m = EMBED_FAILS.lock().unwrap();
            assert_eq!(m.get(&1), Some(&1), "one failure → count 1");
            assert_eq!(m.get(&2), Some(&2), "two failures → count 2");
        }
        // Success clears the counter.
        record_batch_success(&[1, 2]);
        {
            let m = EMBED_FAILS.lock().unwrap();
            assert!(m.get(&1).is_none(), "success clears row 1");
            assert!(m.get(&2).is_none(), "success clears row 2");
        }
    }

    #[test]
    fn poison_filter_drops_rows_at_max() {
        let _g = EMBED_TESTS_LOCK.lock().expect("embed tests lock poisoned");
        reset_embed_fails();
        // Fresh rows pass.
        assert_eq!(filter_poisoned_ids(&[1, 2, 3]), Some(vec![1, 2, 3]));
        // Two failures still pass (< POISON_FAIL_MAX = 3).
        record_batch_failure(&[1, 2]);
        record_batch_failure(&[1, 2]);
        assert_eq!(filter_poisoned_ids(&[1, 2, 3]), Some(vec![1, 2, 3]));
        // A third failure pushes 1 and 2 to the cap → dropped, 3 kept.
        record_batch_failure(&[1, 2]);
        assert_eq!(
            filter_poisoned_ids(&[1, 2, 3]),
            Some(vec![3]),
            "rows at the cap are dropped, others kept"
        );
        // All poison → None (the caller drops the permit and stops).
        assert_eq!(
            filter_poisoned_ids(&[1, 2]),
            None,
            "all-poison batch → None"
        );
    }

    #[test]
    fn poison_fail_open_above_cap() {
        let _g = EMBED_TESTS_LOCK.lock().expect("embed tests lock poisoned");
        reset_embed_fails();
        // Fill the map past the cap (all poison); the filter must log + clear
        // (fail-open) and treat a fresh id as clean.
        {
            let mut m = EMBED_FAILS.lock().unwrap();
            for i in 0..=(POISON_FAIL_MAP_CAP as i64) {
                m.insert(i, POISON_FAIL_MAX);
            }
        }
        assert_eq!(
            filter_poisoned_ids(&[999_999_999]),
            Some(vec![999_999_999]),
            "over-cap map is cleared, fresh id passes"
        );
        {
            let m = EMBED_FAILS.lock().unwrap();
            assert!(m.is_empty(), "over-cap map is cleared (fail-open)");
        }
    }
}
