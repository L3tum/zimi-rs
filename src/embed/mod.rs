//! Optional embedding pipeline for semantic search.
//!
//! Connects to an OpenAI-compatible /v1/embeddings endpoint to generate
//! vector embeddings for article snippets, stored in pgvector.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::db::pool::Pool;
use crate::db::raw;
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
/// Both build paths share the 10-minute `BUILD_BACKOFF_SECS` gate
/// (`build_probe_within_backoff`), so the recurring pipeline-end probe is
/// no hotter than one attempt per 10 minutes.
pub const VECTOR_INDEX_MIN_ROWS: i64 = 1;

/// Shared backoff between vector-index build attempts (probe + build),
/// enforced for BOTH build paths — the `auto_embed_loop` early-build and the
/// `run_pipeline` post-run build — via the module-level
/// [`LAST_BUILD_PROBE`] timestamp. Same 10 minutes the loop's old
/// per-tick `last_attempt` used; sharing it keeps the every-60-s
/// pipeline-end probe from re-running the `COUNT(*)` + build attempt
/// (no-op + warn while a build is in flight, or repeated IVFFlat-threshold
/// warns) on every tick.
const BUILD_BACKOFF_SECS: u64 = 600;

/// Unix-seconds of the most recent vector-index build probe (either build
/// path); 0 = never probed. Shared so the loop's early-build and the
/// pipeline-end probe back off each other, not just themselves.
static LAST_BUILD_PROBE: AtomicU64 = AtomicU64::new(0);

/// Pure backoff decision for the shared build-probe gate: `true` when no
/// probe has ever run (`last_probe_secs == 0`) or at least
/// [`BUILD_BACKOFF_SECS`] have elapsed since the last one. Saturating math
/// keeps a clock skew / wrap safe (treated as "not yet eligible").
pub(crate) fn build_probe_allowed(last_probe_secs: u64, now_secs: u64) -> bool {
    last_probe_secs == 0 || now_secs.saturating_sub(last_probe_secs) >= BUILD_BACKOFF_SECS
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Read-only check of the shared vector-index build-probe backoff: returns
/// `true` when a probe has been claimed within the last
/// [`BUILD_BACKOFF_SECS`] (i.e. we are inside the backoff window and should
/// *not* spawn another build attempt), `false` otherwise. This is a **pure
/// atomic load** — it never stamps [`LAST_BUILD_PROBE`]. The loop's pre-filter
/// uses this to avoid the recurring per-tick work *without* consuming the
/// single shared claim that [`build_probe_within_backoff`] owns: only
/// [`maybe_build_vector_index`] (which runs on the actual spawn) performs the
/// CAS claim, so the loop path and the pipeline-end path still mutually
/// exclude through that one claim.
fn build_probe_claimed_recently() -> bool {
    !build_probe_allowed(LAST_BUILD_PROBE.load(Ordering::SeqCst), now_unix_secs())
}

/// Check-and-claim the shared vector-index build-probe backoff. This is the
/// **single CAS claim** for the whole gate: it returns `true` (and stamps the
/// shared timestamp) when at least [`BUILD_BACKOFF_SECS`] have elapsed since
/// the last probe from *either* build path, and `false` while inside the
/// backoff window. Only [`maybe_build_vector_index`] calls this, so the loop
/// early-build and the pipeline-end build both funnel through this one claim —
/// which is what keeps two concurrent call sites from double-claiming the slot
/// (the CAS makes both count against the same 10-minute window). The loop's
/// pre-filter must use the read-only [`build_probe_claimed_recently`] instead,
/// so its pre-check does not stamp the slot and starve the spawn of the claim.
fn build_probe_within_backoff() -> bool {
    let now = now_unix_secs();
    let mut last = LAST_BUILD_PROBE.load(Ordering::SeqCst);
    loop {
        if !build_probe_allowed(last, now) {
            return false;
        }
        match LAST_BUILD_PROBE.compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            // Another caller claimed the slot concurrently — re-check the
            // backoff against the fresh timestamp (which, if it was just
            // stamped, is `now` itself, so this returns false).
            Err(fresh) => {
                if !build_probe_allowed(fresh, now) {
                    return false;
                }
                last = fresh;
            }
        }
    }
}

/// Test-only reset of the shared build-probe backoff timestamp (a whole
/// process reset, behind `#[cfg(test)]` so it never ships). DB-gated tests
/// that drive the loop's early-build start from "never probed".
#[cfg(test)]
fn reset_last_build_probe() {
    LAST_BUILD_PROBE.store(0, Ordering::SeqCst);
}

/// Configuration for the embedding client.
#[derive(Debug, Clone)]
pub struct EmbedConfig {
    /// Base URL of the OpenAI-compatible `/v1/embeddings` endpoint.
    pub endpoint: String,
    /// API key, sent as `Authorization: Bearer` when non-empty.
    pub api_key: String,
    /// Model name to request in each embeddings call.
    pub model: String,
    /// Expected vector dimension; must match the pgvector column.
    pub dimension: u32,
    /// Number of texts per request batch.
    pub batch_size: usize,
    /// Max batches in flight at once (HTTP + write, held under a semaphore).
    pub max_concurrency: usize,
    /// Per-request HTTP timeout, in seconds.
    pub timeout_secs: u64,
}

impl EmbedConfig {
    /// Build the config from the runtime settings table.
    ///
    /// Returns `None` only when `embedding.endpoint` is unset/empty — the
    /// other fields fall back to the canonical defaults (see
    /// `EMBED_DEFAULT_*`). Values are floored at 1 for the numeric fields.
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
    /// re-validated by the client's policy. Operator-configured endpoint:
    /// this built-in re-validation is accepted with its residual sub-second
    /// rebinding window on a redirect to a *different* host — user-influenced
    /// URLs (direct downloads, OPDS) instead follow redirects manually with
    /// per-hop resolve + pin (`netguard::follow_pinned_get`).
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

/// Probe the stored dimension of the `articles.embedding` column
/// (pgvector stores the dimension as typmod - VARHDRSZ (4)), or `None` when
/// the column does not exist. Factored out of [`ensure_vector_dimension`]
/// so the pipeline's fail-fast dimension check reuses the same probe.
///
/// pg_catalog probe (the generic helpers still serve it via db::raw).
pub(crate) async fn stored_embedding_dimension(pool: &Pool) -> Result<Option<u32>> {
    let typmod: Option<i32> = raw::fetch_scalar_optional(
        pool,
        "SELECT atttypmod FROM pg_attribute
         WHERE attrelid = 'articles'::regclass AND attname = 'embedding'",
        |q| q,
    )
    .await?;
    Ok(typmod.map(|t| t.saturating_sub(4) as u32))
}

/// Reconcile the `articles.embedding` column dimension with the configured
/// model dimension. pgvector columns have a fixed dimension (`vector(N)`),
/// so switching models of a different size requires an ALTER.
///
/// The alter is only performed when no vectors are stored yet — otherwise the
/// data belongs to a different model and would be silently destroyed, so we
/// warn instead (the caller, [`run_pipeline`], then fails fast so the
/// mismatch can never poison a ZIM's rows).
pub async fn ensure_vector_dimension(pool: &Pool, dimension: u32) -> Result<()> {
    let Some(current) = stored_embedding_dimension(pool).await? else {
        return Err(Error::NotFound(
            "articles.embedding column not found".into(),
        ));
    };
    if current == dimension {
        return Ok(());
    }

    let stored: i64 = raw::fetch_scalar_optional(
        pool,
        "SELECT COUNT(*) FROM articles WHERE embedding IS NOT NULL",
        |q| q,
    )
    .await?
    .unwrap_or(0);

    if stored > 0 {
        tracing::warn!(
            "articles.embedding is vector({current}) but configured dimension is {dimension}; \
             {stored} existing vectors kept — run `zimservice embed` after clearing them if you want to switch models"
        );
        return Ok(());
    }

    tracing::info!("altering articles.embedding from vector({current}) to vector({dimension})");
    // Column-type DDL (`ALTER TABLE … TYPE vector(N)`), raw SQL.
    let sql = format!("ALTER TABLE articles ALTER COLUMN embedding TYPE vector({dimension})");
    raw::execute(pool, &sql, |q| q).await?;
    Ok(())
}

/// List ZIMs that are indexed, embed-enabled, and have at least one article
/// without an embedding yet. Used by the background auto-embed loop.
pub async fn list_embeddable_zims(pool: &Pool) -> Result<Vec<String>> {
    raw::fetch_scalar_all(
        pool,
        "SELECT name FROM zims \
         WHERE embed_enabled = true AND index_status = 'ready' \
           AND EXISTS (SELECT 1 FROM articles a WHERE a.zim_id = zims.id AND a.embedding IS NULL) \
         ORDER BY name",
        |q| q,
    )
    .await
}

/// Background task: periodically embed any indexed articles that lack
/// vectors, whenever embedding is enabled. Runs the full pipeline per ZIM,
/// which is itself resumable (skips rows that already have vectors).
pub async fn auto_embed_loop(state: Arc<crate::AppState>) {
    // Track the in-flight index build so we don't spawn overlapping builds.
    // The 10-minute backoff between build attempts is the shared
    // module-level gate (`build_probe_within_backoff`), so the loop's
    // early-build and `run_pipeline`'s post-run build back off each other.
    let mut in_flight: Option<tokio::task::JoinHandle<()>> = None;

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
                // Probe failure fails closed (assume a valid index exists →
                // don't build), as before.
                let (count, st) = vector_index_state(&state.db)
                    .await
                    .unwrap_or((0, VectorIndexState::Present));
                // Read-only backoff check (pure atomic load, no CAS stamp):
                // the loop only *pre-filters* here so the recurring 60 s tick
                // does no work while in backoff. It must NOT claim the shared
                // slot — the actual claim is the single CAS inside
                // `maybe_build_vector_index` below, which is what the spawn
                // funnels through. Claiming here would stamp the slot a
                // microsecond before the spawn, so the spawn's own claim would
                // always fail and the 10k early build would never run.
                if !build_probe_claimed_recently()
                    && should_spawn_build(st, count)
                    && in_flight.is_none()
                {
                    let db = state.db.clone();
                    let settings = state.settings.clone();
                    in_flight = Some(tokio::spawn(async move {
                        // A hung `CREATE INDEX CONCURRENTLY` (e.g. waiting
                        // on a lock held by a long-running query) must not
                        // pin the in-flight slot forever — the loop would
                        // never spawn again until a process restart. The
                        // await is bounded; on timeout we warn (the build
                        // may still be running in the background on its
                        // pooled connection) and let the slot free so a
                        // later tick can retry — the IF NOT EXISTS /
                        // drop-invalid logic in `maybe_build_vector_index`
                        // makes a retry safe. Bound: 1 hour — a concurrent
                        // build on a multi-million-row table can be slow,
                        // and we don't want to give up on a legitimate one.
                        if tokio::time::timeout(
                            Duration::from_secs(INDEX_BUILD_TIMEOUT_SECS),
                            maybe_build_vector_index(&db, &settings, MIN_INDEX_BUILD_ROWS),
                        )
                        .await
                        .is_err()
                        {
                            tracing::warn!(
                                "vector index build await timed out after {}s — the build may \
                                 still be running in the background; the in-flight slot is \
                                 released and a later tick will retry",
                                INDEX_BUILD_TIMEOUT_SECS
                            );
                        }
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

/// Upper bound on how long the auto-embed loop will *await* a spawned index
/// build before releasing the in-flight slot (see the loop's timeout
/// comment). The build itself is not killed — a slow-but-legitimate
/// `CREATE INDEX CONCURRENTLY` on a multi-million-row table can take well
/// over an hour, and killing it would leave the table locked for cleanup.
const INDEX_BUILD_TIMEOUT_SECS: u64 = 3_600;

/// State of the `idx_articles_embedding` partial index in the catalog. A
/// **failed** (or still in-progress) `CREATE INDEX CONCURRENTLY` leaves a
/// catalog entry with `indisvalid = false` — that is `PresentInvalid`, not
/// "absent": `CREATE INDEX CONCURRENTLY IF NOT EXISTS` no-ops on *any*
/// index with that name, valid or not, so the build path must drop the
/// invalid entry first or every subsequent attempt silently no-ops until
/// someone manually drops it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexState {
    /// No `idx_articles_embedding` entry in the catalog at all.
    Absent,
    /// A **valid** (`indisvalid`) index exists and is usable.
    Present,
    /// A catalog entry exists but is **not valid** — a failed or
    /// in-progress `CONCURRENTLY` build. Must be dropped before a fresh
    /// build can take effect.
    PresentInvalid,
}

/// Pure classification of the catalog probe's `(valid, invalid)` flags into
/// a [`VectorIndexState`]. (Both true is impossible for one index name —
/// `indisvalid` is per index — but is classified as `Present` defensively.)
pub(crate) fn classify_index_state(valid: bool, invalid: bool) -> VectorIndexState {
    if valid {
        VectorIndexState::Present
    } else if invalid {
        VectorIndexState::PresentInvalid
    } else {
        VectorIndexState::Absent
    }
}

/// Pure gate for whether the vector-index build should be spawned, given
/// the index state and the embedded-row count:
/// - `Present` (a valid index) → never spawn;
/// - `Absent` / `PresentInvalid` below [`MIN_INDEX_BUILD_ROWS`] → never spawn;
/// - otherwise spawn. The time-based 10-minute backoff lives in the shared
///   module gate, not here: the loop's pre-filter uses the read-only
///   [`build_probe_claimed_recently`] (no claim), and the actual single CAS
///   claim is [`build_probe_within_backoff`] inside `maybe_build_vector_index`.
pub(crate) fn should_spawn_build(state: VectorIndexState, count: i64) -> bool {
    !matches!(state, VectorIndexState::Present) && count >= MIN_INDEX_BUILD_ROWS
}

/// Pure: the fail-fast dimension-mismatch error [`run_pipeline`] returns
/// before claiming/embedding (so the message is unit-testable without a
/// DB). The rows stay unclaimed — no poison counter is bumped — so a
/// dimension fix (model or column) recovers them.
pub(crate) fn dimension_mismatch_error(stored: u32, client: u32) -> Error {
    Error::Embedding(format!(
        "embedding column is vector({stored}) but the configured model emits {client}-dim vectors; change the model back, or clear existing vectors (e.g. `UPDATE articles SET embedding = NULL`) and `ALTER TABLE articles ALTER COLUMN embedding TYPE vector({client})`"
    ))
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

/// Pure: render the bulk `UPDATE … FROM (VALUES …)` statement for an
/// embed batch — `ids[i]` is embedded as a literal, the vector literals
/// are positional placeholders `$1..$n`, and `${n+1}` is the embed-model
/// placeholder. Returns `None` for an **empty** id list: the statement
/// would render a bare `VALUES ` clause, which is a syntax error. Extracted
/// as a pure fn so the empty-batch branch is directly unit-testable without
/// a DB.
pub(crate) fn bulk_update_sql(ids: &[i64]) -> Option<String> {
    if ids.is_empty() {
        return None;
    }
    let values: Vec<String> = (0..ids.len())
        .map(|i| format!("({}, ${})", ids[i], i + 1))
        .collect();
    let model_idx = ids.len() + 1;
    Some(format!(
        "UPDATE articles a SET embedding = v.vec::vector, embed_model = ${model_idx}, embed_at = now() \
         FROM (VALUES {}) AS v(id, vec) WHERE a.id = v.id",
        values.join(", ")
    ))
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

/// Run the full embedding pipeline for one ZIM: configure the client from
/// the settings table, reconcile the vector column dimension, then claim
/// un-embedded article rows in batches (atomic `embed_at` stamp), embed them
/// at bounded concurrency, and bulk-write the vectors.
///
/// Finishes by probing the vector-index build (gated by the shared
/// 10-minute backoff). Fails fast — leaving rows un-claimed, so a fix
/// recovers them — when embedding is unconfigured or when stored vectors'
/// dimension mismatches the configured model.
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
    // Fail fast on a dimension mismatch that the reconcile above could not
    // (and must not) fix: `ensure_vector_dimension` only ALTERs the column
    // when *no* vectors are stored, so a mismatch surviving it means stored
    // vectors of a different dimension exist. Without this, every batch
    // write would fail at the `::vector` cast, each failure would bump the
    // poison counter, and after 3 failures the ZIM's rows would be
    // poison-dropped — embedding dead until a manual clear, for every ZIM
    // including ones with zero stored vectors. Failing here keeps the rows
    // unclaimed (no `embed_at` stamped, `record_batch_failure` never runs)
    // so a dimension fix (model or column) recovers them; the error
    // surfaces the same way any other pipeline error does (the CLI bails,
    // the auto-embed loop logs and retries next tick).
    if let Some(stored) = stored_embedding_dimension(&pool).await? {
        if stored != dimension {
            return Err(dimension_mismatch_error(stored, dimension));
        }
    }

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
        //
        // Raw SQL: `UPDATE … WHERE id IN (SELECT … LIMIT) RETURNING
        // <computed expr>` (the RETURNING column is a `coalesce(…) || …`
        // expression, not a plain column).
        let mut rows: Vec<(i64, String)> = {
            raw::fetch_all(
                &pool,
                "UPDATE articles SET embed_at = now() \
                 WHERE id IN (SELECT id FROM articles \
                     WHERE zim_id = (SELECT id FROM zims WHERE name = $1) \
                       AND embedding IS NULL \
                       AND (embed_at IS NULL OR embed_at < now() - interval '10 minutes') \
                     LIMIT $2) \
                 RETURNING id, coalesce(snippet, '') || ' ' || coalesce(left(content_preview, 500), '')",
                |q| q.bind(zim_name).bind(batch_size),
            )
            .await?
        };

        if rows.is_empty() {
            drop(permit);
            break;
        }

        // W6.5: drop poison rows (failed `POISON_FAIL_MAX`+ times) so they stop
        // being re-claimed forever. If every claimed row is poison there is
        // nothing left to embed — drop the permit and stop.
        {
            let claimed: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
            let kept = match filter_poisoned_ids(&claimed) {
                Some(k) => k,
                None => {
                    drop(permit);
                    break;
                }
            };
            let kept_set: std::collections::HashSet<i64> = kept.iter().copied().collect();
            rows.retain(|(id, _)| kept_set.contains(id));
        }

        let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        let texts: Vec<String> = rows.iter().map(|(_, t)| t.clone()).collect();

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
            // Raw SQL: the bulk `UPDATE … FROM (VALUES …)` form; `v.vec`
            // binds as text and is cast to `vector` exactly as before.
            // An empty id list would render a bare `VALUES ` clause (a
            // syntax error), so `bulk_update_sql` returns `None` and the batch
            // is skipped instead.
            let n = ids.len();
            let vec_strs: Vec<String> = embeddings.iter().map(|e| format_vector(e)).collect();
            let sql = match bulk_update_sql(&ids) {
                Some(sql) => sql,
                None => {
                    // Defensive / unreachable in production: the claim loop
                    // `break`s (dropping the permit) as soon as an entire batch
                    // is poisoned, so `ids` can never be empty here today. Kept
                    // only to guard against a future reordering that reaches
                    // this spawn with an empty id list.
                    tracing::debug!(
                        "skipping batch write: no ids survived the poison filter (zim: {z})"
                    );
                    return Ok(());
                }
            };
            match raw::execute(&p, &sql, |q| {
                let mut q = q;
                for s in &vec_strs {
                    q = q.bind(s);
                }
                q.bind(&m)
            })
            .await
            {
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
    // covers the common case without waiting for a full pipeline run. The
    // shared 10-minute backoff gate inside `maybe_build_vector_index`
    // bounds how often this probe + attempt runs (was: every 60 s tick).
    maybe_build_vector_index(&pool, &settings, VECTOR_INDEX_MIN_ROWS).await;

    Ok(())
}

/// State of `idx_articles_embedding` in the catalog, three-valued (a
/// failed/in-progress `CONCURRENTLY` build is `PresentInvalid`, not absent
/// — see [`VectorIndexState`]).
pub async fn index_state(pool: &Pool) -> Result<VectorIndexState> {
    // pg_catalog index probe; `COUNT(*) FILTER (…)` always returns a row.
    let (valid, invalid): (bool, bool) = raw::fetch_optional(
        pool,
        "SELECT (COUNT(*) FILTER (WHERE pi.indisvalid)) > 0, \
                (COUNT(*) FILTER (WHERE NOT pi.indisvalid)) > 0 \
         FROM pg_indexes i JOIN pg_index pi ON i.indexrelid = pi.indexrelid \
         WHERE i.indexname = 'idx_articles_embedding'",
        |q| q,
    )
    .await?
    .unwrap_or((false, false));
    Ok(classify_index_state(valid, invalid))
}

/// Global vector-index state: (embedded row count, index state). An
/// in-progress `CONCURRENTLY` build shows up in `pg_indexes` but has
/// `indisvalid = false`, so it is `PresentInvalid` — not usable yet, and
/// (because `CREATE INDEX CONCURRENTLY IF NOT EXISTS` no-ops on it) the
/// build path must drop it before a fresh build can take effect.
///
/// Exposed (not just `pub(crate)`) so the integration suite can assert that
/// a **partial** `WHERE embedding IS NOT NULL` index (built by
/// `maybe_build_vector_index` / migration 010) still satisfies the
/// shape-agnostic existence check (H2).
pub async fn vector_index_state(pool: &Pool) -> Result<(i64, VectorIndexState)> {
    let count: i64 = raw::fetch_scalar_optional(
        pool,
        "SELECT COUNT(*) FROM articles WHERE embedding IS NOT NULL",
        |q| q,
    )
    .await?
    .unwrap_or(0);
    Ok((count, index_state(pool).await?))
}

/// Decide whether an exact [`vector_index_state`] count is worth running.
///
/// The recurring 60 s [`auto_embed_loop`] tick historically ran a full
/// `COUNT(*) ... WHERE embedding IS NOT NULL` every tick, but that count
/// only matters once ≥ [`MIN_INDEX_BUILD_ROWS`] vectors exist **and** no
/// **valid** index is present yet. Two O(1) catalog/stat probes capture
/// exactly that:
///
/// - a valid `idx_articles_embedding` already exists → the decision is
///   permanently "no" (`should_spawn_build` never spawns for `Present`), so
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
    // (1) A valid index already exists → nothing to build, skip the count.
    // (PresentInvalid does NOT skip: the count is exactly what the build
    // decision needs once the invalid entry is dropped.) On any probe error
    // the filter fails closed (skip the count), as before.
    let exists = match index_state(pool).await {
        Ok(st) => matches!(st, VectorIndexState::Present),
        Err(_) => true,
    };
    if exists {
        return false;
    }
    // (2) Whole table below the build threshold → embedded subset is too.
    let live: i64 = raw::fetch_scalar_optional(
        pool,
        "SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE relname = 'articles'",
        |q| q,
    )
    .await
    .ok()
    .flatten()
    .unwrap_or(0);
    live >= MIN_INDEX_BUILD_ROWS
}

/// Decide whether a vector index is needed (≥ `min_rows` embedded vectors,
/// no **valid** index yet per the catalog, below the IVFFlat ceiling) and,
/// if so, build it with `CREATE INDEX CONCURRENTLY` so search stays live
/// during the build. Returns `true` if a build completed. Tolerates a
/// pre-existing valid or in-progress index (no-op via `IF NOT EXISTS`), and
/// **repairs a failed one**: an `indisvalid = false` catalog entry
/// (left by a failed/in-progress `CONCURRENTLY` build) would otherwise make
/// every `CREATE INDEX CONCURRENTLY IF NOT EXISTS` silently no-op forever,
/// so it is dropped (concurrently, falling back to a plain drop if that is
/// refused) before the fresh build.
///
/// Both build paths (the [`auto_embed_loop`] early-build and the
/// [`run_pipeline`] post-run build) funnel through this one CAS claim — the
/// only place that stamps `LAST_BUILD_PROBE` — so the recurring 60 s
/// pipeline-end probe is no hotter than one attempt per 10 minutes, and a
/// loop-spawned build and a pipeline-end build can't double-claim the slot.
/// (The loop's pre-filter uses the read-only `build_probe_claimed_recently`,
/// which never claims, so it cannot starve this claim.)
pub async fn maybe_build_vector_index(
    pool: &Pool,
    settings: &SettingsCache,
    min_rows: i64,
) -> bool {
    // The single CAS claim for the shared 10-minute backoff: a probe/build
    // attempt from *either* build path within the last 10 minutes skips the
    // expensive `COUNT(*)` + build attempt entirely. This is the one and only
    // place the slot is stamped — the loop's pre-filter only reads it (via
    // `build_probe_claimed_recently`), so it cannot consume this claim.
    if !build_probe_within_backoff() {
        tracing::debug!("vector index build probe skipped (shared 10-min backoff)");
        return false;
    }
    let (count, state) = match vector_index_state(pool).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("vector index state check failed: {e}");
            return false;
        }
    };
    if count < min_rows || matches!(state, VectorIndexState::Present) {
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
    // A failed (or in-progress) `CONCURRENTLY` build leaves a catalog entry
    // with `indisvalid = false`, and `CREATE INDEX CONCURRENTLY IF NOT
    // EXISTS` no-ops on *any* entry with that name — so without this drop,
    // one transient failure (lock wait, restart) would stall every
    // subsequent build until a manual `DROP INDEX`. If the drop fails (e.g.
    // the entry belongs to an in-progress build on another connection),
    // the create below still no-ops/errors harmlessly and a later tick
    // retries.
    if matches!(state, VectorIndexState::PresentInvalid) {
        match raw::execute(
            pool,
            "DROP INDEX CONCURRENTLY IF EXISTS idx_articles_embedding",
            |q| q,
        )
        .await
        {
            Ok(_) => tracing::info!("dropped invalid vector index; rebuilding"),
            Err(e) => {
                // `DROP INDEX CONCURRENTLY` is refused for an index in an
                // invalid state (and while a concurrent build is active),
                // and that is exactly the state we are in here — fall back
                // to a plain drop. The drop can in principle target an
                // in-progress concurrent build orphaned by the loop's 1-hour
                // await timeout: that timeout releases the in-flight slot
                // while `CREATE INDEX CONCURRENTLY` may still be running
                // (and the shared backoff slot can expire in the meantime),
                // so a later drop can hit the still-running build. That is
                // PostgreSQL's documented abort mechanism for an invalid / in-
                // progress index: the in-progress build is aborted, the fresh
                // `CREATE INDEX CONCURRENTLY` below then lands — self-healing,
                // no corruption.
                tracing::warn!(
                    "DROP INDEX CONCURRENTLY failed ({e}); falling back to a plain drop"
                );
                if let Err(e2) =
                    raw::execute(pool, "DROP INDEX IF EXISTS idx_articles_embedding", |q| q).await
                {
                    tracing::warn!("dropping invalid vector index failed: {e2}");
                    return false;
                }
                tracing::info!("dropped invalid vector index; rebuilding");
            }
        }
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
    // Raw SQL: `CREATE INDEX CONCURRENTLY` is session-scoped DDL the
    // `db::raw` helper shapes cannot express. Any failure (pool acquire or
    // build error) is tolerated and logged, as before.
    match raw::execute(pool, &sql, |q| q).await {
        Ok(_) => true,
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

    // ── VectorIndexState classification (FIX: invalid-index stall) ─────────

    #[test]
    fn classify_index_state_four_flags() {
        assert_eq!(classify_index_state(true, false), VectorIndexState::Present);
        assert_eq!(
            classify_index_state(false, true),
            VectorIndexState::PresentInvalid,
            "a failed CONCURRENTLY build (indisvalid = false) is NOT absent — CREATE IF NOT EXISTS would no-op on it"
        );
        assert_eq!(classify_index_state(false, false), VectorIndexState::Absent);
        // Defensive: impossible for one index name (indisvalid is per index).
        assert_eq!(classify_index_state(true, true), VectorIndexState::Present);
    }

    #[test]
    fn should_spawn_build_gates_on_state_and_count() {
        use VectorIndexState::*;
        // A valid index never spawns, even at scale.
        assert!(!should_spawn_build(Present, 10_000));
        assert!(!should_spawn_build(Present, i64::MAX));
        // No index (or an invalid one that will be dropped first) below the
        // 10k threshold never spawns.
        assert!(!should_spawn_build(Absent, 0));
        assert!(!should_spawn_build(Absent, 9_999));
        assert!(!should_spawn_build(PresentInvalid, 9_999));
        // At/above threshold, spawn whenever no *valid* index exists —
        // including the PresentInvalid recovery case that was the bug.
        assert!(should_spawn_build(Absent, 10_000));
        assert!(should_spawn_build(PresentInvalid, 10_000));
        assert!(should_spawn_build(Absent, 999_999));
    }

    // ── shared build-probe backoff (FIX: pipeline-end probe hot loop) ──────

    #[test]
    fn build_probe_allowed_never_probed_always_allowed() {
        // last == 0 = never probed: allowed immediately, regardless of now.
        assert!(build_probe_allowed(0, 0));
        assert!(build_probe_allowed(0, 1));
        assert!(build_probe_allowed(0, 1_000_000));
    }

    #[test]
    fn build_probe_allowed_10_minute_window() {
        // Inside the window: not allowed.
        assert!(!build_probe_allowed(100, 100));
        assert!(!build_probe_allowed(100, 100 + BUILD_BACKOFF_SECS - 1));
        // At exactly the boundary and beyond: allowed.
        assert!(build_probe_allowed(100, 100 + BUILD_BACKOFF_SECS));
        assert!(build_probe_allowed(100, 100 + BUILD_BACKOFF_SECS + 1));
        assert_eq!(BUILD_BACKOFF_SECS, 600, "backoff stays 10 minutes");
    }

    #[test]
    fn build_probe_allowed_clock_skew_is_safe() {
        // now < last (skew / wrap) must saturate to "not yet eligible",
        // never panic or overflow.
        assert!(!build_probe_allowed(u64::MAX, 0));
        assert!(!build_probe_allowed(10, 5));
    }

    // ── read-only pre-filter vs the single CAS claim ────────────────────

    // Both tests below drive the shared `LAST_BUILD_PROBE` global; a mutex
    // serializes them so concurrent test threads can't interleave a claim
    // into each other's reset/claim/observe sequence (they are otherwise
    // deterministic).
    static BUILD_PROBE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn build_probe_claimed_recently_is_read_only() {
        let _g = BUILD_PROBE_TEST_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        reset_last_build_probe();
        // Never probed → not claimed. Repeated checks must NOT stamp the slot.
        assert!(!build_probe_claimed_recently());
        assert!(!build_probe_claimed_recently());
        assert_eq!(
            LAST_BUILD_PROBE.load(Ordering::SeqCst),
            0,
            "read-only check must never stamp the slot"
        );

        // The CAS claim stamps the slot…
        assert!(
            build_probe_within_backoff(),
            "first claim within window succeeds"
        );
        let stamped = LAST_BUILD_PROBE.load(Ordering::SeqCst);
        assert_ne!(stamped, 0, "claim stamps the slot");
        // …and the read-only check now reports it as claimed…
        assert!(
            build_probe_claimed_recently(),
            "read-only check reports claimed"
        );
        // …without re-stamping (the pre-filter cannot consume the slot).
        assert_eq!(
            LAST_BUILD_PROBE.load(Ordering::SeqCst),
            stamped,
            "read-only check must not restamp the slot"
        );
        reset_last_build_probe();
    }

    #[test]
    fn build_probe_claim_mutual_excludes_two_call_sites() {
        let _g = BUILD_PROBE_TEST_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // The loop's spawned build and the pipeline-end build both funnel
        // through this single CAS claim: the first wins, the second (within
        // the window) loses — so the loop pre-filter (read-only) can never
        // starve the actual claim of its slot.
        reset_last_build_probe();
        assert!(
            build_probe_within_backoff(),
            "first call site claims the slot"
        );
        assert!(
            !build_probe_within_backoff(),
            "second call site within the window fails to claim"
        );
        assert!(
            build_probe_claimed_recently(),
            "read-only pre-filter agrees the slot is claimed"
        );
        reset_last_build_probe();
    }

    // ── bulk_update_sql (FIX: empty-batch VALUES syntax error) ─────────────

    #[test]
    fn bulk_update_sql_empty_is_none() {
        // An empty id list would render a bare `VALUES ` clause (syntax
        // error) — the caller skips the batch instead.
        assert!(bulk_update_sql(&[]).is_none());
    }

    #[test]
    fn bulk_update_sql_shape() {
        assert_eq!(
            bulk_update_sql(&[7]).unwrap(),
            "UPDATE articles a SET embedding = v.vec::vector, embed_model = $2, embed_at = now() \
             FROM (VALUES (7, $1)) AS v(id, vec) WHERE a.id = v.id"
        );
        assert_eq!(
            bulk_update_sql(&[7, 8, 9]).unwrap(),
            "UPDATE articles a SET embedding = v.vec::vector, embed_model = $4, embed_at = now() \
             FROM (VALUES (7, $1), (8, $2), (9, $3)) AS v(id, vec) WHERE a.id = v.id"
        );
    }

    // ── dimension fail-fast error (FIX: model dim change poisons all ZIMs) ─

    #[test]
    fn dimension_mismatch_error_message() {
        let err = dimension_mismatch_error(1536, 1024);
        let msg = err.to_string();
        assert!(matches!(err, Error::Embedding(_)));
        assert!(msg.contains("vector(1536)"), "stored dim: {msg}");
        assert!(msg.contains("emits 1024-dim"), "client dim: {msg}");
        assert!(
            msg.contains("UPDATE articles SET embedding = NULL"),
            "recovery hint: {msg}"
        );
        assert!(
            msg.contains("ALTER TABLE articles ALTER COLUMN embedding TYPE vector(1024)"),
            "recovery hint: {msg}"
        );
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
        // The 500-endpoint loop test (DB-gated) also mutates `EMBED_FAILS`
        // via the pipeline's failure path — serialize on the shared DB
        // guard so its exact-map assertions never race.
        let _g = crate::testing::DbExclusiveGuard::acquire();
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
        let _g = crate::testing::DbExclusiveGuard::acquire();
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
        let _g = crate::testing::DbExclusiveGuard::acquire();
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

    // ─── auto_embed_loop lifecycle (Tests Major #4) ─────────────────────────
    //
    // The loop's 60 s cadence is a plain `tokio::time::sleep`, so these tests
    // run under `#[tokio::test(start_paused = true)]` (mock time — the code
    // already supports it; no production restructuring). Two tokio
    // auto-advance facts drive the plumbing below (verified against the
    // tokio 1.53 runtime source *and* a scratch repro):
    //
    // 1. A parked worker with no ready tasks JUMPS the paused clock to the
    //    next timer (no real time elapses); a real I/O event landing while
    //    the worker is parked (or about to park) wins the race — the park
    //    returns via that wake and no jump happens. So real loopback I/O
    //    (DB probes, embed HTTP) always completes in real time, while pure
    //    waits (the loop's 60 s tick, the poll sleeps) jump instantly.
    // 2. While a `spawn_blocking` task runs, auto-advance is INHIBITED and
    //    the clock stays frozen — so any `tokio::time` deadline inside a
    //    `Handle::block_on` on a blocking thread can never fire. A connect
    //    gate built on `spawn_blocking` + `Handle::block_on` therefore
    //    *deadlocks* against a downed DB (sqlx's pool connect touches
    //    timers internally), not "measures real time". That is why
    //    `loop_pool_or_skip` uses the plain established pattern instead.
    //
    // Consequences the helpers rely on:
    // - a real DB probe from the test task parks the worker with the loop's
    //   60 s tick as the next timer; either the probe's loopback response
    //   wins (no jump) or the park yields the 60 s jump — either way one
    //   probe cycle ≈ one loop tick, in milliseconds of real time;
    // - `loop_pool_or_skip` uses a plain `tokio::time::timeout(3 s, connect)`
    //   (the repo's established DB-gate pattern): a downed DB yields a
    //   skip in ~1 ms of real time, and a live DB's real handshake wins
    //   the race against the virtual deadline;
    // - `embedding.timeout_secs` is huge, so a virtual jump can never trip
    //   an in-flight request's timeout.
    //
    // All three are DB-gated with the lib's established pattern
    // (`DATABASE_URL` + 3 s connect timeout + `ZIMSERVICE_REQUIRE_DB`
    // hard-fail) and serialize via `DbExclusiveGuard` like every other DB
    // test in the crate. The 500-endpoint test also bumps the in-process
    // `EMBED_FAILS` poison counters (via the pipeline's failure path), which
    // the pure poison unit tests below assert on exactly — they take the
    // guard as well, so the shared static never races.

    const LOOP_ZIM: &str = "__embedloop__";
    /// Real-time budget for polling a live condition. Ticks themselves are
    /// virtual (milliseconds of real time); this bounds real I/O waits.
    const LOOP_WAIT_BUDGET: Duration = Duration::from_secs(60);

    /// DB gate for the loop tests — the lib's established pattern
    /// (`smoke_single_instance_refusal` & co. and `tests/integration/common.rs`):
    /// `DATABASE_URL` + 3 s connect timeout around the eager connect +
    /// `ZIMSERVICE_REQUIRE_DB` hard-fail, then serialize via
    /// `DbExclusiveGuard` like every other DB test in the crate.
    ///
    /// The timeout is a plain `tokio::time::timeout` in the test task — no
    /// `spawn_blocking`/`Handle::block_on` indirection. That indirection is
    /// a *deadlock* under the paused clock, not a real-time measurement:
    /// while a blocking task runs, auto-advance is inhibited, so the virtual
    /// 3 s deadline never fires and any connect path that touches a tokio
    /// timer internally (sqlx pool connect does) hangs forever. The plain
    /// timeout is safe under auto-advance: the clock only jumps when the
    /// worker's park *times out* (no real I/O event within the window), so a
    /// slow-but-live DB still completes its handshake in real time and wins
    /// the race, while a refused/black-holed connect yields an error/timeout
    /// within 3 s of real time.
    async fn loop_pool_or_skip(test: &str) -> Option<Pool> {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        let pool = match tokio::time::timeout(
            Duration::from_secs(3),
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(8)
                // Virtual-time guard: the paused clock can jump while the
                // worker parks, so the (virtual) acquire timeout sits far
                // away from any plausible tick window.
                .acquire_timeout(Duration::from_secs(86_400))
                .connect(&url),
        )
        .await
        {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return skip_loop_test(test, &url, &format!("connect failed: {e}")),
            Err(_) => return skip_loop_test(test, &url, "connect timed out (3s)"),
        };
        if pool.acquire().await.is_err() {
            return skip_loop_test(test, &url, "pool acquire failed");
        }
        Some(pool)
    }

    fn skip_loop_test(test: &str, url: &str, why: &str) -> Option<Pool> {
        if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
            panic!("{test}: ZIMSERVICE_REQUIRE_DB is set but cannot reach {url}: {why}");
        }
        eprintln!("skipping {test}: cannot reach {url} ({why})");
        None
    }

    /// Current `articles.embedding` column dimension (pgvector stores it as
    /// typmod - VARHDRSZ). Matching it keeps `ensure_vector_dimension` a
    /// no-op, so the tests never ALTER the shared dev column.
    async fn embedding_column_dim(pool: &Pool) -> u32 {
        let typmod: i32 = raw::fetch_scalar_optional(
            pool,
            "SELECT atttypmod FROM pg_attribute
             WHERE attrelid = 'articles'::regclass AND attname = 'embedding'",
            |q| q,
        )
        .await
        .expect("pg_attribute probe")
        .expect("articles.embedding column present");
        typmod.saturating_sub(4) as u32
    }

    /// Settings for the loop under test: endpoint at `endpoint`, enabled per
    /// `enabled`, dimension matched to the live column, and a huge request
    /// timeout (mock time — see section note).
    async fn loop_settings(pool: &Pool, endpoint: &str, enabled: bool) -> SettingsCache {
        let dim = embedding_column_dim(pool).await;
        let mut values = crate::settings::default_settings();
        values.insert(KEY_EMBEDDING_ENDPOINT.into(), serde_json::json!(endpoint));
        values.insert(
            crate::settings::KEY_EMBEDDING_ENABLED.into(),
            serde_json::json!(enabled),
        );
        values.insert(KEY_EMBEDDING_MODEL.into(), serde_json::json!("test-model"));
        values.insert(KEY_EMBEDDING_DIMENSION.into(), serde_json::json!(dim));
        values.insert(KEY_EMBEDDING_TIMEOUT_SECS.into(), serde_json::json!(86_400));
        SettingsCache::new_with_map(pool.clone(), values, std::collections::HashMap::new())
    }

    /// Live `AppState` for the loop (same shape as `testing::test_state`,
    /// but with a real pool and real settings).
    fn loop_state(pool: Pool, settings: SettingsCache) -> Arc<crate::AppState> {
        Arc::new(crate::AppState {
            zims: crate::zim::ZimManager::new(
                std::path::PathBuf::from("/nonexistent-embedloop"),
                pool.clone(),
            ),
            search: crate::search::SearchEngine::new(
                pool.clone(),
                settings.clone(),
                crate::health::DegradationTracker::default(),
            ),
            db: pool,
            settings,
            torrent: crate::torrent::QbitClientCache::new(),
            rate_limiter: std::sync::Arc::new(crate::serve::ratelimit::RateLimiterHandle::new()),
            probes: crate::HealthProbes::default(),
            auth_lockout: std::sync::Arc::new(Default::default()),
            degradation: crate::health::DegradationTracker::default(),
        })
    }

    /// Seed a fresh ready/embeddable ZIM (idempotent) and one unembedded
    /// article in it; returns the article id.
    async fn seed_zim_article(pool: &Pool, path: &str) -> i64 {
        raw::execute(pool, "DELETE FROM zims WHERE name = $1", |q| {
            q.bind(LOOP_ZIM)
        })
        .await
        .expect("delete zim");
        let zim_id: i32 = raw::fetch_scalar_optional(
            pool,
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                               index_status, indexed_entries, article_count)
             VALUES ($1, $1, $1, 0, now(), 'ready', 1, 1) RETURNING id",
            |q| q.bind(LOOP_ZIM),
        )
        .await
        .expect("insert zim")
        .expect("zim row");
        raw::fetch_scalar_optional(
            pool,
            "INSERT INTO articles (path, title, content_preview, snippet, search_vector,
                                   language, namespace, zim_id)
             VALUES ($1, $2, 'preview', 'snip', to_tsvector('simple', $2), 'en', 'C', $3)
             RETURNING id",
            |q| q.bind(path).bind(path).bind(zim_id),
        )
        .await
        .expect("insert article")
        .expect("article row")
    }

    /// One more unembedded article in the loop ZIM (no ZIM churn).
    async fn seed_article(pool: &Pool, path: &str) -> i64 {
        let zim_id: i32 =
            raw::fetch_scalar_optional(pool, "SELECT id FROM zims WHERE name = $1", |q| {
                q.bind(LOOP_ZIM)
            })
            .await
            .expect("zim lookup")
            .expect("zim row");
        raw::fetch_scalar_optional(
            pool,
            "INSERT INTO articles (path, title, content_preview, snippet, search_vector,
                                   language, namespace, zim_id)
             VALUES ($1, $2, 'preview', 'snip', to_tsvector('simple', $2), 'en', 'C', $3)
             RETURNING id",
            |q| q.bind(path).bind(path).bind(zim_id),
        )
        .await
        .expect("insert article")
        .expect("article row")
    }

    async fn drop_loop_zim(pool: &Pool) {
        let _ = raw::execute(pool, "DELETE FROM zims WHERE name = $1", |q| {
            q.bind(LOOP_ZIM)
        })
        .await;
    }

    async fn article_embedded(pool: &Pool, id: i64) -> Option<bool> {
        raw::fetch_scalar_optional(
            pool,
            "SELECT embedding IS NOT NULL FROM articles WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .ok()
        .flatten()
    }

    async fn article_unclaimed(pool: &Pool, id: i64) -> Option<bool> {
        raw::fetch_scalar_optional(
            pool,
            "SELECT embed_at IS NULL FROM articles WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .ok()
        .flatten()
    }

    /// Valid `idx_articles_embedding` present (the same pg_catalog probe the
    /// loop's `vector_index_state` runs).
    async fn index_valid(pool: &Pool) -> Option<bool> {
        vector_index_state(pool)
            .await
            .ok()
            .map(|(_, st)| matches!(st, VectorIndexState::Present))
    }

    /// `pg_stat_user_tables.n_live_tup` for `articles`, forcing a stats
    /// flush first (PG 15+), so the loop's O(1) pre-filter
    /// (`index_build_worth_probing`) sees freshly inserted rows.
    async fn articles_live_tup(pool: &Pool) -> Option<i64> {
        if raw::execute(pool, "SELECT pg_stat_force_next_flush()", |q| q)
            .await
            .is_err()
        {
            return None;
        }
        raw::fetch_scalar_optional(
            pool,
            "SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE relname = 'articles'",
            |q| q,
        )
        .await
        .ok()
        .flatten()
    }

    /// A 200 embed response with exactly one vector of `dim` dims — the
    /// tests claim one row per pipeline run, so one entry per request.
    fn embed_one_body(dim: u32) -> String {
        let v = vec_of(dim, "0.1");
        format!(
            "{{\"data\":[{{\"index\":0,\"embedding\":[{}]}}]}}",
            v.join(",")
        )
    }

    fn vec_of(n: u32, val: &str) -> Vec<String> {
        (0..n).map(|_| val.to_string()).collect()
    }

    /// Poll `check` (real DB I/O) within a real-time budget. Each failed
    /// probe parks the worker with the loop's 60 s tick as the next timer;
    /// the µs park cycle is usually shorter than the loopback DB round-trip,
    /// so auto-advance jumps 60 s of virtual time and the loop ticks —
    /// one probe ≈ one loop tick, in milliseconds of real time. `None` from
    /// a check (a failed probe) retries.
    async fn wait_until<F, Fut>(mut check: F, what: &str)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Option<bool>>,
    {
        let deadline = std::time::Instant::now() + LOOP_WAIT_BUDGET;
        loop {
            if check().await.is_some_and(|b| b) {
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!("loop test: timed out waiting for {what}");
            }
            // Virtual sleep: a `spawn_blocking` sleep would inhibit
            // auto-advance and freeze the clock, so the loop could not tick
            // while the poll waits.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Settle: run `n` probe cycles so the loop has (very likely) ticked at
    /// least once per cycle — each real DB probe parks the worker with the
    /// loop's 60 s tick as next timer, so a 60 s virtual jump (and a tick)
    /// lands while the probe's response is in flight.
    async fn settle_ticks(pool: &Pool, n: u32) {
        for _ in 0..n {
            // `std::result::Result` (not the module's `Result<T>` alias,
            // pulled in by `use super::*`) with an inferred error type.
            let r: std::result::Result<Option<i32>, _> =
                raw::fetch_scalar_optional(pool, "SELECT 1", |q| q).await;
            let _ = r;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// (1) Recurring tick: the loop is not a one-shot — it keeps ticking on
    /// its cadence and re-evaluates its pre-conditions (`embedding_enabled`,
    /// `list_embeddable_zims`) every tick, so work that appears *after* the
    /// first successful pass is picked up on a later tick, with no config
    /// change and no restart.
    #[tokio::test(start_paused = true)]
    async fn auto_embed_loop_repeats_ticks_and_picks_up_new_work() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let pool =
            match loop_pool_or_skip("auto_embed_loop_repeats_ticks_and_picks_up_new_work").await {
                Some(p) => p,
                None => return,
            };
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");

        let server = MockServer::start().await;
        let dim = embedding_column_dim(&pool).await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_string(embed_one_body(dim)))
            .mount(&server)
            .await;

        let state = loop_state(
            pool.clone(),
            loop_settings(&pool, &server.uri(), true).await,
        );
        let first = seed_zim_article(&pool, "A/wave1").await;
        let loop_task = tokio::spawn(auto_embed_loop(state));

        // Wave 1: the first tick's pipeline pass embeds the seeded article.
        wait_until(|| article_embedded(&pool, first), "wave-1 article embedded").await;

        // Wave 2: new work appears while the loop is running (no setting
        // change, no restart). Only a loop that re-runs `list_embeddable_zims`
        // on later ticks embeds it — a one-shot would not.
        let second = seed_article(&pool, "A/wave2").await;
        wait_until(
            || article_embedded(&pool, second),
            "wave-2 article embedded on a later tick",
        )
        .await;

        let n_reqs = server
            .received_requests()
            .await
            .expect("wiremock requests")
            .len();
        assert!(
            n_reqs >= 2,
            "two separate pipeline runs must each call the endpoint (got {n_reqs} request(s))"
        );

        loop_task.abort();
        drop_loop_zim(&pool).await;
    }

    /// (2) Config-change re-trigger: flipping `embedding.enabled` at runtime
    /// through the production write path (persist + cache update) is picked
    /// up by the already-running loop on its next tick — enabling starts a
    /// pipeline pass, and re-disabling stops the loop from claiming new work
    /// again (the setting is re-read every tick, never latched).
    #[tokio::test(start_paused = true)]
    async fn auto_embed_loop_re_evaluates_enabled_setting() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let pool = match loop_pool_or_skip("auto_embed_loop_re_evaluates_enabled_setting").await {
            Some(p) => p,
            None => return,
        };
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");

        let server = MockServer::start().await;
        let dim = embedding_column_dim(&pool).await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_string(embed_one_body(dim)))
            .mount(&server)
            .await;

        // Start disabled: the seeded article must sit untouched while the
        // loop ticks (no claim, no HTTP).
        let settings = loop_settings(&pool, &server.uri(), false).await;
        let first = seed_zim_article(&pool, "A/phase-off").await;
        let state = loop_state(pool.clone(), settings.clone());
        let loop_task = tokio::spawn(auto_embed_loop(state));

        // Let the loop tick a few times while disabled: each tick re-reads
        // `embedding.enabled` and must skip — no claim, no HTTP.
        settle_ticks(&pool, 5).await;
        assert!(
            article_unclaimed(&pool, first).await.unwrap_or(false),
            "disabled loop must not claim the article"
        );
        assert_eq!(
            server
                .received_requests()
                .await
                .expect("wiremock requests")
                .len(),
            0,
            "disabled loop must not call the embed endpoint"
        );

        // Enable at runtime (the production write path). The running loop
        // re-reads the setting on a later tick and embeds the article.
        let mut updates = std::collections::HashMap::new();
        updates.insert(
            crate::settings::KEY_EMBEDDING_ENABLED.into(),
            serde_json::json!(true),
        );
        settings
            .update(&updates, true)
            .await
            .expect("enable embedding");
        wait_until(
            || article_embedded(&pool, first),
            "article embedded after runtime enable",
        )
        .await;
        assert!(
            !server
                .received_requests()
                .await
                .expect("wiremock requests")
                .is_empty(),
            "enabling must trigger a pipeline pass"
        );

        // Disable again *before* any new work appears: later ticks must honor
        // the new value — the new article is never claimed nor embedded, and
        // the request count does not grow (nothing left to claim in the
        // meantime, so it stays exactly at the one wave-1 request).
        let mut updates = std::collections::HashMap::new();
        updates.insert(
            crate::settings::KEY_EMBEDDING_ENABLED.into(),
            serde_json::json!(false),
        );
        settings
            .update(&updates, true)
            .await
            .expect("disable embedding");
        let second = seed_article(&pool, "A/phase-off2").await;
        // Let the loop tick a few times with the setting re-read as disabled.
        settle_ticks(&pool, 5).await;
        assert!(
            article_unclaimed(&pool, second).await.unwrap_or(false),
            "re-disabled loop must not claim new work"
        );
        assert!(
            !article_embedded(&pool, second).await.unwrap_or(true),
            "re-disabled loop must not embed new work"
        );
        assert_eq!(
            server
                .received_requests()
                .await
                .expect("wiremock requests")
                .len(),
            1,
            "re-disabled loop must send no further embed calls"
        );

        loop_task.abort();
        // Leave the shared dev DB as found (the default value).
        let mut updates = std::collections::HashMap::new();
        updates.insert(
            crate::settings::KEY_EMBEDDING_ENABLED.into(),
            serde_json::json!(false),
        );
        settings
            .update(&updates, true)
            .await
            .expect("restore embedding.enabled default");
        drop_loop_zim(&pool).await;
    }

    /// (3) 10k early-build: once ≥ `MIN_INDEX_BUILD_ROWS` (10 000) vectors
    /// exist and no valid index is present, the loop's tick spawns the
    /// partial `idx_articles_embedding` build in the background and keeps
    /// going (non-blocking) — per the loop's doc comment. The failing (500)
    /// endpoint makes the attribution exact: `run_pipeline` returns early on
    /// embed failure and never reaches its post-run build, so only the
    /// loop's early-build path can have created the index.
    #[tokio::test(start_paused = true)]
    async fn auto_embed_loop_early_builds_index_at_10k() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let pool = match loop_pool_or_skip("auto_embed_loop_early_builds_index_at_10k").await {
            Some(p) => p,
            None => return,
        };
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        // The early-build decision is gated by the shared module-level
        // 10-minute build-probe backoff — start from "never probed" so this
        // test's tick can spawn the build regardless of earlier tests' probes.
        reset_last_build_probe();

        // At/above the IVFFlat ceiling no build is attempted at all (the
        // documented skip), so the test can only make sense below it.
        let preexisting: i64 = raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM articles WHERE embedding IS NOT NULL",
            |q| q,
        )
        .await
        .expect("embedded count")
        .expect("row present");
        if preexisting + 10_000 >= EMBED_DEFAULT_IVFFLAT_THRESHOLD {
            eprintln!("skipping: dev DB already holds {preexisting} vectors (IVFFlat ceiling)");
            return;
        }

        // Failing endpoint: the per-ZIM pipeline always errors out, so its
        // post-run build is unreachable (see section note for why that is
        // what makes this assertion airtight).
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let dim = embedding_column_dim(&pool).await;
        let settings = loop_settings(&pool, &server.uri(), true).await;

        // Start from no valid index (the loop's `exists` gate).
        let had_index = index_valid(&pool).await.unwrap_or(false);
        raw::execute(&pool, "DROP INDEX IF EXISTS idx_articles_embedding", |q| q)
            .await
            .expect("drop index (setup)");

        // One unembedded article keeps `list_embeddable_zims` non-empty, and
        // 10 000 already-embedded rows cross `MIN_INDEX_BUILD_ROWS`.
        let one = seed_zim_article(&pool, "A/live").await;
        raw::execute(
            &pool,
            "WITH vec AS (
                 SELECT '[' || (SELECT string_agg('0.1', ',') FROM generate_series(1, $2)) || ']' AS v
             )
             INSERT INTO articles (zim_id, path, title, content_preview, snippet,
                                   search_vector, embedding, embed_model)
             SELECT (SELECT id FROM zims WHERE name = $1),
                    'E/' || g, 'Early ' || g, 'preview', 'snip',
                    to_tsvector('simple', 'early'), vec.v::vector, 'test-early'
             FROM generate_series(1, 10000) g, vec",
            |q| q.bind(LOOP_ZIM).bind(dim as i32),
        )
        .await
        .expect("seed 10k vectors");

        // Make the stats pre-filter (`n_live_tup >= 10k`) see the new rows
        // (the O(1) probe is a stats estimate; force a flush and poll).
        wait_until(
            || async { articles_live_tup(&pool).await.map(|t| t >= 10_000) },
            "stats to reflect 10k live rows",
        )
        .await;

        let state = loop_state(pool.clone(), settings);
        let loop_task = tokio::spawn(auto_embed_loop(state));

        // The early build lands as a valid index in the background.
        wait_until(
            || async { index_valid(&pool).await },
            "early background index build",
        )
        .await;

        // The loop did not block on the build: its tick proceeded to the
        // per-ZIM pipeline and attempted an embed (which 500'd).
        assert!(
            !server
                .received_requests()
                .await
                .expect("wiremock requests")
                .is_empty(),
            "loop must continue to the per-ZIM pipeline after spawning the build"
        );
        // The endpoint failed, so the article is still unembedded — the
        // index came from the loop's early-build path, not from a
        // post-success pipeline build.
        assert!(
            !article_embedded(&pool, one).await.unwrap_or(true),
            "failing endpoint keeps the article unembedded (not a post-success build)"
        );

        loop_task.abort();
        drop_loop_zim(&pool).await;
        // Restore the pre-test index state when it was the migration's
        // default (index present, HNSW-safe scale) and we removed it.
        if had_index && preexisting < 1_000_000 {
            raw::execute(
                &pool,
                "CREATE INDEX IF NOT EXISTS idx_articles_embedding ON articles
                 USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL",
                |q| q,
            )
            .await
            .expect("restore index (cleanup)");
        }
    }
}
