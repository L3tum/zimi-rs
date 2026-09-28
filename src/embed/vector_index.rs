//! `idx_articles_embedding` partial vector index: catalog state probing,
//! build decisions, and the `CREATE INDEX CONCURRENTLY` build path.

use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::Result;
use crate::settings::{SettingsCache, EMBED_DEFAULT_HNSW_THRESHOLD, KEY_EMBEDDING_HNSW_THRESHOLD};

use crate::embed::auto_loop::build_probe_within_backoff;

/// Minimum number of embedded vectors before `run_pipeline` attempts a
/// vector-index build on completion (H2). Kept at 1 to preserve the original
/// "build after embedding" behavior; the 10k early-build in `auto_embed_loop`
/// covers big libraries so `run_pipeline` doesn't wait on a full run.
/// Both build paths share the 10-minute `BUILD_BACKOFF_SECS` gate
/// (`build_probe_within_backoff`), so the recurring pipeline-end probe is
/// no hotter than one attempt per 10 minutes.
pub const VECTOR_INDEX_MIN_ROWS: i64 = 1;

/// Minimum number of embedded vectors before the background auto-embed loop
/// considers building the partial vector index. Must stay in sync with the
/// `min_rows` passed to [`maybe_build_vector_index`] from `auto_embed_loop`
/// and the pre-filter in [`index_build_prefilter`].
pub(crate) const MIN_INDEX_BUILD_ROWS: i64 = 10_000;

/// Upper bound on how long the auto-embed loop will *await* a spawned index
/// build before releasing the in-flight slot (see the loop's timeout
/// comment). The build itself is not killed — a slow-but-legitimate
/// `CREATE INDEX CONCURRENTLY` on a multi-million-row table can take well
/// over an hour, and killing it would leave the table locked for cleanup.
pub(crate) const INDEX_BUILD_TIMEOUT_SECS: u64 = 3_600;

/// State of the `idx_articles_embedding` partial index in the catalog. A
/// **failed** (or still in-progress) `CREATE INDEX CONCURRENTLY` leaves a
/// catalog entry with `indisvalid = false` — that is `PresentInvalid`, not
/// "absent": `CREATE INDEX CONCURRENTLY IF NOT EXISTS` no-ops on *any*
/// index with that name, valid or not, so the build path must drop the
/// invalid entry first or every subsequent attempt silently no-ops until
/// someone manually drops it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
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

/// Pure `idx_articles_embedding` build-statement selection: HNSW below
/// `hnsw_threshold`, IVFFlat at/above (with `lists = √count`, floored at
/// 100). This is the *only* threshold that chooses an index kind — there is
/// no seq-scan fallback at any scale.
pub(crate) fn index_build_sql(count: i64, hnsw_threshold: i64) -> String {
    if count < hnsw_threshold {
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_embedding ON articles USING hnsw \
        (embedding vector_cosine_ops) WHERE embedding IS NOT NULL"
            .to_string()
    } else {
        let lists = ((count as f64).sqrt() as i32).max(100);
        format!(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_embedding ON articles USING \
            ivfflat (embedding vector_cosine_ops) WITH (lists = {lists}) WHERE embedding IS NOT \
            NULL"
        )
    }
}

/// Operator-facing degradation note (for `/diagnostic`): `Some` when no
/// **valid** vector index covers a non-trivial number of embedded rows
/// (≥ [`MIN_INDEX_BUILD_ROWS`]) — i.e. the planner cannot use a vector
/// index and every vector search is a brute-force sequential cosine scan
/// over `count` rows.
pub(crate) fn vector_index_degradation_note(count: i64, state: VectorIndexState) -> Option<String> {
    (count >= MIN_INDEX_BUILD_ROWS && !matches!(state, VectorIndexState::Present)).then(|| {
        format!(
            "no valid vector index over {count} embedded rows — vector search \
                 is a sequential cosine scan over every embedded row"
        )
    })
}

/// Pure gate for whether the vector-index build should be spawned, given
/// the index state and the embedded-row count:
/// - `Present` (a valid index) → never spawn;
/// - `Absent` / `PresentInvalid` below [`MIN_INDEX_BUILD_ROWS`] → never spawn;
/// - otherwise spawn. The time-based 10-minute backoff lives in the shared
///   module gate, not here: the loop's pre-filter uses the read-only
///   `build_probe_claimed_recently` (no claim), and the actual single CAS
///   claim is [`build_probe_within_backoff`] inside `maybe_build_vector_index`.
pub(crate) fn should_spawn_build(state: VectorIndexState, count: i64) -> bool {
    !matches!(state, VectorIndexState::Present) && count >= MIN_INDEX_BUILD_ROWS
}

/// Shared diagnostic snapshot of the `(embedded_rows, index_state)`
/// probe (2026-10 review, Medium — `/diagnostic` ran the exact `COUNT(*)`
/// on **every** pull). The auto-embed loop's 60 s tick and the
/// pipeline-end build path ([`maybe_build_vector_index`]) publish here —
/// the exact-probe result where the build decision can change, and a
/// catalog-only estimate (`n_live_tup` + exact index state) on the
/// loop's skip ticks — so `/diagnostic` reads a pure in-memory snapshot
/// when it is fresh and only falls back to the exact probe (one bounded,
/// operator-pulled checkout) when the snapshot is missing or older than
/// `VECTOR_INDEX_SNAPSHOT_TTL`.
///
/// H2 (2026-10 review): `VECTOR_INDEX_SNAPSHOT_TTL` is deliberately NOT
/// re-exported from `crate::embed` — it is an implementation constant of
/// this module, so the reference is plain code text, not an intra-doc
/// link (linking from a re-exported item to a non-re-exported one is an
/// unresolved-link warning under `RUSTDOCFLAGS="-D warnings"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorIndexSnapshot {
    /// `articles` rows with a non-NULL embedding at publish time — exact
    /// when published from an exact probe (`vector_index_state`), the
    /// `pg_stat_user_tables.n_live_tup` stats estimate (an upper bound on
    /// the embedded subset) when published from the loop's catalog-only
    /// skip ticks. The degradation note depends only on the 10k-row
    /// threshold, so the estimate is observationally equivalent in every
    /// case where it is published.
    pub embedded_rows: i64,
    /// Catalog state of `idx_articles_embedding` at publish time (always
    /// exact — the catalog probe).
    pub index: VectorIndexState,
    /// Unix seconds when the snapshot was last published. `0` = never
    /// published — the construction default, which makes the first
    /// `/diagnostic` pull pay the exact probe itself (honest first read)
    /// and then publish.
    pub at_unix: u64,
}

/// How fresh `/diagnostic` considers a cached [`VectorIndexSnapshot`] before
/// falling back to the exact probe. The auto-embed loop refreshes it on
/// every 60 s tick — exact probe where the build decision can change,
/// catalog-only estimate otherwise — so 5 minutes is generous while the
/// degradation note (which only depends on the 10k-row threshold) cannot
/// meaningfully change in that window.
pub const VECTOR_INDEX_SNAPSHOT_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Current unix seconds (0 when the system clock is pre-epoch — never in
/// practice).
pub(crate) fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Freshness decision for a cached snapshot (see [`VECTOR_INDEX_SNAPSHOT_TTL`])
/// — pure over `(snapshot age, now)` so it is unit-pinnable.
pub(crate) fn snapshot_is_fresh(snap: &VectorIndexSnapshot, now_unix: u64) -> bool {
    // `at_unix == 0` = never probed (the construction default) → always
    // stale, so the first pull pays the exact probe itself.
    snap.at_unix != 0
        && now_unix.saturating_sub(snap.at_unix) <= VECTOR_INDEX_SNAPSHOT_TTL.as_secs()
}

/// Publish a successful probe to the shared diagnostic snapshot (see
/// [`VectorIndexSnapshot`]). `embedded_rows` is exact from
/// `vector_index_state` (the loop's `Probe` tick and
/// `maybe_build_vector_index`) and the `n_live_tup` stats estimate from
/// the loop's catalog-only skip ticks.
// LINT-3 (2026-10 review): deliberate panic — the poisoned-lock idiom (a
// poison here means a publisher already panicked; the snapshot is
// best-effort diagnostics, so failing the publish is not recoverable).
#[allow(clippy::expect_used)]
pub(crate) fn publish_vector_index_snapshot(
    cache: &std::sync::Mutex<VectorIndexSnapshot>,
    embedded_rows: i64,
    index: VectorIndexState,
) {
    *cache.lock().expect("vector-index snapshot lock poisoned") = VectorIndexSnapshot {
        embedded_rows,
        index,
        at_unix: now_unix_secs(),
    };
}

/// The `/diagnostic` vector-index probe result (M-diag, 2026-10 review):
/// the embedded row count, the catalog state, the degradation note, and the
/// measurement time. The serve-layer DTO (`serve::diagnostics::
/// VectorIndexDiagnostic`) is a direct mapping of this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexDiagnosis {
    /// `articles` rows with a non-NULL embedding.
    pub embedded_rows: i64,
    /// Catalog state of `idx_articles_embedding`.
    pub index: VectorIndexState,
    /// The degradation note (≥ 10k embedded rows, no valid index), if any.
    pub degraded: Option<String>,
    /// Unix seconds when `embedded_rows`/`index` were last measured.
    pub count_at: u64,
}

/// The `/diagnostic` vector-index probe with the fresh/stale snapshot
/// policy (M-diag, 2026-10 review — moved out of the `zims` handler so the
/// policy lives next to the snapshot it governs): reads the in-memory
/// snapshot published by the auto-embed loop while it is fresh (within
/// `VECTOR_INDEX_SNAPSHOT_TTL` — a pure in-memory read, zero checkouts)
/// and only pays the exact catalog probe when the snapshot is missing or
/// stale (one bounded, operator-pulled checkout; the result is published
/// to warm the cache). The snapshot's `embedded_rows` is the exact count on
/// probe ticks and the `n_live_tup` stats estimate on the loop's
/// catalog-only skip ticks — the degradation note only depends on the
/// 10k-row threshold, so the two are observationally equivalent.
///
/// Returns `None` (with a warn) when the exact probe itself fails — the DB
/// is unreachable, which `/health`'s `db_connected` already reports.
// LINT-3 (2026-10 review): deliberate panic — the poisoned-lock idiom
// (a poison here means a publisher already panicked; the snapshot is
// best-effort diagnostics, so failing the read is not recoverable).
#[allow(clippy::expect_used)]
pub async fn vector_index_diagnostic(
    cache: &std::sync::Mutex<VectorIndexSnapshot>,
    pool: &Pool,
) -> Option<VectorIndexDiagnosis> {
    let now = now_unix_secs();
    // Read the snapshot under the lock; the guard is dropped before any
    // await (RwLock/Mutex guards are !Send).
    let snap = *cache.lock().expect("vector-index snapshot lock poisoned");
    if snapshot_is_fresh(&snap, now) {
        return Some(VectorIndexDiagnosis {
            embedded_rows: snap.embedded_rows,
            index: snap.index,
            degraded: vector_index_degradation_note(snap.embedded_rows, snap.index),
            count_at: snap.at_unix,
        });
    }
    // Stale/missing → exact probe (one bounded, operator-pulled checkout).
    match vector_index_state(pool).await {
        Ok((embedded_rows, index)) => {
            publish_vector_index_snapshot(cache, embedded_rows, index);
            Some(VectorIndexDiagnosis {
                embedded_rows,
                index,
                degraded: vector_index_degradation_note(embedded_rows, index),
                count_at: now,
            })
        }
        Err(e) => {
            tracing::warn!("vector index diagnostic probe failed: {e}");
            None
        }
    }
}

/// State of `idx_articles_embedding` in the catalog, three-valued (a
/// failed/in-progress `CONCURRENTLY` build is `PresentInvalid`, not absent
/// — see [`VectorIndexState`]).
pub async fn index_state(pool: &Pool) -> Result<VectorIndexState> {
    // pg_catalog index probe; `COUNT(*) FILTER (…)` always returns a row.
    // `pg_indexes` has no `indexrelid` — join `pg_index` to the index's
    // `pg_class` row (via `indexrelid = oid`) to look up the index by name.
    let (valid, invalid): (bool, bool) = raw::fetch_optional(
        pool,
        "SELECT (COUNT(*) FILTER (WHERE pi.indisvalid)) > 0, \
                (COUNT(*) FILTER (WHERE NOT pi.indisvalid)) > 0 \
         FROM pg_index pi JOIN pg_class pcl ON pcl.oid = pi.indexrelid \
         WHERE pcl.relname = 'idx_articles_embedding'",
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

/// Outcome of the loop-tick O(1) pre-filter (see [`index_build_prefilter`]).
pub(crate) enum IndexBuildPrefilter {
    /// The exact `COUNT(*)` is worth running (≥ threshold rows, no valid
    /// index — or an invalid index whose count the build decision needs).
    Probe,
    /// The exact count is provably unnecessary, and the catalog already
    /// knows enough to publish a snapshot: `estimate` is the
    /// `pg_stat_user_tables.n_live_tup` stats estimate for `articles`
    /// (an upper bound on the embedded subset) with the exact catalog
    /// index state. The degradation note is identical to the exact
    /// probe's in every skip case (`Present` ⇒ never degraded; below
    /// threshold ⇒ count < 10k ⇒ never degraded), so publishing it keeps
    /// `/diagnostic` fresh with zero steady-state exact probes.
    Skip {
        estimate: i64,
        state: VectorIndexState,
    },
    /// A pre-filter probe failed — fail closed (skip the count) and
    /// publish nothing (the index state is unknown).
    SkipUnknown,
}

/// `articles` live-row estimate from `pg_stat_user_tables` — O(1) stats
/// read, no table scan. `0` on any probe error: a low estimate only ever
/// *defers* an exact count the filter cannot prove unnecessary (the
/// post-batch build path still probes exactly), and a published `0` can
/// only understate `embedded_rows` below the degradation threshold.
pub(crate) async fn live_rows_estimate(pool: &Pool) -> i64 {
    raw::fetch_scalar_optional(
        pool,
        "SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE relname = 'articles'",
        |q| q,
    )
    .await
    .ok()
    .flatten()
    .unwrap_or(0)
}

/// Decide whether an exact [`vector_index_state`] count is worth running,
/// and — when it is not — what catalog-only snapshot the loop should
/// publish in its place (2026-10 quick review: steady state used to
/// publish nothing, so `/diagnostic` paid one exact `COUNT(*)` per
/// 5-min TTL per process).
///
/// The recurring 60 s `auto_embed_loop` tick historically ran a full
/// `COUNT(*) ... WHERE embedding IS NOT NULL` every tick, but that count
/// only matters once ≥ [`MIN_INDEX_BUILD_ROWS`] vectors exist **and** no
/// **valid** index is present yet. Two O(1) catalog/stat probes capture
/// exactly that:
///
/// - a valid `idx_articles_embedding` already exists → the decision is
///   permanently "no" (`should_spawn_build` never spawns for `Present`), so
///   the count is skipped — and the catalog knows the state, so a
///   snapshot is published ([`IndexBuildPrefilter::Skip`]);
/// - the whole `articles` table is below the build threshold → the embedded
///   subset is too, so the count can't reach it and is skipped — snapshot
///   published likewise.
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
/// filter fails closed ([`IndexBuildPrefilter::SkipUnknown`] — skip the
/// count, publish nothing); the loop retries next tick.
pub(crate) async fn index_build_prefilter(pool: &Pool) -> IndexBuildPrefilter {
    // (1) A valid index already exists → nothing to build, skip the count.
    // (PresentInvalid does NOT skip: the count is exactly what the build
    // decision needs once the invalid entry is dropped.)
    let state = match index_state(pool).await {
        Ok(st) => st,
        Err(_) => return IndexBuildPrefilter::SkipUnknown,
    };
    if matches!(state, VectorIndexState::Present) {
        return IndexBuildPrefilter::Skip {
            estimate: live_rows_estimate(pool).await,
            state,
        };
    }
    // (2) Whole table below the build threshold → embedded subset is too.
    let estimate = live_rows_estimate(pool).await;
    if estimate >= MIN_INDEX_BUILD_ROWS {
        IndexBuildPrefilter::Probe
    } else {
        IndexBuildPrefilter::Skip { estimate, state }
    }
}

/// Decide whether a vector index is needed (≥ `min_rows` embedded vectors,
/// no **valid** index yet per the catalog) and, if so, build it with
/// `CREATE INDEX CONCURRENTLY` so search stays live during the build.
/// Returns `true` if a build completed. Tolerates a
/// pre-existing valid or in-progress index (no-op via `IF NOT EXISTS`), and
/// **repairs a failed one**: an `indisvalid = false` catalog entry
/// (left by a failed/in-progress `CONCURRENTLY` build) would otherwise make
/// every `CREATE INDEX CONCURRENTLY IF NOT EXISTS` silently no-op forever,
/// so it is dropped (concurrently, falling back to a plain drop if that is
/// refused) before the fresh build.
///
/// The index *kind* is threshold-driven (`index_build_sql`): HNSW below
/// `embedding.hnsw_threshold`, IVFFlat at/above it. A build is **always**
/// attempted once the row-count and catalog gates pass — there is no
/// seq-scan fallback at any scale.
///
/// While the build itself runs (the pre-build invalid-index drop and the
/// `CREATE INDEX CONCURRENTLY`), the shared in-flight flag (`AppState`
/// `index_building`, passed as `in_flight`) is held set via RAII so
/// `/health` can report `index.building`; it is cleared on completion,
/// failure, or cancellation.
///
/// Both build paths (the `auto_embed_loop` early-build and the
/// `run_pipeline` post-run build) funnel through this one CAS claim — the
/// only place that stamps the shared build-probe backoff timestamp on
/// `AppState` — so the recurring 60 s
/// pipeline-end probe is no hotter than one attempt per 10 minutes, and a
/// loop-spawned build and a pipeline-end build can't double-claim the slot.
/// (The loop's pre-filter uses the read-only `build_probe_claimed_recently`,
/// which never claims, so it cannot starve this claim.)
pub async fn maybe_build_vector_index(
    pool: &Pool,
    settings: &SettingsCache,
    min_rows: i64,
    probe: &std::sync::atomic::AtomicU64,
    in_flight: &std::sync::atomic::AtomicBool,
    snapshot: &std::sync::Mutex<VectorIndexSnapshot>,
) -> bool {
    // The single CAS claim for the shared 10-minute backoff: a probe/build
    // attempt from *either* build path within the last 10 minutes skips the
    // expensive `COUNT(*)` + build attempt entirely. This is the one and only
    // place the slot is stamped — the loop's pre-filter only reads it (via
    // `build_probe_claimed_recently`), so it cannot consume this claim.
    if !build_probe_within_backoff(probe) {
        tracing::debug!("vector index build probe skipped (shared 10-min backoff)");
        return false;
    }
    let (count, state) = match vector_index_state(pool).await {
        Ok(s) => {
            // 2026-10 review (Medium): this is one of the two sites that pay
            // the exact COUNT(*) — publish it so `/diagnostic` reads the
            // shared snapshot instead of re-running the probe per pull.
            publish_vector_index_snapshot(snapshot, s.0, s.1);
            s
        }
        Err(e) => {
            tracing::warn!("vector index state check failed: {e}");
            return false;
        }
    };
    if count < min_rows || matches!(state, VectorIndexState::Present) {
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
    // M-A: hold the in-flight flag across the whole build (drop + create)
    // so `/health` reports `index.building` for its full duration.
    let _in_flight = BuildInFlight::new(in_flight);
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
    // Strategy is pure and threshold-driven (`index_build_sql`): IVFFlat is
    // chosen over HNSW at/above the threshold — the index is still built;
    // a brute-force seq scan is never the fallback.
    let sql = index_build_sql(count, hnsw_threshold);
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

/// RAII holder for the shared in-flight build flag: sets it on creation and
/// clears it on drop — so every exit path (build complete, build failed, an
/// early `return`, or the `tokio::timeout` in `auto_embed_loop` dropping the
/// future mid-build) leaves the flag cleared. A `&AtomicBool` (not a lock)
/// so `maybe_build_vector_index` can hold it across `.await` without
/// blocking anyone; only one build can run at a time (the shared backoff
/// CAS inside `maybe_build_vector_index`), so no caller ever clears another
/// build's flag.
struct BuildInFlight<'a>(&'a std::sync::atomic::AtomicBool);

impl<'a> BuildInFlight<'a> {
    fn new(flag: &'a std::sync::atomic::AtomicBool) -> Self {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        Self(flag)
    }
}

impl Drop for BuildInFlight<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── VectorIndexState classification (FIX: invalid-index stall) ─────────

    #[test]
    fn classify_index_state_four_flags() {
        assert_eq!(classify_index_state(true, false), VectorIndexState::Present);
        assert_eq!(
            classify_index_state(false, true),
            VectorIndexState::PresentInvalid,
            "a failed CONCURRENTLY build (indisvalid = false) is NOT absent — CREATE IF NOT \
            EXISTS would no-op on it"
        );
        assert_eq!(classify_index_state(false, false), VectorIndexState::Absent);
        // Defensive: impossible for one index name (indisvalid is per index).
        assert_eq!(classify_index_state(true, true), VectorIndexState::Present);
    }

    // ── index kind selection (the IVFFlat threshold picks the kind, never skips) ──

    #[test]
    fn index_build_sql_hnsw_below_threshold() {
        let sql = index_build_sql(999_999, 1_000_000);
        assert!(
            sql.contains("USING hnsw"),
            "below the HNSW threshold: {sql}"
        );
        assert!(!sql.contains("ivfflat"));
    }

    #[test]
    fn index_build_sql_ivfflat_at_and_above_threshold() {
        // At the HNSW threshold, IVFFlat wins; lists = √count = 1000.
        let sql = index_build_sql(1_000_000, 1_000_000);
        assert!(sql.contains("USING ivfflat"), "at the threshold: {sql}");
        assert!(sql.contains("lists = 1000"), "lists = √count: {sql}");
        // √100 = 10 < 100 → the 100-list floor applies.
        let sql_small = index_build_sql(100, 100);
        assert!(
            sql_small.contains("lists = 100"),
            "lists floor: {sql_small}"
        );
    }

    #[test]
    fn index_build_sql_never_skips_above_the_hnsw_threshold() {
        // Regression: an earlier code revision read the (now-deleted,
        // migration-016-removed) `embedding.ivfflat_threshold` and built
        // *no* index above it, degrading vector search to a sequential scan.
        // The strategy threshold (`embedding.hnsw_threshold`) only picks the
        // kind (IVFFlat), it never suppresses the build. √10_000_000 ≈
        // 3162.27 → 3162.
        let sql = index_build_sql(10_000_000, 1_000_000);
        assert!(
            sql.contains("USING ivfflat"),
            "at the IVFFlat threshold: {sql}"
        );
        assert!(sql.contains("lists = 3162"), "{sql}");
    }

    // ── /diagnostic degradation note ───────────────────────────────────────

    #[test]
    fn degradation_note_only_without_valid_index_at_scale() {
        // Below the 10k build threshold: no note (the index is simply not
        // worth building; seq scan on < 10k rows is cheap).
        assert_eq!(
            vector_index_degradation_note(0, VectorIndexState::Absent),
            None
        );
        assert_eq!(
            vector_index_degradation_note(9_999, VectorIndexState::Absent),
            None
        );
        // A valid index at any scale: no note.
        assert_eq!(
            vector_index_degradation_note(10_000, VectorIndexState::Present),
            None
        );
        assert_eq!(
            vector_index_degradation_note(i64::MAX, VectorIndexState::Present),
            None
        );
        // No index (or an invalid one) at scale: explicit note naming the
        // seq scan and the row count.
        for st in [VectorIndexState::Absent, VectorIndexState::PresentInvalid] {
            let note = vector_index_degradation_note(123_456, st)
                .expect("degraded at scale without a valid index");
            assert!(note.contains("123456"), "{note}");
            assert!(note.contains("sequential"), "{note}");
        }
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

    // ── M-A: in-flight build flag guard ────────────────────────────────────

    #[test]
    fn build_in_flight_guard_sets_and_clears() {
        let flag = std::sync::atomic::AtomicBool::new(false);
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        {
            let _guard = BuildInFlight::new(&flag);
            assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        }
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    // ── 2026-10 review (Medium): /diagnostic snapshot freshness ──────────────

    #[test]
    fn snapshot_freshness_gates_on_ttl() {
        let snap = VectorIndexSnapshot {
            embedded_rows: 42,
            index: VectorIndexState::Absent,
            at_unix: 1_000,
        };
        // Fresh at the boundary: age == TTL is still fresh (the loop
        // refreshes every 60 s, well inside the 5 min TTL).
        assert!(
            snapshot_is_fresh(&snap, 1_000 + VECTOR_INDEX_SNAPSHOT_TTL.as_secs()),
            "age == TTL → fresh"
        );
        assert!(
            !snapshot_is_fresh(&snap, 1_000 + VECTOR_INDEX_SNAPSHOT_TTL.as_secs() + 1),
            "age == TTL + 1 s → stale (the pull pays the exact probe)"
        );
        // `at_unix: 0` (the construction default, “never probed”) is always
        // stale — the first pull pays the exact probe itself.
        let never = VectorIndexSnapshot {
            embedded_rows: 0,
            index: VectorIndexState::Absent,
            at_unix: 0,
        };
        assert!(!snapshot_is_fresh(&never, 0), "never-probed at t=0 → stale");
        assert!(
            !snapshot_is_fresh(&never, u64::MAX),
            "never-probed → stale forever"
        );
    }

    #[test]
    fn publish_updates_the_snapshot() {
        let cache = std::sync::Mutex::new(VectorIndexSnapshot {
            embedded_rows: 0,
            index: VectorIndexState::Absent,
            at_unix: 0,
        });
        publish_vector_index_snapshot(&cache, 123, VectorIndexState::Present);
        let snap = *cache.lock().unwrap();
        assert_eq!(snap.embedded_rows, 123);
        assert_eq!(snap.index, VectorIndexState::Present);
        assert!(
            snap.at_unix >= 1_600_000_000,
            "published with a real timestamp, got {snap:?}"
        );
    }

    // ── vector_index_diagnostic (M-diag, 2026-10 review) ─────────────────

    /// Fresh snapshot → served from memory: a DEAD pool proves no checkout
    /// was attempted (a probe attempt would return `None`).
    #[tokio::test]
    async fn diagnostic_fresh_snapshot_served_from_memory() {
        let cache = std::sync::Mutex::new(VectorIndexSnapshot {
            embedded_rows: 12_000,
            index: VectorIndexState::Absent,
            at_unix: now_unix_secs(),
        });
        let pool = crate::testing::dead_pool();
        let d = vector_index_diagnostic(&cache, &pool)
            .await
            .expect("fresh snapshot → Some without a probe");
        assert_eq!(d.embedded_rows, 12_000);
        assert_eq!(d.index, VectorIndexState::Absent);
        assert!(
            d.degraded.is_some(),
            "≥10k rows + absent index → degradation note"
        );
    }

    /// Stale snapshot + unreachable DB → the exact probe fails → `None`
    /// (the `/diagnostic` field is omitted; `/health` reports the DB).
    #[tokio::test]
    async fn diagnostic_stale_snapshot_dead_pool_is_none() {
        let cache = std::sync::Mutex::new(VectorIndexSnapshot {
            embedded_rows: 5,
            index: VectorIndexState::Present,
            at_unix: now_unix_secs() - (VECTOR_INDEX_SNAPSHOT_TTL.as_secs() + 1),
        });
        let pool = crate::testing::dead_pool();
        assert!(vector_index_diagnostic(&cache, &pool).await.is_none());
    }

    /// Below the 10k threshold (or with a valid index) → no degradation
    /// note, even when served from a fresh snapshot.
    #[tokio::test]
    async fn diagnostic_no_degradation_below_threshold() {
        let cache = std::sync::Mutex::new(VectorIndexSnapshot {
            embedded_rows: 9_999,
            index: VectorIndexState::Absent,
            at_unix: now_unix_secs(),
        });
        let pool = crate::testing::dead_pool();
        let d = vector_index_diagnostic(&cache, &pool).await.unwrap();
        assert!(d.degraded.is_none(), "9_999 < 10k threshold → no note");
    }
}
