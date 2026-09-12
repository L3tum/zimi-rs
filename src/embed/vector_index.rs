//! `idx_articles_embedding` partial vector index: catalog state probing,
//! build decisions, and the `CREATE INDEX CONCURRENTLY` build path.

use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::Result;
use crate::settings::{
    SettingsCache, EMBED_DEFAULT_HNSW_THRESHOLD, EMBED_DEFAULT_IVFFLAT_THRESHOLD,
    KEY_EMBEDDING_HNSW_THRESHOLD, KEY_EMBEDDING_IVFFLAT_THRESHOLD,
};

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
/// and the pre-filter in [`index_build_worth_probing`].
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
/// 100). This is the *only* threshold that chooses an index kind — the
/// separate `embedding.ivfflat_threshold` merely documents that IVFFlat is
/// preferred over HNSW at/above it; it never suppresses a build (there is
/// no seq-scan fallback above it).
pub(crate) fn index_build_sql(count: i64, hnsw_threshold: i64) -> String {
    if count < hnsw_threshold {
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_embedding ON articles USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL".to_string()
    } else {
        let lists = ((count as f64).sqrt() as i32).max(100);
        format!(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_embedding ON articles USING ivfflat (embedding vector_cosine_ops) WITH (lists = {lists}) WHERE embedding IS NOT NULL"
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
/// The recurring 60 s `auto_embed_loop` tick historically ran a full
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
pub(crate) async fn index_build_worth_probing(pool: &Pool) -> bool {
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
/// The index *kind* is threshold-driven ([`index_build_sql`]): HNSW below
/// `embedding.hnsw_threshold`, IVFFlat at/above it. `embedding.ivfflat_threshold`
/// only documents that IVFFlat is preferred over HNSW at/above it — a build
/// is **always** attempted once the row-count and catalog gates pass
/// (including at/above the IVFFlat threshold); there is no seq-scan
/// fallback at any scale.
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
        Ok(s) => s,
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
    let ivfflat_threshold = settings
        .get_typed(KEY_EMBEDDING_IVFFLAT_THRESHOLD)
        .unwrap_or(EMBED_DEFAULT_IVFFLAT_THRESHOLD);
    // Strategy is pure and threshold-driven (`index_build_sql`): IVFFlat is
    // preferred over HNSW at/above the IVFFlat threshold — the index is
    // still built; a brute-force seq scan is never the fallback.
    let sql = index_build_sql(count, hnsw_threshold);
    if count >= ivfflat_threshold {
        tracing::info!(
            "{count} vectors ≥ IVFFlat threshold — building an IVFFlat index \
             (preferred over HNSW at this scale; no seq-scan fallback)"
        );
    }
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
            "a failed CONCURRENTLY build (indisvalid = false) is NOT absent — CREATE IF NOT EXISTS would no-op on it"
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
    fn index_build_sql_never_skips_above_the_ivfflat_threshold() {
        // Regression: the old code built *no* index above
        // `embedding.ivfflat_threshold` (10M), degrading vector search to a
        // sequential scan. The threshold only picks the kind (IVFFlat), it
        // never suppresses the build. √10_000_000 ≈ 3162.27 → 3162.
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
}
