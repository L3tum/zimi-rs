//! Background auto-embed loop + the shared vector-index build-probe backoff
//! gate (the single 10-minute CAS claim shared by both build paths).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::Result;

use crate::embed::pipeline::run_pipeline;
use crate::embed::vector_index::{
    index_build_worth_probing, maybe_build_vector_index, should_spawn_build, vector_index_state,
    VectorIndexState, INDEX_BUILD_TIMEOUT_SECS, MIN_INDEX_BUILD_ROWS,
};

/// Shared backoff between vector-index build attempts (probe + build),
/// enforced for BOTH build paths — the `auto_embed_loop` early-build and the
/// `run_pipeline` post-run build — via the shared build-probe backoff
/// timestamp on `AppState`. Same 10 minutes the loop's old
/// per-tick `last_attempt` used; sharing it keeps the every-60-s
/// pipeline-end probe from re-running the `COUNT(*)` + build attempt
/// (no-op + warn while a build is in flight) on every tick.
const BUILD_BACKOFF_SECS: u64 = 600;

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
/// atomic load** — it never stamps the probe. The loop's pre-filter
/// uses this to avoid the recurring per-tick work *without* consuming the
/// single shared claim that [`build_probe_within_backoff`] owns: only
/// [`maybe_build_vector_index`] (which runs on the actual spawn) performs the
/// CAS claim, so the loop path and the pipeline-end path still mutually
/// exclude through that one claim.
fn build_probe_claimed_recently(probe: &std::sync::atomic::AtomicU64) -> bool {
    !build_probe_allowed(probe.load(Ordering::SeqCst), now_unix_secs())
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
pub(crate) fn build_probe_within_backoff(probe: &std::sync::atomic::AtomicU64) -> bool {
    let now = now_unix_secs();
    let mut last = probe.load(Ordering::SeqCst);
    loop {
        if !build_probe_allowed(last, now) {
            return false;
        }
        match probe.compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst) {
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
///
/// `tick` is the cadence between passes. Production passes 60 s; the loop
/// tests pass a small value (e.g. 50 ms) so they run on a *real* clock and
/// stay green on a remote (slow-RTT) DB — see the module's test note.
pub async fn auto_embed_loop(state: Arc<crate::AppState>, tick: Duration) {
    // Track the in-flight index build so we don't spawn overlapping builds.
    // The 10-minute backoff between build attempts is the shared
    // module-level gate (`build_probe_within_backoff`), so the loop's
    // early-build and `run_pipeline`'s post-run build back off each other.
    let mut in_flight: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        tokio::time::sleep(tick).await;
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
                if !build_probe_claimed_recently(&state.build_probe)
                    && should_spawn_build(st, count)
                    && in_flight.is_none()
                {
                    let db = state.db.clone();
                    let settings = state.settings.clone();
                    let probe = state.build_probe.clone();
                    let index_building = state.index_building.clone();
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
                            maybe_build_vector_index(
                                &db,
                                &settings,
                                MIN_INDEX_BUILD_ROWS,
                                &probe,
                                &index_building,
                            ),
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
        // P3 (2026-09 review): pipelines for distinct ZIMs are disjoint row
        // sets, so run them with bounded concurrency (2) instead of strictly
        // sequentially — the initial backfill (many ZIMs, no vectors) is
        // otherwise wall-clock-bound on the slowest single pipeline. Steady
        // state (one ZIM left with work) still runs exactly one pipeline;
        // the per-pipeline post-run index build funnels through the shared
        // `build_probe` CAS + `index_building` flag, which already
        // de-duplicates concurrent build attempts.
        const EMBED_CONCURRENCY: usize = 2;
        let mut set = tokio::task::JoinSet::new();
        for name in zims {
            if set.len() >= EMBED_CONCURRENCY {
                // Block on the oldest in-flight pipeline before starting the
                // next one (bounded fan-out, FIFO-ish drain).
                // LINT-3 (2026-09 sweep): checked invariant — the set is
                // non-empty under the `set.len()` guard — grandfathered
                // expect_used.
                #[allow(clippy::expect_used)]
                {
                    let outcome = set
                        .join_next()
                        .await
                        .expect("join_next before set is empty");
                    match outcome {
                        Ok((done, Err(e))) => {
                            tracing::error!("auto-embed failed for '{done}': {e}")
                        }
                        Ok((done, Ok(()))) => {
                            let _ = done;
                        }
                        Err(je) => tracing::error!("auto-embed task panicked: {je}"),
                    }
                }
            }
            tracing::info!("auto-embedding ZIM '{name}'");
            let db = state.db.clone();
            let settings = state.settings.clone();
            let build_probe = state.build_probe.clone();
            let index_building = state.index_building.clone();
            set.spawn(async move {
                let r = run_pipeline(db, settings, &name, &build_probe, &index_building).await;
                (name, r)
            });
        }
        while let Some(outcome) = set.join_next().await {
            match outcome {
                Ok((done, Err(e))) => tracing::error!("auto-embed failed for '{done}': {e}"),
                Ok((done, Ok(()))) => {
                    let _ = done;
                }
                Err(je) => tracing::error!("auto-embed task panicked: {je}"),
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::settings::{
        SettingsCache, EMBED_DEFAULT_IVFFLAT_THRESHOLD, KEY_EMBEDDING_DIMENSION,
        KEY_EMBEDDING_ENDPOINT, KEY_EMBEDDING_MODEL, KEY_EMBEDDING_TIMEOUT_SECS,
    };

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

    // Each test uses its own local `Arc<AtomicU64>` probe (no shared state),
    // so no test mutex is needed.

    #[test]
    fn build_probe_claimed_recently_is_read_only() {
        let probe = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        // Never probed → not claimed. Repeated checks must NOT stamp the slot.
        assert!(!build_probe_claimed_recently(&probe));
        assert!(!build_probe_claimed_recently(&probe));
        assert_eq!(
            probe.load(Ordering::SeqCst),
            0,
            "read-only check must never stamp the slot"
        );

        // The CAS claim stamps the slot…
        assert!(
            build_probe_within_backoff(&probe),
            "first claim within window succeeds"
        );
        let stamped = probe.load(Ordering::SeqCst);
        assert_ne!(stamped, 0, "claim stamps the slot");
        // …and the read-only check now reports it as claimed…
        assert!(
            build_probe_claimed_recently(&probe),
            "read-only check reports claimed"
        );
        // …without re-stamping (the pre-filter cannot consume the slot).
        assert_eq!(
            probe.load(Ordering::SeqCst),
            stamped,
            "read-only check must not restamp the slot"
        );
    }

    #[test]
    fn build_probe_claim_mutual_excludes_two_call_sites() {
        let probe = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        // The loop's spawned build and the pipeline-end build both funnel
        // through this single CAS claim: the first wins, the second (within
        // the window) loses — so the loop pre-filter (read-only) can never
        // starve the actual claim of its slot.
        assert!(
            build_probe_within_backoff(&probe),
            "first call site claims the slot"
        );
        assert!(
            !build_probe_within_backoff(&probe),
            "second call site within the window fails to claim"
        );
        assert!(
            build_probe_claimed_recently(&probe),
            "read-only pre-filter agrees the slot is claimed"
        );
    }

    // ─── auto_embed_loop lifecycle (Tests Major #4) ─────────────────────────
    //
    // The loop's cadence is a plain `tokio::time::sleep(tick)`, where `tick`
    // is a *parameter* (60 s in production, `TEST_TICK` = 50 ms here). These
    // tests run on a **real clock** (`#[tokio::test]`, *not* paused) with the
    // small `TEST_TICK` cadence.
    //
    // Why a real clock, not a paused one: the loop is a *spawned* background
    // task, and tokio's paused-clock auto-advance does not reliably wake a
    // *spawned* task's timer. A scratch repro (a spawned `sleep(60s)` + a
    // main task driving the clock) shows the virtual clock advancing far past
    // the deadline while the spawned task is never woken — so a spawned loop
    // under `start_paused = true` never ticks. On a *local* DB this was masked
    // (fast loopback I/O kept the worker busy enough that the loop happened
    // to run), but on a *remote* DB the slow real I/O let the wait budget
    // expire before the spawned loop ever ticked. A real clock sidesteps the
    // whole class of problem: the loop's `sleep(TEST_TICK)` is wall-clock, the
    // pipeline's real I/O is wall-clock, and the poll waits are wall-clock —
    // no paused-clock/auto-advance interaction at all, on any host.
    //
    // Plumbing that follows from the real clock:
    // - `loop_pool_or_skip` is a plain wall-clock connect timeout (no
    //   `tokio::time::resume()`/`pause()` dance — those only make sense under
    //   a paused clock). The pool is pre-opened (`min_connections =
    //   max_connections`) so the initial `acquire` is immediate.
    // - `TEST_TICK` (50 ms) keeps the tests fast: each pass does a handful of
    //   real DB round-trips, so a pass takes a few ms of real time even on a
    //   remote host, and a 50 ms cadence means the loop embeds new work
    //   within tens of ms.
    // - `LOOP_WAIT_BUDGET` (60 s real) bounds the real-time poll waits — it is
    //   a generous safety net, not the expected duration.
    //
    // All three are DB-gated with the lib's established pattern
    // (`DATABASE_URL` + a wall-clock connect timeout + `ZIMSERVICE_REQUIRE_DB`
    // hard-fail) and serialize via `DbExclusiveGuard` like every other DB
    // test in the crate. The 500-endpoint test also bumps the in-process
    // `EMBED_FAILS` poison counters (via the pipeline's failure path), which
    // the pure poison unit tests below assert on exactly — they take the
    // guard as well, so the shared static never races.

    const LOOP_ZIM: &str = "__embedloop__";
    /// Real-time budget for polling a live condition. The loop tests run on a
    /// real clock, so this bounds the wall-clock poll waits; it is a generous
    /// safety net, not the expected duration.
    const LOOP_WAIT_BUDGET: Duration = Duration::from_secs(60);
    /// Small real-time cadence for the loop tests (production uses 60 s). A small
    /// tick keeps the tests fast and, running on a *real* clock, immune to the
    /// paused-clock auto-advance limitation that breaks a spawned loop under a
    /// paused clock on a remote (slow-RTT) DB.
    const TEST_TICK: Duration = Duration::from_millis(50);

    /// DB gate for the loop tests — the lib's established pattern
    /// (`smoke_single_instance_refusal` & co. and `tests/integration/common.rs`):
    /// `DATABASE_URL` + a wall-clock connect timeout around the eager connect +
    /// `ZIMSERVICE_REQUIRE_DB` hard-fail, then serialize via
    /// `DbExclusiveGuard` like every other DB test in the crate.
    ///
    /// The loop tests run on a **real clock** (not paused — see the module's
    /// test note), so the gate is a plain wall-clock timeout: a downed DB
    /// yields a skip/timeout in ≤ 15 s of real time, and a live DB (local or
    /// remote) completes its real handshake well inside that. The pool is
    /// pre-opened (`min_connections = max_connections`) so the initial
    /// `acquire` below is immediate.
    async fn loop_pool_or_skip(test: &str) -> Option<Pool> {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        // Real clock (the loop tests run on a real clock, not a paused one —
        // see the module's test note), so the gate is a plain wall-clock
        // timeout. `min_connections = max_connections` pre-opens the pool so
        // the initial `acquire` is immediate.
        let pool = match tokio::time::timeout(
            Duration::from_secs(15),
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(8)
                .min_connections(8)
                .acquire_timeout(Duration::from_secs(86_400))
                .connect(&url),
        )
        .await
        {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return skip_loop_test(test, &url, &format!("connect failed: {e}")),
            Err(_) => return skip_loop_test(test, &url, "connect timed out (15s)"),
        };
        if pool.acquire().await.is_err() {
            return skip_loop_test(test, &url, "pool acquire failed");
        }
        Some(pool)
    }

    fn skip_loop_test(test: &str, url: &str, why: &str) -> Option<Pool> {
        // The lib's single skip/panic authority: counts LIB_SKIPPED for the
        // #[dtor] exit summary and hard-fails under ZIMSERVICE_REQUIRE_DB.
        crate::testing::gate_skip(test, url, why)
    }

    /// Current `articles.embedding` column dimension. pgvector stores the
    /// dimension **directly** as the column `atttypmod` (no VARHDRSZ offset),
    /// so this returns `atttypmod` as-is. Matching it keeps
    /// `ensure_vector_dimension` a no-op, so the tests never ALTER the shared
    /// dev column.
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
        typmod as u32
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
        Arc::new(crate::testing::state_from_parts(pool, settings))
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

    /// Poll `check` (real DB I/O) within a real-time budget (`LOOP_WAIT_BUDGET`).
    /// The loop tests run on a real clock, so the 25 ms sleep between probes
    /// is wall-clock: the loop (tick = `TEST_TICK`) makes its real I/O pass
    /// between polls, and the poll simply waits for it. `None` from a check
    /// (a failed probe) retries.
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
            // Wall-clock pause between probes (real clock — the loop makes
            // its pass between polls).
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Settle: sleep `n` × 25 ms of wall-clock time (with a `SELECT 1` probe
    /// per cycle) so the loop (tick = `TEST_TICK`) has very likely made at
    /// least one pass.
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

    /// 2026-09 review round 2 (Tests): `settle_ticks` is probabilistic (5 ×
    /// 25 ms wall clock), so a "disabled loop must not claim" negative
    /// assertion could pass vacuously if the loop never ticked — a false
    /// green. A disabled tick makes **no** I/O (the loop `continue`s right
    /// after its 50 ms sleep + in-memory `embedding_enabled()` read), so the
    /// proof is wall-clock, not DB-observable: a tokio task that has stayed
    /// alive across ≥2 full `TEST_TICK` periods since spawn (or since the
    /// settings write this test made) has necessarily executed its
    /// check path at least once — the runtime polls it while this test
    /// awaits its own timers/I-O. The liveness check also catches a
    /// panicked/aborted loop, which would otherwise make the negative
    /// assertions pass vacuously.
    async fn prove_ticked(loop_task: &tokio::task::JoinHandle<()>) {
        use std::time::Instant;

        // ≥ 2 full TEST_TICK periods (200 ms for the 50 ms test tick) with
        // 2× slack: the loop necessarily ran its check path ≥ once.
        let deadline = Instant::now() + Duration::from_millis(TEST_TICK.as_millis() as u64 * 4);
        loop {
            if loop_task.is_finished() {
                // JoinHandle (tokio 1.53) exposes no try-join, so the stored
                // JoinError (abort or panic) isn't readable without
                // consuming the handle — the liveness failure itself is the
                // signal: a dead loop makes the negative assertions vacuous.
                panic!(
                    "loop task exited before the negative assertions — a \
                       dead loop would make them pass vacuously"
                );
            }
            if Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// (1) Recurring tick: the loop is not a one-shot — it keeps ticking on
    /// its cadence and re-evaluates its pre-conditions (`embedding_enabled`,
    /// `list_embeddable_zims`) every tick, so work that appears *after* the
    /// first successful pass is picked up on a later tick, with no config
    /// change and no restart.
    #[tokio::test]
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
        let loop_task = tokio::spawn(auto_embed_loop(state, TEST_TICK));

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
    #[tokio::test]
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
        let loop_task = tokio::spawn(auto_embed_loop(state, TEST_TICK));

        // Let the loop tick a few times while disabled: each tick re-reads
        // `embedding.enabled` and must skip — no claim, no HTTP.
        settle_ticks(&pool, 5).await;
        // 2026-09 review round 2 (Tests): prove the loop actually ticked
        // (alive across ≥2 ticks) before the negative assertions — and that
        // it didn't die (a dead loop passes them vacuously).
        prove_ticked(&loop_task).await;
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
        // 2026-09 review round 2 (Tests): prove ≥1 tick after the re-disable
        // (alive across ≥2 ticks) before the negative assertions.
        prove_ticked(&loop_task).await;
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
    #[tokio::test]
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
        // The early-build decision is gated by the shared build-probe backoff
        // timestamp on `AppState` — the loop's state starts from "never probed"
        // (fresh AtomicU64::new(0)) so this test's tick can spawn the build
        // regardless of earlier tests' probes.

        // At/above the IVFFlat threshold the loop *would* build an IVFFlat
        // index (lists = √count, the documented kind preference) — but that
        // is expensive against a dev DB that already holds millions of
        // vectors, so this test only makes sense on a small DB.
        let preexisting: i64 = raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM articles WHERE embedding IS NOT NULL",
            |q| q,
        )
        .await
        .expect("embedded count")
        .expect("row present");
        if preexisting + 10_000 >= EMBED_DEFAULT_IVFFLAT_THRESHOLD {
            eprintln!(
                "skipping: dev DB already holds {preexisting} vectors (an IVFFlat build at \
                 this scale would be expensive)"
            );
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
                 SELECT '[' || (SELECT string_agg('0.1', ',') FROM generate_series(1, $2)) || ']'
                 AS v
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
        let loop_task = tokio::spawn(auto_embed_loop(state, TEST_TICK));

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
