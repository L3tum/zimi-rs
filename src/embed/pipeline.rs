//! Per-ZIM embedding pipeline: batch claim → HTTP embed → bulk vector write.
//!
//! Includes the W6.5 bounded poison-row re-claim guard (in-process failure
//! counters) and the pure store-guard / SQL-rendering helpers.

use std::sync::Arc;

use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::{Error, Result};
use crate::settings::SettingsCache;

use crate::embed::client::{format_vector, EmbedClient, EmbedConfig};
use crate::embed::column::{ensure_vector_dimension, stored_embedding_dimension};
use crate::embed::vector_index::{maybe_build_vector_index, VECTOR_INDEX_MIN_ROWS};

/// Pure: the fail-fast dimension-mismatch error [`run_pipeline`] returns
/// before claiming/embedding (so the message is unit-testable without a
/// DB). The rows stay unclaimed — no poison counter is bumped — so a
/// dimension fix (model or column) recovers them.
pub(crate) fn dimension_mismatch_error(stored: u32, client: u32) -> Error {
    Error::Embedding(format!(
        "embedding column is vector({stored}) but the configured model emits {client}-dim \
        vectors; change the model back, or clear existing vectors (e.g. `UPDATE articles SET \
        embedding = NULL`) and `ALTER TABLE articles ALTER COLUMN embedding TYPE vector({client})`"
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
        "UPDATE articles a SET embedding = v.vec::vector, \
         embed_model = ${model_idx}, embed_at = now() \
         FROM (VALUES {}) AS v(id, vec) WHERE a.id = v.id",
        values.join(", ")
    ))
}

/// The atomic batch-claim statement of the embedding pipeline: stamp
/// `embed_at = now()` on the next un-embedded batch for the ZIM, returning
/// each row's `id` and the text to embed. Raw SQL: `UPDATE … WHERE id IN
/// (SELECT … LIMIT) RETURNING <computed expr>` (the second RETURNING column
/// is a `coalesce(…) || …` expression, not a plain column).
///
/// Deterministic FIFO (2026-09-18, PERF review item 3): the inner `SELECT`
/// carries `ORDER BY id`, so batch membership is the OLDEST (lowest-id)
/// un-embedded rows first, batch after batch, instead of the planner's
/// arbitrary choice. Un-embedded rows accumulate since ingest, so id order
/// ≈ insertion order; the deterministic order makes a failing batch
/// reproducible (the same rows fail the same way on every retry) and keeps
/// the W6.5 per-`id` poison counters stable across claim-window restarts.
///
/// `pub` (and re-exported from `crate::embed`) so the temp-DB regression
/// test (`tests/integration/embed_claim_fifo.rs`) executes the exact
/// production statement.
pub const CLAIM_EMBED_BATCH_SQL: &str = "UPDATE articles SET embed_at = now() \
 WHERE id IN (SELECT id FROM articles \
     WHERE zim_id = (SELECT id FROM zims WHERE name = $1) \
       AND embedding IS NULL \
       AND (embed_at IS NULL OR embed_at < now() - interval '10 minutes') \
     ORDER BY id \
     LIMIT $2) \
 RETURNING id, coalesce(snippet, '') || ' ' || coalesce(left(content_preview, \
 500), '')";

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
// LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
#[allow(clippy::expect_used)]
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
// LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
#[allow(clippy::expect_used)]
fn record_batch_failure(ids: &[i64]) {
    let mut map = EMBED_FAILS.lock().expect("embed-fails lock poisoned");
    for id in ids {
        *map.entry(*id).or_insert(0) += 1;
    }
}

/// W6.5: an embed batch succeeded — clear its rows' failure counters.
// LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
#[allow(clippy::expect_used)]
fn record_batch_success(ids: &[i64]) {
    let mut map = EMBED_FAILS.lock().expect("embed-fails lock poisoned");
    for id in ids {
        map.remove(id);
    }
}

/// Test-only reset of the in-process poison counter (the first in-module
/// global state; kept behind `#[cfg(test)]` so it never ships).
#[cfg(test)]
// LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
#[allow(clippy::expect_used)]
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
pub async fn run_pipeline(
    pool: Pool,
    settings: SettingsCache,
    zim_name: &str,
    probe: &std::sync::atomic::AtomicU64,
    in_flight: &std::sync::atomic::AtomicBool,
) -> Result<()> {
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
        // `CLAIM_EMBED_BATCH_SQL`: the inner `SELECT` is `ORDER BY id` —
        // deterministic FIFO (the oldest rows are claimed first, batch after
        // batch); see the const's doc for the 2026-09-18 decision.
        let mut rows: Vec<(i64, String)> = raw::fetch_all(&pool, CLAIM_EMBED_BATCH_SQL, |q| {
            q.bind(zim_name).bind(batch_size)
        })
        .await?;

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
    maybe_build_vector_index(&pool, &settings, VECTOR_INDEX_MIN_ROWS, probe, in_flight).await;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
}
