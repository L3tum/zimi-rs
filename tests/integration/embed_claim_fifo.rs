//! PERF review item 3 (2026-09-18) — the embed pipeline's batch-claim is
//! deterministic FIFO: the claim statement's inner `SELECT` carries
//! `ORDER BY id`, so the oldest (lowest-id) un-embedded rows are claimed
//! first, batch after batch, instead of the planner's arbitrary choice.
//!
//! The test executes the EXACT production statement
//! (`zimservice::embed::CLAIM_EMBED_BATCH_SQL`) in a dedicated temp
//! database (the `migrations.rs` pattern — created and dropped around the
//! test so the shared dev schema is never touched): seed N un-embedded
//! rows, claim a batch smaller than N, and assert the claimed ids are
//! exactly the lowest ids — twice in a row. The first batch's fresh
//! `embed_at` stamp makes those rows un-claimable within the 10-minute
//! staleness window, so the second claim must move to the NEXT lowest ids
//! and must never re-pick the first batch's rows.
//!
//! The id sets are compared as SETS (sorted): `UPDATE … RETURNING` makes
//! no guarantee about the row order of the outer statement — only the
//! batch MEMBERSHIP is deterministic (the pipeline zips the API's vectors
//! to rows by the response's `index` field, never by row order).
//!
//! DB-gated like the rest of the suite: skips cleanly without a reachable
//! Postgres, hard-fails under `ZIMSERVICE_REQUIRE_DB`. Holds the
//! `DbExclusiveGuard` for the shared-server `CREATE/DROP DATABASE`.

use super::common::*;
use super::migrations::{close_and_drop, create_temp_db};

const ZIM: &str = "__itest_claim_fifo__";
/// More rows than one batch (the claim must be a strict subset).
const N_ROWS: i32 = 7;
/// The batch size claimed per pass.
const BATCH: i64 = 3;

#[tokio::test]
async fn embed_claim_is_fifo_by_id() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };
    run_migrations(&pool)
        .await
        .expect("fresh apply (temp DB must migrate cleanly)");

    // One ZIM + N un-embedded articles (ids assigned by the sequence).
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                       index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 7, 7)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    let zim_id: i32 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT id FROM zims WHERE name = $1",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap()
    .expect("row present");
    for i in 1..=N_ROWS {
        zimservice::db::raw::execute(
            &pool,
            "INSERT INTO articles (path, title, content_preview, snippet, search_vector, \
            language, namespace, zim_id)
             VALUES ($1, $1, 'preview', 'snip', to_tsvector('simple', $1), 'en', 'C', $2)",
            |q| q.bind(format!("A/row-{i}")).bind(zim_id),
        )
        .await
        .unwrap();
    }
    // Rows were just inserted contiguously, so the lowest id is the FIFO
    // head; derive it rather than assuming sequence state.
    let min_id: i64 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT min(id) FROM articles WHERE zim_id = $1",
        |q| q.bind(zim_id),
    )
    .await
    .unwrap()
    .expect("row present");

    // Claim 1: exactly the 3 lowest ids (as a set — RETURNING row order is
    // not guaranteed; membership is the FIFO property).
    let mut claim_1: Vec<i64> =
        zimservice::db::raw::fetch_all(&pool, zimservice::embed::CLAIM_EMBED_BATCH_SQL, |q| {
            q.bind(ZIM).bind(BATCH)
        })
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _): (i64, String)| id)
        .collect();
    claim_1.sort_unstable();
    assert_eq!(
        claim_1,
        vec![min_id, min_id + 1, min_id + 2],
        "claim 1 must take exactly the 3 lowest ids"
    );

    // Claim 2 (same 10-minute window: claim 1's `embed_at` is fresh, so its
    // rows are un-claimable): exactly the NEXT 3 lowest ids — never a
    // re-pick of claim 1's rows.
    let mut claim_2: Vec<i64> =
        zimservice::db::raw::fetch_all(&pool, zimservice::embed::CLAIM_EMBED_BATCH_SQL, |q| {
            q.bind(ZIM).bind(BATCH)
        })
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _): (i64, String)| id)
        .collect();
    claim_2.sort_unstable();
    assert_eq!(
        claim_2,
        vec![min_id + 3, min_id + 4, min_id + 5],
        "claim 2 must take exactly the next 3 lowest ids (claim 1's rows \
         stay stamped and are not re-claimed)"
    );

    // Exactly the 6 claimed rows carry the stamp; the 2 leftovers are
    // still un-stamped and claimable.
    let stamped: i64 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT count(*) FROM articles WHERE zim_id = $1 AND embed_at IS NOT NULL",
        |q| q.bind(zim_id),
    )
    .await
    .unwrap()
    .expect("row present");
    assert_eq!(stamped, 6, "both claims stamped exactly 6 rows");

    // Cleanup: drop the temp DB (everything lives in it).
    close_and_drop(&base_pool, &pool, &name).await;
}
