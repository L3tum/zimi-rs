//! PERF review item 3 (2026 project-wide review) — plan-shape regression
//! gate for the embed batch-claim.
//!
//! The claim statement (`zimservice::embed::CLAIM_EMBED_BATCH_SQL`) selects,
//! per 64-row batch, `WHERE zim_id = … AND embedding IS NULL … ORDER BY id
//! LIMIT $2` over the still-un-embedded rows of a ZIM. With the 002 index
//! (`idx_articles_unembedded_zim` on `(zim_id) WHERE embedding IS NULL`),
//! the per-`zim_id` index order does NOT match the claim's `ORDER BY id`,
//! so a claim that rides that index has to scan AND sort the whole un-embedded
//! remainder — O(remaining) per batch, quadratic overall (hours of pure claim
//! overhead on a multi-million-article ZIM). Migration 017 extends the index
//! to `(zim_id, id) WHERE embedding IS NULL`, so the per-`zim_id` index order
//! IS the claim's `ORDER BY id` and the claim becomes an early-terminating
//! index scan: O(batch) per claim.
//!
//! This gate pins that shape. Corpus: 10 fixture ZIMs x 10k un-embedded
//! articles (100k total — the deliberate `trgm_plan.rs` scale, below which
//! the index-vs-scan cost flip is unreliable). The corpus is deliberately
//! MULTI-ZIM: probed on PG 16.15 (CI's `pgvector:pg16` major), on a
//! single-ZIM corpus (target = the whole table) the planner correctly picks
//! the `articles_pkey` path (scan-in-id-order + filter + `LIMIT` — also
//! O(batch), where the extended index adds no selectivity), while on a
//! multi-ZIM corpus it picks `idx_articles_unembedded_zim` (`Index Cond:
//! (zim_id = $0)`, `LIMIT` directly above the scan — early termination).
//! The multi-ZIM shape is the real regression scenario: reverting 017
//! drops the extended index and the plan falls back to the pkey path, so
//! the index-use assertion below fails on a 017 revert.
//!
//! Method: a dedicated temp database (the `migrations.rs` pattern — created
//! and dropped around the test, so the shared dev schema is never touched),
//! migrated fresh (which builds the 017 index on an empty table), then
//! set-based-populated (20 `generate_series` INSERTs, no COPY needed).
//! After `ANALYZE`, the EXACT production statement (rebuilt from
//! `zimservice::embed::CLAIM_EMBED_BATCH_SQL`, never a copy) and its inner
//! candidate `SELECT` (extracted from that same constant, so a changed
//! statement shape fails the `find` loudly instead of silently un-gating)
//! must both (a) reference `idx_articles_unembedded_zim`, (b) contain NO
//! `Sort` node (the `ORDER BY id` is served by the index order — a `Sort`
//! is exactly the quadratic regression), and (c) contain NO `Seq Scan on
//! articles` (the candidate scan must be an index scan; on the full UPDATE
//! the outer target probes ride `articles_pkey`, which is the right shape).
//!
//! Wall clock: 20 small INSERTs + `ANALYZE` + 2 EXPLAINs — a few seconds
//! (probed: ~3 s), far under the ~60 s budget.
//!
//! DB-gated like every test here: skips without a reachable `DATABASE_URL`,
//! hard-fails under `ZIMSERVICE_REQUIRE_DB=1` when the base DB is
//! unreachable (CI's `test` job). If the base user cannot `CREATE
//! DATABASE`, the `create_temp_db` `None` arm records a mid-test skip via
//! `skip_midtest` — which itself consults `REQUIRE_DB` and hard-fails in
//! strict mode (the inherited `migrations.rs` convention); CI is
//! unaffected because its service user is a superuser. Holds the
//! `DbExclusiveGuard` for the shared-server `CREATE/DROP DATABASE`.

use super::common::*;
use super::migrations::{close_and_drop, create_temp_db};

/// Corpus shape: 10 fixture ZIMs, 10k un-embedded articles each — 100k
/// total (the deliberate `trgm_plan.rs` scale; below it the index-vs-scan
/// cost flip is unreliable). MULTI-ZIM on purpose (see the module docs):
/// it is the shape in which the extended partial index is the planner's
/// choice, and a 017 revert is detectable.
const N_ZIMS: i64 = 10;
const ROWS_PER_ZIM: i64 = 10_000;

/// Fixture target ZIM name — the claim looks the ZIM up by name.
const ZIM_NAME: &str = "__embed_claim_plan__";

/// The production claim batch size (`embed::pipeline`'s default batch).
const BATCH: i64 = 64;

/// `EXPLAIN (FORMAT TEXT)` one statement with its two bound params (the ZIM
/// name, the batch size); join the plan lines for substring assertions.
async fn explain_claim(pool: &Pool, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN (FORMAT TEXT) {sql}");
    let lines: Vec<String> = zimservice::db::raw::fetch_scalar_all(pool, &explain_sql, |q| {
        let mut q = q;
        q = q.bind(ZIM_NAME);
        q = q.bind(BATCH);
        q
    })
    .await
    .expect("EXPLAIN of the embed claim SQL must run");
    lines.join("\n")
}

/// Load the multi-ZIM un-embedded corpus (10 x 10k = 100k rows, `ANALYZE`);
/// ZIM_NAME is the first (target) ZIM. Set-based inserts (20 chunks of
/// 5000 rows) — no COPY needed at this scale.
async fn load_corpus(pool: &Pool) {
    for z in 0..N_ZIMS {
        let name = if z == 0 {
            ZIM_NAME.to_string()
        } else {
            format!("{ZIM_NAME}_filler{z}")
        };
        let zim_id: i32 = zimservice::db::raw::fetch_scalar_optional(
            pool,
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                               index_status, indexed_entries, article_count)
             VALUES ($1, $1, $1, 0, now(), 'ready', 0, 0) RETURNING id",
            |q| q.bind(&name),
        )
        .await
        .expect("insert fixture zim")
        .expect("zim id");
        for chunk in 0..ROWS_PER_ZIM / 5_000 {
            zimservice::db::raw::execute(
                pool,
                "INSERT INTO articles (zim_id, path, title, content_preview, search_vector)
                 SELECT $1, 'A/row_' || (n + $2), 'title ' || (n + $2),
                        'preview ' || (n + $2), to_tsvector('simple', 'title ' || (n + $2))
                 FROM generate_series(1, 5000) AS n",
                |q| q.bind(zim_id).bind(chunk * 5_000),
            )
            .await
            .expect("insert article chunk");
        }
    }

    let count: i64 =
        zimservice::db::raw::fetch_scalar_optional(pool, "SELECT count(*) FROM articles", |q| q)
            .await
            .expect("count articles")
            .expect("count row");
    assert_eq!(
        count,
        N_ZIMS * ROWS_PER_ZIM,
        "corpus must be fully populated"
    );

    // Fresh table: no statistics until analyzed, and the planner needs them
    // to price the partial-index early-terminating scan against the pkey
    // fallback and a seq scan of the whole remainder.
    zimservice::db::raw::execute(pool, "ANALYZE", |q| q)
        .await
        .expect("ANALYZE");
}

/// Extract the claim's inner candidate `SELECT` from the production
/// statement constant — the regression locus (the per-batch candidate
/// scan with its `ORDER BY id`). Rebuilt from `CLAIM_EMBED_BATCH_SQL`
/// itself, never copied: if the production statement changes shape, the
/// `find`s fail loudly at runtime instead of the gate silently testing a
/// stale string.
fn inner_candidate_select() -> String {
    let sql = zimservice::embed::CLAIM_EMBED_BATCH_SQL;
    let start = sql
        .find("SELECT id FROM articles")
        .expect("claim statement must embed the candidate select");
    let end_marker = "LIMIT $2";
    let end = sql
        .find(end_marker)
        .expect("claim statement must bound the candidate select with LIMIT $2")
        + end_marker.len();
    sql[start..end].to_string()
}

/// PERF review item 3: on a 10-ZIM x 10k-row un-embedded corpus, the embed
/// claim must plan an early-terminating index scan on the extended
/// `idx_articles_unembedded_zim` (`(zim_id, id) WHERE embedding IS NULL`,
/// migration 017) — never a scan + sort of the un-embedded remainder (the
/// quadratic regression). Runs in a dedicated temp database.
#[tokio::test]
async fn smoke_embed_claim_uses_extended_partial_index_100k() {
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
        .expect("fresh apply (temp DB must migrate cleanly — builds the 017 index)");

    load_corpus(&pool).await;

    // (1) The EXACT production statement: the candidate scan must ride the
    // extended partial index (a 017 revert drops it and the plan falls
    // back to the pkey path — see the module docs), NO `Sort` node may
    // appear anywhere (the `ORDER BY id` is served by the index order — a
    // `Sort` is exactly the per-batch quadratic regression), and no
    // `Seq Scan on articles` (the candidate scan is an index scan; the
    // UPDATE's outer target probes ride `articles_pkey`, which is the
    // right shape for 64 rows out of 100k).
    let plan = explain_claim(&pool, zimservice::embed::CLAIM_EMBED_BATCH_SQL).await;
    assert!(
        plan.contains("idx_articles_unembedded_zim"),
        "embed claim must use the extended un-embedded partial index \
         (migrations/017):\n{plan}"
    );
    assert!(
        !plan.contains("Sort"),
        "embed claim must NOT sort the un-embedded remainder per batch — \
         the ORDER BY id is served by the (zim_id, id) index order:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on articles"),
        "embed claim must NOT degrade to a sequential scan of articles:\n{plan}"
    );

    // (2) The inner candidate select on its own: the regression locus.
    // Early-terminating index scan only — no seq scan of the remainder,
    // no sort.
    let inner = inner_candidate_select();
    let plan = explain_claim(&pool, &inner).await;
    assert!(
        plan.contains("idx_articles_unembedded_zim"),
        "the claim's candidate select must use the extended un-embedded \
         partial index (migrations/017):\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on articles"),
        "the claim's candidate select must NOT degrade to a sequential scan \
         of the un-embedded remainder on a 100k-row corpus:\n{plan}"
    );
    assert!(
        !plan.contains("Sort"),
        "the claim's candidate select must NOT sort — ORDER BY id is the \
         index order (zim_id, id):\n{plan}"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}
