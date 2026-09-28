//! DB-gated behavioral search test at 10k rows (M3, 2026-10 review).
//!
//! `tests/integration/search.rs` pins search behavior against a 3-article
//! fixture; `tests/integration/trgm_plan.rs` pins the PLANNER at 100k rows.
//! This test closes the gap between them: the production search path
//! (hybrid + FTS arms, merge/rank, pagination, ZIM scoping) exercised
//! against the bench fixture — the 10,000-row corpus `benches/common`
//! seeds, shared through the single definition in `tests/common/
//! corpus10k.rs` (included textually, the same seam the bench uses).
//!
//! The corpus is deterministic (`zimservice::testing::trgm_corpus`: 40-word
//! grid; `w1 = WORDS[i % 40]`, `w2 = WORDS[(i / 40) % 40]`,
//! `w3 = WORDS[(i / 1600) % 40]`), so the expected match sets are exact:
//!
//! - The probe phrase `quixotic granite` (`WORDS[17]` + `WORDS[7]`) occurs
//!   in exactly 7 TITLES (`i ≡ 297 (mod 1600)` — the bench's documented
//!   probe contract), and in exactly 13 title+content rows: the 7
//!   `w1=quixotic, w2=granite` rows (`i ≡ 297 (mod 1600)` — 7 rows below
//!   10k) plus the 6 `w1=granite, w2=quixotic` rows (`i ≡ 687 (mod 1600)`
//!   — only 6 below 10k, since 687 + 6·1600 = 10287). Both words also
//!   appear in each row's `content_preview`, and no `w3` slot reaches
//!   either word index below 10k. The production tsvector
//!   (`setweight(title,'A') || setweight(content_preview,'B')`,
//!   `UPSERT_ARTICLES_FROM_STAGING_SQL`) therefore matches exactly those 13
//!   rows for the FTS arm.
//!
//! No reachable DB → the test skips (counted, per `pool_or_skip`); a
//! reachable DB that cannot seed → hard failure (a mis-seeded corpus would
//! make every assertion below vacuous).

mod corpus10k {
    include!("../common/corpus10k.rs");
}
use super::common::*;
use corpus10k::{seed_fixture, FIXTURE_ZIM, ROWS};
use std::collections::BTreeSet;
use zimservice::search::{SearchEngine, SearchParams};
use zimservice::testing::trgm_corpus::PROBE;
use zimservice::testing::DbExclusiveGuard;

/// The bench's documented probe contract: the 7 titles carrying the probe
/// phrase (`i ≡ 297 (mod 1600)`).
const PROBE_TITLE_PATHS: [&str; 7] = [
    "A/bench_00297",
    "A/bench_01897",
    "A/bench_03497",
    "A/bench_05097",
    "A/bench_06697",
    "A/bench_08297",
    "A/bench_09897",
];

/// The full exact-FTS match set for the probe phrase at 10k rows: the 7
/// probe titles plus the 6 mirror rows where the words swap slots
/// (`w1=granite, w2=quixotic` — `i ≡ 687 (mod 1600)`; 687 + 6·1600 = 10287
/// is past the corpus, so 6, not 7).
const PROBE_FTS_PATHS: [&str; 13] = [
    "A/bench_00297",
    "A/bench_00687",
    "A/bench_01897",
    "A/bench_02287",
    "A/bench_03497",
    "A/bench_03887",
    "A/bench_05097",
    "A/bench_05487",
    "A/bench_06697",
    "A/bench_07087",
    "A/bench_08297",
    "A/bench_08687",
    "A/bench_09897",
];

/// Seed the 10k fixture and build the production engine over it (settings
/// loaded from the DB, degradation tracker at its default).
async fn seeded_engine(
    pool: &zimservice::db::pool::Pool,
    _db_gate: &DbExclusiveGuard,
) -> SearchEngine {
    run_migrations(pool).await.expect("migrations");
    seed_fixture(pool)
        .await
        .expect("10k fixture seed (a reachable DB that cannot seed is a hard failure)");
    let settings = zimservice::settings::SettingsCache::load(
        pool.clone(),
        std::collections::HashMap::new(),
        std::collections::HashMap::new(),
    )
    .await
    .expect("settings load");
    SearchEngine::new(
        pool.clone(),
        settings,
        zimservice::health::DegradationTracker::default(),
    )
}

/// Drop the fixture (the cascade covers its 10k articles) — the suite's
/// shared DB must not keep the standing footprint between runs.
async fn drop_fixture(pool: &zimservice::db::pool::Pool) {
    zimservice::db::raw::execute(pool, "DELETE FROM zims WHERE name = $1", |q| {
        q.bind(FIXTURE_ZIM)
    })
    .await
    .expect("fixture cleanup");
}

/// The 10k-row behavioral contract:
/// 1. FTS-only: the probe phrase matches EXACTLY the 13 corpus rows that
///    carry both words (title weight-A + content weight-B tsvector).
/// 2. Hybrid: the 7 documented probe titles are all in the result set,
///    every result is a fixture row, and scores are non-increasing.
/// 3. Pagination: `limit=10` pages over a high-frequency word are full and
///    disjoint (the offset-aware per-branch fetch covers the requested
///    page — the regression the `branch_fetch_limit` cap documents).
/// 4. ZIM scoping: `zim` filters to the named ZIM; an absent ZIM is empty.
#[tokio::test]
async fn search_behavior_at_10k_rows() {
    let (pool, db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    let engine = seeded_engine(&pool, &db_gate).await;

    // ── 1. FTS-only: the exact match set ────────────────────────────────
    let fts = engine
        .search(
            PROBE,
            &SearchParams {
                zim: None,
                language: None,
                mode: Some("fts"),
                limit: Some(50),
                offset: None,
                highlight: false,
            },
        )
        .await
        .expect("FTS search must run");
    let got: BTreeSet<&str> = fts.iter().map(|r| r.path.as_str()).collect();
    let want: BTreeSet<&str> = PROBE_FTS_PATHS.iter().copied().collect();
    assert_eq!(
        got,
        want,
        "FTS must match exactly the 13 corpus rows carrying both probe \
         words at 10k rows (got {} rows: {:?})",
        fts.len(),
        fts.iter().map(|r| r.path.as_str()).collect::<Vec<_>>()
    );
    assert!(
        fts.iter().all(|r| r.zim_name == FIXTURE_ZIM),
        "every FTS hit must belong to the fixture ZIM"
    );

    // ── 2. Hybrid: the documented probe titles survive the merge ───────
    let hybrid = engine
        .search(
            PROBE,
            &SearchParams {
                zim: None,
                language: None,
                mode: None,
                limit: Some(50),
                offset: None,
                highlight: false,
            },
        )
        .await
        .expect("hybrid search must run");
    let hybrid_paths: BTreeSet<&str> = hybrid.iter().map(|r| r.path.as_str()).collect();
    for path in PROBE_TITLE_PATHS {
        assert!(
            hybrid_paths.contains(path),
            "the documented probe title {path} must be in the hybrid result set"
        );
    }
    assert!(
        !hybrid.is_empty() && hybrid.iter().all(|r| r.zim_name == FIXTURE_ZIM),
        "hybrid results must be fixture rows"
    );
    // The merged ranking is score-ordered (the contract the /search API
    // surfaces — a regression that breaks the merge ordering shows here).
    for pair in hybrid.iter().zip(hybrid.iter().skip(1)) {
        assert!(
            pair.0.score >= pair.1.score,
            "hybrid results must be score-ordered: {} ({}) before {} ({})",
            pair.0.path,
            pair.0.score,
            pair.1.path,
            pair.1.score
        );
    }

    // ── 3. Pagination: full, disjoint pages over a high-frequency word ─
    // `WORDS[0]` ("amber") occurs in ~1,800 corpus rows (the `w3` slot
    // alone covers rows 0..1599), so two 10-row pages have real depth.
    let page0 = engine
        .search(
            "amber",
            &SearchParams {
                zim: Some(FIXTURE_ZIM),
                language: None,
                mode: Some("fts"),
                limit: Some(10),
                offset: Some(0),
                highlight: false,
            },
        )
        .await
        .expect("page 0 must run");
    let page1 = engine
        .search(
            "amber",
            &SearchParams {
                zim: Some(FIXTURE_ZIM),
                language: None,
                mode: Some("fts"),
                limit: Some(10),
                offset: Some(10),
                highlight: false,
            },
        )
        .await
        .expect("page 1 must run");
    assert_eq!(page0.len(), 10, "page 0 must be full at 10k rows");
    assert_eq!(
        page1.len(),
        10,
        "page 1 (offset 10) must be full at 10k rows"
    );
    let p0: BTreeSet<&str> = page0.iter().map(|r| r.path.as_str()).collect();
    let p1: BTreeSet<&str> = page1.iter().map(|r| r.path.as_str()).collect();
    let overlap: Vec<&str> = p0.intersection(&p1).copied().collect();
    assert!(
        overlap.is_empty(),
        "consecutive pages must be disjoint (overlap: {overlap:?})"
    );

    // ── 4. ZIM scoping ──────────────────────────────────────────────────
    let scoped = engine
        .search(
            PROBE,
            &SearchParams {
                zim: Some(FIXTURE_ZIM),
                language: None,
                mode: Some("fts"),
                limit: Some(50),
                offset: None,
                highlight: false,
            },
        )
        .await
        .expect("scoped search must run");
    assert_eq!(
        scoped.len(),
        13,
        "the zim= filter must not drop fixture rows"
    );
    let absent = engine
        .search(
            PROBE,
            &SearchParams {
                zim: Some("no-such-zim-10k"),
                language: None,
                mode: Some("fts"),
                limit: Some(50),
                offset: None,
                highlight: false,
            },
        )
        .await
        .expect("scoped-to-absent search must run");
    assert!(
        absent.is_empty(),
        "an absent ZIM must return no rows (got {})",
        absent.len()
    );

    // Sanity: the fixture really is 10k rows (a mis-seeded corpus would
    // make the exact-set assertions above vacuous).
    let count: i64 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT count(*) FROM articles WHERE zim_id = (SELECT id FROM zims WHERE name = $1)",
        |q| q.bind(FIXTURE_ZIM),
    )
    .await
    .expect("article count")
    .expect("fixture zims row present");
    assert_eq!(
        count, ROWS as i64,
        "fixture must carry exactly 10k articles"
    );

    drop_fixture(&pool).await;
}
