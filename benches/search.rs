//! Criterion perf micro-bench of the multi-engine SEARCH path
//! (`zimservice::search::SearchEngine`), driven end-to-end against the
//! shared dev DB at `DATABASE_URL`.
//!
//! # What each bench measures
//!
//! - `search/hybrid` — the full `engine.search()` with no `mode` filter:
//!   FTS + trigram + vector arms run concurrently (4 pooled connections),
//!   merged + de-duplicated. The default request shape (limit 10, no
//!   offset/highlight/zim/lang filters).
//! - `search/fts_only` — `mode = "fts"`: `websearch_to_tsquery` on
//!   `search_vector` (GIN), single arm.
//! - `search/trgm_arms` — `mode = "trgm"`: prefix + contains + similarity
//!   arms (the full ≥3-char query path; ≥ 12 chars).
//! - `search/trgm_prefix_short` — `mode = "trgm"` with a 2-char query
//!   (`"gr"`): the short-query path where PERF-2 skips the
//!   contains/similarity arms (prefix arm only, btree-backed).
//! - `search/vector_ann` — the production vector arm SQL (`vector_sql`)
//!   executed on its own pooled connection via `run_sql_on` — an HNSW
//!   ANN seek with a real 1536-dim probe (~1800 distance-0 rows from the
//!   fixture's batch one-hot embeddings), no merge overhead.
//! - `search/suggest` — `engine.suggest()`: the pure-trgm `/suggest` path
//!   (prefix + contains + similarity on one connection).
//!
//! # Fixture
//!
//! `benches/common::setup()` seeds the shared dev DB with a dedicated
//! `bench_fixture` ZIM (10,000 deterministic articles from the
//! `tests/integration/trgm_plan.rs` corpus, real 1536-dim embeddings) and
//! loads the live settings — weights/thresholds are the dev DB's, not
//! defaults. The fixture is dropped on exit; with no reachable DB the
//! bench prints a skip banner and exits 0 (see `benches/common`).

// LINT-3: bench harness — expect/unwrap are the loud-failure idiom here
// (same grandfathering as the test modules); see benches/common.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::hint::black_box;
use std::process::ExitCode;

use criterion::{BatchSize, Criterion};
use zimservice::db::pool::acquire_timed;
use zimservice::embed::format_vector;
use zimservice::health::DegradationTracker;
use zimservice::search::{run_sql_on, vector_sql, SearchParams};

use common::{cleanup, setup, SetupError, PROBE, RUNTIME};

/// `search/hybrid` — the full multi-engine request: FTS + trgm + vector
/// arms concurrent, merged. One engine clone per setup (criterion's
/// black_box'd setup) so no state leaks between measured iterations.
fn hybrid_search(c: &mut Criterion, ctx: &common::Ctx) {
    let params = SearchParams::default();
    c.bench_function("search/hybrid", |b| {
        b.iter_batched(
            || ctx.engine.clone(),
            |engine| {
                RUNTIME.block_on(async {
                    let res = engine
                        .search(black_box(PROBE), black_box(&params))
                        .await
                        .expect("hybrid search");
                    black_box(res.len())
                })
            },
            BatchSize::SmallInput,
        );
    });
}

/// `search/fts_only` — a single FTS arm (`websearch_to_tsquery`, GIN).
fn fts_only(c: &mut Criterion, ctx: &common::Ctx) {
    let params = SearchParams {
        mode: Some("fts"),
        ..Default::default()
    };
    c.bench_function("search/fts_only", |b| {
        b.iter_batched(
            || ctx.engine.clone(),
            |engine| {
                RUNTIME.block_on(async {
                    let res = engine
                        .search(black_box(PROBE), black_box(&params))
                        .await
                        .expect("fts search");
                    black_box(res.len())
                })
            },
            BatchSize::SmallInput,
        );
    });
}

/// `search/trgm_arms` — the full trigram path (prefix + contains +
/// similarity) for a ≥12-char query.
fn trgm_arms(c: &mut Criterion, ctx: &common::Ctx) {
    let params = SearchParams {
        mode: Some("trgm"),
        ..Default::default()
    };
    c.bench_function("search/trgm_arms", |b| {
        b.iter_batched(
            || ctx.engine.clone(),
            |engine| {
                RUNTIME.block_on(async {
                    let res = engine
                        .search(black_box(PROBE), black_box(&params))
                        .await
                        .expect("trgm search");
                    black_box(res.len())
                })
            },
            BatchSize::SmallInput,
        );
    });
}

/// `search/trgm_prefix_short` — the short-query trgm path (2 chars):
/// PERF-2 skips the contains/similarity arms, leaving the btree-backed
/// prefix arm.
fn trgm_prefix_short(c: &mut Criterion, ctx: &common::Ctx) {
    let params = SearchParams {
        mode: Some("trgm"),
        ..Default::default()
    };
    c.bench_function("search/trgm_prefix_short", |b| {
        b.iter_batched(
            || ctx.engine.clone(),
            |engine| {
                RUNTIME.block_on(async {
                    let res = engine
                        .search(black_box("gr"), black_box(&params))
                        .await
                        .expect("trgm prefix search");
                    black_box(res.len())
                })
            },
            BatchSize::SmallInput,
        );
    });
}

/// One `embed_dim`-dim one-hot vector as the `vector_sql` literal
/// (`$1::vector` — the same `format_vector` output shape the search path
/// binds).
fn one_hot_literal(dim: i32, at: usize) -> String {
    let mut v = vec![0f32; dim as usize];
    v[at] = 1.0;
    format_vector(&v)
}

/// `search/vector_ann` — the production vector arm in isolation:
/// `vector_sql` (the exact hybrid-branch arm SQL, bound the same way
/// `run` binds it) executed on its own pooled connection via
/// `run_sql_on`. limit 20 = the production fetch for a default-limit
/// unfiltered request (`2 × 10`); weight 0.5 = the dev-DB default
/// `search.vector_weight`. The probe is one-hot at coordinate 2 — the
/// fixture's 3rd embedding batch — so the HNSW seek has ~1800
/// distance-0 rows to resolve.
fn vector_ann(c: &mut Criterion, ctx: &common::Ctx) {
    // Coordinate 2 = the 3rd fixture embedding batch (one-hot per 2000-row
    // batch, 0-based) — falls back to the last coordinate if the live
    // column ever has fewer than 3 dims. dim is read from the live column
    // in setup.
    let at = if ctx.embed_dim >= 3 {
        2
    } else {
        (ctx.embed_dim - 1) as usize
    };
    let probe = one_hot_literal(ctx.embed_dim, at);
    let sq = vector_sql(&probe, None, None, 20, 0.5);
    let degradation = DegradationTracker::default();
    c.bench_function("search/vector_ann", |b| {
        b.iter(|| {
            RUNTIME.block_on(async {
                let mut client = acquire_timed(&ctx.pool, "bench_vector_ann")
                    .await
                    .expect("pool checkout");
                let rows = run_sql_on(
                    &mut *client,
                    black_box(&sq),
                    "bench vector arm",
                    &degradation,
                    "vector_ann_bench",
                )
                .await;
                black_box(rows.len())
            })
        });
    });
}

/// `search/suggest` — the pure-trgm `/suggest` path (3 arms on one
/// connection).
fn suggest(c: &mut Criterion, ctx: &common::Ctx) {
    c.bench_function("search/suggest", |b| {
        b.iter_batched(
            || ctx.engine.clone(),
            |engine| {
                RUNTIME.block_on(async {
                    let res = engine
                        .suggest(black_box("quix"), black_box(None), black_box(None))
                        .await
                        .expect("suggest");
                    black_box(res.len())
                })
            },
            BatchSize::SmallInput,
        );
    });
}

/// Bencher-side smoke check (outside the measurement): every arm must
/// actually return rows against the fixture — a 0-row arm would make the
/// bench measure a no-op. Prints the observed row counts.
fn smoke_check(ctx: &common::Ctx) {
    let counts: Vec<(String, usize)> = RUNTIME.block_on(async {
        let fts = SearchParams {
            mode: Some("fts"),
            ..Default::default()
        };
        let trgm = SearchParams {
            mode: Some("trgm"),
            ..Default::default()
        };
        let hybrid = ctx
            .engine
            .search(PROBE, &SearchParams::default())
            .await
            .expect("smoke hybrid")
            .len();
        let fts = ctx
            .engine
            .search(PROBE, &fts)
            .await
            .expect("smoke fts")
            .len();
        let trgm = ctx
            .engine
            .search(PROBE, &trgm)
            .await
            .expect("smoke trgm")
            .len();
        let suggest = ctx
            .engine
            .suggest("quix", None, None)
            .await
            .expect("smoke suggest")
            .len();
        vec![
            ("hybrid".to_string(), hybrid),
            ("fts_only".to_string(), fts),
            ("trgm_arms".to_string(), trgm),
            ("suggest".to_string(), suggest),
        ]
    });
    for (name, n) in &counts {
        println!("smoke: {name} returned {n} rows");
    }
}

fn main() -> ExitCode {
    // 1. Dev-DB check + fixture seed (skip cleanly when unreachable).
    let ctx = match RUNTIME.block_on(setup()) {
        Ok(ctx) => ctx,
        Err(SetupError::NoDb(why)) => {
            eprintln!("BENCH SKIPPED (search): no reachable Postgres — {why}");
            eprintln!("Run with DATABASE_URL set to a reachable dev DB (make bench).");
            return ExitCode::SUCCESS;
        }
        Err(SetupError::Seed(why)) => {
            eprintln!("BENCH SETUP FAILED (search): {why}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "fixture: {} seeded ({} rows, embed_dim={})",
        common::FIXTURE_ZIM,
        common::ROWS,
        ctx.embed_dim
    );

    // 2. Smoke check (unmeasured) — every arm must return rows.
    smoke_check(&ctx);

    // 3. The criterion group (CLI: `cargo bench --bench search -- <args>`
    //    pass-through, e.g. `--save-baseline` / `--test`).
    let mut criterion = Criterion::default().configure_from_args();
    hybrid_search(&mut criterion, &ctx);
    fts_only(&mut criterion, &ctx);
    trgm_arms(&mut criterion, &ctx);
    trgm_prefix_short(&mut criterion, &ctx);
    vector_ann(&mut criterion, &ctx);
    suggest(&mut criterion, &ctx);
    criterion.final_summary();

    // 4. Fixture cleanup (cheap; the next run re-seeds regardless).
    match RUNTIME.block_on(cleanup(&ctx.pool)) {
        Ok(()) => println!("cleanup: {z} removed", z = common::FIXTURE_ZIM),
        Err(e) => eprintln!(
            "cleanup: {z} left behind (dropped by the next run): {e}",
            z = common::FIXTURE_ZIM
        ),
    }
    ExitCode::SUCCESS
}
