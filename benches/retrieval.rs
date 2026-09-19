//! Criterion perf micro-bench of the article RETRIEVAL paths (ZIM entry
//! reads + Postgres article fetches), driven against the shared dev DB at
//! `DATABASE_URL`.
//!
//! # What each bench measures
//!
//! - `retrieval/raw_full` — `GET /w/{zim}/{path}` (the `raw_content`
//!   handler) with no Range header: ZIM handle lookup, entry resolve,
//!   mmap→Vec copy of `main.html` (207 bytes) off the async worker,
//!   full-body response.
//! - `retrieval/raw_range` — the same handler with `Range: bytes=0-10`:
//!   the satisfiable-slice path (window copy + `Content-Range` 206).
//! - `retrieval/raw_304` — the same handler with `If-None-Match` set to
//!   the file-level ETag: revalidation that returns `NotModified` before
//!   any entry lookup / mmap copy (PERF-7).
//! - `retrieval/read_article` — `GET /read` (the `read_article` handler):
//!   ZIM text extraction (deformat) of the article, truncated to the
//!   default 8000 chars, JSON response.
//! - `retrieval/snippet` — the db-layer `fetch_article_snippet`
//!   (btree on `(zim_id, path)`): title + snippet + 600-char preview for
//!   a fixture article — the cheapest search-result click-through.
//! - `retrieval/random` — the `GET /random` handler
//!   (`fetch_random_article`): bounds probe + random-id seek scoped to the
//!   fixture ZIM.
//! - `retrieval/embed_claim` — `CLAIM_EMBED_BATCH_SQL` (the
//!   auto-embed loop's claim): the partial-index `UPDATE … LIMIT 64`
//!   against the fixture's ~1000-row mid-embed tail. A per-sample
//!   (unmeasured) reset re-NULLs `embed_at` so every sample claims the
//!   same first-64-row workload.
//!
//! # Fixture
//!
//! `benches/common::setup()` seeds the shared dev DB with the dedicated
//! `bench_fixture` ZIM (10,000 deterministic articles), and
//! `build_state()` upserts the committed one-article `tests/fixtures/tiny.zim`
//! (served by the /w and /read benches). The fixture is dropped on exit;
//! with no reachable DB the bench prints a skip banner and exits 0
//! (see `benches/common`).

// LINT-3: bench harness — expect/unwrap are the loud-failure idiom here
// (same grandfathering as the test modules); see benches/common.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::hint::black_box;
use std::process::ExitCode;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap};
use criterion::{BatchSize, Criterion};
use zimservice::db::articles::fetch_article_snippet;
use zimservice::db::pool::Pool;
use zimservice::db::random_article::fetch_random_article;
use zimservice::db::raw;
use zimservice::embed::CLAIM_EMBED_BATCH_SQL;
use zimservice::serve::handlers::{
    random_article, raw_content, read_article, RandomQuery, ReadQuery,
};
use zimservice::AppState;

use common::{build_state, cleanup, setup, SetupError, FIXTURE_ZIM, RUNTIME, TINY_ZIM};

/// The committed fixture's only article (207 bytes — the /w ranges and the
/// 304 revalidation run against it).
const TINY_PATH: &str = "main.html";

/// Body-drain limit for the raw-content benches (the fixture entry is 207
/// bytes; 64 KiB leaves headroom for a grown fixture).
const DRAIN_CAP: usize = 64 * 1024;

/// The retrieval context: the shared [`common::Ctx`] plus a handler-ready
/// [`AppState`] and the `tiny` ETag captured from one primed 200.
struct RetCtx {
    ctx: common::Ctx,
    state: AppState,
    tiny_etag: String,
}

/// Setup for the retrieval benches: seed the fixture, build the
/// [`AppState`], prime the ZIM handle (first `open_zim` parses the central
/// dir — it must not land in the measurement region), and capture the
/// file-level ETag the 304 bench revalidates against.
async fn build_retrieval_ctx() -> Result<RetCtx, SetupError> {
    let ctx = setup().await?;
    let state = build_state(&ctx.pool, &ctx.settings)
        .await
        .map_err(SetupError::Seed)?;

    state
        .zims
        .open_zim(TINY_ZIM)
        .await
        .map_err(|e| SetupError::Seed(format!("open_zim {TINY_ZIM}: {e}")))?;

    // One real 200 to capture the ETag the 304 bench must send back
    // (file-level: mtime+size of the archive on disk).
    let primed = raw_content(
        State(state.clone()),
        HeaderMap::new(),
        Path((TINY_ZIM.to_string(), TINY_PATH.to_string())),
    )
    .await
    .map_err(|e| SetupError::Seed(format!("prime {TINY_ZIM}/{TINY_PATH}: {e}")))?;
    if primed.status() != axum::http::StatusCode::OK {
        return Err(SetupError::Seed(format!(
            "prime {TINY_ZIM}/{TINY_PATH}: status {}",
            primed.status()
        )));
    }
    let tiny_etag = primed
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .ok_or_else(|| "prime response carried no ETag".to_string())
        .map_err(SetupError::Seed)?;

    Ok(RetCtx {
        ctx,
        state,
        tiny_etag,
    })
}

/// Drain an axum `Response` body (the handler responses carry the payload
/// in the body; draining keeps the copy out of the optimizer's reach).
async fn drain(resp: axum::http::Response<axum::body::Body>) -> usize {
    let bytes = axum::body::to_bytes(resp.into_body(), DRAIN_CAP)
        .await
        .expect("drain body");
    bytes.len()
}

/// `retrieval/raw_full` — the no-Range /w path (200 + full body).
fn raw_full(c: &mut Criterion, rc: &RetCtx) {
    c.bench_function("retrieval/raw_full", |b| {
        b.iter(|| {
            RUNTIME.block_on(async {
                let resp = raw_content(
                    State(black_box(rc.state.clone())),
                    black_box(HeaderMap::new()),
                    black_box(Path((TINY_ZIM.to_string(), TINY_PATH.to_string()))),
                )
                .await
                .expect("raw 200");
                if resp.status() != axum::http::StatusCode::OK {
                    panic!("raw_full expected 200, got {}", resp.status());
                }
                black_box(drain(resp).await)
            })
        });
    });
}

/// `retrieval/raw_range` — the satisfiable Range /w path (206 slice).
fn raw_range(c: &mut Criterion, rc: &RetCtx) {
    c.bench_function("retrieval/raw_range", |b| {
        b.iter(|| {
            RUNTIME.block_on(async {
                let mut h = HeaderMap::new();
                h.insert(header::RANGE, "bytes=0-10".parse().expect("range header"));
                let resp = raw_content(
                    State(black_box(rc.state.clone())),
                    black_box(h),
                    black_box(Path((TINY_ZIM.to_string(), TINY_PATH.to_string()))),
                )
                .await
                .expect("raw 206");
                if resp.status() != axum::http::StatusCode::PARTIAL_CONTENT {
                    panic!("raw_range expected 206, got {}", resp.status());
                }
                black_box(drain(resp).await)
            })
        });
    });
}

/// `retrieval/raw_304` — revalidation with the captured ETag: the handler
/// stats the file, sees the tag match, and returns NotModified before any
/// entry lookup / mmap copy.
fn raw_304(c: &mut Criterion, rc: &RetCtx) {
    c.bench_function("retrieval/raw_304", |b| {
        b.iter(|| {
            RUNTIME.block_on(async {
                let mut h = HeaderMap::new();
                h.insert(
                    header::IF_NONE_MATCH,
                    black_box(rc.tiny_etag.as_str())
                        .parse()
                        .expect("etag header"),
                );
                let resp = raw_content(
                    State(black_box(rc.state.clone())),
                    black_box(h),
                    black_box(Path((TINY_ZIM.to_string(), TINY_PATH.to_string()))),
                )
                .await
                .expect("raw 304");
                if resp.status() != axum::http::StatusCode::NOT_MODIFIED {
                    panic!("raw_304 expected 304, got {}", resp.status());
                }
                black_box(drain(resp).await)
            })
        });
    });
}

/// `retrieval/read_article` — the /read handler (ZIM text extraction,
/// default 8000-char truncation).
fn read_article_bench(c: &mut Criterion, rc: &RetCtx) {
    c.bench_function("retrieval/read_article", |b| {
        b.iter(|| {
            RUNTIME.block_on(async {
                let params = ReadQuery {
                    zim: TINY_ZIM.to_string(),
                    path: TINY_PATH.to_string(),
                    max_length: None,
                };
                let body =
                    read_article(State(black_box(rc.state.clone())), black_box(Query(params)))
                        .await
                        .expect("read 200");
                black_box(body)
            })
        });
    });
}

/// `retrieval/snippet` — the db-layer article-snippet fetch (the
/// search-result click-through): btree seek on `(zim_id, path)`.
fn snippet(c: &mut Criterion, rc: &RetCtx) {
    c.bench_function("retrieval/snippet", |b| {
        b.iter(|| {
            RUNTIME.block_on(async {
                let row = fetch_article_snippet(
                    &rc.ctx.pool,
                    black_box(FIXTURE_ZIM),
                    black_box(&rc.ctx.sample_path),
                )
                .await
                .expect("snippet fetch");
                black_box(row.is_some())
            })
        });
    });
}

/// `retrieval/random` — the /random handler (bounds probe + random-id
/// seek, scoped to the fixture ZIM).
fn random(c: &mut Criterion, rc: &RetCtx) {
    c.bench_function("retrieval/random", |b| {
        b.iter(|| {
            RUNTIME.block_on(async {
                let params = RandomQuery {
                    zim: Some(FIXTURE_ZIM.to_string()),
                };
                let body =
                    random_article(State(black_box(rc.state.clone())), black_box(Query(params)))
                        .await
                        .expect("random 200");
                black_box(body)
            })
        });
    });
}

/// Reset the fixture's mid-embed tail (unmeasured setup for
/// `embed_claim`): the claim SQL only re-claims rows whose `embed_at` is
/// ≥ 10 min old, so without a reset each sample would claim a shrinking
/// tail. Re-NULLing `embed_at` makes every sample claim the same
/// first-64-by-id rows.
async fn reset_unembedded(ctx: &common::Ctx) {
    raw::execute(
        &ctx.pool,
        "UPDATE articles SET embed_at = NULL WHERE zim_id = $1 AND embedding IS NULL",
        |q| q.bind(ctx.fixture_zim_id),
    )
    .await
    .expect("claim reset");
}

/// The measured claim: `CLAIM_EMBED_BATCH_SQL` with the production batch
/// size (64). Returns the claimed row count (black-boxed).
async fn claim_batch(pool: &Pool, zim_name: &str) -> usize {
    let rows: Vec<(i64, String)> = raw::fetch_all(pool, CLAIM_EMBED_BATCH_SQL, |q| {
        q.bind(zim_name).bind(64i64)
    })
    .await
    .expect("embed claim");
    rows.len()
}

/// `retrieval/embed_claim` — the auto-embed claim UPDATE (partial btree
/// over the fixture's ~1000 NULL-embedding rows, `LIMIT 64`, RETURNING the
/// batch payloads).
fn embed_claim(c: &mut Criterion, rc: &RetCtx) {
    c.bench_function("retrieval/embed_claim", |b| {
        b.iter_batched(
            || RUNTIME.block_on(reset_unembedded(&rc.ctx)),
            |()| RUNTIME.block_on(claim_batch(&rc.ctx.pool, FIXTURE_ZIM)),
            BatchSize::SmallInput,
        );
    });
}

/// Unmeasured smoke check: every retrieval path must return a row/payload
/// before anything is measured (a 404 arm would make a bench measure a
/// no-op).
fn smoke_check(rc: &RetCtx) {
    RUNTIME.block_on(async {
        let raw_status = raw_content(
            State(rc.state.clone()),
            HeaderMap::new(),
            Path((TINY_ZIM.to_string(), TINY_PATH.to_string())),
        )
        .await
        .expect("smoke raw")
        .status();
        let snippet = fetch_article_snippet(&rc.ctx.pool, FIXTURE_ZIM, &rc.ctx.sample_path)
            .await
            .expect("smoke snippet");
        let random = fetch_random_article(&rc.ctx.pool, Some(FIXTURE_ZIM))
            .await
            .expect("smoke random");
        let claimed: Vec<(i64, String)> =
            raw::fetch_all(&rc.ctx.pool, CLAIM_EMBED_BATCH_SQL, |q| {
                q.bind(FIXTURE_ZIM).bind(64i64)
            })
            .await
            .expect("smoke claim");
        let claimed = claimed.len();
        println!(
            "smoke: raw={raw_status:?} snippet={:?} random id={} claimed={claimed}",
            snippet.is_some(),
            random.id
        );
        assert!(snippet.is_some(), "fixture snippet row missing");
        assert!(
            claimed > 0,
            "no claimable rows — fixture embeddings mis-seeded"
        );
    });
    // The smoke claim just stamped embed_at on 64 rows; the first
    // embed_claim sample's setup resets them, so the measurement is
    // unaffected.
}

fn main() -> ExitCode {
    // 1. Dev-DB check + fixture seed (skip cleanly when unreachable).
    let rc = match RUNTIME.block_on(build_retrieval_ctx()) {
        Ok(rc) => rc,
        Err(SetupError::NoDb(why)) => {
            eprintln!("BENCH SKIPPED (retrieval): no reachable Postgres — {why}");
            eprintln!("Run with DATABASE_URL set to a reachable dev DB (make bench).");
            return ExitCode::SUCCESS;
        }
        Err(SetupError::Seed(why)) => {
            eprintln!("BENCH SETUP FAILED (retrieval): {why}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "fixture: {} seeded ({} rows), {t} primed (etag present)",
        common::FIXTURE_ZIM,
        common::ROWS,
        t = common::TINY_ZIM
    );

    // 2. Smoke check (unmeasured) — every retrieval path must work.
    smoke_check(&rc);

    // 3. The criterion group (CLI pass-through, e.g. `--save-baseline`).
    let mut criterion = Criterion::default().configure_from_args();
    raw_full(&mut criterion, &rc);
    raw_range(&mut criterion, &rc);
    raw_304(&mut criterion, &rc);
    read_article_bench(&mut criterion, &rc);
    snippet(&mut criterion, &rc);
    random(&mut criterion, &rc);
    embed_claim(&mut criterion, &rc);
    criterion.final_summary();

    // 4. Fixture cleanup (cheap; the next run re-seeds regardless). The
    //    `tiny` zims row is left in place — it is the committed fixture's
    //    row (idempotent upsert; the integration tests manage it the same
    //    way).
    match RUNTIME.block_on(cleanup(&rc.ctx.pool)) {
        Ok(()) => println!("cleanup: {z} removed", z = common::FIXTURE_ZIM),
        Err(e) => eprintln!(
            "cleanup: {z} left behind (dropped by the next run): {e}",
            z = common::FIXTURE_ZIM
        ),
    }
    ExitCode::SUCCESS
}
