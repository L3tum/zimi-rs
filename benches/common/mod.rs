//! Shared fixture + runtime plumbing for the criterion benches
//! (`benches/search.rs`, `benches/retrieval.rs`).
//!
//! # Fixture
//!
//! Both bench targets measure against one dedicated fixture — a ZIM named
//! [`FIXTURE_ZIM`] with 10,000 deterministic articles, seeded into the
//! SHARED dev DB at `DATABASE_URL` (never a temp DB: benches run
//! repeatedly and the fixture IS the measurement subject). Seeding is
//! idempotent — the fixture ZIM row (and its articles, via `ON DELETE
//! CASCADE`) is dropped and recreated at the start of every bench run, so
//! re-runs measure identical data. [`cleanup`] drops the fixture at bench
//! end; a killed run leaves it behind (clearly named, and dropped by the
//! next run).
//!
//! The corpus (word list / probe / batch + the 10k seed) is the shared
//! single definition in `tests/common/corpus10k.rs` — the same 40-word grid
//! as `tests/integration/trgm_plan.rs` (one definition in
//! `zimservice::testing::trgm_corpus`; 10,000 rows instead of 100,000 so a
//! bench run stays in minutes), included textually here because a bench
//! target cannot `mod` tests/ code (see that file's docs):
//! - `path`:   `A/bench_00000` … `A/bench_09999`
//! - `title`:  `"{w1} {w2} {w3} entry {i}"` with `w1 = WORDS[i % 40]`,
//!   `w2 = WORDS[(i / 40) % 40]`, `w3 = WORDS[(i / 1600) % 40]` — the
//!   probe phrase `quixotic granite` occurs in exactly 7 titles
//!   (`i ≡ 297 (mod 1600)`)
//! - `content_preview` / `snippet`: deterministic per-row text
//! - `embedding`: a deterministic 1536-dim one-hot per 2,000-row batch —
//!   the live `articles.embedding` column dimension is read from the
//!   catalog (`atttypmod`, as-is — see AGENTS.md "Reading the vector
//!   column"); every 10th row keeps `embedding = NULL`, a realistic
//!   mid-embed tail for the embed-claim bench
//!
//! The bulk load reuses the PRODUCTION path: COPY into `articles_staging`
//! followed by `zim::index::UPSERT_ARTICLES_FROM_STAGING_SQL`, so
//! `search_vector` is built by the exact production tsvector expression.
//!
//! # Skip policy
//!
//! No reachable DB → the bench prints a skip banner and exits 0 (the same
//! vacuous-run-loud policy as `tests/integration/common.rs`). A reachable
//! DB that cannot be seeded → hard failure (the run would measure nothing
//! real).

// LINT-3: bench harness — panics on setup/seed failure are the desired
// loud-failure behavior (a mis-seeded fixture must never be measured), so
// expect/unwrap are grandfathered for this module, as in the test modules.
// `dead_code`: each bench target compiles this module but reads only the
// fields/items it measures (search reads `engine`/`embed_dim`, retrieval
// reads `sample_path`/`build_state`/`TINY_ZIM*`) — the shared module must
// carry all of them, so per-target unused warnings are expected.
#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

use zimservice::access::lockout::LockoutTracker;
use zimservice::access::ratelimit::RateLimiterHandle;
use zimservice::db::migrate::run_migrations;
use zimservice::db::pool::Pool;
use zimservice::db::raw;
use zimservice::health::{DegradationTracker, HealthProbes};
use zimservice::search::{SearchEngine, SearchParams};
use zimservice::settings::SettingsCache;
// Single definition in zimservice::testing::trgm_corpus — M2, 2026-09 review
// (`pub` so `common::PROBE` stays reachable from benches/search.rs).
pub use zimservice::testing::trgm_corpus::PROBE;
use zimservice::torrent::QbitClientCache;
use zimservice::zim::ZimManager;
use zimservice::AppState;

// The 10k fixture builder is the shared single definition in
// `tests/common/corpus10k.rs` (a bench target cannot `mod` tests/ code —
// the textual `include!` is the seam; see that file's docs). The `pub use`
// keeps `common::FIXTURE_ZIM` / `common::ROWS` reachable from the bench
// targets (search.rs, retrieval.rs) and `seed_fixture` from `setup()`.
mod corpus10k {
    include!("../../tests/common/corpus10k.rs");
}
pub use corpus10k::{seed_fixture, FIXTURE_ZIM, ROWS};

/// The committed one-article ZIM the retrieval benches serve (its
/// `main.html` is 207 bytes — the /w range bench slices against that).
pub const TINY_ZIM: &str = "tiny";

/// Absolute path of the committed fixture archive (the zims row's
/// `file_path`; `open_zim` stats/opens it directly).
pub const TINY_ZIM_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tiny.zim");

/// A single multi-thread runtime driving every measured async call:
/// criterion is synchronous, so each measured future is driven with
/// `block_on`. 4 workers = the bench pool size, so one hybrid search's 4
/// concurrent arms each get a worker.
pub static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("bench tokio runtime")
});

/// Everything the bench functions need: the dev-DB pool, a live settings
/// snapshot, a warmed search engine, and the seeded fixture's identity.
pub struct Ctx {
    /// Pool over `DATABASE_URL` (4 connections — the dev-DB default: enough
    /// for one hybrid search's 4 concurrent arms, bounded so a bench can't
    /// exhaust the shared dev DB).
    pub pool: Pool,
    /// Live settings snapshot (search weights/thresholds come from the DB,
    /// not defaults).
    pub settings: SettingsCache,
    /// Engine sharing `pool` + `settings` (trgm probe warmed in setup).
    pub engine: SearchEngine,
    /// id of the seeded `bench_fixture` zims row.
    pub fixture_zim_id: i32,
    /// The live `articles.embedding` column dimension (`atttypmod`, as-is).
    pub embed_dim: i32,
    /// A seeded article path (the snippet bench's subject).
    pub sample_path: String,
}

/// Setup failure split: [`NoDb`] → SKIP (exit 0, the suite's vacuous-run
/// policy); [`Seed`] → hard failure (the run would measure nothing real).
///
/// [`NoDb`]: SetupError::NoDb
/// [`Seed`]: SetupError::Seed
#[derive(Debug)]
pub enum SetupError {
    /// `DATABASE_URL` (or the loopback fallback) is unreachable — print a
    /// skip banner and exit 0.
    NoDb(String),
    /// The DB is reachable but migrations/settings/seeding failed.
    Seed(String),
}

/// Connect to the dev DB, migrate it, load settings, seed the fixture, and
/// warm the search engine. See the module docs for the skip policy.
pub async fn setup() -> Result<Ctx, SetupError> {
    let url = database_url();
    let pool = connect_pool(&url)
        .await
        .map_err(|e| SetupError::NoDb(format!("{url} unreachable: {e}")))?;

    run_migrations(&pool)
        .await
        .map_err(|e| SetupError::Seed(format!("migrations: {e}")))?;

    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .map_err(|e| SetupError::Seed(format!("settings load: {e}")))?;

    let (zim_id, embed_dim) = seed_fixture(&pool)
        .await
        .map_err(|e| SetupError::Seed(format!("fixture seed: {e}")))?;

    // Warm the trgm probe + pool priming OUTSIDE the measurement region
    // (a first `search()` would pay a catalog probe otherwise). Also the
    // seed's sanity check: a queryable fixture must return the 7
    // probe-title rows.
    let engine = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        DegradationTracker::default(),
    );
    let warm = engine
        .search(PROBE, &SearchParams::default())
        .await
        .map_err(|e| SetupError::Seed(format!("warm-up search: {e}")))?;
    if warm.is_empty() {
        return Err(SetupError::Seed(
            "warm-up search returned no rows — fixture not queryable".into(),
        ));
    }

    Ok(Ctx {
        pool,
        settings,
        engine,
        fixture_zim_id: zim_id,
        embed_dim,
        sample_path: "A/bench_00007".to_string(),
    })
}

/// Drop the fixture (the benches' only standing DB footprint). Cheap: one
/// DELETE whose cascade covers the 10k articles.
pub async fn cleanup(pool: &Pool) -> Result<(), String> {
    raw::execute(pool, "DELETE FROM zims WHERE name = $1", |q| {
        q.bind(FIXTURE_ZIM)
    })
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Build an [`AppState`] over the bench pool for the handler-level
/// retrieval benches. Mirrors the production field-by-field assembly
/// (`testing::state_from_parts` shape), with the `zims` manager loaded from
/// the DB.
///
/// Deliberately `load_from_db()` and NOT `resync()`: a directory scan would
/// `DELETE` zims rows for files absent from the scanned dir — which would
/// wreck both the fixture and any concurrent test fixtures in the shared
/// dev DB. The `tiny` row is upserted explicitly instead (same values the
/// scan would write for `tests/fixtures/tiny.zim`).
pub async fn build_state(pool: &Pool, settings: &SettingsCache) -> Result<AppState, String> {
    // Upsert the committed one-article fixture (idempotent; the retrieval
    // benches open only this ZIM, never the bench_fixture).
    let file_size = std::fs::metadata(TINY_ZIM_PATH)
        .map(|m| m.len())
        .map_err(|e| format!("stat {TINY_ZIM_PATH}: {e}"))?;
    let _tiny_id: i32 = raw::fetch_scalar_optional(
        pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime, \
                 index_status, indexed_entries, article_count) \
         VALUES ($1, $2, $3, $4, now(), 'ready', 16, 1) \
         ON CONFLICT (name) DO UPDATE SET \
             file_path = EXCLUDED.file_path, \
             file_size = EXCLUDED.file_size, \
             file_mtime = EXCLUDED.file_mtime \
         RETURNING id",
        |q| {
            q.bind(TINY_ZIM)
                .bind(TINY_ZIM)
                .bind(TINY_ZIM_PATH)
                .bind(file_size as i64)
        },
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "tiny upsert returned no id".to_string())?;

    let zims = ZimManager::new(PathBuf::from("/nonexistent-zims-bench"), pool.clone());
    zims.load_from_db()
        .await
        .map_err(|e| format!("zims load_from_db: {e}"))?;

    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        DegradationTracker::default(),
    );
    Ok(AppState {
        // Benches run against a single live pool: no read replica, and the
        // dedicated background pool is the same pool (no background work is
        // spawned from a bench state).
        db_read: None,
        db_bg: pool.clone(),
        db: pool.clone(),
        settings: settings.clone(),
        zims,
        search,
        torrent: QbitClientCache::new(),
        rate_limiter: std::sync::Arc::new(RateLimiterHandle::new()),
        probes: HealthProbes::default(),
        auth_lockout: std::sync::Arc::new(LockoutTracker::default()),
        degradation: DegradationTracker::default(),
        build_probe: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        index_building: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        // Benches never pull `/diagnostic` and never run the auto-embed
        // loop, so the snapshot stays at its "never probed" default
        // (`at_unix: 0`) — same contract as `startup::build_state`.
        vector_index_snapshot: std::sync::Arc::new(std::sync::Mutex::new(
            zimservice::embed::VectorIndexSnapshot {
                embedded_rows: 0,
                index: zimservice::embed::VectorIndexState::Absent,
                at_unix: 0,
            },
        )),
        // No cross-process invalidation listener for the bench: it only
        // matters for long-lived servers, and None is the no-DB path's
        // shape (see AppState::notify). The bench reads settings once at
        // setup, so invalidation can't skew a measurement.
        notify: None,
    })
}

/// The dev-DB DSN: `DATABASE_URL` when set, else the loopback dev DSN
/// (the same fallback `tests/integration/common.rs` uses).
fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into())
}

/// A bounded 4-connection pool (see [`Ctx::pool`]).
async fn connect_pool(url: &str) -> Result<Pool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(4)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await
}
