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
//! The corpus mirrors `tests/integration/trgm_plan.rs` (the same 40-word
//! grid; 10,000 rows instead of 100,000 so a bench run stays in minutes):
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
use zimservice::torrent::QbitClientCache;
use zimservice::zim::index::UPSERT_ARTICLES_FROM_STAGING_SQL;
use zimservice::zim::ZimManager;
use zimservice::AppState;

/// The fixture ZIM name — the benches' only standing DB footprint (besides
/// the `tiny` row [`build_state`] upserts for the committed
/// `tests/fixtures/tiny.zim`).
pub const FIXTURE_ZIM: &str = "bench_fixture";

/// Article count in the fixture. 10k keeps a full bench run in minutes;
/// `tests/integration/trgm_plan.rs` proves the same corpus shape at 100k.
pub const ROWS: usize = 10_000;

/// 40 varied words; `quixotic` + `granite` are the probe phrase (kept in
/// lockstep with `tests/integration/trgm_plan.rs`, so the benches and the
/// plan gate measure the same title shapes).
pub const WORDS: [&str; 40] = [
    "amber", "boulder", "canyon", "dune", "ember", "fjord", "glacier", "granite", "heath", "islet",
    "jungle", "lichen", "meadow", "niche", "oasis", "plateau", "quarry", "quixotic", "ridge",
    "shoal", "tundra", "upland", "valley", "wadi", "xylem", "yarrow", "zephyr", "basalt", "cinder",
    "delta", "estuary", "fissure", "gully", "habitat", "inlet", "lagoon", "moraine", "nexus",
    "outcrop", "pinnacle",
];

/// The probe phrase (FTS/trgm benches): long enough for both trgm arms
/// (≥ 3 chars), selective enough that the trgm index is overwhelmingly
/// cheaper than a seq scan — `quixotic granite` occurs in exactly 7 of the
/// 10,000 fixture titles.
pub const PROBE: &str = "quixotic granite";

/// The committed one-article ZIM the retrieval benches serve (its
/// `main.html` is 207 bytes — the /w range bench slices against that).
pub const TINY_ZIM: &str = "tiny";

/// Absolute path of the committed fixture archive (the zims row's
/// `file_path`; `open_zim` stats/opens it directly).
pub const TINY_ZIM_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tiny.zim");

/// One embedding batch: `EMBED_BATCH` rows share one one-hot vector bind
/// (mirrors `trgm_plan.rs` — 10k rows → 5 statements instead of 10k
/// per-row updates).
const EMBED_BATCH: usize = 2000;

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

/// Read the live `articles.embedding` column dimension from the catalog.
/// `atttypmod` is used as-is (AGENTS.md: no `vector_dims()` translation —
/// the benches must bind what the column actually stores). A plain
/// `vector` column (no dimension, `atttypmod = -1`) is a hard setup
/// failure — the one-hot seeding has nothing to bind against.
async fn probe_embed_dim(pool: &Pool) -> Result<i32, String> {
    let dim: i32 = raw::fetch_scalar_optional::<i32, _, _>(
        pool,
        "SELECT atttypmod FROM pg_attribute \
         WHERE attrelid = 'articles'::regclass AND attname = 'embedding'",
        |q| q,
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "articles.embedding column missing".to_string())?;
    if dim <= 0 {
        return Err(format!(
            "articles.embedding has no fixed dimension (atttypmod={dim}) — \
             one-hot seeding needs a vector(N) column"
        ));
    }
    Ok(dim)
}

/// Seed (idempotently) the `bench_fixture` ZIM, 10k deterministic articles,
/// and batch one-hot embeddings. Returns the fixture zims id and the live
/// embedding dimension.
async fn seed_fixture(pool: &Pool) -> Result<(i32, i32), String> {
    // Idempotent drop (the cascade covers the previous fixture's articles).
    raw::execute(pool, "DELETE FROM zims WHERE name = $1", |q| {
        q.bind(FIXTURE_ZIM)
    })
    .await
    .map_err(|e| e.to_string())?;

    // Fresh zims row. `file_path` points nowhere: search never opens it,
    // and the retrieval benches open only `tiny`.
    let zim_id: i32 = raw::fetch_scalar_optional(
        pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime, \
                 index_status, indexed_entries, article_count) \
         VALUES ($1, $2, $1, 0, now(), 'ready', $3, $3) RETURNING id",
        |q| q.bind(FIXTURE_ZIM).bind("Bench Fixture").bind(ROWS as i64),
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "fixture zims INSERT returned no id".to_string())?;

    // Bulk-load ROWS articles: COPY into `articles_staging` (one ~3 MB
    // payload, not 10k INSERTs) through the production staging path, then
    // the production upsert (`search_vector` built in Postgres by the exact
    // production tsvector expression).
    {
        let mut client = pool.acquire().await.map_err(|e| e.to_string())?;
        let mut copy = client
            .copy_in_raw(
                "COPY articles_staging (path, title, content_preview, snippet, language, \
                 namespace, zim_id) FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut buf = String::with_capacity(ROWS * 320);
        for i in 0..ROWS {
            let w1 = WORDS[i % 40];
            let w2 = WORDS[(i / 40) % 40];
            let w3 = WORDS[(i / 1600) % 40];
            let path = format!("A/bench_{i:05}");
            let title = format!("{w1} {w2} {w3} entry {i}");
            // ~300 chars of deterministic body text (weight-B in the
            // production tsvector expression; the probe words repeat so FTS
            // ranking has real signal).
            let preview = format!(
                "{w1} {w2} {w3} entry {i} body text. The {w2} beside the {w3} repeats for \
                 full-text matching and ranking. "
            );
            let snippet = format!("Snippet of {w1} {w2} {w3} entry {i}.");
            copy_escape(&mut buf, &path);
            buf.push('\t');
            copy_escape(&mut buf, &title);
            buf.push('\t');
            copy_escape(&mut buf, &preview);
            buf.push('\t');
            copy_escape(&mut buf, &snippet);
            buf.push_str("\ten\tC\t");
            buf.push_str(&zim_id.to_string());
            buf.push('\n');
        }
        copy.send(buf.as_bytes()).await.map_err(|e| e.to_string())?;
        copy.finish().await.map_err(|e| e.to_string())?;
        // Drop the client so the COPY connection returns to the pool before
        // the upsert takes its own checkout.
        drop(client);
    }

    raw::execute(pool, UPSERT_ARTICLES_FROM_STAGING_SQL, |q| q.bind(zim_id))
        .await
        .map_err(|e| e.to_string())?;
    raw::execute(
        pool,
        "DELETE FROM articles_staging WHERE zim_id = $1",
        |q| q.bind(zim_id),
    )
    .await
    .map_err(|e| e.to_string())?;

    // Deterministic embeddings: each EMBED_BATCH-row batch shares one
    // one-hot vector at the batch's index (0..4) — the ANN probe
    // (one-hot at coordinate 2) then has ~1800 distance-0 rows and the
    // HNSW index real rows to seek. Every 10th row (global `id % 10 = 0`)
    // is left NULL: the embed-claim bench's mid-embed tail (~1000 rows).
    let dim = probe_embed_dim(pool).await?;
    let (min_id, max_id): (i64, i64) = raw::fetch_optional::<(i64, i64), _, _>(
        pool,
        "SELECT min(id), max(id) FROM articles WHERE zim_id = $1",
        |q| q.bind(zim_id),
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "no fixture articles after upsert".to_string())?;

    let mut v = vec!["0"; dim as usize];
    for (batch, start) in (min_id..=max_id).step_by(EMBED_BATCH).enumerate() {
        let end = (start + EMBED_BATCH as i64 - 1).min(max_id);
        v.fill("0");
        v[batch % dim as usize] = "1";
        // RAW-OK: bench-only fixture seeding — dynamic dimension +
        // per-batch one-hot literal; no production equivalent.
        raw::execute(
            pool,
            "UPDATE articles SET embedding = $1::vector WHERE zim_id = $2 \
              AND id BETWEEN $3 AND $4 AND id % 10 <> 0",
            |q| {
                q.bind(format!("[{}]", v.join(",")))
                    .bind(zim_id)
                    .bind(start)
                    .bind(end)
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    }

    // Fresh planner statistics for the new 10k-row corpus (the shared dev
    // DB otherwise plans against whatever the last test run left).
    raw::execute(pool, "ANALYZE articles", |q| q)
        .await
        .map_err(|e| e.to_string())?;

    Ok((zim_id, dim))
}

/// Append `s` to `buf`, escaping backslashes/tabs/newlines/CRs for the
/// COPY text format (the fixture text never contains them, but the escape
/// keeps the payload honest if the corpus shape changes).
fn copy_escape(buf: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '\\' => buf.push_str("\\\\"),
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            c => buf.push(c),
        }
    }
}
