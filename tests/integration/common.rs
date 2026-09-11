//! Shared integration test harness: pool management, skip logic, fixture
//! seeding, and server boot. All items are `pub` so sibling modules can use
//! them via `use super::common::*;`.

pub use std::collections::HashMap;
pub use std::sync::Arc;
pub use std::time::{Duration, Instant};

pub use zimservice::db::migrate::run_migrations;
pub use zimservice::db::pool::Pool;
pub use zimservice::search::{SearchEngine, SearchParams};
pub use zimservice::serve::ratelimit::RateLimiterHandle;
pub use zimservice::settings::SettingsCache;
pub use zimservice::zim::ZimManager;
pub use zimservice::{AppState, HealthProbes};

pub use tower::ServiceExt;

pub const DEFAULT_URL: &str = "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice";
/// Dedicated fixture ZIM name — cleaned up at the end of the test.
pub const ZIM: &str = "__itest__";
/// Committed fixture ZIMs directory, resolved at compile time to an absolute
/// path so the suite does not depend on the directory cargo was launched from.
pub const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

/// Live counter of DB-gated tests that were skipped (no database available).
/// Read by the `#[dtor::dtor]` exit summary at process end.
pub static SKIPPED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The **single** entry point for every DB-gated test in this suite. It is the
/// only place that builds a pool *and* acquires the cross-process
/// [`zimservice::testing::DbExclusiveGuard`], which serializes the shared-
/// fixture mutation across concurrent `cargo test` processes. **Do not build a
/// pool directly in a test** — that would bypass the guard and reintroduce the
/// fixture-interleaving the `DbExclusiveGuard` exists to prevent
/// (Tests Minor #7; `migrations.rs` is the one exception: a private temp DB,
/// guarded).
pub async fn pool_or_skip() -> Option<(Pool, zimservice::testing::DbExclusiveGuard)> {
    let url_explicit = std::env::var("DATABASE_URL").is_ok();
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.into());
    // sqlx has no `connect_with_timeout`; the 3 s bound is applied with
    // `tokio::time::timeout` around the eager connect instead.
    let pool = match tokio::time::timeout(
        Duration::from_secs(3),
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url),
    )
    .await
    {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => return skip(&url, url_explicit, &e.to_string()),
        Err(_) => return skip(&url, url_explicit, "connect timed out (3s)"),
    };
    match pool.acquire().await {
        Ok(_) => Some((pool, zimservice::testing::DbExclusiveGuard::acquire())),
        Err(e) => skip(&url, url_explicit, &e.to_string()),
    }
}

/// `url_explicit` is true when the operator set `DATABASE_URL` in the
/// environment (as opposed to the compose default). An explicit URL is a
/// signal of intent — "there is a database here" — so an unreachable
/// explicit DB is a hard failure rather than a silent skip (High #3 / Tests
/// Major #1): a plain `cargo test` with a set-but-unreachable `DATABASE_URL`
/// must not report a vacuous green.
pub fn skip(
    url: &str,
    url_explicit: bool,
    why: &str,
) -> Option<(Pool, zimservice::testing::DbExclusiveGuard)> {
    // In strict mode (CI / `make test-strict`) an unreachable DB is a hard
    // failure rather than a silent skip.
    if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
        panic!("ZIMSERVICE_REQUIRE_DB is set but cannot reach {url}: {why}");
    }
    // An explicitly-configured `DATABASE_URL` that is unreachable is also a
    // hard failure: the operator pointed the suite at a specific database,
    // so silently skipping would report a vacuous green (zero DB behavior
    // verified) as a clean pass.
    if url_explicit {
        panic!("DATABASE_URL is set but unreachable: {url}: {why}");
    }
    // One-shot banner (T1): without it, every DB-gated test below early-
    // returns and libtest reports a clean "N passed" — a run where NO database
    // behavior was actually verified (the hole that let the held-connection
    // bug hide behind a "passing" regression test). Printed once so `make
    // test`'s `--nocapture` output isn't buried under one line per test; a
    // passing test otherwise hides this `eprintln` from the harness output.
    static BANNER_PRINTED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    if !BANNER_PRINTED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        eprintln!("\n================ INTEGRATION SUITE SKIPPED ================");
        eprintln!("No reachable Postgres at {url} ({why}).");
        eprintln!("Every DB-gated test below passes vacuously (no-op, ~0 ms).");
        eprintln!("Database behavior was NOT verified in this run — a 'clean pass'");
        eprintln!("here is meaningless for the DB paths. To actually run them:");
        eprintln!("  make test-integration");
        eprintln!("============================================================");
    }
    SKIPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    None
}

/// Record a **mid-test** skip: the test entered via [`pool_or_skip`] (so a
/// live DB was present) but bailed before its real assertions because
/// secondary optional infra failed (an extra pool build/connect, temp-DB
/// create, a missing fixture file). Without this counter such a test would
/// report a clean green pass with 0 skips recorded — the exact hole the
/// `SKIPPED` counter exists to close.
pub fn skip_midtest(why: &str) {
    eprintln!("SKIPPED (mid-test): {why}");
    SKIPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Seed the search fixture (one ZIM, three articles: Alpine / Baltic Sea /
/// Andes) and return a ready `SearchEngine`. Runs `run_migrations` first —
/// a no-op when already applied — so the callers are standalone-order-
/// independent (each smoke test passes on its own).
pub async fn seed_search_fixture(pool: &Pool) -> SearchEngine {
    run_migrations(pool)
        .await
        .expect("migrations (fixture tables must exist)");

    zimservice::db::raw::execute(pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    zimservice::db::raw::execute(
        pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 3, 3)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    {
        // One statement per `raw::execute` (sqlx has no multi-statement
        // protocol call; `split_statements` replaces the old `batch_execute`).
        for stmt in zimservice::db::raw::split_statements(&format!(
            "INSERT INTO articles (zim_id, path, title, content_preview, search_vector) VALUES
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Alpine', 'Alpine',
              'The Alps are mountains.', to_tsvector('simple','alpine peaks europe')),
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Baltic', 'Baltic Sea',
              'The Baltic is a sea.', to_tsvector('simple','baltic sea')),
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Andes', 'Andes',
              'The Andes are mountains in South America.',
              to_tsvector('simple','andes mountains south america'))"
        )) {
            zimservice::db::raw::execute(pool, &stmt, |q| q)
                .await
                .unwrap();
        }
    }

    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");
    SearchEngine::new(
        pool.clone(),
        settings,
        zimservice::health::DegradationTracker::default(),
    )
}

// ── B5.6: contention (unique active download + seeding visibility) ──────

/// Insert a row with the given `status`; returns the new id or the raw error.
pub async fn insert_download(
    pool: &Pool,
    name: &str,
    url: &str,
    status: &str,
) -> std::result::Result<i32, zimservice::error::Error> {
    zimservice::db::raw::fetch_scalar_optional(
        pool,
        "INSERT INTO downloads (name, url, status) VALUES ($1, $2, $3) RETURNING id",
        |q| q.bind(name).bind(url).bind(status),
    )
    .await
    .map(|id| id.expect("insert row present"))
}

// ── B5.7: DB-backed handler happy paths ────────────────────────────────

/// Assemble a full `AppState` from the four parts that vary per test
/// (pool, settings, ZIM manager, search engine). The five remaining
/// infrastructure fields are always fresh defaults — this is the single
/// spelling of that boilerplate in the test suite.
pub fn assemble_state(
    db: Pool,
    settings: SettingsCache,
    zims: Arc<ZimManager>,
    search: SearchEngine,
) -> AppState {
    AppState {
        db,
        settings,
        zims,
        search,
        torrent: zimservice::torrent::QbitClientCache::new(),
        rate_limiter: Arc::new(RateLimiterHandle::new()),
        probes: HealthProbes::default(),
        auth_lockout: Arc::new(Default::default()),
        degradation: zimservice::health::DegradationTracker::default(),
        build_probe: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        index_building: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }
}

/// Build a live `AppState` the way `mcp_tools_db_backed` does.
pub async fn live_state(pool: Pool) -> AppState {
    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");
    let zims = ZimManager::new(
        std::path::PathBuf::from("/nonexistent-handler"),
        pool.clone(),
    );
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    assemble_state(pool.clone(), settings, zims, search)
}

/// Like `live_state`, but the `ZimManager` points at `tests/fixtures` and is
/// `resync()`-ed, so the in-memory ZIM handle cache holds the committed
/// `tiny.zim` (needed by handlers that open the archive directly, e.g.
/// `/chunks`). The `tiny` zims row is upserted by `resync` itself.
pub async fn fixture_state(pool: Pool) -> AppState {
    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");
    let zims = ZimManager::new(std::path::PathBuf::from(FIXTURES_DIR), pool.clone());
    zims.resync().await.expect("resync");
    assert!(
        zims.get("tiny").is_some(),
        "resync must cache the committed fixture"
    );
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    assemble_state(pool.clone(), settings, zims, search)
}

// ── TEST-6: real-listener tests (real TCP ⇒ real ConnectInfo) ─────────────

/// Boot `app` on an ephemeral loopback port. Real TCP ⇒ real
/// `ConnectInfo` (the per-IP lockout needs it; oneshot tests can't carry it).
/// Returns (base URL, graceful-shutdown trigger). Port 0 ⇒ the OS assigns a
/// unique ephemeral port per listener, so even parallel tests never collide.
pub async fn boot_server(app: axum::Router) -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().unwrap();
    let (trigger, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = rx.await;
        })
        .await;
    });
    (format!("http://{addr}"), trigger)
}

/// Print a summary of DB-gated test skips at process exit. Driven by the
/// live `SKIPPED` counter (no hardcoded count) so the message is always
/// accurate regardless of how many tests were added.
#[dtor::dtor]
pub fn report_db_skips() {
    let n = SKIPPED.load(std::sync::atomic::Ordering::Relaxed);
    if n > 0 {
        eprintln!("\n=============== INTEGRATION SUITE SUMMARY ===============");
        eprintln!("{n} DB-gated test(s) SKIPPED (no database) — this process");
        eprintln!("verified NO database behavior; its 'clean pass' is vacuous.");
        eprintln!("To actually run them: make test-integration");
        eprintln!("===========================================================");
    }
}
