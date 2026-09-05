//! Shared integration test harness: pool management, skip logic, fixture
//! seeding, and server boot. All items are `pub` so sibling modules can use
//! them via `use super::common::*;`.

pub use std::collections::HashMap;
pub use std::sync::Arc;
pub use std::time::{Duration, Instant};

pub use deadpool_postgres::Config as DpConfig;

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
/// fixture-interleaving the single-threaded `--test-threads=1` gate exists to
/// prevent (Tests Minor #7).
pub async fn pool_or_skip() -> Option<(Pool, zimservice::testing::DbExclusiveGuard)> {
    let url_explicit = std::env::var("DATABASE_URL").is_ok();
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.into());
    let mut cfg = DpConfig::new();
    cfg.url = Some(url.clone());
    let pool = match cfg.builder(tokio_postgres::NoTls) {
        Ok(b) => b,
        Err(e) => return skip(&url, url_explicit, &e.to_string()),
    };
    let pool = match pool.max_size(4).build() {
        Ok(p) => p,
        Err(e) => return skip(&url, url_explicit, &e.to_string()),
    };
    match tokio::time::timeout(Duration::from_secs(3), pool.get()).await {
        Ok(Ok(_)) => Some((pool, zimservice::testing::DbExclusiveGuard::acquire())),
        Ok(Err(e)) => skip(&url, url_explicit, &e.to_string()),
        Err(_) => skip(&url, url_explicit, "timed out connecting"),
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

    {
        let c = pool.get().await.unwrap();
        c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
            .await
            .unwrap();
        c.execute(
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                               index_status, indexed_entries, article_count)
             VALUES ($1, $1, $1, 0, now(), 'ready', 3, 3)",
            &[&ZIM],
        )
        .await
        .unwrap();
        c.batch_execute(&format!(
            "INSERT INTO articles (zim_id, path, title, content_preview, search_vector) VALUES
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Alpine', 'Alpine',
              'The Alps are mountains.', to_tsvector('simple','alpine peaks europe')),
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Baltic', 'Baltic Sea',
              'The Baltic is a sea.', to_tsvector('simple','baltic sea')),
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Andes', 'Andes',
              'The Andes are mountains in South America.',
              to_tsvector('simple','andes mountains south america'))"
        ))
        .await
        .unwrap();
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

/// C12 (Step 4.2 / DEC-5) — DB-gated perf check. Seeds a dedicated ZIM with
/// 100k articles, then runs `EXPLAIN (ANALYZE, BUFFERS)` against each of the
/// three split trgm queries that `SearchEngine::search` builds (btree prefix,
/// GIN contains, GiST similarity) plus a pure `ORDER BY title_lower` shape
/// (Q5, WI-36 PERF-4 keep/drop decision), and asserts **none** of the
/// regression-gated shapes falls back to a `Seq Scan` over `articles`.
/// Prints the plans for `docs/perf-notes.md`.
/// Runs entirely in a dedicated temporary database (dropped afterwards) —
/// the shared dev DB is never seeded or `ANALYZE`d by this test.
/// Skipped cleanly when the DB is unreachable; run with
/// `--include-ignored` (e.g. `make test-strict-perf`).
/// Name of the perf tests' dedicated temporary database: a fixed prefix
/// plus the current pid, so concurrent or restarted runs never collide and
/// stale-DB cleanup can find leftovers by prefix.
pub fn perf_tmp_db_name() -> String {
    format!("zimservice_it_perf_{}", std::process::id())
}

/// The suite's `DATABASE_URL` (or the compose default) with its path segment
/// replaced by `db_name`.
pub fn with_db_name(db_name: &str) -> String {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.into());
    let mut u = url::Url::parse(&url).expect("DATABASE_URL must parse");
    u.set_path(&format!("/{db_name}"));
    u.to_string()
}

/// Build the perf tests' dedicated temporary database and a pool for it.
///
/// Both perf tests seed bulk rows and run `ANALYZE`, so they must never
/// touch the shared dev DB (Tests Minor #9): everything runs in this temp
/// DB, which [`perf_drop_temp_db`] drops again afterwards. Before creating
/// its own, this also **sweeps** any stale `zimservice_it_perf_*` databases
/// left behind by a killed run (a panic between create and drop would
/// otherwise leak it), so a killed run self-heals on the next start.
///
/// Returns `None` — recording a mid-test skip — when the role cannot
/// `CREATE DATABASE` or the temp pool fails to build/connect.
pub async fn perf_temp_db(shared: &Pool) -> Option<(Pool, String)> {
    let c = shared.get().await.expect("pool conn");
    let can_create: bool = c
        .query_one(
            "SELECT has_database_privilege(current_user, 'template1', 'CREATE')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    if !can_create {
        skip_midtest("role cannot CREATE DATABASE — perf temp DB unavailable");
        return None;
    }
    // Startup sweep: force-drop stale temp DBs from killed runs (any pid).
    // `WITH (FORCE)` (PG 13+) detaches any connections a killed run left.
    let stale: Vec<String> = c
        .query(
            "SELECT datname FROM pg_database
             WHERE datname LIKE 'zimservice_it_perf_%' AND datname <> current_database()",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    for db in &stale {
        let _ = c
            .execute(
                &format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"),
                &[],
            )
            .await;
    }
    let tmp_db = perf_tmp_db_name();
    if c.execute(&format!("CREATE DATABASE \"{tmp_db}\""), &[])
        .await
        .is_err()
    {
        skip_midtest(&format!("perf temp DB create failed: {tmp_db}"));
        return None;
    }
    let mut tc = DpConfig::new();
    tc.url = Some(with_db_name(&tmp_db));
    let tpool = match tc
        .builder(tokio_postgres::NoTls)
        .ok()
        .and_then(|b| b.max_size(2).build().ok())
    {
        Some(p) => p,
        None => {
            let _ = c
                .execute(
                    &format!("DROP DATABASE IF EXISTS \"{tmp_db}\" WITH (FORCE)"),
                    &[],
                )
                .await;
            skip_midtest("perf temp pool build failed");
            return None;
        }
    };
    if tpool.get().await.is_err() {
        drop(tpool);
        let _ = c
            .execute(
                &format!("DROP DATABASE IF EXISTS \"{tmp_db}\" WITH (FORCE)"),
                &[],
            )
            .await;
        skip_midtest("perf temp DB connect failed");
        return None;
    }
    Some((tpool, tmp_db))
}

/// Post-test cleanup for the perf temp DB: drop it (force-detaching any
/// remaining connections). Mirrors the legacy-upgrade test's same-scope drop;
/// if an assertion panics before this runs, the next run's startup sweep in
/// [`perf_temp_db`] removes the leftover.
pub async fn perf_drop_temp_db(shared: &Pool, tmp_db: &str) {
    let c = shared.get().await.expect("pool conn");
    let _ = c
        .execute(
            &format!("DROP DATABASE IF EXISTS \"{tmp_db}\" WITH (FORCE)"),
            &[],
        )
        .await;
}

// ── B5.6: contention (unique active download + seeding visibility) ──────

/// Insert a row with the given `status`; returns the new id or the raw error.
pub async fn insert_download(
    pool: &Pool,
    name: &str,
    url: &str,
    status: &str,
) -> std::result::Result<i32, tokio_postgres::Error> {
    let c = pool.get().await.expect("pool conn");
    c.query_one(
        "INSERT INTO downloads (name, url, status) VALUES ($1, $2, $3) RETURNING id",
        &[&name, &url, &status],
    )
    .await
    .map(|r| r.get::<_, i32>(0))
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
/// Returns (base URL, graceful-shutdown trigger). Port 0 +
/// --test-threads=1 ⇒ sequential tests each own an ephemeral port; no
/// collisions.
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
