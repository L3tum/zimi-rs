//! Migration subsystem tests: fresh apply, idempotent re-run, hash-drift
//! detection, and legacy-schema refusal.
//!
//! Each test runs in a **dedicated temporary database** created on the same
//! server as the shared dev DB and dropped afterwards, so the shared
//! `schema_migrations` (and its recorded hashes) are never touched — the
//! reason the suite is safe under the default parallel `--test-threads`
//! (see `main.rs`). The temp DB also provides a genuinely fresh schema, so
//! the fresh-apply path is exercised here (every other test only ever hits
//! the "already applied" no-op path on the shared dev DB).
//!
//! The temp-DB pool is the one sanctioned exception to "don't build a pool
//! directly in a test": it targets a *private* database, and the test has
//! already acquired the `DbExclusiveGuard` via `pool_or_skip`, so no
//! shared-fixture interleaving is possible.
//!
//! Requires the dev-DB user to have `CREATE DATABASE` (true for the
//! compose / CI `zimservice` superuser). When it does not, the tests record
//! a mid-test skip rather than failing.

use super::common::*;

/// Unique temp-database name for this test run (pid + nanosecond clock).
/// Leftovers from a crashed run are harmless: the next `create_temp_db`
/// sweeps them (`sweep_stale_temp_dbs`) when the embedded pid is no longer
/// alive; names the sweep can't parse (or whose pid is still live) are
/// left for manual cleanup (`DROP DATABASE "zimservice_itest_mig_..."`).
fn temp_db_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    format!(
        "zimservice_itest_mig_{pid}_{nanos}",
        pid = std::process::id()
    )
}

/// The base URL the shared pool connected with (same source of truth as
/// `common::pool_or_skip`).
fn base_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.into())
}

/// Best-effort drop. Postgres refuses to drop a database with live
/// connections, so the temp pool must be `close()`-d by the caller first.
/// Never fails: a leftover temp DB is harmless (see `temp_db_name`).
async fn drop_db(base_pool: &Pool, name: &str) {
    let _ =
        zimservice::db::raw::execute(base_pool, &format!("DROP DATABASE IF EXISTS {name}"), |q| q)
            .await;
}

/// Create a temp database on the shared server and build a pool for it.
/// Returns `None` (after recording a mid-test skip) when the server user
/// cannot `CREATE DATABASE` or the fresh database is unreachable.
///
/// Before creating the new database, best-effort sweeps stale temp DBs
/// left by crashed/killed runs (see [`sweep_stale_temp_dbs`]) so a
/// long-lived dev server doesn't accumulate them.
///
/// `pub` for sibling temp-DB tests (e.g. `trgm_plan.rs`): the pattern is
/// deliberate — a private database per test keeps the shared dev schema
/// untouched, and the caller already holds the `DbExclusiveGuard`.
pub async fn create_temp_db(base_pool: &Pool) -> Option<(Pool, String)> {
    sweep_stale_temp_dbs(base_pool).await;
    let name = temp_db_name();
    // CREATE/DROP DATABASE cannot run inside a transaction; the plain pooled
    // statement is exactly that (sqlx opens no implicit BEGIN).
    if let Err(e) =
        zimservice::db::raw::execute(base_pool, &format!("CREATE DATABASE {name}"), |q| q).await
    {
        drop_db(base_pool, &name).await;
        skip_midtest(&format!(
            "CREATE DATABASE {name} failed (user lacks CREATEDB?): {e}"
        ));
        return None;
    }
    let mut url = url::Url::parse(&base_url()).expect("valid base URL");
    url.set_path(&format!("/{name}"));
    let pool = match tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(url.as_str()),
    )
    .await
    {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            drop_db(base_pool, &name).await;
            skip_midtest(&format!("temp DB {name} unreachable: {e}"));
            return None;
        }
        Err(_) => {
            drop_db(base_pool, &name).await;
            skip_midtest(&format!("temp DB {name} connect timed out (5s)"));
            return None;
        }
    };
    Some((pool, name))
}

/// Best-effort sweep of temp databases left behind by crashed/killed test
/// runs. Names follow the `temp_db_name` pattern
/// `zimservice_itest_mig_{pid}_{nanos}`: the embedded pid is liveness-
/// probed with `kill -0`, and a database whose pid is no longer a live
/// process is dropped. Safety rules:
/// - **never fails**: a probe or drop error is `tracing::warn!`-ed and the
///   sweep continues — a leftover database must never block test setup;
/// - **conservative**: unparseable names (wrong shape, non-numeric pid,
///   pid 0) are SKIPPED, never dropped; a live pid (including a reused one
///   or an EPERM from another user) is kept.
///
/// Runs on the BASE pool (the catalog query is server-wide; temp DB names
/// are global to the server, not to a database).
async fn sweep_stale_temp_dbs(base_pool: &Pool) {
    const PREFIX: &str = "zimservice_itest_mig_";
    let names: Vec<(String,)> = match zimservice::db::raw::fetch_all(
        base_pool,
        "SELECT datname FROM pg_database WHERE datname LIKE 'zimservice_itest_mig\\_%' ESCAPE '\\'",
        |q| q,
    )
    .await
    {
        Ok(names) => names,
        Err(e) => {
            tracing::warn!("stale temp-DB sweep: pg_database probe failed: {e}");
            return;
        }
    };
    for (name,) in names {
        // Exactly `{pid}_{nanos}` after the prefix; anything else is
        // unparseable → skip (not confirmed stale).
        let rest = match name.strip_prefix(PREFIX) {
            Some(rest) => rest,
            None => continue,
        };
        let mut parts = rest.split('_');
        let (Some(pid_s), Some(_nanos)) = (parts.next(), parts.next()) else {
            continue;
        };
        if parts.next().is_some() {
            continue; // more than two fields — unparseable
        }
        let Ok(pid) = pid_s.parse::<u32>() else {
            continue;
        };
        if pid == 0 {
            continue; // `kill -0 0` targets the process group — never a pid
        }
        // `kill -0` succeeds iff the pid is a live process we may signal.
        // A live foreign-user pid answers EPERM (reads as "not alive"); in
        // this environment the suite only ever runs as the dev-box/CI user,
        // so the sweep's conservative failure mode is keeping a DB, not
        // dropping one.
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if alive {
            continue;
        }
        tracing::warn!("stale temp-DB sweep: dropping {name} (pid {pid} not alive)");
        let _ = zimservice::db::raw::execute(
            base_pool,
            &format!("DROP DATABASE IF EXISTS {name}"),
            |q| q,
        )
        .await;
    }
}

/// Close `pool` (releasing its connections), then drop the database
/// best-effort on the shared server. `pub` for sibling temp-DB tests (see
/// [`create_temp_db`]).
pub async fn close_and_drop(base_pool: &Pool, pool: &Pool, name: &str) {
    pool.close().await;
    drop_db(base_pool, name).await;
}

/// Fresh-apply → idempotent re-run → hash-tamper drift error, all in a
/// dedicated temp database (dropped afterwards) so the shared dev DB's
/// `schema_migrations` is never tampered.
#[tokio::test]
async fn smoke_migration_drift_detection() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };

    // (1) Fresh apply: every migration runs on an empty database.
    run_migrations(&pool).await.expect("fresh apply");
    let applied: i64 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT count(*) FROM schema_migrations",
        |q| q,
    )
    .await
    .expect("count applied migrations")
    .expect("schema_migrations row");
    // Derives from `MIGRATIONS` in src/db/migrate.rs — no manual lockstep.
    assert_eq!(
        applied,
        zimservice::db::migrate::MIGRATION_COUNT as i64,
        "all migrations must be recorded"
    );
    // Spot-check artifacts from the first and the latest migrations so a
    // silent early failure (e.g. only 001-004 applied) can't pass.
    for regclass in [
        "zims",
        "articles",
        "downloads",
        "articles_staging",
        "idx_downloads_status_updated",
    ] {
        let exists: bool = zimservice::db::raw::fetch_scalar_optional(
            &pool,
            &format!("SELECT to_regclass('{regclass}') IS NOT NULL"),
            |q| q,
        )
        .await
        .expect("to_regclass")
        .expect("regclass row");
        assert!(exists, "fresh apply must create {regclass}");
    }

    // (2) Idempotent re-run: no error, nothing re-applied or duplicated.
    run_migrations(&pool)
        .await
        .expect("re-run must be idempotent");
    let still: i64 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT count(*) FROM schema_migrations",
        |q| q,
    )
    .await
    .expect("count after re-run")
    .expect("schema_migrations row");
    assert_eq!(
        still,
        zimservice::db::migrate::MIGRATION_COUNT as i64,
        "re-run must not duplicate or drop rows"
    );

    // (3) Drift: tamper a recorded hash → the next run must refuse.
    zimservice::db::raw::execute(
        &pool,
        "UPDATE schema_migrations SET hash = 'tampered' WHERE name = '002_fixes.sql'",
        |q| q,
    )
    .await
    .expect("tamper recorded hash");
    let err = run_migrations(&pool)
        .await
        .expect_err("tampered hash must be refused");
    assert!(
        matches!(err, zimservice::error::Error::Config(_)),
        "drift must surface as a Config error, got: {err}"
    );
    assert!(
        err.to_string().contains("was modified after being applied"),
        "drift error must name the failure mode, got: {err}"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}

/// A database whose `schema_migrations` is the legacy `(version INTEGER)`
/// shape must be refused with the manual-migration recipe — never
/// auto-upgraded (PONY-D2). Runs in its own temp database.
#[tokio::test]
async fn smoke_migration_legacy_schema_refused() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };
    // Rebuild the pre-005 legacy tracking table (version INTEGER,
    // sequentially numbered — the shape zimservice no longer upgrades).
    zimservice::db::raw::execute(
        &pool,
        "CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
        |q| q,
    )
    .await
    .expect("legacy tracking table");
    let err = run_migrations(&pool)
        .await
        .expect_err("legacy schema must be refused");
    assert!(
        matches!(err, zimservice::error::Error::Config(_)),
        "legacy refusal must surface as a Config error, got: {err}"
    );
    assert!(
        err.to_string()
            .contains("found legacy schema_migrations (version INTEGER)"),
        "legacy refusal must carry the manual recipe, got: {err}"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}

/// Two concurrent `run_migrations` calls against a FRESH temp database must
/// serialize on the session-level advisory lock: both complete `Ok`, and
/// `schema_migrations` ends with exactly `MIGRATION_COUNT` rows, all names
/// distinct.
///
/// This is the guard against a regression where the advisory lock is taken
/// on one pooled connection while the DDL loop runs on a different one (the
/// lock would then serialize nothing — both runs apply in parallel, and one
/// of them double-applies a migration, failing on the primary key or
/// corrupting the tracking table). Both pools must be real, separate pools
/// so each `run_migrations` acquires its own connection for the lock AND
/// the DDL.
#[tokio::test]
async fn smoke_concurrent_migrations_serialize_on_advisory_lock() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool1, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };
    // Second independent pool to the SAME private temp DB (the pool is the
    // sanctioned temp-DB exception — see the module doc).
    let mut url = url::Url::parse(&base_url()).expect("valid base URL");
    url.set_path(&format!("/{name}"));
    let pool2 = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(url.as_str())
        .await
        .expect("second pool to the private temp DB");

    // Concurrent fresh applies: the loser must block on the advisory lock
    // until the winner finishes, then no-op — never double-apply.
    let (first, second) = tokio::join!(run_migrations(&pool1), run_migrations(&pool2));
    first.expect("first concurrent run_migrations must complete Ok");
    second.expect("second concurrent run_migrations must complete Ok");

    let expected = zimservice::db::migrate::MIGRATION_COUNT as i64;
    let count: i64 = zimservice::db::raw::fetch_scalar_optional(
        &pool1,
        "SELECT count(*) FROM schema_migrations",
        |q| q,
    )
    .await
    .expect("count applied migrations")
    .expect("schema_migrations row");
    let distinct: i64 = zimservice::db::raw::fetch_scalar_optional(
        &pool1,
        "SELECT count(DISTINCT name) FROM schema_migrations",
        |q| q,
    )
    .await
    .expect("count distinct migration names")
    .expect("schema_migrations row");
    assert_eq!(
        count, expected,
        "concurrent runs must apply every migration exactly once"
    );
    assert_eq!(
        distinct, expected,
        "every recorded migration name must be distinct — a lost advisory \
         lock would double-apply a migration"
    );

    pool2.close().await;
    close_and_drop(&base_pool, &pool1, &name).await;
}
