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
/// Leftovers from a crashed run are harmless: the next run uses a fresh name
/// (drop them manually — `DROP DATABASE "zimservice_itest_mig_..."` — if
/// they accumulate on a long-lived dev server).
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
async fn create_temp_db(base_pool: &Pool) -> Option<(Pool, String)> {
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

/// Close `pool` (releasing its connections), then drop the database
/// best-effort on the shared server.
async fn close_and_drop(base_pool: &Pool, pool: &Pool, name: &str) {
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
