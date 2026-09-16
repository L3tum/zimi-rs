//! Invalid-index startup-cleanup tests for `db::migrate::drop_invalid_indexes`
//! (ARCH M1: all DDL cleanup lives in the migration layer).
//!
//! The production path cleans up indexes left INVALID by a crashed
//! `CREATE INDEX CONCURRENTLY` build: a catalog probe finds
//! `indisvalid = false` names matching `idx_%`, each name is validated as a
//! plain SQL identifier before being spliced into `DROP INDEX`, the probe
//! failure is a warn-and-skip no-op (startup is never blocked), and each
//! drop is best-effort.
//!
//! The INVALID indexes below are produced with `build_invalid_index`: a
//! first connection holds an **uncommitted write** (a real `UPDATE`, which
//! gives it an active xid) so a second connection's `CREATE INDEX CONCURRENTLY`
//! stops in its wait-for-concurrent-xacts phase with the catalog entry already
//! created but still `indisvalid = false`; we then `pg_terminate_backend` the
//! build. An interrupted `CONCURRENTLY` build is the reliable PG 16 way to
//! leave a real invalid index (a build that merely waits for a concurrent
//! write re-validates and finishes *valid* once the write ends). If a future
//! Postgres change alters this, the `index_indisvalid` assertion is the source
//! of truth and will name the deviation.
//!
//! DB-gated like every test here: skips without a reachable `DATABASE_URL`,
//! hard-fails under `ZIMSERVICE_REQUIRE_DB` when the base DB is unreachable;
//! the temp-DB pattern is the `migrations.rs` one (private database per
//! test, `DbExclusiveGuard` held, `close_and_drop` at the end).

use super::common::*;
use super::migrations::{close_and_drop, create_temp_db};

/// Fixture table for the invalid-index recipes.
const TABLE: &str = "idx_probe_t";

/// `CREATE TABLE` + one row so the invalid-index recipe has a lock to hold.
async fn make_table(pool: &Pool) {
    zimservice::db::raw::execute(pool, &format!("CREATE TABLE {TABLE} (x INT)"), |q| q)
        .await
        .expect("fixture table");
    zimservice::db::raw::execute(pool, &format!("INSERT INTO {TABLE} VALUES (1)"), |q| q)
        .await
        .expect("fixture row");
}

/// Produce a real INVALID index with a recipe that is deterministic on PG 16.
/// A `CREATE INDEX CONCURRENTLY` that merely *waits* for a concurrent write
/// still finishes **valid** (it re-validates once the write ends), so the
/// only reliable way to get `indisvalid = false` is to **interrupt** the build
/// mid-flight:
///
/// 1. connection 1: `BEGIN` then a real `UPDATE` — a write assigns an active
///    xid to the transaction (a read like `SELECT … FOR UPDATE` has no xid, so
///    the build would not wait for it). conn1 does **not** commit, so the
///    build's wait-for-concurrent-xacts phase never ends on its own;
/// 2. connection 2 (its own pooled connection — `CONCURRENTLY` cannot run
///    inside a transaction): `CREATE INDEX CONCURRENTLY`. It creates the
///    catalog entry (initially `indisvalid = false`) and then stops in the
///    wait phase, blocked on conn1's uncommitted write;
/// 3. we `pg_terminate_backend` the build's backend mid-wait. An interrupted
///    `CONCURRENTLY` build leaves the catalog entry INVALID — exactly the
///    state startup cleanup (`drop_invalid_indexes`) is meant to repair;
/// 4. conn1 `ROLLBACK`s (releasing its write) so the table is left usable.
///
/// The `index_indisvalid` assertion after the call is the source of truth: if
/// a future Postgres change alters this behavior, it names the deviation.
async fn build_invalid_index(pool: &Pool, create_index_sql: &str) {
    let mut conn1 = pool.acquire().await.expect("held connection");
    zimservice::db::raw::execute(&mut *conn1, "BEGIN", |q| q)
        .await
        .expect("BEGIN");
    // A write (not a bare read-lock): it gives conn1 an active xid so the
    // build actually stops in its wait-for-concurrent-xacts phase.
    zimservice::db::raw::execute(&mut *conn1, &format!("UPDATE {TABLE} SET x = x + 1"), |q| q)
        .await
        .expect("write (active xid, uncommitted)");

    // `CONCURRENTLY` must run in auto-commit, so it is a standalone task on a
    // fresh pooled connection (no open transaction). We keep the handle so we
    // can tell if the build finished before we could terminate it.
    let ci_sql = create_index_sql.to_string();
    let build_pool = pool.clone();
    let build =
        tokio::spawn(
            async move { zimservice::db::raw::execute(&build_pool, &ci_sql, |q| q).await },
        );

    // Poll for the build's backend (active, in its wait phase) and terminate
    // it. Polling rather than a fixed sleep keeps this robust on slow CI: the
    // 1-row pass-1 finishes in a few ms and the build then stays in the wait
    // phase until conn1 ends, so the pid is stable once seen.
    let like = format!("CREATE INDEX CONCURRENTLY %{}%", TABLE);
    let pid = loop {
        let found: Option<i32> = zimservice::db::raw::fetch_scalar_optional(
            pool,
            "SELECT pid FROM pg_stat_activity \
             WHERE query LIKE $1 AND state = 'active' ORDER BY backend_start LIMIT 1",
            |q| q.bind(&like),
        )
        .await
        .ok()
        .flatten();
        if let Some(p) = found {
            break Some(p);
        }
        if build.is_finished() {
            break None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    if let Some(p) = pid {
        zimservice::db::raw::execute(pool, "SELECT pg_terminate_backend($1)", |q| q.bind(p))
            .await
            .expect("terminate the in-flight build");
    }
    // The build was interrupted (or, defensively, already finished); we do not
    // assert on its result — the catalog entry is the source of truth (the
    // `index_indisvalid` check in the caller names any deviation).
    let _ = build.await;
    zimservice::db::raw::execute(&mut *conn1, "ROLLBACK", |q| q)
        .await
        .expect("ROLLBACK");
}

/// `indisvalid` for the index named `name` on the fixture table, or `None`
/// when the index does not exist at all.
async fn index_indisvalid(pool: &Pool, name: &str) -> Option<bool> {
    zimservice::db::raw::fetch_optional(
        pool,
        "SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON c.oid = i.indexrelid \
         WHERE c.relname = $1",
        |q| q.bind(name),
    )
    .await
    .expect("pg_class/pg_index probe")
    .map(|(v,)| v)
}

/// (a) A deterministic invalid index must be found by the catalog probe,
/// dropped by `drop_invalid_indexes`, and gone afterwards — cleanup returns
/// `Ok` and never blocks startup.
#[tokio::test]
async fn smoke_invalid_index_dropped_by_startup_cleanup() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };

    make_table(&pool).await;
    build_invalid_index(
        &pool,
        "CREATE INDEX CONCURRENTLY idx_conc_test ON idx_probe_t (x)",
    )
    .await;

    // The recipe must have left a real INVALID index (not just a missing one).
    assert_eq!(
        index_indisvalid(&pool, "idx_conc_test").await,
        Some(false),
        "recipe must leave the index present and invalid (indisvalid = false)"
    );

    zimservice::db::migrate::drop_invalid_indexes(&pool)
        .await
        .expect("startup cleanup must return Ok");

    assert_eq!(
        index_indisvalid(&pool, "idx_conc_test").await,
        None,
        "the invalid index must be dropped"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}

/// (b) An invalid index whose name matches the probe's `idx_%` LIKE but
/// contains characters outside `[A-Za-z0-9_]` must be SKIPPED by the
/// identifier sanitizer — never spliced into the `DROP INDEX` — the cleanup
/// still returns `Ok`, and the table stays intact (no partial injection).
#[tokio::test]
async fn smoke_invalid_index_sanitizer_skips_non_conforming_name() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };

    // Starts with `idx_` (the probe LIKE matches) but carries a space,
    // `)`, `;` and `--` — a real injection payload if spliced unquoted.
    const EVIL: &str = "idx_evil_) DROP TABLE t; --";
    make_table(&pool).await;
    build_invalid_index(
        &pool,
        &format!("CREATE INDEX CONCURRENTLY \"{EVIL}\" ON {TABLE} (x)"),
    )
    .await;
    assert_eq!(
        index_indisvalid(&pool, EVIL).await,
        Some(false),
        "recipe must leave the evil-named index present and invalid"
    );

    zimservice::db::migrate::drop_invalid_indexes(&pool)
        .await
        .expect("cleanup must return Ok even for a non-conforming name");

    // Skipped, not dropped — and the table survived the payload.
    assert_eq!(
        index_indisvalid(&pool, EVIL).await,
        Some(false),
        "the non-conforming index must be skipped, not dropped (or injected)"
    );
    let rows: i64 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        &format!("SELECT count(*) FROM {TABLE}"),
        |q| q,
    )
    .await
    .expect("table must still be queryable")
    .expect("row count");
    assert_eq!(
        rows, 1,
        "the table must be intact (payload must not have run)"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}

/// (c) When the catalog probe fails, `drop_invalid_indexes` must degrade to
/// a warn-and-skip no-op: `Ok(())`, startup not blocked, the invalid index
/// untouched.
///
/// Mechanism: the `testing::` seam `drop_invalid_indexes_with_probe` runs
/// the exact production body with an injectable catalog probe, so a
/// deterministically failing probe (a nonexistent catalog) exercises the
/// real degradation path — no server-wide catalog-grant mutation, no
/// superuser requirement, no role juggling.
#[tokio::test]
async fn smoke_invalid_index_probe_failure_is_a_noop() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };

    make_table(&pool).await;
    build_invalid_index(
        &pool,
        "CREATE INDEX CONCURRENTLY idx_conc_probe ON idx_probe_t (x)",
    )
    .await;
    assert_eq!(
        index_indisvalid(&pool, "idx_conc_probe").await,
        Some(false),
        "recipe must leave the index present and invalid"
    );

    // The production path under a failing probe: warn + skip, `Ok(())`,
    // startup not blocked, the invalid index untouched.
    zimservice::testing::drop_invalid_indexes_with_probe(
        &pool,
        "SELECT c.relname FROM no_such_catalog c JOIN pg_index i ON c.oid = i.indexrelid \
         WHERE i.indisvalid = false AND c.relname LIKE 'idx\\_%' ESCAPE '\\'",
    )
    .await
    .expect("probe failure must degrade to a warn-and-skip no-op (Ok)");
    assert_eq!(
        index_indisvalid(&pool, "idx_conc_probe").await,
        Some(false),
        "a failing probe must skip the cleanup — the invalid index stays"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}
