//! Bidirectional drift test for `zimservice::error::CONSTRAINT_FIELDS`
//! (the hand-maintained registry that maps every unique constraint /
//! unique index declared in `migrations/*.sql` to the human field label a
//! 23505 `"duplicate value for '…'"` message names).
//!
//! Direction 1 (registry → DB): every registry name must exist in the
//! database as a unique constraint or unique index. This is the guard that
//! catches STALE entries — when a table or constraint is dropped by a later
//! migration (as `search_history_pkey` / `qid_cache_pkey` were when 002
//! dropped the tables), the dead registry entry whose 409 label can never
//! fire surfaces here.
//!
//! Direction 2 (DB → registry): every unique constraint (contype 'u'/'p')
//! and every unique index in the `public` schema must be a registry entry.
//! This is the guard that catches UNREGISTERED constraints — without one,
//! `duplicate_field` falls back to prefix-stripping and the client sees a
//! raw constraint fragment instead of a field name.
//!
//! Read-only against the shared dev DB (no temp DB needed): it holds the
//! `DbExclusiveGuard` via `pool_or_skip` like every other DB-gated test and
//! runs `run_migrations` first — a no-op on an up-to-date database, and the
//! applier of any pending migration on a stale one, so the comparison always
//! targets the schema the current migrations produce. The harness-managed
//! `schema_migrations` tracking table is excluded: it is created by the
//! migration runner (src/db/migrate.rs), not by a `migrations/*.sql` file,
//! so it is outside the registry's documented scope.

use std::collections::BTreeSet;

use super::common::*;

#[tokio::test]
async fn smoke_constraint_registry_matches_live_unique_objects() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool)
        .await
        .expect("migrations (compare against the current schema)");

    // Source A: unique constraints ('u') and primary keys ('p') on
    // `public` tables, minus the harness tracking table (see module doc).
    let from_constraints: Vec<(String,)> = zimservice::db::raw::fetch_all(
        &pool,
        "SELECT c.conname FROM pg_constraint c
         JOIN pg_class t ON t.oid = c.conrelid
         JOIN pg_namespace n ON n.oid = t.relnamespace
         WHERE n.nspname = 'public'
           AND c.contype IN ('u', 'p')
           AND t.relname <> 'schema_migrations'
         ORDER BY c.conname",
        |q| q,
    )
    .await
    .expect("pg_constraint probe");

    // Source B: unique indexes. Primary keys surface here too (as
    // `CREATE UNIQUE INDEX <table>_pkey …`), so the union deduplicates.
    let from_indexes: Vec<(String,)> = zimservice::db::raw::fetch_all(
        &pool,
        "SELECT indexname FROM pg_indexes
         WHERE schemaname = 'public'
           AND tablename <> 'schema_migrations'
           AND indexdef LIKE '%UNIQUE%'
         ORDER BY indexname",
        |q| q,
    )
    .await
    .expect("pg_indexes probe");

    let mut db_set: BTreeSet<String> = from_constraints.into_iter().map(|(name,)| name).collect();
    db_set.extend(from_indexes.into_iter().map(|(name,)| name));

    let registry: BTreeSet<&str> = zimservice::error::CONSTRAINT_FIELDS
        .iter()
        .map(|(name, _)| *name)
        .collect();

    // Direction 1: every registry name must exist in the DB.
    let stale: Vec<&str> = registry
        .iter()
        .copied()
        .filter(|name| !db_set.contains(*name))
        .collect();
    assert!(
        stale.is_empty(),
        "stale registry entries — no such unique constraint/index in the database: {stale:?} \
         (dropping a table/constraint must also drop its registry entry)"
    );

    // Direction 2: every unique constraint/index in the DB must be registered.
    let unregistered: Vec<String> = db_set
        .iter()
        .filter(|name| !registry.contains(name.as_str()))
        .cloned()
        .collect();
    assert!(
        unregistered.is_empty(),
        "unregistered unique constraint/index — its 23505 degrades to a raw constraint \
         fragment in the 409 message: {unregistered:?} (add a CONSTRAINT_FIELDS entry)"
    );
}
