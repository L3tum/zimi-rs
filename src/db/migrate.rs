//! SQL migrations: a numbered, hash-tracked list applied in order at startup.
//!
//! `run_migrations` applies any unapplied migrations (tracked in
//! `schema_migrations` by filename + content hash) and is idempotent — safe to
//! call on every start.
use tokio_postgres::Client;

use crate::db::pool::Pool;
use crate::error::{Error, Result};

/// Numbering is historical: 005 was removed/superseded. Do NOT renumber —
/// migrations are tracked by filename + content hash. New migrations: 013, 014, …
const MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_initial.sql",
        include_str!("../../migrations/001_initial.sql"),
    ),
    (
        "002_fixes.sql",
        include_str!("../../migrations/002_fixes.sql"),
    ),
    (
        "003_active_downloads_unique.sql",
        include_str!("../../migrations/003_active_downloads_unique.sql"),
    ),
    (
        "004_drop_zims_uuid.sql",
        include_str!("../../migrations/004_drop_zims_uuid.sql"),
    ),
    (
        "006_seeding_visibility.sql",
        include_str!("../../migrations/006_seeding_visibility.sql"),
    ),
    (
        "007_articles_embedded_partial.sql",
        include_str!("../../migrations/007_articles_embedded_partial.sql"),
    ),
    (
        "008_articles_title_indexes.sql",
        include_str!("../../migrations/008_articles_title_indexes.sql"),
    ),
    (
        "009_articles_title_index_dedup.sql",
        include_str!("../../migrations/009_articles_title_index_dedup.sql"),
    ),
    (
        "010_embedding_partial_index_and_download_sort.sql",
        include_str!("../../migrations/010_embedding_partial_index_and_download_sort.sql"),
    ),
    (
        "011_downloads_active_name_unique.sql",
        include_str!("../../migrations/011_downloads_active_name_unique.sql"),
    ),
    (
        "012_articles_staging_zim_id.sql",
        include_str!("../../migrations/012_articles_staging_zim_id.sql"),
    ),
    (
        "013_downloads_status_updated_index.sql",
        include_str!("../../migrations/013_downloads_status_updated_index.sql"),
    ),
];

/// Historical migration order for legacy databases (numbered 1..N sequentially
/// before the 005 removal). Legacy versions 1..5 corresponded to the first
/// five migrations (001, 002, 003, 004, 006 — 005 is the permanent gap).
/// Returned at runtime as an error when a legacy (version INTEGER) schema is
/// detected — zimservice no longer auto-upgrades (PONY-D2).
const LEGACY_SCHEMA_MSG: &str = "found legacy schema_migrations (version INTEGER) — \
     zimservice no longer auto-upgrades. Manual recipe: \
     `pg_dump --schema-only -t schema_migrations` the DB, `DROP TABLE schema_migrations`, \
     run `zimservice serve` once to apply migrations 001-012, then restore your rows into \
     schema_migrations(name, hash) — or re-download your ZIMs and rebuild the database \
     from scratch.";

/// Fixed key for the startup-migration serialization lock (session-level
/// advisory lock). Any fixed constant works; this spells "zims".
const MIGRATION_LOCK_KEY: i64 = 0x7A69_6D73;

/// Run all pending migrations against a pool.
///
/// Concurrent startups are serialized with a session-level advisory lock so
/// two processes can't race to apply the same migration: the second process
/// blocks until the first releases the lock, then sees every migration already
/// applied. The lock is released explicitly on every path so the pooled
/// connection isn't left holding it.
pub async fn run_migrations(pool: &Pool) -> Result<()> {
    let mut client = pool.get().await.map_err(Error::Pool)?;
    client
        .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_KEY])
        .await
        .map_err(Error::Database)?;
    let result = run_migrations_on(&mut client).await;
    // Best-effort release; the lock also drops if the connection closes.
    let _ = client
        .execute("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_KEY])
        .await;
    result
}

/// Drop invalid (crashed `CONCURRENTLY` build) indexes left behind by a
/// prior crashed build. All DDL cleanup lives in the migration layer (ARCH M1)
/// — the `serve` startup path calls this instead of inlining the DDL in
/// `main.rs`. Mirrors the original inline behavior: the pool-get error is
/// fatal, but the catalog probe and each drop are best-effort (a probe
/// failure skips the cleanup, a failing drop is logged and retried on the
/// next startup). `CONCURRENTLY` cannot run inside a transaction, so each
/// drop is a standalone statement on its own pooled connection.
pub async fn drop_invalid_indexes(pool: &Pool) -> Result<()> {
    let client = pool.get().await.map_err(Error::Pool)?;
    let rows = match client
        .query(
            "SELECT c.relname FROM pg_class c JOIN pg_index i ON c.oid = i.indexrelid \
             WHERE i.indisvalid = false AND c.relname LIKE 'idx\\_%' ESCAPE '\\'",
            &[],
        )
        .await
    {
        Ok(rows) => rows,
        // Best-effort (the original inline code used `unwrap_or_default`):
        // a failing probe skips the cleanup rather than blocking startup.
        Err(e) => {
            tracing::warn!("invalid-index cleanup: catalog probe failed: {e}");
            return Ok(());
        }
    };
    for row in &rows {
        let idx_name: String = row.get(0);
        tracing::warn!("dropping invalid index {idx_name}");
        // Safe: identifier comes from the pg_class catalog, not user input.
        if let Err(e) = client
            .execute(
                &format!(r#"DROP INDEX CONCURRENTLY IF EXISTS "{idx_name}""#),
                &[],
            )
            .await
        {
            tracing::warn!("failed to drop invalid index {idx_name}: {e}");
        }
    }
    Ok(())
}

pub async fn run_migrations_on(client: &mut Client) -> Result<()> {
    // Normalise the tracking table to the (name, hash) shape. A fresh database
    // gets the table created; an existing database built by the legacy
    // (version INTEGER) system is upgraded in place so it keeps its applied
    // set. The hash is Postgres md5() of the file content, computed
    // server-side, so no client-side hash crate is needed.
    let shape: Option<String> = client
        .query_opt(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = 'schema_migrations' AND column_name IN ('name','version') \
             ORDER BY column_name DESC LIMIT 1",
            &[],
        )
        .await
        .map_err(Error::Database)?
        .map(|r| r.get::<_, String>(0));

    match shape.as_deref() {
        None => {
            client
                .batch_execute(
                    "CREATE TABLE schema_migrations (
                        name TEXT PRIMARY KEY,
                        hash TEXT NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )",
                )
                .await
                .map_err(Error::Database)?;
        }
        Some("name") => {
            // Already the current shape.
        }
        Some("version") => {
            return Err(Error::Config(LEGACY_SCHEMA_MSG.to_string()));
        }
        _ => unreachable!("column_name was constrained to 'name' or 'version'"),
    }

    for (name, sql) in MIGRATIONS {
        // md5 of the *current* file content, compared against the recorded
        // hash, entirely server-side.
        let matches: Option<bool> = client
            .query_opt(
                "SELECT hash = md5($2) FROM schema_migrations WHERE name = $1",
                &[name, sql],
            )
            .await
            .map_err(Error::Database)?
            .map(|r| r.get::<_, bool>(0));

        match matches {
            Some(true) => continue, // applied and unchanged
            Some(false) => {
                let stored = client
                    .query_opt(
                        "SELECT hash FROM schema_migrations WHERE name = $1",
                        &[name],
                    )
                    .await
                    .map_err(Error::Database)?
                    .map(|r| r.get::<_, String>(0));
                return Err(Error::Config(format!(
                    "migration {name} was modified after being applied \
                     (recorded hash {stored:?}, file content now differs). \
                     Applied migrations must not be edited — add a new \
                     migration file instead"
                )));
            }
            None => { /* not applied yet */ }
        }

        tracing::info!("applying migration {name}");
        // Run the migration DDL and the tracking INSERT in ONE transaction so
        // a crash between them can't leave the schema changed but
        // unrecorded (which would re-apply the migration on the next startup).
        // A failure aborts the transaction, rolls the schema back cleanly, and
        // leaves the file unrecorded.
        let tx = client.transaction().await.map_err(Error::Database)?;
        tx.batch_execute(sql).await.map_err(Error::Database)?;
        tx.execute(
            "INSERT INTO schema_migrations (name, hash) VALUES ($1, md5($2))",
            &[name, sql],
        )
        .await
        .map_err(Error::Database)?;
        tx.commit().await.map_err(Error::Database)?;

        tracing::info!("applied migration {name}");
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn legacy_schema_message_carries_manual_recipe() {
        for needle in [
            "legacy schema_migrations (version INTEGER)",
            "pg_dump",
            "no longer auto-upgrades",
        ] {
            assert!(
                LEGACY_SCHEMA_MSG.contains(needle),
                "message missing needle: {needle:?}"
            );
        }
    }
}
