//! Database layer: migrations, connection pooling, and query helpers.
//!
//! The pool is a [`sqlx::PgPool`](sqlx::postgres::PgPool) (see `pool`), and
//! `raw` provides the shared query helpers. The lint boundary
//! (`scripts/check-raw-sql.sh`) enforces two rules:
//!
//! 1. **Presentation layer** (`src/serve/handlers/`) must not own SQL —
//!    any `db::raw::*` call there needs a `// RAW-OK: <reason>` marker.
//!    The intended fix is to move SQL into a named helper in `src/db/`.
//!
//! 2. **Direct `sqlx::query*` calls** outside `src/db/` need a
//!    `// RAW-OK: <reason>` marker on the call line or the line directly
//!    above it (call lines are often too long for a trailing comment).
//!    Background layers (torrent, zim, search, embed, startup) call
//!    `db::raw::*` directly as the intended path and are exempt from this rule.

/// Explicit-ID lookups of a single article's indexed metadata (the non-random half
/// of article fetching, split from `random_article`).
pub mod articles;
/// User-collection data access (ARCH M1 repository extraction).
pub mod collections;
/// Download-queue data access (ARCH M1 repository extraction).
pub mod downloads;
/// Download lifecycle state machine: status/hash/error/seed-stats
/// transitions on the `downloads` table.
pub mod downloads_lifecycle;
/// SQL migrations: a numbered, hash-tracked list applied in order at startup.
pub mod migrate;
/// Postgres connection pool configuration and TLS handling.
pub mod pool;
/// Wikidata Q-ID lookup for cross-language article discovery.
pub mod qid;
/// O(1) random article selection via index seek.
pub mod random_article;

pub use pool::Pool;

/// Shared raw-SQL helpers that the lint enforces as the boundary between
/// presentation and persistence layers, including SQL the generic helper
/// shapes cannot take (session-level advisory locks, batch DDL —
/// multi-statement migration files, `DROP INDEX CONCURRENTLY` — and
/// catalog probes).
///
/// Executor argument: pass `&pool` (`&Pool` implements `Executor`),
/// `&mut conn` (a `&mut sqlx::PgConnection`), or `&mut *tx` / `&mut *conn`
/// (a `&mut PgConnection` deref-borrowed out of a `Transaction` /
/// `PoolConnection`).
///
/// Binding: this sqlx build has a consuming builder-style `bind` (`self
/// -> Self`), so the `bind` closure receives the prepared query and must
/// return it after chaining `.bind(...)` calls — `|q| q.bind(a).bind(b)`
/// for bound parameters, `|q| q` when there are none.
///
/// Multi-statement DDL: pass the whole script to ONE [`raw::execute_script`]
/// — raw-string execution runs on the simple query protocol, and Postgres
/// splits the script server-side (the same mechanism `sqlx::migrate!` uses
/// for migration scripts; sqlx documents `raw_sql()` as the path that
/// "accepts multiple queries separated by semicolons … never uses prepared
/// statements"). This replaced the old client-side `split_statements`
/// lexer (deleted, 2026-09 review: the premise "sqlx has no
/// multi-statement protocol call" was false for the raw-string path);
/// `tests/integration/migrations.rs` pins the behavior.
pub mod raw {
    use crate::error::{Error, Result};
    use sqlx::postgres::{PgRow, Postgres};
    use sqlx::{Database, Decode, Executor, FromRow, Type};

    /// Raw Postgres query with the default (bindable) argument list.
    ///
    /// sqlx 0.9: `Arguments` lost its lifetime parameter (it is `PgArguments`,
    /// a flat struct, no more `<'q>`), but `Query`/`QueryAs`/`QueryScalar`
    /// still carry the SQL lifetime `<'q>`.
    pub type PgQuery<'q> = sqlx::query::Query<'q, Postgres, <Postgres as Database>::Arguments>;

    /// Raw Postgres query whose rows decode as `R` (tuple or `FromRow` struct).
    pub type PgQueryAs<'q, R> =
        sqlx::query::QueryAs<'q, Postgres, R, <Postgres as Database>::Arguments>;

    /// Raw Postgres query extracting the first column of each row as `O`
    /// (`O: Type<Postgres> + Decode`; any extra columns are ignored).
    pub type PgScalarQuery<'q, O> =
        sqlx::query::QueryScalar<'q, Postgres, O, <Postgres as Database>::Arguments>;

    /// Execute a raw SQL statement (optionally binding `$1..$n` through
    /// `bind`) and return the number of affected rows.
    pub async fn execute<'q, 'c, E, B>(executor: E, sql: &'q str, bind: B) -> Result<u64>
    where
        E: Executor<'c, Database = Postgres>,
        B: FnOnce(PgQuery<'q>) -> PgQuery<'q>,
    {
        // sqlx 0.9: `query*` takes `impl SqlSafeStr` (`&'static str` /
        // `AssertSqlSafe` only). These helpers are the sanctioned dynamic-SQL
        // escape hatch (the raw-sql lint keeps call sites here), so the
        // runtime `&str` is wrapped in `AssertSqlSafe` — the central
        // injection-audit point for the whole crate.
        let query = bind(sqlx::query(sqlx::AssertSqlSafe(sql)));
        Ok(query
            .execute(executor)
            .await
            .map_err(Error::Database)?
            .rows_affected())
    }

    /// Fetch at most one row, decoding it as `R` (tuple or `FromRow` struct).
    pub async fn fetch_optional<'q, 'c, R, E, B>(
        executor: E,
        sql: &'q str,
        bind: B,
    ) -> Result<Option<R>>
    where
        R: for<'r> FromRow<'r, PgRow> + Unpin + Send,
        E: Executor<'c, Database = Postgres>,
        B: FnOnce(PgQueryAs<'q, R>) -> PgQueryAs<'q, R>,
    {
        let query = bind(sqlx::query_as::<_, R>(sqlx::AssertSqlSafe(sql)));
        query
            .fetch_optional(executor)
            .await
            .map_err(Error::Database)
    }

    /// Fetch all rows, decoding each as `R` (tuple or `FromRow` struct).
    pub async fn fetch_all<'q, 'c, R, E, B>(executor: E, sql: &'q str, bind: B) -> Result<Vec<R>>
    where
        R: for<'r> FromRow<'r, PgRow> + Unpin + Send,
        E: Executor<'c, Database = Postgres>,
        B: FnOnce(PgQueryAs<'q, R>) -> PgQueryAs<'q, R>,
    {
        let query = bind(sqlx::query_as::<_, R>(sqlx::AssertSqlSafe(sql)));
        query.fetch_all(executor).await.map_err(Error::Database)
    }

    /// Fetch at most one row and decode its first column as `O`
    /// (single-column queries; `O` implements `Type`/`Decode`, e.g.
    /// `String`, `bool`, `i64`, `chrono::NaiveDateTime`). This sqlx build
    /// has no `FromRow` impls for bare primitives — rows must be decoded
    /// through tuples ([`fetch_optional`]) or the scalar path; use this
    /// helper for one-column selects.
    pub async fn fetch_scalar_optional<'q, 'c, 'e, O, E, B>(
        executor: E,
        sql: &'q str,
        bind: B,
    ) -> Result<Option<O>>
    where
        O: 'e + for<'r> Decode<'r, Postgres> + Type<Postgres> + Unpin + Send,
        E: 'e + Executor<'c, Database = Postgres>,
        B: FnOnce(PgScalarQuery<'q, O>) -> PgScalarQuery<'q, O>,
    {
        let query = bind(sqlx::query_scalar::<_, O>(sqlx::AssertSqlSafe(sql)));
        query
            .fetch_optional(executor)
            .await
            .map_err(Error::Database)
    }

    /// Fetch all rows, decoding the first column of each as `O`
    /// (see [`fetch_scalar_optional`]).
    pub async fn fetch_scalar_all<'q, 'c, 'e, O, E, B>(
        executor: E,
        sql: &'q str,
        bind: B,
    ) -> Result<Vec<O>>
    where
        O: 'e + for<'r> Decode<'r, Postgres> + Type<Postgres> + Unpin + Send,
        E: 'e + Executor<'c, Database = Postgres>,
        B: FnOnce(PgScalarQuery<'q, O>) -> PgScalarQuery<'q, O>,
    {
        let query = bind(sqlx::query_scalar::<_, O>(sqlx::AssertSqlSafe(sql)));
        query.fetch_all(executor).await.map_err(Error::Database)
    }

    /// Execute a multi-statement SQL script (no binds) as ONE simple-protocol
    /// query — Postgres splits the script server-side.
    ///
    /// Protocol note (why this exists, 2026-09 review): a *no-bind*
    /// [`execute`] still runs on the **extended** protocol — in sqlx
    /// 0.9 the untyped `sqlx::query` carries an empty (but `Some`) argument
    /// list, so it is parsed as a prepared statement, and Postgres rejects
    /// multi-command prepared strings (`cannot insert multiple commands
    /// into a prepared statement`). Only raw-string execution
    /// (`AssertSqlSafe` + `Executor::execute` with `take_arguments() ==
    /// None`) takes the simple query protocol — the same mechanism
    /// `sqlx::migrate!` uses for migration scripts (sqlx documents its
    /// `raw_sql()` as "accepts multiple queries separated by semicolons …
    /// never uses prepared statements").
    ///
    /// The script is compile-time/fixture SQL only (the sole production
    /// caller is the migration runner; `MIGRATIONS` is a `const`): there is
    /// nothing to bind, and the single-argument `AssertSqlSafe` keeps this
    /// on the crate's one injection-audit point. All statements run
    /// atomically on the caller's executor (pass `&mut *tx` for the
    /// migration transaction).
    ///
    /// Returns the row count of the script's **last** statement (the simple
    /// protocol reports a single count per query) — callers that need
    /// per-statement counts should split the script.
    pub async fn execute_script<'q, 'c, E>(executor: E, sql: &'q str) -> Result<u64>
    where
        E: 'c + Executor<'c, Database = Postgres>,
    {
        Ok(executor
            .execute(sqlx::AssertSqlSafe(sql))
            .await
            .map_err(Error::Database)?
            .rows_affected())
    }
}
