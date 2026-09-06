//! Database layer: migrations, connection pooling, and query helpers.
//!
//! The pool is a [`sqlx::PgPool`](sqlx::postgres::PgPool) (see
//! [`pool`]); SeaORM is used on top of it — build a
//! [`sea_orm::DatabaseConnection`] handle from the pool with [`sea_orm_db`]
//! (cheap: it wraps the same pool) whenever an entity query is needed.

pub mod collections;
pub mod downloads;
pub mod downloads_lifecycle;
pub mod entities;
pub mod migrate;
pub mod pool;
pub mod qid;
pub mod random_article;

pub use pool::Pool;

/// Build a SeaORM [`sea_orm::DatabaseConnection`] handle over the shared
/// pool. Cheap: `PgPool` is an `Arc`-backed clone — no extra connections,
/// and the SeaORM entity / query-builder API shares exactly the same pool
/// (and its TLS mode and pool-size settings) as the [`raw`] escape hatch.
///
/// Note (sea-orm 1.x): `sea_orm::Database` is a unit-struct namespace, not a
/// connection handle — the handle type is
/// [`sea_orm::DatabaseConnection`](sea_orm::DatabaseConnection) (alias
/// `sea_orm::DbConn`).
pub fn sea_orm_db(pool: &Pool) -> sea_orm::DatabaseConnection {
    sea_orm::DatabaseConnection::from(pool.clone())
}

/// Shared raw-SQL helper — the **sanctioned escape hatch** for SQL the
/// SeaORM query builder cannot express: session-level advisory locks,
/// batch DDL (multi-statement migration files, `DROP INDEX CONCURRENTLY`),
/// and catalog probes.
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
/// Batch DDL: iterate [`split_statements`] and run each statement through
/// [`execute`] on the same executor — this is the replacement for the old
/// `tokio_postgres` `batch_execute` (sqlx has no multi-statement
/// protocol call).
pub mod raw {
    use crate::error::{Error, Result};
    use sqlx::postgres::{PgRow, Postgres};
    use sqlx::{Database, Decode, Executor, FromRow, Type};

    /// Raw Postgres query with the default (bindable) argument list.
    pub type PgQuery<'q> = sqlx::query::Query<'q, Postgres, <Postgres as Database>::Arguments<'q>>;

    /// Raw Postgres query whose rows decode as `R` (tuple or `FromRow` struct).
    pub type PgQueryAs<'q, R> =
        sqlx::query::QueryAs<'q, Postgres, R, <Postgres as Database>::Arguments<'q>>;

    /// Raw Postgres query extracting the first column of each row as `O`
    /// (`O: Type<Postgres> + Decode`; any extra columns are ignored).
    pub type PgScalarQuery<'q, O> =
        sqlx::query::QueryScalar<'q, Postgres, O, <Postgres as Database>::Arguments<'q>>;

    /// Execute a raw SQL statement (optionally binding `$1..$n` through
    /// `bind`) and return the number of affected rows.
    pub async fn execute<'q, 'c, E, B>(executor: E, sql: &'q str, bind: B) -> Result<u64>
    where
        E: Executor<'c, Database = Postgres>,
        B: FnOnce(PgQuery<'q>) -> PgQuery<'q>,
    {
        let query = bind(sqlx::query(sql));
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
        let query = bind(sqlx::query_as::<_, R>(sql));
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
        let query = bind(sqlx::query_as::<_, R>(sql));
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
        let query = bind(sqlx::query_scalar::<_, O>(sql));
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
        let query = bind(sqlx::query_scalar::<_, O>(sql));
        query.fetch_all(executor).await.map_err(Error::Database)
    }

    /// Split a multi-statement SQL script at top-level `;` boundaries,
    /// respecting single-quoted strings (`''` escapes), dollar-quoted
    /// bodies (`$$ … $$`, `$tag$ … $tag$` — e.g. `DO` blocks), line
    /// comments, and (nested) block comments. Trailing/whitespace-only
    /// fragments are dropped.
    pub fn split_statements(script: &str) -> Vec<String> {
        let chars: Vec<char> = script.chars().collect();
        let mut statements = Vec::new();
        let mut current = String::new();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if c == '\'' {
                // Single-quoted string: copy verbatim, `''` is an escaped quote.
                current.push(c);
                i += 1;
                while i < chars.len() {
                    current.push(chars[i]);
                    if chars[i] == '\'' {
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            i += 1;
                            current.push('\'');
                        } else {
                            i += 1;
                            break;
                        }
                    }
                    i += 1;
                }
                continue;
            }
            if c == '$' && is_dollar_quote_start(&chars, i) {
                let tag = read_dollar_tag(&chars, i);
                let tag_len = tag.chars().count();
                current.push_str(&tag);
                i += tag_len;
                let end = find_dollar_tag(&chars, i, &tag);
                if let Some(end) = end {
                    current.push_str(&chars[i..end + tag_len].iter().collect::<String>());
                    i = end + tag_len;
                } else {
                    // Unterminated: keep the rest as-is (Postgres will reject it).
                    current.push_str(&chars[i..].iter().collect::<String>());
                    i = chars.len();
                }
                continue;
            }
            if c == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
                // Line comment: keep to end of line.
                while i < chars.len() && chars[i] != '\n' {
                    current.push(chars[i]);
                    i += 1;
                }
                continue;
            }
            if c == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
                // Block comment (Postgres allows nesting): copy verbatim.
                let mut depth = 0usize;
                while i < chars.len() {
                    if c == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
                        depth += 1;
                        current.push_str("/*");
                        i += 2;
                        continue;
                    }
                    if c == '*' && i + 1 < chars.len() && chars[i + 1] == '/' {
                        depth -= 1;
                        current.push_str("*/");
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                        continue;
                    }
                    current.push(chars[i]);
                    i += 1;
                }
                continue;
            }
            if c == ';' {
                push_statement(&mut statements, &current);
                current.clear();
                i += 1;
                continue;
            }
            current.push(c);
            i += 1;
        }
        push_statement(&mut statements, &current);
        statements
    }

    fn push_statement(statements: &mut Vec<String>, current: &str) {
        let trimmed = current.trim();
        if !trimmed.is_empty() {
            statements.push(trimmed.to_string());
        }
    }

    /// True when the `$` at `chars[i]` starts a dollar-quote tag
    /// (`$$` or `$identifier$`).
    fn is_dollar_quote_start(chars: &[char], i: usize) -> bool {
        if i + 1 >= chars.len() {
            return false;
        }
        if chars[i + 1] == '$' {
            return true;
        }
        // $identifier$: letters/digits/underscore, must not start with a digit.
        let mut j = i + 1;
        let first = chars[j];
        if !(first.is_ascii_alphabetic() || first == '_') {
            return false;
        }
        j += 1;
        while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
            j += 1;
        }
        j < chars.len() && chars[j] == '$'
    }

    /// Read the dollar-quote tag (including both `$`s) starting at `chars[i]`.
    fn read_dollar_tag(chars: &[char], i: usize) -> String {
        if i + 1 < chars.len() && chars[i + 1] == '$' {
            return "$$".to_string();
        }
        let mut j = i + 1;
        while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
            j += 1;
        }
        // j is at the closing `$` (guaranteed by is_dollar_quote_start).
        chars[i..=j].iter().collect()
    }

    /// Index of the dollar-quote tag `tag` at/after position `i`, or `None`.
    fn find_dollar_tag(chars: &[char], i: usize, tag: &str) -> Option<usize> {
        let tag_len = tag.chars().count();
        let mut j = i;
        while j + tag_len <= chars.len() {
            if chars[j..j + tag_len].iter().collect::<String>() == tag {
                return Some(j);
            }
            j += 1;
        }
        None
    }
}
