//! User-collection data access (ARCH M1 repository extraction).
//!
//! The `collections` table's SQL lives here — mirroring `db/downloads.rs` and
//! `db/qid.rs` — so the HTTP handlers (`serve/handlers/settings.rs`) stay thin:
//! they validate input, resolve ZIM names ↔ ids via the in-memory ZIM registry,
//! and map the domain outcomes to HTTP responses. The repo works in
//! `zim_ids` (`i32`); id↔name resolution is a presentation concern (it reads
//! the ZIM registry, not the DB), so it stays in the handler.
//!
//! Query layer: raw SQL via [`crate::db::raw`]. `updated_at = now()` stays
//! a *server-side* timestamp (the SQL `now()`, never a Rust-side clock), so
//! the SQL semantics match the old raw statements exactly.
use crate::db::{pool::Pool, raw};
use crate::error::{Error, Result};

/// One `collections` row — raw, with `zim_ids` still unresolved. The handler
/// maps this to the wire `Collection` DTO (resolving `zim_ids` → names).
pub struct CollectionRow {
    pub id: i32,
    pub name: String,
    pub label: String,
    pub zim_ids: Vec<i32>,
    pub is_favorite: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Decode tuple for [`list_collections`] — one field per [`CollectionRow`]
/// column, in SELECT order.
type CollectionRowCols = (
    i32,
    String,
    String,
    Vec<i32>,
    bool,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
);

/// All collections, ordered by name.
pub async fn list_collections(pool: &Pool) -> Result<Vec<CollectionRow>> {
    let rows: Vec<CollectionRowCols> = raw::fetch_all(
        pool,
        "SELECT id, name, label, zim_ids, is_favorite, created_at, updated_at \
         FROM collections ORDER BY name",
        |q| q,
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, name, label, zim_ids, is_favorite, created_at, updated_at)| CollectionRow {
                id,
                name,
                label,
                zim_ids,
                is_favorite,
                created_at,
                updated_at,
            },
        )
        .collect())
}

/// Outcome of [`insert_collection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The new row's id.
    Inserted(i32),
    /// A collection with this `name` already exists.
    Duplicate,
}

/// Insert a collection. A duplicate `name` hits the UNIQUE constraint and is
/// surfaced as [`InsertOutcome::Duplicate`] (BUG-9: the handler maps it to a
/// domain-specific 409, not the generic constraint-derived message).
pub async fn insert_collection(
    pool: &Pool,
    name: &str,
    label: &str,
    zim_ids: &[i32],
    is_favorite: bool,
) -> Result<InsertOutcome> {
    // Only the four caller-provided columns are set; `created_at` /
    // `updated_at` keep the table's `now()` defaults (same column list as
    // the old raw INSERT).
    let id: Option<i32> = match raw::fetch_scalar_optional(
        pool,
        "INSERT INTO collections (name, label, zim_ids, is_favorite) \
         VALUES ($1, $2, $3, $4) RETURNING id",
        |q| q.bind(name).bind(label).bind(zim_ids).bind(is_favorite),
    )
    .await
    {
        Ok(id) => id,
        // BUG-9: a concurrent INSERT hitting the name UNIQUE index (23505) is
        // a duplicate, not a DB fault.
        Err(Error::Database(e)) if is_unique_violation(&e) => return Ok(InsertOutcome::Duplicate),
        Err(e) => return Err(e),
    };
    Ok(InsertOutcome::Inserted(
        id.expect("INSERT … RETURNING id always yields a row"),
    ))
}

/// Fields to set on [`update_collection`]; a `None` field keeps its current
/// value (the repo builds a dynamic `UPDATE` so only the provided fields are
/// written).
pub struct UpdateFields {
    pub name: Option<String>,
    pub label: Option<String>,
    pub zim_ids: Option<Vec<i32>>,
    pub is_favorite: Option<bool>,
}

/// Outcome of [`update_collection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// The row was updated.
    Updated,
    /// No collection with that id.
    NotFound,
}

/// Update the provided [`UpdateFields`] on a collection (dynamic `SET`; omitted
/// fields keep their current values).
pub async fn update_collection(
    pool: &Pool,
    id: i32,
    fields: &UpdateFields,
) -> Result<UpdateOutcome> {
    // Dynamic `UPDATE`: only the provided fields are written. `updated_at`
    // stays server-side (`now()`) — always appended. Binds: `id` is `$1`;
    // the provided fields follow in a fixed column order (name, label,
    // zim_ids, is_favorite). The raw bind chain is positional and the four
    // field types differ, so the dispatch is one arm per provided-set
    // combination.
    let updated = match (&fields.name, &fields.label, &fields.zim_ids, &fields.is_favorite) {
        // Unreachable in practice (the handler 400s on an all-`None` body);
        // a benign fallback so we never build a malformed `UPDATE`.
        (None, None, None, None) => return Ok(UpdateOutcome::NotFound),
        (Some(name), None, None, None) => raw::execute(
            pool,
            "UPDATE collections SET name = $2, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(name),
        )
        .await?,
        (None, Some(label), None, None) => raw::execute(
            pool,
            "UPDATE collections SET label = $2, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(label),
        )
        .await?,
        (None, None, Some(zim_ids), None) => raw::execute(
            pool,
            "UPDATE collections SET zim_ids = $2, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(zim_ids),
        )
        .await?,
        (None, None, None, Some(is_favorite)) => raw::execute(
            pool,
            "UPDATE collections SET is_favorite = $2, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(is_favorite),
        )
        .await?,
        (Some(name), Some(label), None, None) => raw::execute(
            pool,
            "UPDATE collections SET name = $2, label = $3, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(name).bind(label),
        )
        .await?,
        (Some(name), None, Some(zim_ids), None) => raw::execute(
            pool,
            "UPDATE collections SET name = $2, zim_ids = $3, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(name).bind(zim_ids),
        )
        .await?,
        (Some(name), None, None, Some(is_favorite)) => raw::execute(
            pool,
            "UPDATE collections SET name = $2, is_favorite = $3, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(name).bind(is_favorite),
        )
        .await?,
        (None, Some(label), Some(zim_ids), None) => raw::execute(
            pool,
            "UPDATE collections SET label = $2, zim_ids = $3, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(label).bind(zim_ids),
        )
        .await?,
        (None, Some(label), None, Some(is_favorite)) => raw::execute(
            pool,
            "UPDATE collections SET label = $2, is_favorite = $3, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(label).bind(is_favorite),
        )
        .await?,
        (None, None, Some(zim_ids), Some(is_favorite)) => raw::execute(
            pool,
            "UPDATE collections SET zim_ids = $2, is_favorite = $3, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(zim_ids).bind(is_favorite),
        )
        .await?,
        (Some(name), Some(label), Some(zim_ids), None) => raw::execute(
            pool,
            "UPDATE collections SET name = $2, label = $3, zim_ids = $4, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(name).bind(label).bind(zim_ids),
        )
        .await?,
        (Some(name), Some(label), None, Some(is_favorite)) => raw::execute(
            pool,
            "UPDATE collections SET name = $2, label = $3, is_favorite = $4, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(name).bind(label).bind(is_favorite),
        )
        .await?,
        (Some(name), None, Some(zim_ids), Some(is_favorite)) => raw::execute(
            pool,
            "UPDATE collections SET name = $2, zim_ids = $3, is_favorite = $4, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(name).bind(zim_ids).bind(is_favorite),
        )
        .await?,
        (None, Some(label), Some(zim_ids), Some(is_favorite)) => raw::execute(
            pool,
            "UPDATE collections SET label = $2, zim_ids = $3, is_favorite = $4, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(label).bind(zim_ids).bind(is_favorite),
        )
        .await?,
        (Some(name), Some(label), Some(zim_ids), Some(is_favorite)) => raw::execute(
            pool,
            "UPDATE collections SET name = $2, label = $3, zim_ids = $4, is_favorite = $5, updated_at = now() WHERE id = $1",
            |q| q.bind(id).bind(name).bind(label).bind(zim_ids).bind(is_favorite),
        )
        .await?,
    };
    Ok(if updated > 0 {
        UpdateOutcome::Updated
    } else {
        UpdateOutcome::NotFound
    })
}

/// Outcome of [`delete_collection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    /// The row was deleted.
    Deleted,
    /// No collection with that id.
    NotFound,
}

/// Delete a collection by id.
pub async fn delete_collection(pool: &Pool, id: i32) -> Result<DeleteOutcome> {
    let deleted = raw::execute(pool, "DELETE FROM collections WHERE id = $1", |q| {
        q.bind(id)
    })
    .await?;
    Ok(if deleted > 0 {
        DeleteOutcome::Deleted
    } else {
        DeleteOutcome::NotFound
    })
}

/// BUG-9: true when the SQLSTATE is a unique-constraint violation (23505).
/// Same convention as `crate::error::sqlstate_status`.
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .is_some_and(|db| db.code().as_deref() == Some("23505"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::is_unique_violation;

    /// A non-DB error (e.g. pool timeout) is never a unique violation.
    #[test]
    fn is_unique_violation_rejects_non_db_errors() {
        assert!(!is_unique_violation(&sqlx::Error::PoolTimedOut));
    }
}
