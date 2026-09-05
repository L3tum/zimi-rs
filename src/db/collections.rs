//! User-collection data access (ARCH M1 repository extraction).
//!
//! The `collections` table's SQL lives here — mirroring `db/downloads.rs` and
//! `db/qid.rs` — so the HTTP handlers (`serve/handlers/settings.rs`) stay thin:
//! they validate input, resolve ZIM names ↔ ids via the in-memory ZIM registry,
//! and map the domain outcomes to HTTP responses. The repo works in
//! `zim_ids` (`i32`); id↔name resolution is a presentation concern (it reads
//! the ZIM registry, not the DB), so it stays in the handler.
use crate::db::pool::Pool;
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

fn collection_row(r: &tokio_postgres::Row) -> CollectionRow {
    CollectionRow {
        id: r.get(0),
        name: r.get(1),
        label: r.get(2),
        zim_ids: r.get(3),
        is_favorite: r.get(4),
        created_at: r.get(5),
        updated_at: r.get(6),
    }
}

/// All collections, ordered by name.
pub async fn list_collections(pool: &Pool) -> Result<Vec<CollectionRow>> {
    let client = pool.get().await.map_err(Error::Pool)?;
    let rows = client
        .query(
            "SELECT id, name, label, zim_ids, is_favorite, created_at, updated_at FROM collections ORDER BY name",
            &[],
        )
        .await
        .map_err(Error::Database)?;
    Ok(rows.iter().map(collection_row).collect())
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
    let client = pool.get().await.map_err(Error::Pool)?;
    match client
        .query_one(
            "INSERT INTO collections (name, label, zim_ids, is_favorite) VALUES ($1, $2, $3, $4) RETURNING id",
            &[&name, &label, &zim_ids, &is_favorite],
        )
        .await
    {
        Ok(row) => Ok(InsertOutcome::Inserted(row.get(0))),
        Err(e) if is_unique_violation(e.code().cloned()) => Ok(InsertOutcome::Duplicate),
        Err(e) => Err(Error::Database(e)),
    }
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
    let client = pool.get().await.map_err(Error::Pool)?;
    // `+ Send` is required because `params` is held across the `.await` below
    // (the repo future is `Send`).
    let mut sets: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn postgres_types::ToSql + Send + Sync>> = Vec::new();
    if let Some(n) = &fields.name {
        params.push(Box::new(n.clone()));
        sets.push(format!("name = ${}", params.len()));
    }
    if let Some(l) = &fields.label {
        params.push(Box::new(l.clone()));
        sets.push(format!("label = ${}", params.len()));
    }
    if let Some(z) = &fields.zim_ids {
        params.push(Box::new(z.clone()));
        sets.push(format!("zim_ids = ${}", params.len()));
    }
    if let Some(f) = &fields.is_favorite {
        params.push(Box::new(*f));
        sets.push(format!("is_favorite = ${}", params.len()));
    }
    // Unreachable in practice (the handler 400s on an all-`None` body); a
    // benign fallback so we never build a malformed `UPDATE`.
    if sets.is_empty() {
        return Ok(UpdateOutcome::NotFound);
    }
    params.push(Box::new(id));
    let sql = format!(
        "UPDATE collections SET updated_at = now(), {} WHERE id = ${}",
        sets.join(", "),
        params.len()
    );
    let refs: Vec<&(dyn postgres_types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn postgres_types::ToSql + Sync))
        .collect();
    let updated = client.execute(&sql, &refs).await.map_err(Error::Database)?;
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
    let client = pool.get().await.map_err(Error::Pool)?;
    let deleted = client
        .execute("DELETE FROM collections WHERE id = $1", &[&id])
        .await
        .map_err(Error::Database)?;
    Ok(if deleted > 0 {
        DeleteOutcome::Deleted
    } else {
        DeleteOutcome::NotFound
    })
}

/// BUG-9: true when the SQLSTATE is a unique-constraint violation (23505).
/// Pure — it takes the code, not the driver `Error` (whose constructor is
/// not public), so it can be unit-tested without a live connection. Same
/// convention as `crate::error::sqlstate_status`.
pub(crate) fn is_unique_violation(code: Option<tokio_postgres::error::SqlState>) -> bool {
    code == Some(tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::is_unique_violation;

    #[test]
    fn is_unique_violation_matches_only_23505() {
        use tokio_postgres::error::SqlState as S;
        assert!(is_unique_violation(Some(S::UNIQUE_VIOLATION)));
        assert!(!is_unique_violation(Some(S::FOREIGN_KEY_VIOLATION)));
        assert!(!is_unique_violation(None));
    }
}
