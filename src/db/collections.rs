//! User-collection data access (ARCH M1 repository extraction).
//!
//! The `collections` table's SQL lives here — mirroring `db/downloads.rs` and
//! `db/qid.rs` — so the HTTP handlers (`serve/handlers/settings.rs`) stay thin:
//! they validate input, resolve ZIM names ↔ ids via the in-memory ZIM registry,
//! and map the domain outcomes to HTTP responses. The repo works in
//! `zim_ids` (`i32`); id↔name resolution is a presentation concern (it reads
//! the ZIM registry, not the DB), so it stays in the handler.
//!
//! Query layer: the SeaORM query builder (entity `find` / active-model
//! `insert` / `update_many` / `delete_by_id`) over the shared pool via
//! [`crate::db::sea_orm_db`]. `updated_at = now()` stays a *server-side*
//! timestamp via `Expr::cust("now()")`, so the SQL semantics match the old
//! raw statements exactly.
use crate::db::entities::collections::{ActiveModel, Column, Entity};
use crate::db::{pool::Pool, sea_orm_db};
use crate::error::{Error, Result};
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ActiveValue, EntityTrait, QueryFilter, QueryOrder};

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

/// All collections, ordered by name.
pub async fn list_collections(pool: &Pool) -> Result<Vec<CollectionRow>> {
    let db = sea_orm_db(pool);
    let models = Entity::find()
        .order_by_asc(Column::Name)
        .all(&db)
        .await
        .map_err(Error::from)?;
    Ok(models
        .into_iter()
        .map(|m| CollectionRow {
            id: m.id,
            name: m.name,
            label: m.label,
            zim_ids: m.zim_ids,
            is_favorite: m.is_favorite,
            created_at: m.created_at,
            updated_at: m.updated_at,
        })
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
    let db = sea_orm_db(pool);
    // Only the four caller-provided columns are set; `created_at` /
    // `updated_at` stay `NotSet` and keep the table's `now()` defaults
    // (same column list as the old raw INSERT).
    let model = ActiveModel {
        name: ActiveValue::Set(name.to_owned()),
        label: ActiveValue::Set(label.to_owned()),
        zim_ids: ActiveValue::Set(zim_ids.to_vec()),
        is_favorite: ActiveValue::Set(is_favorite),
        ..Default::default()
    };
    match model.insert(&db).await {
        Ok(m) => Ok(InsertOutcome::Inserted(m.id)),
        Err(e) if orm_is_unique_violation(&e) => Ok(InsertOutcome::Duplicate),
        Err(e) => Err(Error::from(e)),
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
    // Dynamic `UPDATE`: a `NotSet` active value is omitted from the SET list,
    // so only the provided fields are written. `updated_at` stays server-side
    // (`now()`) — it is added separately, not via the active model.
    let mut model = ActiveModel {
        ..Default::default()
    };
    if let Some(nm) = &fields.name {
        model.name = ActiveValue::Set(nm.clone());
    }
    if let Some(l) = &fields.label {
        model.label = ActiveValue::Set(l.clone());
    }
    if let Some(z) = &fields.zim_ids {
        model.zim_ids = ActiveValue::Set(z.clone());
    }
    if let Some(f) = &fields.is_favorite {
        model.is_favorite = ActiveValue::Set(*f);
    }
    // Unreachable in practice (the handler 400s on an all-`None` body); a
    // benign fallback so we never build a malformed `UPDATE`.
    if !model.name.is_set()
        && !model.label.is_set()
        && !model.zim_ids.is_set()
        && !model.is_favorite.is_set()
    {
        return Ok(UpdateOutcome::NotFound);
    }
    let db = sea_orm_db(pool);
    let stmt = Entity::update_many()
        .set(model)
        .col_expr(Column::UpdatedAt, Expr::cust("now()"));
    let updated = stmt
        .filter(Expr::col(Column::Id).eq(Expr::val(id)))
        .exec(&db)
        .await
        .map_err(Error::from)?
        .rows_affected;
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
    let db = sea_orm_db(pool);
    let deleted = Entity::delete_by_id(id)
        .exec(&db)
        .await
        .map_err(Error::from)?
        .rows_affected;
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

/// BUG-9 (SeaORM flavor): true when a SeaORM error wraps a sqlx
/// unique-constraint violation (SQLSTATE 23505). SeaORM runs on the same
/// sqlx driver, so the violation surfaces as
/// `DbErr::{Exec, Query}(RuntimeErr::SqlxError(…))`.
pub(crate) fn orm_is_unique_violation(e: &sea_orm::DbErr) -> bool {
    match e {
        sea_orm::DbErr::Exec(sea_orm::RuntimeErr::SqlxError(sqlx_err))
        | sea_orm::DbErr::Query(sea_orm::RuntimeErr::SqlxError(sqlx_err)) => {
            is_unique_violation(sqlx_err)
        }
        _ => false,
    }
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
