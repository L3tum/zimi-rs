//! `articles_staging` — UNLOGGED COPY-based bulk-insert staging table.
//!
//! The table has **no physical primary key**; `PrimaryKey` is the logical
//! row identity `(zim_id, path)` (mirrors the `articles (zim_id, path)`
//! unique constraint the staging rows are merged into) so SeaORM's
//! `EntityTrait::pk_columns` has something to name.

use sea_orm::entity::prelude::*;

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No Relation")
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "articles_staging")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub path: String,
    pub title: String,
    pub content_preview: Option<String>,
    pub snippet: String,
    pub language: String,
    pub namespace: String,
    #[sea_orm(primary_key)]
    pub zim_id: i32,
}

impl ActiveModelBehavior for ActiveModel {}
