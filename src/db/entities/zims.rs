//! `zims` — ZIM archive metadata (one row per installed ZIM).
//!
//! Post-migration shape (001, then 002/004): `uuid`, `embed_status`,
//! `embed_progress` are dropped; embed state lives on `articles`.

use sea_orm::entity::prelude::*;

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No Relation")
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "zims")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i32,
    pub name: String,
    pub display_title: String,
    pub description: Option<String>,
    pub language: String,
    pub creator: Option<String>,
    pub publisher: Option<String>,
    pub date: Option<chrono::NaiveDate>,
    pub entry_count: i64,
    pub article_count: i64,
    pub file_path: String,
    pub file_size: i64,
    pub category: Option<String>,
    pub index_status: String,
    pub index_progress: f32,
    pub indexed_entries: i64,
    pub indexed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub embed_enabled: bool,
    pub file_mtime: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
