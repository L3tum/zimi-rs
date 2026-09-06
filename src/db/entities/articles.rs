//! `articles` — the main search table (one row per ZIM article).
//!
//! `title_lower` is a stored generated column (`lower(title)`) — it is read
//! by search queries but never written by the application.
//! `search_vector` (tsvector) and `embedding` (pgvector) are modeled as
//! `String` — see the module docs in [`super`] for the raw-SQL casting rules.

use sea_orm::entity::prelude::*;

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No Relation")
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "articles")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i64,
    #[sea_orm(foreign_key = "zims.id", on_delete = "CASCADE")]
    pub zim_id: i32,
    pub path: String,
    pub title: String,
    /// Stored generated column: `lower(title)`. Read-only from the app's view.
    pub title_lower: String,
    pub content_preview: Option<String>,
    pub snippet: String,
    /// tsvector, modeled as `String` (see module docs).
    pub search_vector: String,
    pub language: String,
    pub namespace: String,
    /// pgvector `vector(1536)`, modeled as `String` (see module docs).
    pub embedding: Option<String>,
    pub embed_model: Option<String>,
    pub embed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
