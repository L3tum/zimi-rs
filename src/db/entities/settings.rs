//! `settings` — server settings (key-value, managed via the UI settings page).

use sea_orm::entity::prelude::*;

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No Relation")
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "settings")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub key: String,
    /// JSONB payload.
    pub value: serde_json::Value,
    /// Help text for the UI.
    pub description: Option<String>,
    /// `general|search|torrent|embedding|access`.
    pub category: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
