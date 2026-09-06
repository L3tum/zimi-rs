//! `collections` — user-defined groups of ZIMs.

use sea_orm::entity::prelude::*;

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No Relation")
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "collections")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i32,
    pub name: String,
    pub label: String,
    /// `INT[]` of `zims.id` values.
    pub zim_ids: Vec<i32>,
    pub is_favorite: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
