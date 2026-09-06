//! `qid_index` — Wikidata Q-ID index (built in full at index time).
//!
//! Composite primary key `(zim_id, path)`.

use sea_orm::entity::prelude::*;

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No Relation")
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "qid_index")]
pub struct Model {
    #[sea_orm(primary_key, foreign_key = "zims.id", on_delete = "CASCADE")]
    pub zim_id: i32,
    #[sea_orm(primary_key)]
    pub path: String,
    /// Wikidata Q-ID as an integer (1468 = Q1468).
    pub qid: i64,
}

impl ActiveModelBehavior for ActiveModel {}
