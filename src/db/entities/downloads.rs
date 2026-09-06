//! `downloads` — download-queue tracking (torrent + direct `.zim` downloads).
//!
//! Post-migration shape (001 + 002/006): `ratio`, `up_speed_bps`, `num_seeds`
//! are the seeding-visibility columns. Partial unique indexes
//! (`idx_downloads_active_url`, `uq_downloads_active_name`) are schema
//! constraints only — they surface as 23505 on conflicting inserts.

use sea_orm::entity::prelude::*;

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No Relation")
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "downloads")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i32,
    pub name: String,
    pub url: String,
    /// Torrent `info_hash`, or `NULL` for direct downloads.
    pub hash: Option<String>,
    /// `queued|downloading|complete|error|cancelled|seeding` (free-form).
    pub status: String,
    pub progress: f32,
    pub speed_bps: Option<i64>,
    pub eta_secs: Option<i64>,
    pub file_path: Option<String>,
    pub error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub ratio: Option<f32>,
    pub up_speed_bps: Option<i64>,
    pub num_seeds: Option<i64>,
}

impl ActiveModelBehavior for ActiveModel {}
