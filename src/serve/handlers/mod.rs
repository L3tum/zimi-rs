//! HTTP route handlers: ZIMs, search, content, settings, and downloads.
//!
//! Thin axum handlers over `AppState`; each submodule carries its own
//! `#[utoipa::path]` OpenAPI docs, re-exported here so `serve::openapi`
//! resolves `crate::serve::handlers::*` paths.
pub mod content;
pub mod downloads;
pub mod search;
pub mod settings;
pub mod web;
pub mod zims;

pub use content::*;
pub use downloads::*;
pub use search::*;
pub use settings::*;
pub use web::*;
pub use zims::*;

// ─── Shared response schemas ─────────────────────────────────────────────────

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct OkResponse {
    /// Always true.
    pub ok: bool,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct OkIdResponse {
    /// Always true.
    pub ok: bool,
    pub id: i32,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct CreatedIdResponse {
    /// New resource id.
    pub id: i32,
}
