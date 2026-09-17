//! HTTP route handlers: ZIMs, search, content, settings, and downloads.
//!
//! Thin axum handlers over `AppState`; each submodule carries its own
//! `#[utoipa::path]` OpenAPI docs, re-exported here so `serve::openapi`
//! resolves `crate::serve::handlers::*` paths.
/// Content handlers: article reading, RAG chunks, and raw content.
pub mod content;
/// Download-queue handlers: list, enqueue (SSRF guard, URL dedup), and cancel.
pub mod downloads;
/// Search handlers: full-text / fuzzy / semantic search, suggestions, random articles,
/// cross-language links.
pub mod search;
/// Settings and collections handlers (auth-aware redaction, per-ZIM settings, user collections).
pub mod settings;
/// Embedded Web UI handlers: the `include_str!` HTML pages and their JS files.
pub mod web;
/// ZIM service handlers: the /health probe and the /list ZIM listing.
pub mod zims;

pub use content::*;
pub use downloads::*;
pub use search::*;
pub use settings::*;
pub use web::*;
pub use zims::*;

// ─── Shared response schemas ─────────────────────────────────────────────────

/// Generic `{ "ok": true }` acknowledgement for mutating endpoints that create no resource.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct OkResponse {
    /// Always true.
    pub ok: bool,
}

/// Acknowledgement that also returns the affected resource id.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct OkIdResponse {
    /// Always true.
    pub ok: bool,
    /// The affected resource id.
    pub id: i32,
}

/// Creation response: the new resource id.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct CreatedIdResponse {
    /// New resource id.
    pub id: i32,
}
