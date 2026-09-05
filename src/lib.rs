//! zimservice — Cross-ZIM search and serve platform
//!
//! Architecture:
//! - Postgres FTS + pg_trgm + optional pgvector for search
//! - qBittorrent for download management (hardlink-based file sharing)
//! - deformat for SIMD-accelerated HTML→text extraction
//! - rayon for parallel ZIM indexing
//! - MCP server for AI agent integration
//!
//! # API stability
//!
//! This crate is currently `0.x`: **no public API is stable across minor
//! versions**. The module marked `#[doc(hidden)]` (`testing`) is internal
//! test support, not a public API. Treat the remaining public surface as
//! internal until a `1.0` is cut, when the stability guarantees will be
//! documented explicitly.

pub mod config;
pub mod content;
pub mod db;
pub mod embed;
pub mod error;
pub mod health;
pub mod mcp;
pub mod netguard;
pub mod search;
pub mod serve;
pub mod settings;
/// Startup orchestration (state + single-instance guards). Moved from the
/// binary so its guard tests run under `--lib` and can use the lib's
/// `testing` DB-gating infrastructure (M3).
pub mod startup;
pub mod state;
/// Internal test support; hidden from rustdoc. Not a public API.
#[doc(hidden)]
pub mod testing;
pub mod torrent;
pub mod util;
pub mod zim;

// Crate-root re-exports: keep `zimservice::AppState` /
// `zimservice::HealthProbes` / `zimservice::redact_url` stable (ARCH-7).
pub use health::{DegradationTracker, HealthProbes};
pub use state::AppState;
pub use util::redact_url;
