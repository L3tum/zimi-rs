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
//! versions**. The `testing` module is internal test support, not a public
//! API: it is `#[doc(hidden)]` *and* `cfg`-gated behind the `testing` cargo
//! feature, so it is not compiled into the production library build at all
//! (the feature is enabled for `#[cfg(test)]` unit tests and for integration
//! tests via the self-referential dev-dependency). Treat the remaining
//! public surface as internal until a `1.0` is cut, when the stability
//! guarantees will be documented explicitly.

/// Access-control policy objects shared across layers (rate limiter, per-IP
/// auth-failure lockout, trusted-proxy CIDR / `X-Forwarded-For` resolution).
pub mod access;
/// Process configuration: environment overrides + validation.
pub mod config;
/// Shared content service: article text reading, ZIM entry reads, and text chunking.
pub mod content;
/// Database layer: migrations, connection pooling, and raw-SQL query helpers.
pub mod db;
/// Optional embedding pipeline for semantic search (OpenAI-compatible endpoint).
pub mod embed;
/// Top-level application error type and its HTTP mapping.
pub mod error;
/// Memoized liveness probes for /health.
pub mod health;
/// MCP (Model Context Protocol) server over stdio (hand-rolled JSON-RPC 2.0).
pub mod mcp;
/// Shared SSRF / network guards for outbound HTTP.
pub mod netguard;
/// Multi-engine article search (FTS + trigram + pgvector) with score merge.
pub mod search;
/// HTTP serving: the axum router plus its middleware, handlers, and OpenAPI.
pub mod serve;
/// Runtime settings store: Postgres-backed, env-locked, with typed accessors.
pub mod settings;
/// Startup orchestration (state + single-instance guards). Moved from the
/// binary so its guard tests run under `--lib` and can use the lib's
/// `testing` DB-gating infrastructure (M3).
pub mod startup;
/// Shared application state passed to all axum handlers.
pub mod state;
/// Internal test support. Not a public API: gated behind the `testing` cargo
/// feature (active for unit tests via `cfg(test)`), hidden from rustdoc, and
/// absent from the production library build.
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub mod testing;
/// qBittorrent integration: REST client, download helpers, OPDS, and poller.
pub mod torrent;
/// URL helpers (re-exported at the crate root).
pub mod util;
/// ZIM archive manager: on-disk discovery, index state, and DB reconciliation.
pub mod zim;

// Crate-root re-exports: keep `zimservice::AppState` /
// `zimservice::HealthProbes` / `zimservice::redact_url` stable (ARCH-7).
pub use health::{DegradationTracker, HealthProbes};
pub use state::AppState;
pub use util::redact_url;
