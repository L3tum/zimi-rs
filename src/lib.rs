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

/// Doc-adjacent tripwires (2026-09 review, Arch Major-1), trimmed by the
/// M1 follow-up (2026-09 review) to the checks that stay zero-maintenance:
/// the migration directory-count tripwire below (every `migrations/*.sql`
/// file on disk has an embedded entry and vice versa, by count —
/// `include_str!` already fails the build for the missing-file direction)
/// plus the module's lint self-test (the LINT-3 allow below is the
/// grandfathered, self-documented exception for the tripwire's deliberate
/// `.expect()` panics). The old mirrored-by-name design — the consts
/// `state::APP_STATE_FIELD_COUNT` and `db::migrate::LATEST_MIGRATION` plus
/// the tests that policed the doc prose about them — is gone: pinning
/// machine-checkable counts in the docs forced 4-5 coordinated edits per new
/// migration or `AppState` field. The doc prose (`docs/ARCHITECTURE.md`) is
/// descriptive now and deliberately no longer machine-pinned, and `AppState`
/// field drift is still caught at compile time by every exhaustive
/// struct-literal construction site (startup, test harnesses, benches).
#[cfg(test)]
// LINT-3 (2026-09 sweep): the tripwire must fail the suite LOUDLY — the
// .expect() panics below are the whole point, not accidental panics, so
// the lints are grandfathered for this module.
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod docs_freshness {

    /// No `migrations/*.sql` file may exist on disk without an embedded
    /// entry (or vice versa, by count) — `include_str!` already fails the
    /// build if an embedded file is missing; this catches the other
    /// direction.
    #[test]
    fn embedded_migration_count_matches_migrations_directory() {
        let on_disk = std::fs::read_dir("migrations")
            .expect("migrations dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".sql"))
            .count();
        assert_eq!(
            on_disk,
            crate::db::migrate::MIGRATION_COUNT,
            "migrations/ on disk ({on_disk}) != embedded MIGRATIONS \
             ({}): every NNN_*.sql must be embedded in src/db/migrate.rs",
            crate::db::migrate::MIGRATION_COUNT
        );
    }
}
