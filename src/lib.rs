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

/// `docs/ARCHITECTURE.md` freshness tripwires (2026-09 review, Arch Major-1):
/// three independent facts drifted silently in the doc (AppState field count,
/// pool topology, migration range). These two machine-checkable facts get a
/// cheap include_str! guard — the pool-topology prose stays a review item.
#[cfg(test)]
// LINT-3 (doc-freshness tripwires): a stale doc must fail the suite LOUDLY —
// the .expect()/.unwrap() panics below are the whole point, not accidental
// panics, so the lints are grandfathered for this module.
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod docs_freshness {
    /// The doc's `migrations/` (001–NNN) range must end at the newest
    /// embedded migration, and no `migrations/*.sql` file may exist on disk
    /// without an embedded entry (or vice versa by count).
    #[test]
    fn architecture_migration_range_matches_migrations() {
        let doc = include_str!("../docs/ARCHITECTURE.md");
        let start = doc
            .find("migrations/` (")
            .expect("doc must name the migrations/ range");
        let window = &doc[start..start + 40];
        let dash = window.find('\u{2013}').expect("001–NNN en-dash range");
        let doc_max: i64 = window[dash + 3..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .expect("numeric range end");
        let code_max = crate::db::migrate::MIGRATIONS
            .iter()
            .map(|(name, _)| name[..3].parse::<i64>().expect("NNN_ prefix"))
            .max()
            .expect("non-empty MIGRATIONS");
        assert_eq!(
            doc_max, code_max,
            "ARCHITECTURE.md says migrations 001–{doc_max:03}, but MIGRATIONS tops \
             out at {code_max:03} — update the doc"
        );
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

    /// The doc's "N-field shared state" must match the actual `AppState`
    /// field count in `src/state.rs`.
    #[test]
    fn architecture_appstate_field_count_matches() {
        let doc = include_str!("../docs/ARCHITECTURE.md");
        let marker = "-field shared state";
        let start = doc
            .find(marker)
            .expect("doc must name the AppState field count");
        let doc_count: usize = doc[..start]
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<String>()
            .parse()
            .expect("digits immediately before '-field shared state'");
        let state = include_str!("state.rs");
        let block = state
            .split_once("pub struct AppState {")
            .expect("AppState struct")
            .1
            .split_once("\n}\n")
            .expect("AppState closing brace")
            .0;
        let fields = block
            .lines()
            .filter(|l| {
                l.starts_with("    pub ")
                    && l.split_whitespace()
                        .nth(1)
                        .is_some_and(|t| t.ends_with(':'))
            })
            .count();
        assert_eq!(
            doc_count, fields,
            "ARCHITECTURE.md says {doc_count}-field AppState, but src/state.rs \
             has {fields} fields — update the doc"
        );
    }
}
