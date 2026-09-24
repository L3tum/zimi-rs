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

/// `docs/ARCHITECTURE.md` freshness tripwires (2026-09 review, Arch
/// Major-1; reworked when the review follow-up found the include_str!
/// window-parsing of the doc prose too brittle): the doc's machine-checkable
/// values — the AppState field count and the migration range — are each
/// mirrored **by name** from a source-of-truth const defined where the value
/// lives (`state::APP_STATE_FIELD_COUNT`, `db::migrate::LATEST_MIGRATION`).
/// The tests below compare those consts against the actual code state — no
/// file reads of the doc, no string parsing. The pool-topology prose stays a
/// review item.
#[cfg(test)]
// LINT-3 (doc-freshness tripwires): a stale doc must fail the suite LOUDLY —
// the .expect()/.unwrap() panics below are the whole point, not accidental
// panics, so the lints are grandfathered for this module.
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod docs_freshness {
    use crate::AppState;
    /// The doc's `migrations/` (001–NNN) range is mirrored **by name** from
    /// `db::migrate::LATEST_MIGRATION` (the doc prose is human-maintained;
    /// this is the machine check). The const must match the newest embedded
    /// migration number.
    #[test]
    fn migration_range_const_matches_embedded_migrations() {
        let code_max = crate::db::migrate::MIGRATIONS
            .iter()
            .map(|(name, _)| name[..3].parse::<i32>().expect("NNN_ prefix"))
            .max()
            .expect("non-empty MIGRATIONS");
        assert_eq!(
            crate::db::migrate::LATEST_MIGRATION,
            code_max,
            "db::migrate::LATEST_MIGRATION (the value docs/ARCHITECTURE.md \
             mirrors by name) is {}, but MIGRATIONS tops out at {code_max:03} \
             — bump the const (and the doc) in the same change",
            crate::db::migrate::LATEST_MIGRATION
        );
    }

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

    /// The doc's "N-field shared state" is mirrored **by name** from
    /// `state::APP_STATE_FIELD_COUNT` (the doc prose is human-maintained;
    /// this is the machine check). The const must match the real `AppState`
    /// field count: the exhaustive destructure below is a compile error if a
    /// field is added, removed, or renamed without updating the probe, and
    /// the array length ties the probe to the const.
    #[tokio::test]
    async fn appstate_field_count_matches_the_const() {
        let state = dead_state_for_field_probe();
        let AppState {
            db,
            db_read,
            db_bg,
            settings,
            zims,
            search,
            torrent,
            rate_limiter,
            probes,
            auth_lockout,
            degradation,
            build_probe,
            index_building,
            notify,
        } = &state;
        let fields: Vec<&dyn std::any::Any> = vec![
            db,
            db_read,
            db_bg,
            settings,
            zims,
            search,
            torrent,
            rate_limiter,
            probes,
            auth_lockout,
            degradation,
            build_probe,
            index_building,
            notify,
        ];
        assert_eq!(
            fields.len(),
            crate::state::APP_STATE_FIELD_COUNT,
            "state::APP_STATE_FIELD_COUNT (the value docs/ARCHITECTURE.md \
             mirrors by name) is {}, but AppState actually has {} fields — \
             bump the const (and the doc)",
            crate::state::APP_STATE_FIELD_COUNT,
            fields.len()
        );
    }

    /// A throwaway `AppState` for the field-count probe: a lazy pool that
    /// never connects, in-memory settings, no I/O of any kind.
    fn dead_state_for_field_probe() -> AppState {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://user:pass@127.0.0.1:1/nodb")
            .expect("lazy pool needs no live server");
        let settings = crate::settings::SettingsCache::new_with_map(
            pool.clone(),
            crate::settings::default_settings(),
            std::collections::HashMap::new(),
        );
        let zims = crate::zim::ZimManager::new(
            std::path::PathBuf::from("/nonexistent-appstate-probe"),
            pool.clone(),
        );
        let search = crate::search::SearchEngine::new(
            pool.clone(),
            settings.clone(),
            crate::health::DegradationTracker::default(),
        );
        AppState {
            db_read: None,
            db_bg: pool.clone(),
            db: pool,
            settings,
            zims,
            search,
            torrent: crate::torrent::QbitClientCache::new(),
            rate_limiter: std::sync::Arc::new(crate::access::ratelimit::RateLimiterHandle::new()),
            probes: crate::health::HealthProbes::default(),
            auth_lockout: std::sync::Arc::new(Default::default()),
            degradation: crate::health::DegradationTracker::default(),
            build_probe: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            index_building: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            notify: None,
        }
    }

    /// The PERF-4 notes in the two docs asserted a false invariant (014
    /// stays unused, any future migration is 015+); both sections are now
    /// historical records. Neither doc may claim the number is unused, and
    /// both must name the historical anchor: 014 went to
    /// `migrations/014_drop_dead_schema.sql`.
    #[test]
    fn migration_014_docs_are_historical() {
        let perf = include_str!("../docs/perf-notes.md");
        let globals = include_str!("../docs/process_globals.md");
        for (name, doc) in [
            ("docs/perf-notes.md", perf),
            ("docs/process_globals.md", globals),
        ] {
            for stale in [
                "remains unused",
                "permanently unused",
                "last applied migration is 013",
            ] {
                assert!(
                    !doc.contains(stale),
                    "{name} still claims the 014 number is `{stale}` — 014 was \
                     allocated to 014_drop_dead_schema.sql; keep the section a \
                     historical record (no unused/next-number claims)"
                );
            }
            assert!(
                doc.contains("014_drop_dead_schema"),
                "{name} must name migrations/014_drop_dead_schema.sql as the \
                 historical anchor for the 014 number"
            );
        }
    }

    /// `tests/integration/trgm_plan.rs` and `benches/common/mod.rs` each
    /// hand-define the same 40-word corpus so the plan gate and the benches
    /// measure identical data — nothing else enforces that lockstep.
    /// Compare the three shared literals as text; a one-sided edit fails.
    #[test]
    fn trgm_corpus_lockstep() {
        let test_src = include_str!("../tests/integration/trgm_plan.rs");
        let bench_src = include_str!("../benches/common/mod.rs");
        for (name, a, b) in [
            ("WORDS", corpus_words(test_src), corpus_words(bench_src)),
            ("PROBE", corpus_probe(test_src), corpus_probe(bench_src)),
            (
                "EMBED_BATCH",
                corpus_batch(test_src),
                corpus_batch(bench_src),
            ),
        ] {
            assert_eq!(
                a, b,
                "corpus constant {name} drifted between \
                 tests/integration/trgm_plan.rs and benches/common/mod.rs — \
                 the plan gate and the benches must measure identical data; \
                 edit both"
            );
        }
    }

    /// WORDS array body (between `= [` and `];`), whitespace-normalized.
    fn corpus_words(src: &str) -> String {
        let start = src.find("const WORDS").expect("const WORDS");
        let open = start + src[start..].find(" = [").expect("WORDS literal") + 4;
        let close = src[open..].find(";").expect("WORDS close");
        src[open + 1..open + close]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The PROBE string literal after `const PROBE`.
    fn corpus_probe(src: &str) -> String {
        let start = src.find("const PROBE").expect("const PROBE");
        let q1 = start + src[start..].find('"').expect("PROBE open quote");
        let q2 = q1 + 1 + src[q1 + 1..].find('"').expect("PROBE close quote");
        src[q1 + 1..q2].to_string()
    }

    /// The EMBED_BATCH integer literal after `const EMBED_BATCH`.
    fn corpus_batch(src: &str) -> String {
        let start = src.find("const EMBED_BATCH").expect("const EMBED_BATCH");
        let eq = start + src[start..].find(" = ").expect("EMBED_BATCH value");
        src[eq + 3..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect()
    }
}
