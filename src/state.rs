//! Shared application state (moved out of `lib.rs`; re-exported at the
//! crate root so `zimservice::AppState` / `crate::AppState` stay stable).
//!
//! The fields are intentionally broad for handler convenience — each handler
//! uses only 1–3 of the 11 fields.
//!
//! Regrouping the fields into sub-states (e.g. `core` / `downloads` /
//! `settings`) was evaluated and rejected: the 11 fields belong to 8
//! distinct top-level modules (`db`, `settings`, `zim`, `search`, `torrent`,
//! `health` ×2, `access` ×2, plus the two `embed` build-state fields
//! `build_probe` / `index_building`) and every handler group would still
//! span more than one sub-state, so sub-states would add indirection without
//! narrowing any handler's surface. Per-handler extractor narrowing is
//! deferred for the same reason: each handler reads only 1–3 fields, so
//! extractors would duplicate the struct's doc comments rather than remove
//! coupling. Re-evaluate the same question whenever a field is added to or
//! removed from `AppState` (update the field/module counts stated above);
//! a handler that ever needs ≥4 fields is a sign it is doing two jobs —
//! split it.
use std::sync::Arc;

use crate::{
    access, db,
    health::{DegradationTracker, HealthProbes},
    search, settings, torrent, zim,
};

/// Shared application state, passed to all axum handlers.
#[derive(Clone)]
pub struct AppState {
    /// Postgres connection pool
    pub db: db::Pool,
    /// In-memory settings cache backed by Postgres
    pub settings: settings::SettingsCache,
    /// ZIM file manager (discovers, opens, tracks ZIM archives)
    pub zims: Arc<zim::ZimManager>,
    /// Search engine (wraps Postgres queries)
    pub search: search::SearchEngine,
    /// qBittorrent client cache (runtime-rebuilds when the connection
    /// fingerprint changes, ARCH M3); `current()` returns the connected
    /// client or `None`.
    pub torrent: torrent::QbitClientCache,
    /// Global HTTP rate limiter (live-reloads from settings)
    pub rate_limiter: Arc<access::ratelimit::RateLimiterHandle>,
    /// Memoized liveness probes for `/health`
    pub probes: HealthProbes,
    /// Per-source-IP auth-failure lockout (SEC-M1). Per-process auth state,
    /// not config data.
    pub auth_lockout: Arc<access::lockout::LockoutTracker>,
    /// Per-branch degradation tracker: surfaces which search capabilities
    /// are silently failing.
    pub degradation: DegradationTracker,
    /// Shared vector-index build-probe backoff timestamp (moved from
    /// `embed::LAST_BUILD_PROBE` process-global static to `AppState` for
    /// testability — the old `#[cfg(test)]` reset raced parallel test threads).
    pub build_probe: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// In-flight vector-index build flag: set while
    /// `embed::vector_index::maybe_build_vector_index` is executing the
    /// `CREATE INDEX CONCURRENTLY` (and its pre-build invalid-index drop),
    /// cleared on completion, failure, or cancellation. `/health` surfaces
    /// it as `index.building` — a multi-hour build is otherwise invisible.
    pub index_building: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
