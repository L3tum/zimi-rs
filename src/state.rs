//! Shared application state (moved out of `lib.rs`; re-exported at the
//! crate root so `zimservice::AppState` / `crate::AppState` stay stable).
//!
//! The fields are intentionally broad for handler convenience — each handler
//! uses only 1–3 of the 14 fields.
//!
//! Regrouping the fields into sub-states (e.g. `core` / `downloads` /
//! `settings`) was evaluated and rejected: the 14 fields belong to 8
//! distinct top-level modules (`db` ×4, `settings`, `zim`, `search`,
//! `torrent`, `health` ×2, `access` ×2, plus the two `embed` build-state
//! fields `build_probe` / `index_building`) and every handler group would
//! still span more than one sub-state, so sub-states would add indirection
//! without narrowing any handler's surface. Per-handler extractor narrowing is
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
    /// Primary Postgres connection pool: authoritative for all writes and
    /// DDL, and the pool every read path uses when no read replica is
    /// configured (`read_database_url == None`).
    pub db: db::Pool,
    /// Optional read-replica pool (built from `DATABASE_URL_READ`,
    /// `db::pool::create_read_pool`); `None` when the env var is absent —
    /// the documented single-pool default. Foreground READ checkouts
    /// (search arms, `suggest`, the `ensure_trgm` slow path, `/snippet`,
    /// `/random`, the article-read DB fallback, the `/health` db probe)
    /// use this pool via [`AppState::db_read_or_primary`] when present.
    /// When the replica is unreachable those paths fail (503) rather than
    /// failing over silently; `/diagnostic` (`pool_read`) is the operator's
    /// pre-503 signal, and unsetting the env var reverts to single-pool.
    pub db_read: Option<db::Pool>,
    /// Dedicated background pool on the **primary** URL, capped at
    /// [`db::pool::BG_POOL_MAX_CONNECTIONS`] (PERF-10): the auto-embed loop
    /// and the vector-index builds check out from here, never from the
    /// shared primary pool, so a background burst can never starve
    /// foreground search/API checkouts. Same DSN → same TLS semantics; the
    /// 10 s acquire timeout bounds any wait background work can pile up.
    pub db_bg: db::Pool,
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
    /// Cross-process cache-invalidation listener (LISTEN/NOTIFY, `db::notify`):
    /// holds the spawned subscriber tasks and reports their liveness for
    /// `/diagnostic`. Cloning `AppState` shares the handle; the listener stops
    /// only when the last `AppState` (and every clone) is dropped — i.e. at
    /// process shutdown. `Option` so the listener can be absent where no
    /// database is reachable at startup (single-instance CLI paths) without
    /// changing the struct shape.
    pub notify: Option<db::notify::NotifyListener>,
}

impl AppState {
    /// The pool foreground READ checkouts should use: the read replica
    /// (`DATABASE_URL_READ`) when enabled, else the primary pool. This is
    /// the single routing point for the read-replica finding — handlers
    /// and the article-read DB fallback call this instead of naming
    /// `self.db`, so a future second read tier changes in one place.
    ///
    /// Note: the search engine does not call this — it holds its own pool
    /// (`SearchEngine::pool`), which `startup::build_state` wires to the
    /// same read-or-primary choice at construction.
    pub fn db_read_or_primary(&self) -> &db::Pool {
        self.db_read.as_ref().unwrap_or(&self.db)
    }

    /// Snapshot of the invalidation listener for `/diagnostic` (`None` when
    /// no listener is running — e.g. a CLI-only process or a startup that
    /// could not reach the database).
    pub fn notify_status(&self) -> Option<db::notify::NotifyStatusSnapshot> {
        self.notify.as_ref().map(|n| n.status())
    }
}
