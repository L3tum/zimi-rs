//! ZIM service handlers: the `/health` probe and the `/list` ZIM listing.
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;

use crate::zim::ZimMeta;
use crate::AppState;

// ─── ZIM + search + content: response DTOs (OpenAPI schemas) ────────────────

/// Typed shapes for the JSON payloads returned by the handlers, used only
/// for OpenAPI documentation (the handlers themselves build the JSON with
/// `serde_json::json!`).

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    /// Always `"ok"` (or `"degraded"` when the DB probe fails).
    pub status: String,
    /// Version of the running crate.
    pub version: String,
    /// Number of ZIM archives known to this instance.
    pub zims_count: usize,
    /// Total indexed articles across all ZIMs.
    pub articles_count: i64,
    /// Postgres liveness probe result (memoized, 2 s TTL).
    pub db_connected: bool,
    /// qBittorrent liveness probe result (memoized, 2 s TTL).
    pub qbit_connected: bool,
    /// True when this process started with `ZIMSERVICE_ALLOW_MULTI_INSTANCE=1`
    /// (M1): its in-memory caches (settings, rate limiter, ZIM metadata) have
    /// no cross-instance invalidation, so settings edits made via another
    /// instance are invisible here until a restart. Monitors polling several
    /// instances can surface the mode from this field.
    pub multi_instance: bool,
    /// Branches with ≥ 3 consecutive failures (WI-5).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub degraded: Vec<String>,
    /// Vector-index build signal (M-A): a `CREATE INDEX CONCURRENTLY` on a
    /// large library can run for hours while everything else looks healthy;
    /// `building: true` is the only /health-visible sign that the service is
    /// busy building (rather than stuck). Always present (additive).
    pub index: IndexHealth,
}

/// `GET /health` → `index`: in-flight vector-index build state (M-A). Only
/// the boolean the process already tracks (`AppState::index_building`, held
/// for the duration of the build by `embed::vector_index::maybe_build_vector_index`)
/// is surfaced — no progress percentage is invented.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct IndexHealth {
    /// `true` while a vector-index build is running in this process.
    pub building: bool,
}

/// `GET /diagnostic` → `pool`: shared Postgres pool saturation signal
/// (Architecture M1). A long reindex `COPY` holding every pooled connection is
/// invisible until queued acquires hit the pool's 10 s acquire timeout and
/// surface as 503s; this snapshot is the pre-503 signal an operator can poll.
///
/// Semantics (sqlx 0.8 `Pool` — the crate exposes no `pending_acquires`):
/// `size` is the number of connections currently **active, idle included**
/// (`Pool::size`), `idle` the idle count (`Pool::num_idle`), so
/// `checked_out = size - idle`; `max_size` is the configured ceiling
/// (`PoolOptions::get_max_connections`, i.e. `db_pool_size` clamped by
/// `db::pool::effective_pool_size`). Saturation is readable as
/// `checked_out == max_size && idle == 0`.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct PoolHealth {
    /// Active connections (checked-out + idle); `Pool::size()`.
    pub size: u32,
    /// Idle (not in use) connections; `Pool::num_idle()`.
    pub idle: u32,
    /// Configured maximum connection count (`PoolOptions::get_max_connections`).
    pub max_size: u32,
    /// `size - idle` — connections currently held by in-flight queries.
    pub checked_out: u32,
}

/// Snapshot the shared pool's saturation state (Architecture M1). Pure sync
/// reads of sqlx's atomic pool state — no connection is opened, so this is
/// safe on a dead/unreachable pool (the unit-test case) and costs nothing on
/// the hot path (called only by the operator-pulled `/diagnostic` handler).
fn pool_health(pool: &crate::db::Pool) -> PoolHealth {
    let size = pool.size();
    let idle = pool.num_idle() as u32;
    let max_size = pool.options().get_max_connections();
    PoolHealth {
        size,
        idle,
        max_size,
        checked_out: size.saturating_sub(idle),
    }
}

/// `GET /diagnostic` → `vector_index`: the partial `idx_articles_embedding`
/// degradation state. A missing (or invalid) vector index makes every
/// vector search a brute-force sequential cosine scan — the most expensive
/// query shape in the system, invisible from `/health`; the `degraded` note
/// (≥ 10k embedded rows, no valid index) is the operator-facing signal.
/// Omitted entirely when the catalog probe itself fails (DB unreachable —
/// `/health`'s `db_connected` already reports that).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct VectorIndexDiagnostic {
    /// `articles` rows with a non-NULL embedding.
    pub embedded_rows: i64,
    /// Catalog state of `idx_articles_embedding`: `absent`, `present`
    /// (valid and usable), or `present_invalid` (a failed/in-progress
    /// `CONCURRENTLY` build that must be dropped before a fresh build takes
    /// effect).
    pub index: crate::embed::VectorIndexState,
    /// Present only when `index` is not `present` and `embedded_rows >=
    /// 10 000`: the planner cannot use a vector index, so vector search is
    /// a full sequential cosine scan over `embedded_rows` rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded: Option<String>,
}

/// `GET /diagnostic` → `checkout_wait`: explicit `pool.acquire()` wait times
/// since process start (Architecture M1). The number behind the "the shared
/// 20-connection pool is the main scalability limiter" revisit decision (the
/// PERF-10 trigger in `search::SearchEngine::search`): a non-trivial
/// `max_us`/`avg_us` under sustained search QPS is the signal to move the
/// search arms onto separate connections. Only explicit checkouts are
/// measured (the `search` DB arm + vector ANN seek, `suggest`, the
/// `ensure_trgm` slow path, and the `/health` db probe); failed acquires
/// (10 s timeout → 503) are not — those surface via the `pool` saturation
/// snapshot instead.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct CheckoutWait {
    /// Explicit checkouts completed since process start.
    pub count: u64,
    /// Longest single checkout wait since start, microseconds.
    pub max_us: u64,
    /// Average checkout wait since start, microseconds (0 when `count == 0`).
    pub avg_us: u64,
}

/// Snapshot the process-global explicit-checkout-wait metric (Architecture
/// M1). Pure atomic reads — no connection is opened, so this is safe on a
/// dead/unreachable pool and costs nothing (called only by the
/// operator-pulled `/diagnostic` handler, like [`pool_health`]).
fn checkout_wait_snapshot() -> CheckoutWait {
    let s = crate::db::pool::checkout_wait_stats();
    CheckoutWait {
        count: s.count,
        max_us: s.max_us,
        avg_us: s.avg_us(),
    }
}

/// `GET /diagnostic` response: operator-facing introspection that `/health`
/// intentionally does not carry (it is an unauthenticated, rate-limit-exempt
/// LB probe, so it must not disclose which settings rows are corrupt).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct DiagnosticResponse {
    /// Version of the running crate.
    pub version: String,
    /// Settings keys whose stored value does not match its expected JSON type
    /// (ARCH Major #3): each silently runs on its default, and the operator
    /// needs the signal. Empty (omitted) when every value deserializes.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub settings_mismatches: Vec<String>,
    /// Shared Postgres pool saturation snapshot (Architecture M1): the
    /// pre-503 signal for a long query (e.g. a reindex `COPY`) holding the
    /// pool at its ceiling. Always present (additive).
    pub pool: PoolHealth,
    /// Partial vector-index degradation state (Architecture M1): the
    /// embedded row count, the `idx_articles_embedding` catalog state, and —
    /// when no valid index exists over ≥ 10k embedded rows — an explicit
    /// note that vector search is a sequential cosine scan. Omitted when
    /// the catalog probe itself fails (DB unreachable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_index: Option<VectorIndexDiagnostic>,
    /// Explicit pool checkout-wait metric since process start (Architecture
    /// M1): the number behind the pool-size revisit decision. Always
    /// present (additive).
    pub checkout_wait: CheckoutWait,
    /// Query-embedding LRU cache entry count (operator signal for cache
    /// effectiveness). Always present (additive).
    pub query_embed_cache_entries: usize,
}

/// `GET /list` response: all ZIM archives with metadata.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ListZimsResponse {
    /// ZIM archives with metadata.
    pub zims: Vec<ZimMeta>,
}

// ─── Health ───────────────────────────────────────────────────────────────────

/// `GET /health` — liveness probe: 503 while the Postgres probe fails (the
/// body always reports the real probe results).
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service healthy", body = HealthResponse),
        (status = 503, description = "Database unreachable")
    )
)]
pub async fn health(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    // No `Vec` clone: count + total articles in one read lock.
    let (zims_count, total_articles) = state.zims.summary();
    // Real liveness probes, memoized (2s TTL) so a burst of health checks
    // doesn't ping Postgres or qBittorrent unthrottled.
    let db_connected = crate::health::probe_db(&state.db).await;
    let qbit_connected = state.probes.probe_qbit(&state.torrent.current()).await;
    // M-health-200: the status code mirrors DB liveness so a monitor can alert
    // on 503 (DB down). The body always reports the real probe results.
    let (code, status) = if db_connected {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "degraded")
    };
    (
        code,
        Json(HealthResponse {
            status: status.into(),
            version: env!("CARGO_PKG_VERSION").into(),
            zims_count,
            articles_count: total_articles,
            db_connected,
            qbit_connected,
            // M1: process-level startup decision (env-var opt-out), so it is
            // read the same way `cmd_serve` reads it — no state field.
            multi_instance: crate::startup::multi_instance_allowed(),
            degraded: state
                .degradation
                .degraded_snapshot()
                .into_iter()
                .map(|(name, _)| name)
                .collect(),
            index: IndexHealth {
                building: state
                    .index_building
                    .load(std::sync::atomic::Ordering::SeqCst),
            },
        }),
    )
}

/// `GET /diagnostic` — operator-facing diagnostics (ARCH Major #3,
/// Security High #2): settings keys whose stored value fails its
/// `json_type` check (each silently running on its default), the shared
/// pool saturation snapshot (Architecture M1), the vector-index
/// degradation state (Architecture M1), and the explicit pool
/// checkout-wait metric (Architecture M1). Unlike `/health` it requires a
/// valid admin token — it names internal config keys and must not be
/// readable by any network peer. In open mode nobody can authenticate, so it
/// always 401s there (loopback operators read the startup warn instead).
#[utoipa::path(
    get,
    path = "/diagnostic",
    responses(
        (status = 200, description = "Diagnostics", body = DiagnosticResponse),
        (status = 401, description = "Missing or invalid admin token")
    )
)]
pub async fn diagnostic(
    State(state): State<AppState>,
    auth: crate::serve::middleware::AuthContext,
) -> Result<Json<DiagnosticResponse>, (StatusCode, Json<serde_json::Value>)> {
    if !auth.authenticated {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "authentication required — present the admin token"
            })),
        ));
    }
    // Architecture M1: surface pool saturation while it is still a warning,
    // not a 503. Emitted here (operator-pulled, authenticated) rather than on
    // the hot path: no per-request logging on the routes that would queue on
    // the pool. sqlx 0.8 exposes no pending-acquire count, so "saturated" is
    // the observable state of a full pool with zero idle connections — the
    // point at which further acquires queue and then time out (10 s → 503).
    let pool = pool_health(&state.db);
    if pool.checked_out > 0 && pool.size == pool.max_size && pool.idle == 0 {
        tracing::warn!(
            size = pool.size,
            max_size = pool.max_size,
            "db pool saturated: {}/{} in use, 0 idle — queued acquires will hit the \
             10s acquire timeout and surface as 503s (Architecture M1)",
            pool.size,
            pool.max_size
        );
    }
    // Architecture M1 (vector index): surface the degradation state — no
    // valid index at scale means every vector search is a brute-force
    // sequential cosine scan. One extra `COUNT(*)` + catalog probe is fine
    // here: this route is authenticated, operator-pulled, and off the hot
    // path. On probe failure the field is omitted (the DB is unreachable —
    // `/health`'s `db_connected` already reports that).
    let vector_index = match crate::embed::vector_index_state(&state.db).await {
        Ok((embedded_rows, index)) => Some(VectorIndexDiagnostic {
            degraded: crate::embed::vector_index_degradation_note(embedded_rows, index),
            embedded_rows,
            index,
        }),
        Err(e) => {
            tracing::warn!("vector index diagnostic probe failed: {e}");
            None
        }
    };
    // Architecture M1 (checkout wait): pure atomic reads — no connection.
    let checkout_wait = checkout_wait_snapshot();
    Ok(Json(DiagnosticResponse {
        version: env!("CARGO_PKG_VERSION").into(),
        settings_mismatches: state.settings.type_mismatches(),
        pool,
        vector_index,
        checkout_wait,
        query_embed_cache_entries: state.search.query_embed_cache_len(),
    }))
}

// ─── ZIM List ─────────────────────────────────────────────────────────────────

/// `GET /list` — all ZIM archives; `file_path` is redacted for unauthenticated callers (WP3.7).
#[utoipa::path(
    get,
    path = "/list",
    responses(
        (status = 200, description = "All ZIM archives", body = ListZimsResponse)
    )
)]
pub async fn list_zims(
    State(state): State<AppState>,
    auth: crate::serve::middleware::AuthContext,
) -> Json<ListZimsResponse> {
    // WP3.7: `file_path` is a server-local absolute path (topology). Redact it
    // for unauthenticated callers, mirroring `get_settings` (S6/BUG-10). In
    // open mode the extractor is always false → every open-mode caller is
    // unauthenticated → redacted. Authenticated callers see the real path.
    let authenticated = auth.authenticated;

    let zims = state.zims.list();
    let zims = if authenticated {
        zims
    } else {
        zims.into_iter()
            .map(|mut z| {
                z.file_path = "[redacted]".into();
                z
            })
            .collect()
    };
    Json(ListZimsResponse { zims })
}
