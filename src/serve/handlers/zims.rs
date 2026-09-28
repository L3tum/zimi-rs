//! ZIM service handlers: the `/health` probe, the `/list` ZIM listing, and
//! the thin `/diagnostic` auth + delegate shell. The `/diagnostic` response
//! shapes and probe collection live in `crate::serve::diagnostics`
//! (M-diag, 2026-10 review) and are re-exported here so the
//! `crate::serve::handlers::PoolHealth` paths in `openapi.rs` keep
//! resolving.
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;

use crate::zim::ZimMeta;
use crate::AppState;

// The `/diagnostic` DTOs (M-diag, 2026-10 review): defined in
// `crate::serve::diagnostics`, re-exported for `openapi.rs` + the
// `handlers::*` glob.
pub use crate::serve::diagnostics::*;

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
    /// (M1): this instance's single-instance guards are off, so it may run
    /// alongside peers. Connected serves still invalidate each other's
    /// in-memory caches (settings, ZIM catalog) over `LISTEN`/`NOTIFY`
    /// (`db::notify`); the residual gap is one-shot CLI instances and any
    /// process whose listener is offline — their changes land here only at
    /// the next local resync or restart. Monitors polling several instances
    /// can surface the mode from this field.
    pub multi_instance: bool,
    /// True when this process started with `ZIMSERVICE_ALLOW_MULTI_DB=1`
    /// (m-7 partial opt-out): the per-database advisory lock is best-effort
    /// (a different-database deployment may share the zim_dir) while the
    /// per-zim_dir PID lock stays enforced. The degraded single-instance
    /// guarantee is visible here, not just in the startup log. Always
    /// present (additive); mutually exclusive with `multi_instance` (the full
    /// opt-out wins and suppresses both guards).
    pub multi_db: bool,
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
    let db_connected = state.probes.probe_db(state.db_read_or_primary()).await;
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
            multi_instance: crate::process::multi_instance_allowed(),
            // m-7: the partial opt-out is visible too (the full opt-out wins
            // and reports `multi_instance` instead — mirroring `cmd_serve`'s
            // `!allow_multi && config.allow_multi_db`).
            multi_db: !crate::process::multi_instance_allowed()
                && crate::process::multi_db_allowed(),
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
///
/// Thin shell (M-diag, 2026-10 review): auth, then
/// `crate::serve::diagnostics::diagnostic_snapshot` — the one place every
/// probe is gathered.
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
    Ok(Json(
        crate::serve::diagnostics::diagnostic_snapshot(&state).await,
    ))
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
