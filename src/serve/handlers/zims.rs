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
        }),
    )
}

/// `GET /diagnostic` — operator-facing settings diagnostics (ARCH Major #3,
/// Security High #2): keys whose stored value fails its `json_type` check
/// (each silently running on its default). Unlike `/health` it requires a
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
    Ok(Json(DiagnosticResponse {
        version: env!("CARGO_PKG_VERSION").into(),
        settings_mismatches: state.settings.type_mismatches(),
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
