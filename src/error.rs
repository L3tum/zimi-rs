//! Top-level application error type and its HTTP mapping.
//!
//! `Error` is the crate-wide error enum; `status_and_message()` maps each
//! variant to an HTTP status + user-safe message — client errors keep their
//! text, while internal/DB errors are redacted so details never leak.
use thiserror::Error;

/// Classification of a [`Error::Torrent`]: `SessionExpired` marks a 401/403
/// (stale session — a re-login would help) so the poller clears the cached
/// client; `Other` is any other qBittorrent / upload / download failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentKind {
    SessionExpired,
    Other,
}

/// Top-level application error type.
#[derive(Error, Debug)]
pub enum Error {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("ZIM error: {0}")]
    Zim(String),

    #[error("config error: {0}")]
    Config(String),

    /// Error from an upstream HTTP request (downloads, API calls to external
    /// services). Distinct from [`Error::Torrent`] which is specific to the
    /// qBittorrent backend. Both map to HTTP 502 to the client.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Error from the qBittorrent backend (login failures, API errors, etc.).
    /// Maps to HTTP 502 Bad Gateway since it represents a failure of the
    /// upstream torrent-management service.
    #[error("torrent error: {msg}")]
    Torrent { kind: TorrentKind, msg: String },

    #[error("embedding error: {0}")]
    Embedding(String),

    #[error("MCP error: {0}")]
    Mcp(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Catch-all for failures that fit no more specific variant.
    ///
    /// **Containment contract (2026-09-04, ARCH minor #2):** construct via
    /// `Error::Internal(anyhow::anyhow!(...))` directly, never via `?` — this
    /// variant deliberately has *no* `From<anyhow::Error>` impl, so an
    /// anyhow error from CLI/startup code cannot silently leak into the
    /// HTTP error path.
    ///
    /// Allowed: blocking-task joins (`tokio::task::JoinError`), pool
    /// configuration failures (building the pool from options), and genuinely
    /// unclassifiable I/O.
    /// Not allowed: anything mappable to a specific variant (DB →
    /// `Database` — including pool checkout timeouts, HTTP → `Http`, I/O →
    /// `Io`, …), and anything carrying client-facing text (this variant
    /// always maps to a redacted 500 — raw text stays in the log).
    #[error("internal error: {0}")]
    Internal(anyhow::Error),
}

/// Result alias used throughout the application.
pub type Result<T> = std::result::Result<T, Error>;

/// Map our errors to axum HTTP responses.
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = self.status_and_message();
        let body = serde_json::json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}

/// Map a Postgres SQLSTATE to an HTTP status (ARCH m2). Pure so it can be
/// unit-tested without constructing a `sqlx::Error` (whose `DatabaseError`
/// builder is fiddly). SQLSTATEs arrive as strings (`"23505"`) via
/// `sqlx::Error::as_database_error().code()`.
///
/// - 23505 unique_violation / 23503 foreign_key_violation → 409
/// - 23502 not_null_violation / 23514 check_violation → 400
/// - anything else (a genuine DB fault) → 503
pub(crate) fn sqlstate_status(code: Option<&str>) -> u16 {
    match code {
        Some("23505" | "23503") => 409,
        Some("23502" | "23514") => 400,
        _ => 503,
    }
}

/// Client-safe message for the non-23505 SQLSTATEs handled by
/// [`sqlstate_status`] (the 23505 message is derived from the constraint name
/// and is handled at the call site).
pub(crate) fn sqlstate_message(code: Option<&str>) -> &'static str {
    match code {
        Some("23502") => "missing required value",
        Some("23503") => "referenced row does not exist",
        Some("23514") => "value violates a constraint",
        // BUG-14: an unknown SQLSTATE is a database fault, not a down pool
        // (the down-pool case is `PoolTimedOut` → "database unavailable").
        _ => "database error",
    }
}

/// Table prefixes a 23505 constraint may carry, in first-match order. Add a
/// new table's prefix here (one line) so its constraint names map cleanly.
const TABLE_PREFIXES: &[&str] = &["zims_", "collections_", "articles_"];

/// Field name for a 23505 message from a Postgres constraint name (BUG-14):
/// strip `_key` then a known table prefix, so `collections_name_key` → "name"
/// (not "collections_name"). Partial-unique indexes without a suffix map
/// explicitly.
pub(crate) fn duplicate_field(constraint: &str) -> String {
    let s = constraint.strip_suffix("_key").unwrap_or(constraint);
    if s == "idx_downloads_active_url" {
        return "url".into();
    }
    let f = TABLE_PREFIXES
        .iter()
        .find_map(|p| s.strip_prefix(p))
        .unwrap_or(s);
    if f.is_empty() {
        "value".into()
    } else {
        f.into()
    }
}

impl Error {
    /// HTTP status + client-safe message for this error.
    ///
    /// Internal errors (database, upstream HTTP, pool, anything else) never
    /// leak their raw text to the client — that goes to the log instead, so
    /// constraint names, SQL state and driver internals stay server-side.
    fn status_and_message(&self) -> (axum::http::StatusCode, String) {
        use axum::http::StatusCode;

        match self {
            Error::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            Error::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            Error::InvalidInput(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Error::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            // Database errors: a pool checkout timeout is a down pool (503,
            // "database unavailable"); otherwise a server-returned SQLSTATE
            // maps to an honest client status (ARCH m2), and connection-level
            // failures (IO/TLS/protocol) are a 503 closed connection.
            // 23505 keeps its constraint-derived field message (no raw DB text).
            Error::Database(e) => sqlx_error_status_message(e),
            // Decision 2026-08-27 (B6.9): keep 502 — upstream qBittorrent /
            // download failures dominate; client-data validation via torrent
            // is the rare case, so 502 is the more honest status.
            Error::Torrent { kind, msg } => {
                // SEC-L2: raw upstream text (qBittorrent API bodies, HTTP failure
                // strings) is logged for the operator but never returned to clients.
                tracing::warn!("torrent error ({kind:?}): {msg}");
                (
                    StatusCode::BAD_GATEWAY,
                    "upstream torrent service failed".into(),
                )
            }
            _ => {
                tracing::error!("internal error: {self:?}");
                let (status, msg) = match self {
                    Error::Http(_) => (StatusCode::BAD_GATEWAY, "upstream request failed"),
                    _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
                };
                (status, msg.into())
            }
        }
    }
}

/// HTTP status + client-safe message for a raw sqlx error. A pool checkout
/// timeout is a down pool (503, "database unavailable"); a server-returned
/// SQLSTATE maps to an honest client status (ARCH m2) with 23505 keeping its
/// constraint-derived field message (no raw DB text); connection-level
/// failures (IO/TLS/protocol) are a 503 closed connection.
fn sqlx_error_status_message(e: &sqlx::Error) -> (axum::http::StatusCode, String) {
    use axum::http::StatusCode;

    if matches!(e, sqlx::Error::PoolTimedOut) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "database unavailable".into(),
        )
    } else if let Some(db) = e.as_database_error() {
        // This sqlx build's `DatabaseError::code()` yields `Option<Cow<str>>`
        // that does not outlive the call — own it before the `&str` comparisons.
        let code: Option<String> = db.code().map(|c| c.into_owned());
        if code.as_deref() == Some("23505") {
            let field = db
                .constraint()
                .map(duplicate_field)
                .unwrap_or_else(|| "value".into());
            (
                StatusCode::CONFLICT,
                format!("duplicate value for '{field}'"),
            )
        } else {
            (
                StatusCode::from_u16(sqlstate_status(code.as_deref()))
                    .unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                sqlstate_message(code.as_deref()).to_string(),
            )
        }
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "database connection closed".into(),
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn internal_errors_do_not_leak_text() {
        // Pool checkout timeout (sqlx has no separate pool error type — a
        // timed-out acquire is a `sqlx::Error::PoolTimedOut`):
        let pool = Error::Database(sqlx::Error::PoolTimedOut);
        let (status, msg) = pool.status_and_message();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(msg, "database unavailable");
        assert!(!msg.contains("postgres://"));
        assert!(!msg.contains("://"));

        let internal = Error::Internal(anyhow::anyhow!("boom: secret-detail"));
        let (status, msg) = internal.status_and_message();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(msg, "internal server error");
        assert!(!msg.contains("secret-detail"));

        let io = Error::Io(std::io::Error::other("disk: /dev/sda1 is on fire"));
        let (status, msg) = io.status_and_message();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!msg.contains("sda1"));
    }

    #[test]
    fn torrent_error_maps_to_502_generic_and_does_not_leak() {
        let err = Error::Torrent {
            kind: TorrentKind::Other,
            msg: "add_torrent failed: HTTP 500 boom".into(),
        };
        let (status, msg) = err.status_and_message();
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(msg, "upstream torrent service failed");
        assert!(!msg.contains("boom"));
    }

    #[tokio::test]
    async fn http_error_maps_to_502() {
        // loopback RST is instant — no real network involved
        let err = reqwest::Client::new()
            .get("http://127.0.0.1:1/x")
            .send()
            .await
            .expect_err("connection refused on closed loopback port");
        let (status, msg) = Error::Http(err).status_and_message();
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(msg, "upstream request failed");
    }

    #[test]
    fn client_errors_keep_their_message() {
        assert_eq!(
            Error::NotFound("ZIM 'x' not found".into()).status_and_message(),
            (StatusCode::NOT_FOUND, "ZIM 'x' not found".into()),
        );
        assert_eq!(
            Error::Forbidden("security-sensitive key".into()).status_and_message(),
            (StatusCode::FORBIDDEN, "security-sensitive key".into()),
        );
        assert_eq!(
            Error::InvalidInput("bad".into()).status_and_message().0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            Error::Conflict("dup".into()).status_and_message().0,
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn sqlstate_status_matrix() {
        // 409 conflicts.
        assert_eq!(sqlstate_status(Some("23505")), 409);
        assert_eq!(sqlstate_status(Some("23503")), 409);
        // 400 client-data violations.
        assert_eq!(sqlstate_status(Some("23502")), 400);
        assert_eq!(sqlstate_status(Some("23514")), 400);
        // No code / unknown code → server-side DB fault.
        assert_eq!(sqlstate_status(None), 503);
        assert_eq!(sqlstate_status(Some("42601")), 503);
    }

    #[test]
    fn sqlstate_message_matrix() {
        assert_eq!(sqlstate_message(Some("23502")), "missing required value");
        assert_eq!(
            sqlstate_message(Some("23503")),
            "referenced row does not exist"
        );
        assert_eq!(
            sqlstate_message(Some("23514")),
            "value violates a constraint"
        );
        assert_eq!(sqlstate_message(None), "database error");
        assert_eq!(sqlstate_message(Some("42601")), "database error");
    }

    #[test]
    fn duplicate_field_matrix() {
        assert_eq!(duplicate_field("zims_name_key"), "name");
        assert_eq!(duplicate_field("collections_name_key"), "name");
        assert_eq!(duplicate_field("articles_zim_id_path_key"), "zim_id_path");
        assert_eq!(duplicate_field("idx_downloads_active_url"), "url");
        assert_eq!(duplicate_field("collections_name"), "name");
        assert_eq!(duplicate_field("widgets_name_key"), "widgets_name");
        assert_eq!(duplicate_field("_key"), "value");
    }

    #[test]
    fn pool_timeout_never_leaks_url() {
        let pool = Error::Database(sqlx::Error::PoolTimedOut);
        let (status, msg) = pool.status_and_message();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(msg, "database unavailable");
        assert!(!msg.contains("postgres://"));
        assert!(!msg.contains("://"));
    }
}
