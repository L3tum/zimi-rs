//! Download-queue handlers: list, enqueue (with SSRF guard and URL-dedup),
//! and cancel.
use axum::extract::{Path as AxumPath, State};
use axum::response::Json;
use serde::Deserialize;

use crate::serve::openapi::ErrorResponse;
use crate::AppState;

use super::OkIdResponse;

/// BUG-4: application-level cap for `POST /downloads` URLs (`downloads.url` is
/// TEXT). Longer URLs are rejected pre-DB — they are not a realistic download
/// target and would bloat the queue table + log lines.
const MAX_DOWNLOAD_URL_BYTES: usize = 2048;

// ─── Downloads: response/request DTOs (OpenAPI schemas) ──────────────────────

/// A single entry in the download queue (one `downloads` table row).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct Download {
    /// Download id.
    pub id: i32,
    /// Display name (also the `.part` file basename).
    pub name: String,
    /// Download URL (redacted for unauthenticated callers — SEC-L1).
    pub url: String,
    /// One of: queued, downloading, complete, seeding, error, cancelled.
    pub status: String,
    /// 0.0–1.0 (fraction of the download complete).
    pub progress: f64,
    /// Download speed in bytes/sec, when known.
    pub speed_bps: Option<i64>,
    /// Estimated time remaining in seconds, when known.
    pub eta_secs: Option<i64>,
    /// Current share ratio (torrents; null for direct downloads).
    pub ratio: Option<f64>,
    /// Upload speed in bytes/sec (torrents).
    pub up_speed_bps: Option<i64>,
    /// Number of seeding peers (torrents).
    pub num_seeds: Option<i64>,
    /// Failure message for `error`-status rows (redacted for unauthenticated callers).
    pub error: Option<String>,
    /// RFC3339 timestamp.
    pub created_at: String,
}

/// `GET /downloads` response: the download queue.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ListDownloadsResponse {
    /// Queue entries (latest 500, newest first).
    pub downloads: Vec<Download>,
}

/// `POST /downloads` response: the newly queued download.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct DownloadQueuedResponse {
    /// New download id.
    pub id: i32,
    /// Always `"queued"`.
    pub status: String,
}

// ─── Downloads ────────────────────────────────────────────────────────────────

/// `GET /downloads` — list the download queue; URLs, names, and error text
/// are redacted for unauthenticated callers (SEC-L1/SEC-L1b).
#[utoipa::path(
    get,
    path = "/downloads",
    responses(
        (status = 200, description = "Download queue", body = ListDownloadsResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn list_downloads(
    State(state): State<AppState>,
    auth: crate::serve::middleware::AuthContext,
) -> Result<Json<ListDownloadsResponse>, crate::error::Error> {
    let rows = crate::db::downloads::list_downloads(&state.db).await?;

    // SEC-L1: a full download URL is topology (LAN mirrors, internal hosts
    // when `downloads.allow_private_networks` is on). Redact it for
    // unauthenticated callers, mirroring `get_settings` (S6) and
    // `list_zims` (WP3.7); authenticated callers get the userinfo-stripped
    // URL (`redact_url`) as before. SEC-L1b: `name` (often the mirror
    // filename / internal host) and `error` (upstream failures can embed
    // internal hosts/ports) are topology too, so redact those as well for
    // unauthenticated callers.
    let downloads: Vec<Download> = rows
        .iter()
        .map(|r| {
            let url = if auth.authenticated {
                crate::redact_url(&r.url)
            } else {
                "[redacted]".to_string()
            };
            let name = if auth.authenticated {
                r.name.clone()
            } else {
                "[redacted]".to_string()
            };
            let error = if auth.authenticated {
                r.error.clone()
            } else {
                r.error.as_ref().map(|_| "[redacted]".to_string())
            };
            Download {
                id: r.id,
                name,
                url,
                status: r.status.clone(),
                progress: r.progress as f64,
                speed_bps: r.speed_bps,
                eta_secs: r.eta_secs,
                ratio: r.ratio.map(|v| v as f64),
                up_speed_bps: r.up_speed_bps,
                num_seeds: r.num_seeds,
                error,
                created_at: r.created_at.to_rfc3339(),
            }
        })
        .collect();

    Ok(Json(ListDownloadsResponse { downloads }))
}

/// `POST /downloads` request body.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct AddDownloadBody {
    /// Direct `.zim` URL or torrent URL (torrents require qBittorrent).
    pub url: String,
    /// Display name; derived from the URL when omitted.
    pub name: Option<String>,
}

/// `POST /downloads` — enqueue a direct `.zim` URL or a torrent (torrents
/// require qBittorrent). Runs the SSRF guard and rejects duplicate
/// URL/name pairs already in the queue.
#[utoipa::path(
    post,
    path = "/downloads",
    request_body = AddDownloadBody,
    responses(
        (status = 200, description = "Download queued", body = DownloadQueuedResponse),
        (status = 400, description = "Invalid URL", body = ErrorResponse),
        (status = 409, description = "A download for this URL is already queued or in \
        progress", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn add_download(
    State(state): State<AppState>,
    Json(body): Json<AddDownloadBody>,
) -> Result<Json<DownloadQueuedResponse>, crate::error::Error> {
    let url = body.url.trim().to_string();
    if url.is_empty() {
        return Err(crate::error::Error::InvalidInput("url is required".into()));
    }
    // BUG-4: reject oversized URLs before any DB touch (pre-DB guard).
    if url.len() > MAX_DOWNLOAD_URL_BYTES {
        return Err(crate::error::Error::InvalidInput(format!(
            "url is too long ({} bytes, max {})",
            url.len(),
            MAX_DOWNLOAD_URL_BYTES
        )));
    }
    // Torrent URLs (non-.zim) need qBittorrent; reject early for clear feedback.
    let is_direct = crate::torrent::is_direct_zim_url(&url);
    if !is_direct && state.torrent.current().is_none() {
        return Err(crate::error::Error::InvalidInput(
            "qBittorrent is not configured — only direct .zim URLs are supported".into(),
        ));
    }
    // SSRF guard at the API boundary for immediate feedback (the poller
    // re-validates at enqueue time as defense in depth).
    {
        let allow_private = state.settings.downloads_allow_private_networks();
        let check = if is_direct {
            crate::netguard::validate_download_url(&url, allow_private)
        } else {
            crate::netguard::assert_host_not_blocked(&url, allow_private, false)
        };
        check?;
    }

    let name = body
        .name
        .clone()
        .unwrap_or_else(|| default_download_name(&url));
    // The name is spliced into a file path under the ZIM dir — reject
    // traversal attempts before anything touches the database.
    crate::torrent::files::validate_download_name(&name)?;

    // Dedup + insert (BUG-3: one live download per URL **and** per name — the
    // `.part` file is named `{name}.part`). The 003/011 partial unique indexes
    // are the authority; the repo's `23505` path closes the concurrent-POST race.
    match crate::db::downloads::insert_download(&state.db, &name, &url).await? {
        crate::db::downloads::InsertOutcome::Inserted(id) => Ok(Json(DownloadQueuedResponse {
            id,
            status: "queued".into(),
        })),
        crate::db::downloads::InsertOutcome::Duplicate => Err(crate::error::Error::Conflict(
            "a download for this URL or name is already in progress".into(),
        )),
    }
}

/// Derive a default download name from a URL: last path segment with the
/// query string **and** the fragment stripped (B9: a URL with `#frag` would
/// otherwise produce a name containing `#`, illegal in the part file name).
/// Same strip order as `is_direct_zim_url`.
pub(crate) fn default_download_name(url: &str) -> String {
    let seg = url.rsplit('/').next().unwrap_or("unknown");
    crate::torrent::strip_query_fragment(seg).to_string()
}

/// `DELETE /downloads/{id}` — cancel a download (409 if it exists but is not
/// cancellable in its current state, 404 if it does not exist).
#[utoipa::path(
    delete,
    path = "/downloads/{id}",
    params(
        ("id" = i32, Path, description = "Download id")
    ),
    responses(
        (status = 200, description = "Download cancelled", body = OkIdResponse),
        (status = 404, description = "Download not found", body = ErrorResponse),
        (status = 409, description = "Download is not cancellable in its current \
        state", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn cancel_download(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i32>,
) -> Result<Json<OkIdResponse>, crate::error::Error> {
    match crate::db::downloads::cancel_download(&state.db, id).await? {
        crate::db::downloads::CancelOutcome::Cancelled(id) => {
            Ok(Json(OkIdResponse { ok: true, id }))
        }
        // BUG-16c: distinguish "row exists but is not cancellable" (a
        // complete/cancelled/… row) from "row does not exist". The former is a
        // conflict (409) — the caller already holds the id — the latter is 404.
        crate::db::downloads::CancelOutcome::NotCancellable { status } => {
            Err(crate::error::Error::Conflict(format!(
                "download {id} is not cancellable (status: {status})"
            )))
        }
        crate::db::downloads::CancelOutcome::NotFound => Err(crate::error::Error::NotFound(
            format!("download {id} not found"),
        )),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::default_download_name;

    /// `strip_query_fragment` keeps the piece before the **first** `?` or `#`
    /// (single `split(['?', '#'])` pass), so whichever delimiter appears first
    /// wins — a fragment before a query still strips at the `#`.
    #[test]
    fn plain_url_takes_last_path_segment() {
        assert_eq!(default_download_name("http://x.com/foo/bar.zim"), "bar.zim");
    }

    #[test]
    fn strips_query_and_fragment_from_last_segment() {
        assert_eq!(
            default_download_name("http://x.com/a/b.zim?sig=1#f"),
            "b.zim"
        );
        assert_eq!(default_download_name("http://x.com/a/b.zim?sig=1"), "b.zim");
        assert_eq!(default_download_name("http://x.com/a/b.zim#f"), "b.zim");
        // Fragment before query: still stripped at the first delimiter.
        assert_eq!(default_download_name("http://x.com/a/b.zim#f?q=1"), "b.zim");
    }

    #[test]
    fn empty_string_yields_empty_name_not_unknown() {
        // `"".rsplit('/')` yields one empty segment, so the
        // `unwrap_or("unknown")` fallback is unreachable for every input —
        // pinning the real behavior: empty input maps to `""`.
        assert_eq!(default_download_name(""), "");
    }

    #[test]
    fn trailing_slash_yields_empty_name() {
        // Regression pin: `rsplit('/')` on a trailing-slash URL yields an
        // empty last segment, so the name is `""` (empty names are rejected
        // later by `validate_download_name` in `queue_download`).
        assert_eq!(default_download_name("http://x.com/a/"), "");
    }

    #[test]
    fn url_without_slash_uses_whole_input_stripped() {
        assert_eq!(default_download_name("bare.zim"), "bare.zim");
        assert_eq!(default_download_name("bare.zim?sig=1"), "bare.zim");
    }
}
