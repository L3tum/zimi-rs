//! Content handlers: thin axum handlers over the shared content service
//! (`crate::content`) for article reading and RAG chunks, plus raw content
//! serving with byte-range support and indexed snippets.
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use std::path::Path;
use std::sync::Arc;

use crate::content::{
    chunk_text, clamp_chunk_params, find_entry, read_article_payload, read_zim_text,
    ChunksResponse, ReadResponse,
};
use crate::serve::openapi::ErrorResponse;
use crate::AppState;

// ─── Snippet: response DTO (OpenAPI schema) ──────────────────────────────────

/// Response DTO for `GET /snippet`: the indexed snippet of one article.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct SnippetResponse {
    /// Name of the ZIM the article belongs to.
    pub zim: String,
    /// Article path within the ZIM.
    pub path: String,
    /// Article title.
    pub title: String,
    /// Indexed snippet text.
    pub snippet: String,
    /// Content preview (may be null).
    pub preview: Option<String>,
}

// ─── Read Article ─────────────────────────────────────────────────────────────

/// Query parameters for `GET /read` (full article text).
#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    /// Name of the ZIM to read from.
    pub zim: String,
    /// Article path within the ZIM.
    pub path: String,
    /// Truncate text at N chars (default 8000).
    pub max_length: Option<usize>,
}

/// Map a ZIM entry's MIME type to a `Content-Type` header value.
fn content_type_for(mt: &zim::MimeType) -> String {
    match mt {
        zim::MimeType::Type(s) => {
            let lower = s.to_ascii_lowercase();
            if lower.starts_with("text/") && !lower.contains("charset") {
                format!("{s}; charset=utf-8")
            } else {
                s.clone()
            }
        }
        // A resolved entry should never be a redirect, but be defensive.
        zim::MimeType::Redirect => "text/html".to_string(),
        _ => "application/octet-stream".to_string(),
    }
}

/// `GET /read` — full article text, truncated at `max_length` (default 8000
/// chars; the underlying raw read is bounded at 256 KB regardless).
#[utoipa::path(
    get,
    path = "/read",
    params(
        ("zim" = String, Query, description = "ZIM name"),
        ("path" = String, Query, description = "Article path within the ZIM"),
        ("max_length" = Option<usize>, Query, description = "Truncate text at N chars (default 8000)")
    ),
    responses(
        (status = 200, description = "Article text", body = ReadResponse),
        (status = 404, description = "ZIM or path not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn read_article(
    State(state): State<AppState>,
    Query(params): Query<ReadQuery>,
) -> Result<Json<ReadResponse>, crate::error::Error> {
    // The 8000-char default is intentionally uncapped client-side (callers may
    // request more for RAG); the underlying raw read is already bounded at 256 KB.
    let max_len = params.max_length.unwrap_or(8000);
    let payload = read_article_payload(&state, &params.zim, &params.path, max_len).await?;
    Ok(Json(payload))
}

// ─── Raw Content (web rendering) ──────────────────────────────────────────────

/// A single byte range from a `Range` header, resolved against the body size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedRange {
    /// Satisfiable inclusive byte range (0-based) within the body.
    Slice { start: u64, end: u64 },
    /// The range cannot be satisfied (e.g. start beyond EOF).
    Unsatisfiable,
}

/// A parsed byte-range numeric component (BUG-15): distinguishes an overflow
/// (`u64::MAX` boundary, RFC 7233 §3.1) from a genuinely invalid token, since
/// the two need different treatment (clamp/whole-body vs. treat as absent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeNum {
    /// A valid non-overflowing value.
    Val(u64),
    /// The token is all digits but exceeds `u64::MAX` (e.g. `18446744073709551616`).
    Overflow,
    /// The token is empty or not all digits (e.g. `abc`, `1x`).
    Invalid,
}

/// Parse one byte-range numeric component, distinguishing overflow from
/// invalid input. Empty or non-digit tokens are `Invalid`; an all-digit token
/// that overflows `u64` is `Overflow`.
fn parse_range_num(s: &str) -> RangeNum {
    let t = s.trim();
    if t.is_empty() || !t.chars().all(|c| c.is_ascii_digit()) {
        return RangeNum::Invalid;
    }
    match t.parse::<u64>() {
        Ok(v) => RangeNum::Val(v),
        Err(_) => RangeNum::Overflow,
    }
}

/// Parse a single-range `Range: bytes=...` header against a body of `total` bytes.
///
/// Returns `None` when the header is absent, not a `bytes` unit, or specifies
/// multiple ranges — callers should serve the full body in that case.
///
/// Overflow handling (BUG-15, RFC 7233 §3.1): a start offset that overflows
/// `u64` is beyond EOF → `Unsatisfiable` (416); an end offset that overflows
/// clamps to the last byte (satisfiable); a suffix length that overflows means
/// "the whole entity" → `(0, total-1)`.
fn parse_single_range(header_value: &str, total: u64) -> Option<ParsedRange> {
    let spec = header_value.trim().strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None; // multi-range: serve the full body
    }
    if total == 0 {
        return Some(ParsedRange::Unsatisfiable);
    }

    let (start, end) = match spec.split_once('-') {
        Some(("", suffix)) => {
            // suffix-byte range: the last `suffix` bytes
            match parse_range_num(suffix) {
                RangeNum::Val(0) => return Some(ParsedRange::Unsatisfiable),
                RangeNum::Val(suffix) => {
                    let start = total.saturating_sub(suffix);
                    (start, total.saturating_sub(1))
                }
                // Suffix overflows u64 → "the whole entity".
                RangeNum::Overflow => (0, total.saturating_sub(1)),
                RangeNum::Invalid => return None,
            }
        }
        Some((start_s, "")) => match parse_range_num(start_s) {
            RangeNum::Val(start) => {
                if start >= total {
                    return Some(ParsedRange::Unsatisfiable);
                }
                (start, total.saturating_sub(1))
            }
            // Start overflows u64 → beyond EOF → unsatisfiable.
            RangeNum::Overflow => return Some(ParsedRange::Unsatisfiable),
            RangeNum::Invalid => return None,
        },
        Some((start_s, end_s)) => {
            let start = match parse_range_num(start_s) {
                RangeNum::Val(v) => v,
                // Start overflows u64 → beyond EOF → unsatisfiable.
                RangeNum::Overflow => return Some(ParsedRange::Unsatisfiable),
                RangeNum::Invalid => return None,
            };
            let end = match parse_range_num(end_s) {
                RangeNum::Val(v) => v,
                // End overflows u64 → clamp to the last byte (satisfiable).
                RangeNum::Overflow => total.saturating_sub(1),
                RangeNum::Invalid => return None,
            };
            let mut end = end;
            if start >= total || end < start {
                return Some(ParsedRange::Unsatisfiable);
            }
            if end >= total {
                end = total.saturating_sub(1);
            }
            (start, end)
        }
        None => return None,
    };

    Some(ParsedRange::Slice { start, end })
}

/// Resolve a satisfiable parsed range to the exclusive byte window `[start,
/// end)` to copy out of the content buffer, or `None` when no slice applies
/// (unsatisfiable / no-range → 416 / full body). `end` is clamped to `total`
/// (PERF-1: the 206 arm copies only this window, never the whole body).
fn slice_range_bytes(total: u64, pr: Option<ParsedRange>) -> Option<(usize, usize)> {
    match pr {
        Some(ParsedRange::Slice { start, end }) => {
            let start_us = start.min(total) as usize;
            let end_us = (end + 1).min(total) as usize;
            Some((start_us, end_us))
        }
        _ => None,
    }
}

/// RFC 7232 §3.2 comparison for a strong validator used in `If-None-Match`:
/// `*` matches any existing entity; otherwise any comma-separated element that
/// equals the tag exactly matches. We only emit strong tags, so weak (`W/…`)
/// elements never match.
fn if_none_match_matches(header_value: &str, etag: &str) -> bool {
    let v = header_value.trim();
    v == "*" || v.split(',').any(|e| e.trim() == etag)
}

/// Upper bound on a single served raw entry (bytes) — a hostile multi-GB
/// entry must not spike memory per request.
const MAX_RAW_BYTES: u64 = 64 * 1024 * 1024;

/// Result of a blocking raw-content read (W6.1): either a 304 revalidation
/// (`NotModified`, resolved before any entry lookup / mmap copy — PERF-7) or
/// the read body plus the file-level ETag for the `ETag` response header.
enum RawRead {
    NotModified {
        etag: String,
    },
    Read {
        etag: Option<String>,
        content_type: String,
        total: u64,
        range: Option<ParsedRange>,
        bytes: Vec<u8>,
    },
}

/// Blocking core of [`raw_content`] (H1): find + resolve the entry, then
/// materialize either the whole body or (for a satisfiable `Range`) just the
/// requested slice window, straight out of the mmap (PERF-1). Runs on the
/// blocking pool; the `entry_content` mmap guard lives entirely inside, so its
/// lifetime never crosses the thread boundary. W6.1: also stats the ZIM file
/// for a fresh ETag + 304 revalidation before touching the archive.
fn read_raw_entry_blocking(
    archive: Arc<zim::Zim>,
    path: &str,
    range_header: Option<&str>,
    file_path: Option<&Path>,
    if_none_match: Option<&str>,
) -> crate::error::Result<RawRead> {
    // W6.1: stat the ZIM file for a *fresh* ETag on the blocking pool (a TTL'd
    // etag would stale-304 after an in-place replace). A matching If-None-Match
    // returns 304 before any entry lookup / mmap copy; the same etag is reused
    // for the `ETag` header on a 200/206.
    let etag = file_path
        .and_then(|p| std::fs::metadata(p).ok())
        .and_then(|md| crate::zim::etag_from_metadata(&md));
    if let (Some(inm), Some(tag)) = (if_none_match, &etag) {
        if if_none_match_matches(inm, tag) {
            return Ok(RawRead::NotModified { etag: tag.clone() });
        }
    }

    let entry = find_entry(&archive, path)?
        .ok_or_else(|| crate::error::Error::NotFound(format!("path '{path}' not found")))?;
    let resolved = archive
        .resolve(entry)
        .map_err(|e| crate::error::Error::Zim(e.to_string()))?;
    let content = archive
        .entry_content(&resolved)
        .map_err(|e| crate::error::Error::Zim(e.to_string()))?
        .ok_or_else(|| crate::error::Error::NotFound("no content".into()))?;
    let total: u64 = content
        .len()
        .map_err(|e| crate::error::Error::Zim(e.to_string()))? as u64;
    // RFC 7233 single ranges (multi-range falls back to the full body).
    let range = range_header.and_then(|s| parse_single_range(s, total));
    // Bound the response size up front: skip the copy when over the cap — the
    // error is raised from async context in `raw_content`.
    let bytes = if total > MAX_RAW_BYTES {
        Vec::new()
    } else {
        match range {
            // PERF-1: copy only the requested window, never the body.
            Some(ParsedRange::Slice { .. }) => {
                let (start_us, end_us) = slice_range_bytes(total, range).unwrap_or((0, 0));
                content
                    .with(move |b: &[u8]| b.get(start_us..end_us).unwrap_or(&[]).to_vec())
                    .map_err(|e| crate::error::Error::Zim(e.to_string()))?
            }
            // 416 needs no body; keep `total` for the Content-Range.
            Some(ParsedRange::Unsatisfiable) => Vec::new(),
            None => content
                .to_vec()
                .map_err(|e| crate::error::Error::Zim(e.to_string()))?,
        }
    };
    Ok(RawRead::Read {
        etag,
        content_type: content_type_for(&resolved.mime_type),
        total,
        range,
        bytes,
    })
}

/// `GET /w/{zim}/{path}` — raw ZIM entry bytes with their MIME type. Supports
/// single-byte Range requests and ETag/304 revalidation; entries above
/// `MAX_RAW_BYTES` are refused.
#[utoipa::path(
    get,
    path = "/w/{zim}/{path}",
    params(
        ("zim" = String, Path, description = "ZIM name"),
        ("path" = String, Path, description = "Entry path within the ZIM (remaining path segments)")
    ),
    responses(
        (status = 200, description = "Raw entry content with its MIME type (supports single-byte Range requests)"),
        (status = 206, description = "Partial content (satisfiable Range request)"),
        (status = 304, description = "Not Modified (If-None-Match revalidation)"),
        (status = 404, description = "ZIM or path not found", body = ErrorResponse),
        (status = 416, description = "Unsatisfiable Range", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn raw_content(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    AxumPath((zim_name, path)): AxumPath<(String, String)>,
) -> Result<Response, crate::error::Error> {
    let meta = state
        .zims
        .get(&zim_name)
        .ok_or_else(|| crate::error::Error::NotFound(format!("ZIM '{zim_name}' not found")))?;

    // W6.1: the ETag stat + 304 revalidation moved into the blocking read
    // below (off the async worker — a `stat` must not run on a tokio worker).
    // The tag is file-level, so a match implies byte-identical entries; the
    // closure stats fresh (a TTL'd etag would stale-304 after an in-place
    // replace) and returns `NotModified` before any entry lookup / mmap copy
    // (PERF-7: a 304 pays no mmap copy).
    let file_path = std::path::PathBuf::from(meta.file_path);
    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let archive = state.zims.open_zim(&zim_name).await?;

    // PERF-1: pass the raw Range header into the blocking read so a
    // satisfiable slice is copied window-by-window straight out of the mmap —
    // no full-body `to_vec()` followed by a second slice-copy on the async side.
    let range_header = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let p = path.clone();
    // H1: the entire blocking read runs on the blocking pool — a ≤64MB
    // mmap→Vec copy must not occupy a tokio worker thread.
    // PERF-10 decision (2026-08-30): keep `spawn_blocking` even for small
    // reads. (1) Worst case is a 64 MB mmap→Vec copy (MAX_RAW_BYTES below):
    // running it on a runtime worker would stall every task multiplexed onto
    // that worker thread, while the blocking pool (512 threads by default)
    // absorbs it with headroom. (2) `spawn_blocking` overhead is
    // microsecond-scale, so it is a good trade at every entry size — an
    // entry-size gate would add a branch to protect a non-bottleneck.
    // (3) Reading inline on the async worker only wins for entries far under
    // 1 MB. Revisit trigger: p99 entry sizes well below 1 MB plus profiling
    // showing spawn overhead dominates.
    let read_res = tokio::task::spawn_blocking(move || {
        read_raw_entry_blocking(
            archive,
            &p,
            range_header.as_deref(),
            Some(&file_path),
            if_none_match.as_deref(),
        )
    })
    .await
    .map_err(|e| crate::error::Error::Internal(anyhow::anyhow!("raw content task: {e}")))?;
    let read = read_res?;
    // W6.1: 304 revalidation resolved inside the blocking read (no mmap copy).
    if let RawRead::NotModified { etag: tag } = &read {
        return Ok((
            StatusCode::NOT_MODIFIED,
            [(header::ETAG, tag.clone())],
            Vec::<u8>::new(),
        )
            .into_response());
    }
    // The only remaining variant is `Read` (`NotModified` returned above);
    // `let-else` makes the destructure non-refutable without re-indenting.
    let RawRead::Read {
        etag,
        content_type,
        total,
        range,
        bytes,
    } = read
    else {
        unreachable!("RawRead::NotModified handled above")
    };
    if total > MAX_RAW_BYTES {
        return Err(crate::error::Error::Zim(format!(
            "entry is {total} bytes, exceeds the {MAX_RAW_BYTES}-byte serving limit"
        )));
    }

    match range {
        Some(ParsedRange::Slice { start, end }) => {
            // PERF-1: `bytes` is already exactly the requested slice (copied
            // window-by-window in the closure) — no second slice-copy here.
            let sec = super::web::raw_content_security_headers(&content_type);
            let mut resp = (
                StatusCode::PARTIAL_CONTENT,
                [
                    (header::CONTENT_TYPE, content_type),
                    (
                        header::CONTENT_RANGE,
                        format!("bytes {start}-{end}/{total}"),
                    ),
                    (header::CONTENT_LENGTH, bytes.len().to_string()),
                    (header::ACCEPT_RANGES, "bytes".to_string()),
                    (
                        axum::http::HeaderName::from_static("x-content-type-options"),
                        "nosniff".to_string(),
                    ),
                ],
                bytes,
            )
                .into_response();
            append_sec_headers(&mut resp, sec);
            insert_etag(&mut resp, &etag);
            Ok(resp)
        }
        Some(ParsedRange::Unsatisfiable) => Ok((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{total}"))],
            Vec::<u8>::new(),
        )
            .into_response()),
        None => {
            let sec = super::web::raw_content_security_headers(&content_type);
            let mut resp = (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, content_type),
                    (header::CONTENT_LENGTH, total.to_string()),
                    (header::ACCEPT_RANGES, "bytes".to_string()),
                    (
                        axum::http::HeaderName::from_static("x-content-type-options"),
                        "nosniff".to_string(),
                    ),
                ],
                bytes,
            )
                .into_response();
            append_sec_headers(&mut resp, sec);
            insert_etag(&mut resp, &etag);
            Ok(resp)
        }
    }
}

/// Append the SEC-M2 sandboxing headers returned by
/// [`super::web::raw_content_security_headers`] (CSP + `X-Frame-Options: DENY`
/// for `text/html`; a no-op for every other content type) to `resp`.
fn append_sec_headers(resp: &mut Response, sec: Vec<(axum::http::HeaderName, String)>) {
    for (name, value) in sec {
        if let Ok(hv) = value.parse::<axum::http::HeaderValue>() {
            resp.headers_mut().append(name, hv);
        }
    }
}

/// Add the ETag (when a tag is present) to a raw-content response (PERF-7).
/// `None` (a stat that failed between checks) means the entity is not
/// cacheable — emit no tag.
fn insert_etag(resp: &mut Response, etag: &Option<String>) {
    if let Some(tag) = etag {
        if let Ok(hv) = tag.parse() {
            resp.headers_mut().insert(header::ETAG, hv);
        }
    }
}

// ─── Chunks ───────────────────────────────────────────────────────────────────

/// Query parameters for `GET /chunks` (overlapping text chunks for RAG).
#[derive(Debug, Deserialize)]
pub struct ChunksQuery {
    /// Name of the ZIM to read from.
    pub zim: String,
    /// Article path within the ZIM.
    pub path: String,
    /// Chunk size in chars (default 1000).
    pub size: Option<usize>,
    /// Overlap between chunks in chars (default 200).
    pub overlap: Option<usize>,
}

/// `GET /chunks` — overlapping text chunks of an article, for RAG ingestion.
#[utoipa::path(
    get,
    path = "/chunks",
    params(
        ("zim" = String, Query, description = "ZIM name"),
        ("path" = String, Query, description = "Article path within the ZIM"),
        ("size" = Option<usize>, Query, description = "Chunk size in chars (default 1000)"),
        ("overlap" = Option<usize>, Query, description = "Overlap between chunks in chars (default 200)")
    ),
    responses(
        (status = 200, description = "Overlapping text chunks for RAG", body = ChunksResponse),
        (status = 404, description = "ZIM or path not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn get_chunks(
    State(state): State<AppState>,
    Query(params): Query<ChunksQuery>,
) -> Result<Json<ChunksResponse>, crate::error::Error> {
    let (chunk_size, overlap) =
        clamp_chunk_params(params.size.unwrap_or(1000), params.overlap.unwrap_or(200));
    let text = read_zim_text(&state, &params.zim, &params.path).await?;
    // Chunking is O(text × chunks) pure CPU; run it off the async worker
    // (same pattern as `read_zim_entry`) so a pathological-but-legal
    // `size`/`overlap` pair can't pin a tokio worker for seconds.
    let chunks = tokio::task::spawn_blocking(move || chunk_text(&text, chunk_size, overlap))
        .await
        .map_err(|e| crate::error::Error::Internal(anyhow::anyhow!("chunk task: {e}")))?;
    Ok(Json(ChunksResponse {
        zim: params.zim,
        path: params.path,
        chunk_count: chunks.len(),
        chunks,
    }))
}

// ─── Snippet ──────────────────────────────────────────────────────────────────

/// Query parameters for `GET /snippet` (the indexed snippet of an article).
#[derive(Debug, Deserialize)]
pub struct SnippetQuery {
    /// Name of the ZIM the article belongs to.
    pub zim: String,
    /// Article path within the ZIM.
    pub path: String,
}

/// `GET /snippet` — the indexed snippet and title of an article (404 if the
/// article has no `articles` row).
#[utoipa::path(
    get,
    path = "/snippet",
    params(
        ("zim" = String, Query, description = "ZIM name"),
        ("path" = String, Query, description = "Article path within the ZIM")
    ),
    responses(
        (status = 200, description = "Indexed snippet", body = SnippetResponse),
        (status = 404, description = "Article not indexed", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn get_snippet(
    State(state): State<AppState>,
    Query(params): Query<SnippetQuery>,
) -> Result<Json<SnippetResponse>, crate::error::Error> {
    // Raw SQL (db::raw): cross-table JOIN — no `db::raw` named helper exists
    // for it, so the read is kept inline (same convention as `crate::db`);
    // tuple shape unchanged.
    let row = crate::db::raw::fetch_optional::<(String, String, Option<String>), _, _>(
        &state.db,
        "SELECT a.snippet, a.title, a.content_preview FROM articles a
         JOIN zims z ON z.id = a.zim_id
         WHERE z.name = $1 AND a.path = $2",
        |q| q.bind(&params.zim).bind(&params.path),
    )
    .await?;

    match row {
        Some((snippet, title, preview)) => Ok(Json(SnippetResponse {
            zim: params.zim,
            path: params.path,
            title,
            snippet,
            preview,
        })),
        None => Err(crate::error::Error::NotFound(format!(
            "article '{}' / '{}' not indexed",
            params.zim, params.path
        ))),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
