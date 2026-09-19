//! Shared content service: article text reading (live ZIM with DB-preview
//! fallback), ZIM entry reads, text chunking, and read/chunk response DTOs.
//!
//! Consumed by BOTH presentation layers — the HTTP handlers
//! (`serve/handlers/content.rs`) and the MCP tools (`mcp/mod.rs`) — so
//! neither layer depends on the other (H2: the MCP module used to reach
//! into `serve::handlers` for these functions). HTTP-specific glue
//! (axum extractors, range parsing, ETag/304 logic, SEC headers) stays in
//! `serve/handlers/content.rs`.
use std::sync::Arc;

use crate::AppState;

// ─── Read + chunks: response DTOs (OpenAPI schemas) ──────────────────────────

/// Response DTO for an article read: extracted text plus provenance.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ReadResponse {
    /// Article title.
    pub title: String,
    /// Name of the ZIM the article was read from.
    pub zim: String,
    /// ZIM entry path of the article.
    pub path: String,
    /// Extracted article text (see `truncated` / `full_length` for caps).
    pub content: String,
    /// True when `content` is not the whole article: either it was cut at
    /// `max_length`, or (with `source == "db"`) the stored preview itself was
    /// capped at index time (`PREVIEW_CHARS`) — the fallback can never return
    /// more than the indexed preview. Also true when the ZIM entry's raw HTML
    /// exceeded `MAX_READ_BYTES` and the read itself was capped (BUG-11).
    pub truncated: bool,
    /// Character count of `content`'s *available* (possibly capped) *source*
    /// text. With `source == "db"` this is the length of the **stored
    /// preview** (capped at index time); with `source == "zim"` it is the
    /// length of the text after the raw `MAX_READ_BYTES` read cap. The true
    /// full length is unknowable without defeating the cap.
    pub full_length: usize,
    /// `"zim"` when read live from the archive, `"db"` when falling back to the indexed preview.
    pub source: String,
}

/// One RAG chunk of an article's extracted text.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct TextChunk {
    /// 0-based position in the returned `chunks` list.
    pub index: usize,
    /// Byte offset (char boundary) of this window's start in the full text.
    pub start: usize,
    /// Exclusive byte offset (char boundary) of this window's end.
    pub end: usize,
    /// Trimmed text of the window; adjacent chunks overlap.
    pub text: String,
}

/// Response DTO for the chunking endpoint: all chunks of one article.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ChunksResponse {
    /// Name of the ZIM the article belongs to.
    pub zim: String,
    /// ZIM entry path of the article.
    pub path: String,
    /// Number of chunks in `chunks`.
    pub chunk_count: usize,
    /// Chunks in `start` order; adjacent chunks overlap.
    pub chunks: Vec<TextChunk>,
}

// ─── ZIM entry reads ─────────────────────────────────────────────────────────

/// Look up a ZIM entry by path, trying the Articles namespace first, then UserContent.
pub(crate) fn find_entry(
    archive: &zim::Zim,
    path: &str,
) -> crate::error::Result<Option<zim::DirectoryEntry>> {
    if let Some(entry) = archive
        .get_by_path(zim::Namespace::Articles, path)
        .map_err(|e| crate::error::Error::Zim(e.to_string()))?
    {
        return Ok(Some(entry));
    }
    archive
        .get_by_path(zim::Namespace::UserContent, path)
        .map_err(|e| crate::error::Error::Zim(e.to_string()))
}

/// Cap a raw article read at `MAX_READ_BYTES` and report whether the source
/// was larger (BUG-11: a >256 KB entry was silently cut but reported
/// `truncated: false`).
pub(crate) const MAX_READ_BYTES: usize = 256 * 1024;

pub(crate) fn cap_read(b: &[u8]) -> (Vec<u8>, bool) {
    let capped = b.len() > MAX_READ_BYTES;
    (b.get(..MAX_READ_BYTES).unwrap_or(b).to_vec(), capped)
}

/// Cap the `max_length` article-read argument at the raw-read cap
/// (`MAX_READ_BYTES`): the available source text is itself bounded by that
/// cap (via `cap_read`), so a larger request — e.g. `u64::MAX` — can never
/// return more content. Shared by both front ends (HTTP `GET /read` and the
/// MCP `read` tool) through `read_article_payload`, so they stay in parity.
pub(crate) fn clamp_read_max_length(max_len: usize) -> usize {
    max_len.min(MAX_READ_BYTES)
}

/// Decode an article's raw HTML bytes to text, tolerating non-UTF-8 input
/// (BUG-12: the old `from_utf8(...).unwrap_or("")` dropped the whole body on
/// a single bad byte; `from_utf8_lossy` keeps the decodable text).
pub(crate) fn html_to_text(html: &[u8]) -> String {
    let lossy = String::from_utf8_lossy(html);
    deformat::html::strip_to_text_with_options(&lossy, &deformat::html::StripOptions::wikipedia())
}

/// Read an article's full text **and** its entry title straight from the
/// (cached) ZIM handle. The title comes from the resolved (post-redirect)
/// directory entry, so it is the final article's title.
/// Sync core of [`read_zim_entry`] — runs on the blocking pool (H1): the
/// mmap read + 256KB copy + HTML deformat must not stall an async worker.
/// The `entry_content` mmap guard is created inside this fn, so its lifetime
/// never crosses the thread boundary.
pub(crate) fn read_zim_entry_blocking(
    archive: Arc<zim::Zim>,
    path: &str,
) -> crate::error::Result<(String, String, bool)> {
    let entry = find_entry(&archive, path)?
        .ok_or_else(|| crate::error::Error::NotFound(format!("path '{path}' not found")))?;
    let resolved = archive
        .resolve(entry)
        .map_err(|e| crate::error::Error::Zim(e.to_string()))?;
    let title = resolved.title.clone();
    let content = archive
        .entry_content(&resolved)
        .map_err(|e| crate::error::Error::Zim(e.to_string()))?
        .ok_or_else(|| crate::error::Error::NotFound("no content for entry".into()))?;
    // Cap the raw read at `MAX_READ_BYTES` — bounds the returned copy for a
    // hostile/huge entry; text extraction truncates further downstream.
    // BUG-11: report whether the source was actually larger so the response
    // can flag `truncated`.
    let (html, raw_capped) = content
        .with(|b: &[u8]| cap_read(b))
        .map_err(|e| crate::error::Error::Zim(e.to_string()))?;
    let text = html_to_text(&html);
    Ok((text, title, raw_capped))
}

/// Read one ZIM entry's text + title, offloading the blocking work (H1).
pub(crate) async fn read_zim_entry(
    state: &AppState,
    zim_name: &str,
    path: &str,
) -> crate::error::Result<(String, String, bool)> {
    let archive = state.zims.open_zim(zim_name).await?;
    let p = path.to_string();
    tokio::task::spawn_blocking(move || read_zim_entry_blocking(archive, &p))
        .await
        .map_err(|e| crate::error::Error::Internal(anyhow::anyhow!("zim read task: {e}")))?
}

/// Read an article's full text straight from the (cached) ZIM handle.
pub(crate) async fn read_zim_text(
    state: &AppState,
    zim_name: &str,
    path: &str,
) -> crate::error::Result<String> {
    read_zim_entry(state, zim_name, path)
        .await
        .map(|(text, _, _)| text)
}

/// Read an article as text with live-ZIM-first / DB-preview-fallback semantics.
/// Shared by the HTTP `/read` handler and the MCP `read` / `deep_search` tools.
pub(crate) async fn read_article_payload(
    state: &AppState,
    zim_name: &str,
    path: &str,
    max_len: usize,
) -> crate::error::Result<ReadResponse> {
    // Clamp uncapped client requests (e.g. `u64::MAX` via MCP) — see
    // `clamp_read_max_length`; keeps the HTTP and MCP front ends in parity.
    let max_len = clamp_read_max_length(max_len);

    state
        .zims
        .get(zim_name)
        .ok_or_else(|| crate::error::Error::NotFound(format!("ZIM '{zim_name}' not found")))?;

    // ZIM-first: the normal path reads text + entry title straight from the
    // live archive with no DB round-trip. The DB point-lookup (title +
    // preview) runs only when the ZIM read fails (e.g. file deleted after
    // indexing) and the article is still in the index.
    let (text, source, title, raw_capped) = match read_zim_entry(state, zim_name, path).await {
        Ok((text, entry_title, raw_capped)) => (
            text,
            "zim".to_string(),
            if entry_title.is_empty() {
                crate::zim::index::derive_title_from_path(path)
            } else {
                entry_title
            },
            raw_capped,
        ),
        Err(zim_err) => {
            let db_row: Option<(String, Option<String>)> =
                // fallback DB point-lookup (title + preview) on ZIM-read
                // failure — a two-table JOIN the entity path doesn't
                // express here.
                // RAW-OK: raw point-lookup JOIN; no db::raw helper for this shape.
                sqlx::query_as("SELECT a.title, a.content_preview FROM articles a \
                    JOIN zims z ON z.id = a.zim_id WHERE z.name = $1 AND a.path = $2")
                    .bind(zim_name)
                    .bind(path)
                    // Foreground READ: routes to the read replica when
                    // `DATABASE_URL_READ` is set (PERF-10 / read-replica
                    // finding), else the primary pool.
                    .fetch_optional(state.db_read_or_primary())
                    .await
                    .map_err(crate::error::Error::Database)?;
            match db_row {
                Some((db_title, Some(preview))) => (preview, "db".to_string(), db_title, false),
                _ => return Err(zim_err),
            }
        }
    };

    let (content_str, full_len) = truncate_preview(&text, max_len);

    Ok(ReadResponse {
        title,
        zim: zim_name.to_string(),
        path: path.to_string(),
        content: content_str,
        truncated: read_response_truncated(&source, full_len, max_len) || raw_capped,
        full_length: full_len,
        source,
    })
}

/// Whether a `ReadResponse` must report `truncated` (B12): true when the
/// content was cut at `max_len`, or when the DB fallback returned a stored
/// preview that itself hit the indexer's `PREVIEW_CHARS` cap.
pub(crate) fn read_response_truncated(source: &str, full_len: usize, max_len: usize) -> bool {
    let preview_capped = source == "db" && full_len >= crate::zim::index::PREVIEW_CHARS;
    preview_capped || full_len > max_len
}

/// Single-pass char truncation (P7): return `(content, full_char_count)`.
/// `content` is `text` capped at `max_len` chars with a `…` suffix when it was
/// cut; `full_char_count` is the true char count of the *source* `text` (not the
/// truncated length). Two streaming passes (`chars().count()` then `take`) avoid
/// a `Vec<char>` intermediate — no 4 bytes/char allocation for text bounded at 256KB.
fn truncate_preview(text: &str, max_len: usize) -> (String, usize) {
    let full_len = text.chars().count();
    let content = if full_len > max_len {
        let mut s: String = text.chars().take(max_len).collect();
        s.push('…');
        s
    } else {
        text.to_string()
    };
    (content, full_len)
}

// ─── Chunks ──────────────────────────────────────────────────────────────────

/// Clamp chunk parameters to safe bounds (B8): a `size` of 0 used to produce
/// an empty-chunk response or (worse) a near-infinite overlap loop; a huge
/// `size` allocates the whole article per request. Floor 10, ceiling 100k
/// chars. Overlap is capped at size-1 **and** size/2: with overlap ≤ size/2
/// every window advances by at least size/2, so a 256 KB article yields at
/// most ~2·len/size chunks instead of ~len/(size-overlap) — the latter
/// exploded to ~10⁹ char-ops for `size=100000&overlap=99999`.
pub(crate) fn clamp_chunk_params(size: usize, overlap: usize) -> (usize, usize) {
    const MIN_CHUNK_SIZE: usize = 10;
    const MAX_CHUNK_SIZE: usize = 100_000;
    let size = size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
    let overlap = overlap.min(size.saturating_sub(1)).min(size / 2);
    (size, overlap)
}

/// Split extracted text into deterministic, overlapping chunks for RAG.
///
/// Each window prefers a line boundary, then a space boundary, near its end,
/// falling back to a hard cut. Always makes strict forward progress, so
/// boundary-dense or boundary-less text can never loop.
pub(crate) fn chunk_text(text: &str, chunk_size: usize, overlap: usize) -> Vec<TextChunk> {
    let mut chunks = Vec::new();
    if chunk_size == 0 {
        return chunks;
    }
    let overlap = overlap.min(chunk_size - 1);
    let len = text.len();
    let mut start = 0;

    while start < len {
        // Byte budget, ceiled to a char boundary so non-ASCII text can never
        // produce an in-character slice. start is always a boundary, so
        // hard_end > start and every slice below is valid.
        let hard_end = text.ceil_char_boundary((start + chunk_size).min(len));
        let chunk_end = if hard_end < len {
            let window = &text[start..hard_end];
            let boundary = window
                .rfind('\n')
                .or_else(|| window.rfind(' '))
                .map(|pos| start + pos + 1)
                .unwrap_or(hard_end);
            boundary.max(start + 1) // never stall
        } else {
            hard_end
        };

        let raw = &text[start..chunk_end];
        if !raw.trim().is_empty() {
            chunks.push(TextChunk {
                index: chunks.len(),
                start,
                end: chunk_end,
                text: raw.trim().to_string(),
            });
        }

        if chunk_end >= len {
            break;
        }
        // `chunk_end - overlap` can land mid-character on non-ASCII text;
        // floor to the previous boundary (start is a boundary, so the result
        // is >= start — the `next <= start` check still handles the stall).
        let next = text.floor_char_boundary(chunk_end.saturating_sub(overlap));
        start = if next <= start { chunk_end } else { next };
    }

    chunks
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
