//! ZIM indexing pipeline: parallel extraction → bulk insert → progress tracking.
//!
//! Pipeline:
//! 1. Open ZIM, read metadata
//! 2. Get namespace C entry range (Articles or UserContent)
//! 3. Rayon parallel: extract text, snippet, Q-ID per entry
//! 4. Bulk insert: COPY to staging → upsert to main (tsvector computed in Postgres)
//! 5. Progress tracking with checkpoint file for resumability

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use chrono::NaiveDate;

use rayon::prelude::*;
use regex::Regex;

use crate::db::{pool::Pool, raw};
use crate::error::{Error, Result};
use crate::zim::ZimManager;

/// Number of chars of article text stored as `content_preview` (B12: the
/// `/read` DB-fallback path uses this cap to decide whether a returned
/// preview was cut off and must be reported as `truncated`).
pub const PREVIEW_CHARS: usize = 2000;

/// Cap for the Q-ID scan window (P2): Wikipedia `#identifiers` / `wikidata`
/// links sit near the top of an article, so we only scan the first `512 KiB` of
/// HTML for the Q-ID instead of the whole (sometimes multi-MB) page.
const QID_SCAN_BYTES: usize = 512 * 1024;

/// A single extracted article row, ready for bulk insert.
#[derive(Debug)]
struct ArticleRow {
    path: String,
    title: String,
    content_preview: Option<String>,
    snippet: String,
    language: String,
    qid: Option<i64>,
}

/// Checkpoint for resumable indexing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Checkpoint {
    file_mtime: u64,
    total_entries: u64,
    completed: u64,
    started_at: std::time::SystemTime,
    /// PERF 1 (availability-preserving reindex): the Postgres `now()` at the
    /// start of this (possibly multi-run, resumable) index, as a timestamptz
    /// string. `finalize_zim` prunes stale rows (`updated_at < index_started_at`)
    /// atomically with the `index_status = 'ready'` flip — replacing the old
    /// up-front `DELETE FROM articles` that left the ZIM's search index empty
    /// for the entire reindex. A resumed run reuses the value the interrupted
    /// run persisted here. `#[serde(default)]` → empty for a legacy
    /// checkpoint → the prune is skipped (safe: no data loss, only possible
    /// leftover stale rows, cleaned on the next run).
    #[serde(default)]
    index_started_at: String,
}

/// Q-ID extraction: matches wikidata.org/wiki/Q12345 or Q12345#identifiers
static QID_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();

fn get_qid_regex() -> &'static regex::Regex {
    QID_RE.get_or_init(|| {
        Regex::new(r"wikidata\.org/wiki/(Q\d+)(#identifiers)?")
            .expect("QID_RE pattern is a valid static regex")
    })
}

/// Run the full indexing pipeline for one or all ZIMs.
pub async fn index_zims(
    zims: &Arc<ZimManager>,
    pool: &Pool,
    target: Option<&str>, // None = all
) -> Result<()> {
    let all = zims.list();
    let to_index: Vec<_> = match target {
        Some(name) => {
            let meta = all
                .iter()
                .find(|z| z.name == name)
                .ok_or_else(|| Error::NotFound(format!("ZIM '{name}' not found")))?
                .clone();
            vec![meta]
        }
        None => all,
    };

    // Targeted run: surface the single ZIM's result directly so the caller
    // knows it failed. Bulk run: isolate per-ZIM failures — one corrupt
    // archive must not abort the rest of the batch.
    if let Some(_name) = target {
        // Single ZIM → rayon's default (full-width) pool; no oversubscription
        // risk with one extraction, so no custom pool is built (WP5.4).
        return index_single_zim(zims, pool, &to_index[0], &None).await;
    }

    // Bulk indexing runs at a bounded concurrency (4) rather than strictly
    // sequential: the per-ZIM `pg_try_advisory_lock` inside `index_single_zim`
    // still serializes same-ZIM runs (e.g. `cmd index --all` vs the poller's
    // auto-index), and different ZIMs parallelize safely (staging rows are
    // independent per zim_id). Per-ZIM failures stay isolated — one corrupt
    // archive must not abort the rest of the batch.
    const MAX_PARALLEL_ZIMS: usize = 4;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_ZIMS));
    // WP5.4: one shared rayon pool sized to (cores / MAX_PARALLEL_ZIMS) so the
    // up-to-4 concurrent extractions share the cores instead of each driving
    // the full-width default pool (oversubscription). A build failure degrades
    // to `None` → per-task default pool. The targeted (single-ZIM) path above
    // passes `None` directly.
    let rayon_pool: Option<Arc<rayon::ThreadPool>> = rayon::ThreadPoolBuilder::new()
        .num_threads(
            (std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                / MAX_PARALLEL_ZIMS)
                .max(1),
        )
        .build()
        .ok()
        .map(Arc::new);
    let mut handles = Vec::new();
    for meta in &to_index {
        let permit = semaphore.clone();
        let zims = zims.clone();
        let pool = pool.clone();
        let meta = meta.clone();
        let rp = rayon_pool.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit.acquire_owned().await;
            tracing::info!("indexing ZIM: {} ({})", meta.name, meta.file_path);
            let res = index_single_zim(&zims, &pool, &meta, &rp).await;
            (meta.name, res)
        }));
    }

    let mut ok = 0usize;
    let mut failed: Vec<String> = Vec::new();
    for h in handles {
        match h.await {
            Ok((_, Ok(()))) => ok += 1,
            Ok((name, Err(e))) => {
                tracing::error!("indexing ZIM '{name}' failed: {e}");
                failed.push(name);
            }
            Err(join_err) => {
                tracing::error!("index task panicked: {join_err}");
            }
        }
    }
    if !failed.is_empty() {
        tracing::warn!(
            "index run: {ok} ok, {n} failed: {names}",
            n = failed.len(),
            names = failed.join(", ")
        );
    }
    Ok(())
}

/// Index a single ZIM archive.
///
/// Cross-run safety: takes a per-ZIM Postgres advisory lock (held on a dedicated
/// pooled connection for the whole run) so two processes — or the poller's
/// auto-index racing a manual `zimi index` — never index the same ZIM at once.
/// A run that cannot acquire the lock skips cleanly.
async fn index_single_zim(
    zims: &Arc<ZimManager>,
    pool: &Pool,
    meta: &crate::zim::ZimMeta,
    rayon_pool: &Option<Arc<rayon::ThreadPool>>,
) -> Result<()> {
    // Session-scoped advisory lock: held on a dedicated pooled connection
    // for the whole run (the pool may recycle a connection mid-run and
    // silently drop the lock, so it must not be checked in and out). Raw
    // SQL — the pool has no advisory-lock helper.
    let mut lock_conn = pool.acquire().await.map_err(Error::Database)?;
    let acquired: bool = raw::fetch_scalar_optional(
        &mut *lock_conn,
        "SELECT pg_try_advisory_lock(hashtext($1))",
        |q| q.bind(&meta.name),
    )
    .await?
    .unwrap_or(false);
    if !acquired {
        tracing::info!(
            "ZIM '{}' is being indexed by another run — skipping",
            meta.name
        );
        // pg_try_advisory_lock returned false: we don't hold the lock, so there
        // is nothing to release.
        return Ok(());
    }

    let outcome = run_index(zims, pool, meta, rayon_pool).await;

    // Record the failure (best-effort) so the UI can surface it.
    if outcome.is_err() {
        mark_index_error(pool, meta).await;
    }

    // Release the lock while we still hold the connection. Advisory locks are
    // connection-scoped — returning it to the pool without unlocking would
    // leave the lock on that pooled connection and block future runs.
    if let Err(e) = raw::execute(
        &mut *lock_conn,
        "SELECT pg_advisory_unlock(hashtext($1))",
        |q| q.bind(&meta.name),
    )
    .await
    {
        tracing::warn!("failed to release index lock for '{}': {e}", meta.name);
    }

    outcome
}

/// Skip-if-unchanged gate in front of the indexing pipeline.
async fn run_index(
    zims: &Arc<ZimManager>,
    pool: &Pool,
    meta: &crate::zim::ZimMeta,
    rayon_pool: &Option<Arc<rayon::ThreadPool>>,
) -> Result<()> {
    let file_path = PathBuf::from(&meta.file_path);

    // Skip when already indexed and the file is unchanged. reconcile/resync
    // reset the status to 'pending' whenever the file size changes, so
    // "ready + same size" reliably means "up to date".
    if meta.index_status == "ready" {
        let actual = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);
        if actual == meta.file_size {
            tracing::info!(
                "ZIM '{}' already indexed and unchanged — skipping",
                meta.name
            );
            return Ok(());
        }
    }

    index_body(zims, pool, meta, rayon_pool).await
}

/// Mark a ZIM's indexing as failed (best-effort; never masks the original
/// error). The old code checked out a client and swallowed both the
/// checkout and the write failure — a single builder call (which folds
/// acquire + execute into one result) is the same best-effort shape.
async fn mark_index_error(pool: &Pool, meta: &crate::zim::ZimMeta) {
    let _ = raw::execute(
        pool,
        "UPDATE zims SET index_status = 'error', updated_at = now() WHERE name = $1",
        |q| q.bind(&meta.name),
    )
    .await;
}

/// The indexing pipeline for one ZIM. The caller holds the advisory lock.
/// Result of the blocking open pass (B11a): everything derived from the
/// freshly-opened archive before the rayon extraction loop starts.
struct OpenedZim {
    archive: zim::Zim,
    display_title: String,
    language: String,
    description: Option<String>,
    creator: Option<String>,
    publisher: Option<String>,
    date: Option<NaiveDate>,
    article_ns: zim::Namespace,
    entry_range: std::ops::Range<u32>,
    article_count: u32,
}

/// Open + metadata + namespace resolution on the blocking pool (B11a).
/// The mmap open, central-directory parse, and sync metadata reads must not
/// run on an async worker — a multi-GB archive parse would stall other
/// requests. `zim::Zim` is `Send` (mmap + owned header), so the opened
/// archive comes back for the rayon extraction pass. Mirrors the
/// `ZimManager::open_zim` pattern.
fn open_zim_blocking(
    file_path: PathBuf,
    fallback_title: String,
    fallback_language: String,
) -> Result<OpenedZim> {
    let archive = zim::Zim::new(&file_path)
        .map_err(|e| Error::Zim(format!("opening {}: {e}", file_path.display())))?;

    let display_title = read_metadata(&archive, "Title").unwrap_or(fallback_title);
    let language = read_metadata(&archive, "Language").unwrap_or(fallback_language);
    let description = read_metadata(&archive, "Description");
    let creator = read_metadata(&archive, "Creator");
    let publisher = read_metadata(&archive, "Publisher");
    // Parse the ZIM "Date" metadata into a real date (formats vary). Stored
    // as a DATE (or NULL when unparseable) — never a raw-text bind into the
    // DATE column.
    let date: Option<NaiveDate> = read_metadata(&archive, "Date")
        .as_deref()
        .and_then(parse_zim_date);

    // Determine the article namespace. Old format (6.0): Namespace::Articles
    // (C); new format (6.1+): Namespace::UserContent.
    let (article_ns, entry_range) = find_article_namespace(&archive)?;

    Ok(OpenedZim {
        article_count: archive.header.article_count,
        archive,
        display_title,
        language,
        description,
        creator,
        publisher,
        date,
        article_ns,
        entry_range,
    })
}

/// Decide whether a ZIM is a Wikipedia archive (drives Q-ID extraction).
///
/// Requires a language that starts with an ASCII letter — case-insensitive,
/// so `"en"`, `"EN"`, and `"English"` all qualify while an empty or
/// digit-leading language does not — AND at least one Wikipedia marker:
/// the description or publisher contains `"wikipedia"` (case-insensitive), or
/// the name contains `"wikipedia"` / `"wiki_"`.
pub(crate) fn is_wikipedia(
    name: &str,
    language: &str,
    description: Option<&str>,
    publisher: Option<&str>,
) -> bool {
    if !language
        .to_lowercase()
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase())
    {
        return false;
    }
    let name_lc = name.to_lowercase();
    description.is_some_and(|d| d.to_lowercase().contains("wikipedia"))
        || publisher.is_some_and(|p| p.to_lowercase().contains("wikipedia"))
        || name_lc.contains("wikipedia")
        || name_lc.contains("wiki_")
}

async fn index_body(
    _zims: &Arc<ZimManager>,
    pool: &Pool,
    meta: &crate::zim::ZimMeta,
    rayon_pool: &Option<Arc<rayon::ThreadPool>>,
) -> Result<()> {
    let start = Instant::now();
    let file_path = PathBuf::from(&meta.file_path);

    // Steps 1–3 on the blocking pool: open the ZIM, read metadata, resolve
    // the article namespace (B11a — the sync mmap work leaves the worker).
    let fallback_title = meta.display_title.clone();
    let fallback_language = meta.language.clone();
    // Kept for the checkpoint mtime comparison below (the original path is
    // moved into the blocking task).
    let file_path_for_mtime = file_path.clone();
    let opened = tokio::task::spawn_blocking(move || {
        open_zim_blocking(file_path, fallback_title, fallback_language)
    })
    .await
    .map_err(|e| Error::Internal(anyhow::anyhow!("zim open task: {e}")))??;
    let OpenedZim {
        archive,
        display_title,
        language,
        description,
        creator,
        publisher,
        date,
        article_ns,
        entry_range,
        article_count,
    } = opened;
    // Record the actual article namespace in the DB: 'U' for the 6.1+
    // Namespace::UserContent, 'C' for the legacy Namespace::Articles.
    let ns_str = match article_ns {
        zim::Namespace::UserContent => "U",
        _ => "C",
    };
    let total_entries = (entry_range.end - entry_range.start) as u64;
    tracing::info!(
        "ZIM '{}': {} entries in namespace {:?} (total entries in ZIM: {})",
        meta.name,
        total_entries,
        article_ns,
        article_count
    );

    if total_entries == 0 {
        tracing::warn!("ZIM '{}' has no article entries — skipping", meta.name);
        return Ok(());
    }

    // Update ZIM row with metadata + status
    update_zim_status(
        pool,
        meta,
        &display_title,
        &language,
        &description,
        &creator,
        &publisher,
        &date,
        total_entries,
    )
    .await?;

    // Check for checkpoint (resume support)
    let checkpoint_path = checkpoint_file_path(&meta.name);
    let checkpoint = load_checkpoint(&checkpoint_path).await;

    let start_idx: u64 = if let Some(cp) = &checkpoint {
        if cp.file_mtime == file_mtime_u64(&file_path_for_mtime)
            && cp.total_entries == total_entries
        {
            tracing::info!(
                "resuming from entry {} / {} (interrupted run started {:?} ago)",
                cp.completed,
                total_entries,
                cp.started_at.elapsed().ok()
            );
            cp.completed
        } else {
            // File changed or entry count mismatch — start fresh
            0
        }
    } else {
        0
    };

    // PERF 1: capture the Postgres `now()` at the start of this (possibly
    // resumable) index run. `finalize_zim` uses it to prune stale rows
    // (`updated_at < index_started_at`) atomically with the ready-flip, instead
    // of the old up-front `DELETE FROM articles` that left the ZIM's search
    // index empty for the whole reindex. Fresh runs capture it now; resumed
    // runs reuse the value the interrupted run persisted in the checkpoint. A
    // missing value (legacy checkpoint) degrades to "skip the prune" — safe
    // (no data loss, only possible leftover stale rows, cleaned next run).
    let index_started_at: String = if start_idx == 0 {
        raw::fetch_scalar_optional(pool, "SELECT now()::text", |q| q)
            .await?
            .expect("SELECT now()::text always returns one row")
    } else {
        checkpoint
            .as_ref()
            .map(|cp| cp.index_started_at.clone())
            .unwrap_or_default()
    };

    if start_idx >= total_entries {
        tracing::info!("ZIM '{}' already fully indexed — skipping", meta.name);
        finalize_zim(pool, meta, total_entries, &index_started_at, &start).await?;
        return Ok(());
    }

    // Clear any staging rows a previously crashed run may have left for this
    // ZIM (crash after the COPY but before the per-chunk cleanup). Scoped to
    // this zim_id so a concurrent run's staging is untouched.
    // Resolve the ZIM's numeric id once - reused for the staging cleanup
    // and every chunk's bulk inserts (avoids re-querying per 10k chunk).
    // (The old `query_one` panicked on a vanished row; the row is written
    // by `persist_to_db`/`update_zim_status` before this point, so this is
    // the same invariant, returned as an error instead of a panic.)
    let zim_id: i32 =
        raw::fetch_scalar_optional(pool, "SELECT id FROM zims WHERE name = $1", |q| {
            q.bind(&meta.name)
        })
        .await?
        .ok_or_else(|| {
            Error::Internal(anyhow::anyhow!(
                "ZIM '{}' row vanished before indexing",
                meta.name
            ))
        })?;
    // Staging cleanup is scoped to this zim_id so a concurrent run's
    // staging is untouched.
    {
        raw::execute(
            pool,
            "DELETE FROM articles_staging WHERE zim_id = $1",
            |q| q.bind(zim_id),
        )
        .await?;
    }

    // PERF 1 (availability-preserving reindex): NO up-front `DELETE FROM
    // articles` here. The old code wiped the ZIM's rows before re-staging them,
    // leaving its search index empty for the whole reindex (a user-facing
    // availability window on an updated ZIM). Instead, the per-chunk upsert
    // below updates existing rows in place and inserts new ones, so `articles`
    // always holds a *complete* row set for this ZIM during the build. Rows
    // whose path was removed from the archive are pruned once, atomically with
    // finalize, via `updated_at < index_started_at` (see `finalize_zim`). This
    // is reachable only on explicit reindex or size change; a resumed run
    // never prunes (skipped chunks are not re-staged) — the same invariant the
    // old code's fresh-only prune enforced.

    // Step 4: Parallel extraction with rayon
    // We process in chunks of 10K entries. Each chunk is extracted in parallel,
    // then bulk-inserted via COPY.
    const CHUNK_SIZE: u64 = 10_000;
    let mut completed = start_idx;
    // Shared across the per-chunk blocking tasks below (P1) — the archive is
    // only used inside the loop, so move it into an Arc once.
    let archive = Arc::new(archive);

    // Determine if this is a Wikipedia ZIM (for Q-ID extraction)
    let is_wikipedia = is_wikipedia(
        &meta.name,
        &language,
        description.as_deref(),
        publisher.as_deref(),
    );

    loop {
        let chunk_end = (completed + CHUNK_SIZE).min(total_entries);
        if completed >= total_entries {
            break;
        }

        let start_global = entry_range.start as u64;
        let zim_ref = Arc::clone(&archive);
        let is_wiki = is_wikipedia;
        let lang = language.clone();
        let range = completed..chunk_end;

        // Parallel extraction on the blocking pool (P1): a 10K-entry rayon
        // pass is synchronous, so the `collect()` below must run off a tokio
        // worker — otherwise each chunk blocks a worker thread for the whole
        // extraction and starves concurrent HTTP requests. WP5.4: when a
        // shared (sized) pool is present, run inside it so concurrent ZIMs
        // don't each drive the full-width default pool.
        let rp = rayon_pool.clone();
        let rows: Vec<Option<ArticleRow>> = tokio::task::spawn_blocking(move || {
            let map_all = |r: std::ops::Range<u64>| {
                r.into_par_iter()
                    .map(|i| {
                        let global_idx = (start_global + i) as u32;
                        extract_entry(&zim_ref, global_idx, &lang, is_wiki)
                    })
                    .collect::<Vec<Option<ArticleRow>>>()
            };
            match &rp {
                Some(p) => p.install(|| map_all(range)),
                None => map_all(range),
            }
        })
        .await
        .map_err(|e| Error::Internal(anyhow::anyhow!("zim extract task: {e}")))?;

        // Filter out None entries (errors, empty content)
        let valid_rows: Vec<&ArticleRow> = rows.iter().filter_map(|r| r.as_ref()).collect();

        if valid_rows.is_empty() {
            completed = chunk_end;
            continue;
        }

        // Step 5: Bulk insert
        bulk_insert(pool, &valid_rows, ns_str, zim_id).await?;

        completed = chunk_end;

        // Update progress
        let progress = completed as f64 / total_entries as f64;
        update_progress(pool, meta, progress, completed).await?;

        // Save checkpoint
        save_checkpoint(
            &checkpoint_path,
            &Checkpoint {
                file_mtime: file_mtime_u64(&file_path_for_mtime),
                total_entries,
                completed,
                started_at: std::time::SystemTime::now(),
                index_started_at: index_started_at.clone(),
            },
        )
        .await;

        tracing::info!(
            "ZIM '{}': {}/{} ({:.1}%) — {}s elapsed",
            meta.name,
            completed,
            total_entries,
            progress * 100.0,
            start.elapsed().as_secs()
        );
    }

    // Cleanup checkpoint on success
    let _ = fs::remove_file(&checkpoint_path);

    // Step 6: Finalize (atomic stale-row prune + ready-flip, PERF 1)
    finalize_zim(pool, meta, total_entries, &index_started_at, &start).await?;

    Ok(())
}

/// Find the namespace that contains articles.
fn find_article_namespace(archive: &zim::Zim) -> Result<(zim::Namespace, std::ops::Range<u32>)> {
    // Try old-format namespace C (Articles)
    if let Ok(range) = archive.namespace_range(zim::Namespace::Articles) {
        if range.end > range.start {
            return Ok((zim::Namespace::Articles, range));
        }
    }
    // Try new-format namespace (UserContent)
    if let Ok(range) = archive.namespace_range(zim::Namespace::UserContent) {
        if range.end > range.start {
            return Ok((zim::Namespace::UserContent, range));
        }
    }
    Err(Error::Zim("no article namespace found in ZIM".into()))
}

/// Extract a single entry: resolve redirects, decompress, extract text.
fn extract_entry(
    archive: &zim::Zim,
    idx: u32,
    language: &str,
    is_wikipedia: bool,
) -> Option<ArticleRow> {
    // Get the directory entry
    let entry = archive.get_by_url_index(idx).ok()?;

    // Clone what we need before the entry may be moved into resolve()
    let original_path = entry.url.clone();
    let entry_title = entry.title.clone();

    // Skip non-HTML/text entries (images, CSS, JS, etc.)
    // Index all text/* content to preserve current coverage.
    let is_html = matches!(
        &entry.mime_type,
        zim::MimeType::Type(s) if s == "text/html" || s.starts_with("text/")
    );
    if !is_html {
        return None;
    }

    // Resolve redirects (bounded — the crate handles this internally)
    let resolved = match archive.resolve(entry) {
        Ok(e) => e,
        Err(_) => return None,
    };

    let title = if entry_title.is_empty() {
        // Derive title from URL path
        derive_title_from_path(&original_path)
    } else {
        entry_title
    };

    // Get content from the resolved entry
    let content = match archive.entry_content(&resolved) {
        Ok(Some(c)) => c,
        Ok(None) => return None, // redirect with no content target
        Err(_) => return None,
    };

    // Read bytes, capped (B11b): snippet (≤200 chars) and preview
    // (PREVIEW_CHARS) both come from the head of the article, and FTS is
    // built from title + preview — a 32MB bound is ~10× the largest
    // realistic head-of-article need and caps worst-case per-entry memory
    // for a pathological entry (same guard API as `read_zim_entry_blocking`).
    const MAX_ENTRY_SCAN_BYTES: usize = 32 * 1024 * 1024;
    let html_bytes =
        match content.with(|b: &[u8]| b.get(..MAX_ENTRY_SCAN_BYTES).unwrap_or(b).to_vec()) {
            Ok(b) => b,
            Err(_) => return None,
        };

    if html_bytes.is_empty() {
        return None;
    }

    // Extract text with deformat
    let html_str = String::from_utf8_lossy(&html_bytes);
    let text = deformat::html::strip_to_text_with_options(
        &html_str,
        &deformat::html::StripOptions::wikipedia(),
    );

    if text.trim().is_empty() {
        return None;
    }

    // WP5.5: bound the extracted *text* before the O(n) post-processing
    // (collapse_newlines) and the downstream head-of-article scans. Every
    // produced output reads only from the head — the content preview is the
    // first PREVIEW_CHARS (2000) chars, FTS indexes title + that preview, and
    // the snippet uses Readability on the full (byte-capped) `html_str` or
    // falls back to the first long line near the top — so truncating here is
    // lossless for all outputs while capping worst-case per-article cost on a
    // pathological entry. `text.len()` (bytes) is O(1); a byte length at or
    // below the cap guarantees the char count is too (each char is ≥1 byte).
    const MAX_TEXT_CHARS: usize = 64_000;
    let text = if text.len() > MAX_TEXT_CHARS {
        text.chars().take(MAX_TEXT_CHARS).collect()
    } else {
        text
    };

    // Post-process: collapse 3+ newlines to 2
    let text = collapse_newlines(&text);

    // Generate snippet
    let snippet = generate_snippet(&text, &html_str, &original_path);

    // Content preview: first PREVIEW_CHARS chars
    let content_preview: String = text.chars().take(PREVIEW_CHARS).collect();

    // Extract Q-ID (Wikipedia only) — bounded scan (P2).
    let qid = if is_wikipedia {
        extract_qid_windowed(&html_str)
    } else {
        None
    };

    Some(ArticleRow {
        path: original_path,
        title,
        content_preview: Some(content_preview),
        snippet,
        language: language.to_string(),
        qid,
    })
}

/// Derive a human-readable title from a ZIM URL path.
pub(crate) fn derive_title_from_path(url: &str) -> String {
    // "A/Water" → "Water", "A/C++" → "C++"
    let path = url.rsplit('/').next().unwrap_or(url);
    // Replace underscores with spaces (Wikipedia convention)
    path.replace('_', " ")
}

/// Collapse 3+ consecutive newlines to 2. The regex is compiled once and
/// reused (it used to be recompiled per article).
fn collapse_newlines(text: &str) -> String {
    static NEWLINE_RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = NEWLINE_RE
        .get_or_init(|| Regex::new(r"\n{3,}").expect("NEWLINE_RE pattern is a valid static regex"));
    re.replace_all(text, "\n\n").to_string()
}

/// Generate a snippet from extracted text.
///
/// Strategy:
/// 1. Try Readability excerpt (if available via feature)
/// 2. Fall back to first "real" paragraph (skip hatnotes, short lines)
fn generate_snippet(text: &str, html: &str, url: &str) -> String {
    // Readability is expensive; skip it for very large HTML.
    if html.len() <= 200_000 {
        if let Some((_, _, Some(excerpt))) = deformat::html::extract_with_readability(html, url) {
            if !excerpt.trim().is_empty() {
                return truncate_at_sentence(&excerpt, 300);
            }
        }
    }

    // Fall back: find first line > 50 chars that isn't a hatnote
    const HATNOTE_PREFIXES: &[&str] = &[
        "Main article",
        "Main topic",
        "For other uses",
        "Part of a series on",
        "This article is about",
        "Not to be confused",
        "This list is incomplete",
        "See also",
        "In other",
    ];

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.len() < 50 {
            continue;
        }
        // Skip hatnotes
        let is_hatnote = HATNOTE_PREFIXES
            .iter()
            .any(|prefix| trimmed.starts_with(prefix));
        if is_hatnote {
            continue;
        }
        // Take first 2-3 sentences, cap at 300 chars
        return truncate_at_sentence(trimmed, 300);
    }

    // Last resort: first 200 chars
    text.chars().take(200).collect()
}

/// Truncate text at a sentence boundary, max `max_bytes` bytes.
///
/// The limit is in **bytes** (not chars): `text.len()` is compared against
/// `max_bytes`, and the cut point is floored to a char boundary so multi-byte
/// UTF-8 sequences can't panic the slice. The result is at most `max_bytes`
/// bytes before the appended ellipsis.
fn truncate_at_sentence(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }

    // Floor to a char boundary so non-ASCII text can't panic the slice.
    let cut = text.floor_char_boundary(max_bytes);
    // Find the last sentence ending before the cut.
    let boundary = text[..cut]
        .rfind(['.', '!', '?'])
        .map(|pos| pos + 1)
        .unwrap_or(cut);

    let result: String = text[..boundary].chars().collect();
    format!("{}…", result.trim_end())
}

/// Extract Wikidata Q-ID from raw HTML bytes.
///
/// Prefers `#identifiers` match (the article's own Q-ID) over the first wikidata link.
/// Scan only the first `QID_SCAN_BYTES` bytes of `html` for a Q-ID (P2).
/// `ceil_char_boundary` floors the cut to a char boundary so a multi-byte char
/// at the cut can't panic the slice; short pages are scanned in full.
fn extract_qid_windowed(html: &str) -> Option<i64> {
    let cut = html.ceil_char_boundary(QID_SCAN_BYTES.min(html.len()));
    extract_qid(&html[..cut])
}

fn extract_qid(html: &str) -> Option<i64> {
    let re = get_qid_regex();

    let mut first_qid: Option<i64> = None;

    for caps in re.captures_iter(html) {
        let q_str = &caps[1]; // e.g., "Q1468"
        let is_identifiers = caps.get(2).is_some(); // "#identifiers" present

        // Skip a Q-number that doesn't fit in an i64 (a hostile/odd page could
        // carry one) instead of aborting the whole match with `?` — a valid
        // Q-ID later in the same page must still be found.
        let Ok(num) = q_str[1..].parse::<i64>() else {
            continue;
        };

        if is_identifiers {
            return Some(num); // Article's own Q-ID — best match
        }
        if first_qid.is_none() {
            first_qid = Some(num);
        }
    }

    first_qid
}

/// Bulk insert a batch of articles using COPY to staging → upsert to main.
///
/// Phase 1 stays a **COPY** (sqlx's `PgConnection::copy_in_raw`): a
/// per-row `INSERT` would round-trip the whole 10k-row chunk as individual
/// bound value sets and measurably lose the bulk-load throughput the
/// pipeline is tuned for. Phases 2–4 stay raw on the same connection: the
/// tsvector upsert (`setweight(to_tsvector(...)) || setweight(...)`)
/// and the `unnest`-zipped Q-ID insert are too exotic for the builder, and
/// keeping them on the COPY connection preserves the old single-connection
/// shape.
async fn bulk_insert(
    pool: &Pool,
    rows: &[&ArticleRow],
    namespace: &str,
    zim_id: i32,
) -> Result<()> {
    let mut client = pool.acquire().await.map_err(Error::Database)?;

    // Phase 1: COPY to staging table.
    // Build the whole chunk's payload in one buffer and send it in a single
    // `copy.send()` — one await per 10k-row chunk instead of one per row.
    let copy_sql = "COPY articles_staging (path, title, content_preview, snippet, language, namespace, zim_id) FROM STDIN WITH (FORMAT text)";
    let mut copy = client
        .copy_in_raw(copy_sql)
        .await
        .map_err(Error::Database)?;

    let mut buf = String::with_capacity(rows.len() * 2304); // ~2.2 KB/row with 2000-char previews
    for row in rows {
        escape_copy_text_into(&mut buf, &row.path);
        buf.push('\t');
        escape_copy_text_into(&mut buf, &row.title);
        buf.push('\t');
        match &row.content_preview {
            Some(s) => escape_copy_text_into(&mut buf, s),
            None => buf.push_str("\\N"),
        }
        buf.push('\t');
        escape_copy_text_into(&mut buf, &row.snippet);
        buf.push('\t');
        escape_copy_text_into(&mut buf, &row.language);
        buf.push('\t');
        escape_copy_text_into(&mut buf, namespace);
        buf.push('\t');
        buf.push_str(&zim_id.to_string());
        buf.push('\n');
    }
    copy.send(buf.as_bytes()).await.map_err(Error::Database)?;

    let _rows: u64 = copy.finish().await.map_err(Error::Database)?;

    // Phase 2: Upsert from staging to main table
    // tsvector is computed in Postgres (guaranteed correct for 'simple' config)
    raw::execute(
        &mut *client,
        "INSERT INTO articles (path, title, content_preview, snippet, search_vector, language, namespace, zim_id)
        SELECT
            path, title, content_preview, snippet,
            setweight(to_tsvector('simple', title), 'A')
            || setweight(to_tsvector('simple', coalesce(content_preview, '')), 'B'),
            language, namespace, zim_id
        FROM articles_staging
        WHERE zim_id = $1
        ON CONFLICT (zim_id, path) DO UPDATE SET
            title = EXCLUDED.title,
            content_preview = EXCLUDED.content_preview,
            snippet = EXCLUDED.snippet,
            search_vector = EXCLUDED.search_vector,
            updated_at = now()",
        |q| q.bind(zim_id),
    )
    .await?;

    // Phase 3: Q-ID batch insert (only if we have Q-IDs)
    let qid_rows: Vec<(&str, i64)> = rows
        .iter()
        .filter_map(|r| r.qid.map(|q| (r.path.as_str(), q)))
        .collect();

    if !qid_rows.is_empty() {
        // One statement per chunk (not one per row) — the same bulk pattern as
        // the articles COPY. Data rides in bound arrays (no string
        // interpolation, so hostile ZIM paths can't inject SQL); `unnest`
        // zips path/qid pairwise. `zim_id` is bound once.
        let qid_paths: Vec<String> = qid_rows.iter().map(|(p, _)| p.to_string()).collect();
        let qid_vals: Vec<i64> = qid_rows.iter().map(|(_, q)| *q).collect();
        raw::execute(
            &mut *client,
            "INSERT INTO qid_index (zim_id, path, qid) \
             SELECT $1, v.path, v.qid \
             FROM unnest($2::text[], $3::bigint[]) AS v(path, qid) \
             ON CONFLICT (zim_id, path) DO UPDATE SET qid = EXCLUDED.qid",
            |q| q.bind(zim_id).bind(qid_paths).bind(qid_vals),
        )
        .await?;
    }

    // Phase 4: clear only THIS ZIM's staging rows. A global TRUNCATE would
    // wipe another concurrently-indexed ZIM's in-flight staging rows (staging
    // is shared); scoping to zim_id keeps parallel runs safe.
    raw::execute(
        &mut *client,
        "DELETE FROM articles_staging WHERE zim_id = $1",
        |q| q.bind(zim_id),
    )
    .await?;

    Ok(())
}

/// Append `s` to `out`, escaping backslashes and control chars for Postgres
/// COPY text format. In-place to avoid a per-line allocation in the hot path.
fn escape_copy_text_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0' => out.push_str("\\0"),
            _ => out.push(c),
        }
    }
}

/// Update the ZIM row with metadata and indexing status.
#[allow(clippy::too_many_arguments)] // one parameter per SQL column
async fn update_zim_status(
    pool: &Pool,
    meta: &crate::zim::ZimMeta,
    title: &str,
    language: &str,
    description: &Option<String>,
    creator: &Option<String>,
    publisher: &Option<String>,
    date: &Option<NaiveDate>,
    article_count: u64,
) -> Result<()> {
    raw::execute(
        pool,
        "UPDATE zims SET display_title = $1, language = $2, description = $3, creator = $4, \
         publisher = $5, date = $6, article_count = $7, index_status = 'indexing', \
         index_progress = 0.0, updated_at = now() WHERE name = $8",
        |q| {
            q.bind(title)
                .bind(language)
                .bind(description.clone())
                .bind(creator.clone())
                .bind(publisher.clone())
                .bind(date.as_ref().cloned())
                .bind(article_count as i64)
                .bind(&meta.name)
        },
    )
    .await?;
    Ok(())
}

/// Update indexing progress.
async fn update_progress(
    pool: &Pool,
    meta: &crate::zim::ZimMeta,
    progress: f64,
    completed: u64,
) -> Result<()> {
    // `index_progress` is `REAL`: bind the Rust `f32` (the `::float8` cast is
    // a no-op widening; Postgres stores it as REAL exactly as the old raw SQL).
    raw::execute(
        pool,
        "UPDATE zims SET index_progress = $1::float8, indexed_entries = $2, updated_at = now() \
         WHERE name = $3",
        |q| {
            q.bind(progress as f32)
                .bind(completed as i64)
                .bind(&meta.name)
        },
    )
    .await?;
    Ok(())
}

/// Mark indexing as complete.
///
/// PERF 1 (availability-preserving reindex): the stale-row prune and the
/// `index_status = 'ready'` flip happen in **one transaction** so a search
/// never observes a partial index — it sees either the old (complete) row set
/// or the new (complete) row set, never a mix with missing rows. Rows
/// re-upserted during the build have `updated_at >= index_started_at`; stale
/// rows (paths removed from the archive, or from a prior index this reindex
/// replaced) are older and are deleted here. An empty `index_started_at`
/// (legacy checkpoint) skips the prune — safe (no data loss, only possible
/// leftover stale rows, cleaned on the next run).
async fn finalize_zim(
    pool: &Pool,
    meta: &crate::zim::ZimMeta,
    total: u64,
    index_started_at: &str,
    start: &Instant,
) -> Result<()> {
    // One transaction (as before): either the old (complete) row set or the
    // new (complete) row set is visible, never a mix. All three statements run
    // through db::raw on the same sqlx transaction (the prune DELETEs use a
    // subquery on `zims.name` + a `$2::timestamptz` text cast).
    let mut tx = pool.begin().await.map_err(Error::Database)?;

    if !index_started_at.is_empty() {
        // Prune rows whose path was removed from the archive (or belonged to a
        // prior index this run replaced). Uses the `updated_at` boundary —
        // only the indexing upsert advances `articles.updated_at` (embed writes
        // touch `embed_at` only), so this is exactly the stale set.
        raw::execute(
            &mut *tx,
            "DELETE FROM articles
             WHERE zim_id = (SELECT id FROM zims WHERE name = $1)
               AND updated_at < $2::timestamptz",
            |q| q.bind(&meta.name).bind(index_started_at),
        )
        .await?;

        // Drop Q-IDs that no longer reference a live article. `NOT EXISTS`
        // against the just-pruned `articles` is idempotent and safe on resume.
        raw::execute(
            &mut *tx,
            "DELETE FROM qid_index q
             WHERE q.zim_id = (SELECT id FROM zims WHERE name = $1)
               AND NOT EXISTS (SELECT 1 FROM articles a
                    WHERE a.zim_id = q.zim_id AND a.path = q.path)",
            |q| q.bind(&meta.name),
        )
        .await?;
    }

    // Ready-flip on the same transaction.
    raw::execute(
        &mut *tx,
        "UPDATE zims SET index_status = 'ready', index_progress = 1.0, \
         indexed_entries = $1, indexed_at = now(), updated_at = now() WHERE name = $2",
        |q| q.bind(total as i64).bind(&meta.name),
    )
    .await?;
    tx.commit().await.map_err(Error::Database)?;

    tracing::info!(
        "Indexed '{}' — {} articles in {:.1}s",
        meta.name,
        total,
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Read a metadata value from the ZIM (synchronous).
fn read_metadata(archive: &zim::Zim, key: &str) -> Option<String> {
    let content = archive.metadata(key).ok()??;
    content
        .to_vec()
        .ok()
        .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string())
}

/// Parse the ZIM "Date" metadata. Formats vary across producers (ISO, `d/m/Y`,
/// compact, with a time part), so try a small set of common layouts. Returns
/// `None` when nothing matches (→ `NULL` in the DB).
fn parse_zim_date(s: &str) -> Option<NaiveDate> {
    let s = s.trim();
    const FMTS: &[&str] = &[
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%Y%m%d",
    ];
    for fmt in FMTS {
        if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
            return Some(d);
        }
    }
    // ISO datetime with a `T` separator (e.g. `2023-12-31T00:00:00`).
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
        .map(|dt| dt.date())
        .ok()
}

// ─── Checkpoint helpers ───────────────────────────────────────────────────────

/// Checkpoint location: per-process (pid-scoped) subdirectory under the
/// system temp dir (SEC L5).
///
/// Checkpoints are **ephemeral**: a successful index removes its checkpoint;
/// a dead process orphans one in its own pid-scoped dir — bounded (one file
/// per interrupted run per process) and reaped with the temp dir by the OS.
/// The pid scope also removes the cross-process cleanup hazard: two
/// processes can no longer share (or clobber) a checkpoint path for the same
/// ZIM name.
fn checkpoint_file_path(zim_name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("zimservice")
        .join(format!("pid-{}", std::process::id()));
    let _ = fs::create_dir_all(&dir);
    dir.join(format!("{zim_name}.index-checkpoint.json"))
}

async fn load_checkpoint(path: &Path) -> Option<Checkpoint> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let contents = fs::read_to_string(&path).ok()?;
        serde_json::from_str(&contents).ok()
    })
    .await
    .ok()
    .flatten()
}

async fn save_checkpoint(path: &Path, cp: &Checkpoint) {
    let path = path.to_path_buf();
    let json = match serde_json::to_string(cp) {
        Ok(j) => j,
        Err(_) => return,
    };
    tokio::task::spawn_blocking(move || {
        let _ = fs::write(&path, json);
    })
    .await
    .ok();
}

fn file_mtime_u64(path: &Path) -> u64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        escape_copy_text_into, extract_qid, extract_qid_windowed, generate_snippet, is_wikipedia,
        mark_index_error, parse_zim_date, truncate_at_sentence, QID_SCAN_BYTES,
    };
    use chrono::NaiveDate;

    // ── is_wikipedia (WP3.4) ───────────────────────────────────────────────

    #[test]
    fn is_wikipedia_requires_wikipedia_marker_and_language() {
        // Name marker + valid language.
        assert!(is_wikipedia("wikipedia", "en", Some("Wikipedia"), None));
        // Uppercase language still qualifies (lowercased before the check).
        assert!(is_wikipedia("wikipedia", "EN", Some("Wikipedia"), None));
        // Empty language fails even with a strong marker.
        assert!(!is_wikipedia("wikipedia", "", Some("Wikipedia"), None));
        // Digit-leading language fails.
        assert!(!is_wikipedia("wikipedia", "1st", Some("Wikipedia"), None));
    }

    #[test]
    fn is_wikipedia_name_markers() {
        // `wiki_` name stem with a valid language, no desc/publisher marker.
        assert!(is_wikipedia("wiki_foobar", "fr", None, None));
        // Uppercase name marker.
        assert!(is_wikipedia("WIKIPEDIA_EN", "en", None, None));
    }

    #[test]
    fn is_wikipedia_description_only_marker() {
        // No name marker, but the description mentions Wikipedia.
        assert!(is_wikipedia(
            "myarchive",
            "en",
            Some("A Wikipedia mirror"),
            None
        ));
        // Publisher marker alone also qualifies.
        assert!(is_wikipedia("myarchive", "en", None, Some("Wikipedia")));
    }

    #[test]
    fn is_wikipedia_no_marker_false() {
        // Valid language but no Wikipedia term anywhere.
        assert!(!is_wikipedia(
            "myarchive",
            "en",
            Some("A general encyclopedia"),
            Some("Acme")
        ));
        // Name has "wiki" but not the `wikipedia`/`wiki_` marker.
        assert!(!is_wikipedia("wiktionary", "en", None, None));
    }

    #[test]
    fn truncate_at_sentence_non_ascii_no_panic() {
        // Regression: byte-indexed slicing panicked on non-ASCII text and
        // killed the whole indexing run.
        // No sentence terminators, multi-byte throughout:
        let accented = "Résumé naïve façade über straße ".repeat(30);
        let out = truncate_at_sentence(&accented, 50);
        assert!(out.ends_with('…'));
        // floored cut (≤ 50 bytes) + the ellipsis (3 bytes)
        assert!(out.len() <= 53);

        // Sentence terminator inside the window → cut there (byte 40 lands
        // just after the second period):
        let mixed = "Première phrase. Seconde phrase. Troisième phrase.";
        let out = truncate_at_sentence(mixed, 40);
        assert_eq!(out, "Première phrase. Seconde phrase.…");

        // Short text passes through unchanged
        assert_eq!(truncate_at_sentence("petit", 100), "petit");
    }

    #[test]
    fn truncate_at_sentence_multibyte_terminator_before_cut() {
        // Byte cut lands inside a multi-byte char region while a sentence
        // terminator sits before it: must not panic and must stop at the
        // terminator (byte-accurate floor + rfind). "Ä." = 3 bytes; the
        // remaining window is all multi-byte.
        let text = "Ä. Über Straße, über Straße, über Straße noch mehr.";
        let out = truncate_at_sentence(text, 12);
        assert!(out.len() <= 12 + 3, "floored cut + ellipsis: {out:?}");
        assert!(out.ends_with('…'));
        assert!(
            out.starts_with("Ä."),
            "terminator before the cut wins: {out:?}"
        );
    }

    // ── generate_snippet (TEST#3) ───────────────────────────────────────────

    #[test]
    fn generate_snippet_readability_path_small_html() {
        // Small HTML (≤200KB): Readability runs and the excerpt comes from
        // the HTML paragraph. `text` has only short lines, so if Readability
        // were skipped the fallback last-resort (first 200 chars of text) would
        // yield a different string.
        let html = "<!DOCTYPE html><html><head><title>T</title></head><body><article>"
            .to_string()
            + "<p>The quick brown fox jumps over the lazy dog, and then it jumps over the lazy dog once more, and then it keeps jumping over the lazy dog until the lazy dog finally asks the quick brown fox to stop jumping over it, which the quick brown fox refuses to do, because the quick brown fox is simply too quick and the lazy dog is simply too lazy.</p>"
            + "</article></body></html>";
        assert!(html.len() <= 200_000);
        let text = "short line\nanother short line\nlast short line";
        let snip = generate_snippet(text, &html, "https://x/a");
        assert!(
            snip.contains("quick brown fox"),
            "expected the Readability excerpt, got: {snip}"
        );
        assert!(snip.ends_with('…'));
    }

    #[test]
    fn generate_snippet_fallback_large_html_first_long_line() {
        // HTML >200KB: Readability is skipped, so the first >50-char line of
        // the stripped text wins (not the short first line, not last-resort).
        let big_html = "x".repeat(200_001);
        let line1 = "This is a long first line that comfortably exceeds fifty characters.";
        let line2 = "Second long line that also exceeds fifty characters for sure.";
        let text = format!("short\n{line1}\n{line2}");
        let snip = generate_snippet(&text, &big_html, "https://x/a");
        assert!(
            snip.starts_with("This is a long first line"),
            "first >50-char line expected, got: {snip}"
        );
    }

    #[test]
    fn generate_snippet_skips_hatnote_lines() {
        // A long hatnote must be skipped in favour of the next real line.
        let hat = "Main article: Something else entirely, long enough to count as a line.";
        let body = "The real opening paragraph that is long enough to be picked as the snippet.";
        let text = format!("{hat}\n{body}");
        let snip = generate_snippet(&text, "", "https://x/a");
        assert!(
            snip.starts_with("The real opening paragraph"),
            "hatnote must be skipped, got: {snip}"
        );
        assert!(!snip.starts_with("Main article"));
    }

    #[test]
    fn generate_snippet_last_resort_short_lines() {
        // No line reaches 50 chars and no Readability excerpt → first 200
        // chars of the text.
        let text = ("ab cd ef \n").repeat(30); // every line short
        let snip = generate_snippet(&text, "", "https://x/a");
        assert_eq!(snip, text.chars().take(200).collect::<String>());
    }

    #[test]
    fn generate_snippet_empty_input() {
        assert_eq!(generate_snippet("", "", "https://x/a"), "");
    }

    #[test]
    fn parse_zim_date_table() {
        let d = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
        for input in [
            "2023-12-31",          // ISO (most common)
            "  2023-12-31  ",      // trimmed
            "2023-12-31 10:20:30", // with a time
            "2023/12/31",          // slashes
            "31/12/2023",          // d/m/Y (common on ZIMs)
            "20231231",            // compact
            "2023-12-31T00:00:00", // ISO datetime with T
        ] {
            assert_eq!(parse_zim_date(input), Some(d), "input: {input}");
        }
        // Unparseable / empty → None (stored as NULL, not a raw-text bind).
        assert_eq!(parse_zim_date("garbage"), None);
        assert_eq!(parse_zim_date(""), None);
        assert_eq!(parse_zim_date("12/31/23"), None); // US-style not handled
    }

    #[test]
    fn extract_qid_skips_overflowing_numbers() {
        // Regression: a single Q-number that overflows i64 used to abort the
        // whole match (via `?`), discarding the valid Q-IDs on the page.
        let html = concat!(
            "<a href=\"https://www.wikidata.org/wiki/Q99999999999999999999999\">bad</a>",
            "<a href=\"https://www.wikidata.org/wiki/Q111\">a</a>",
            "<a href=\"https://www.wikidata.org/wiki/Q222\">b</a>",
            "<a href=\"https://www.wikidata.org/wiki/Q333\">c</a>",
        );
        // The overflowing one is skipped; the first valid Q-ID is returned.
        assert_eq!(extract_qid(html), Some(111));
    }

    #[test]
    fn extract_qid_prefers_identifiers_link() {
        // The article's own Q-ID (a `#identifiers` link) beats the first
        // incidental wikidata link.
        let html = concat!(
            "<a href=\"https://www.wikidata.org/wiki/Q111\">a</a>",
            "<a href=\"https://www.wikidata.org/wiki/Q555#identifiers\">own</a>",
        );
        assert_eq!(extract_qid(html), Some(555));
    }

    #[test]
    fn extract_qid_none_when_absent() {
        assert_eq!(extract_qid("no wikidata links here"), None);
    }

    #[test]
    fn extract_qid_windowed_just_inside_scan_window() {
        // A Q-ID near the top is found.
        let html = "<a href=\"https://www.wikidata.org/wiki/Q4242#identifiers\">own</a>";
        assert_eq!(extract_qid_windowed(html), Some(4242));

        // ...and one whose link sits near (but inside) the 512 KiB window edge,
        // with the page long enough that truncation actually happens.
        let pad = "x".repeat(QID_SCAN_BYTES - 64);
        let html = format!(
            "{pad}<a href=\"https://www.wikidata.org/wiki/Q7#identifiers\">own</a>{}",
            "z".repeat(200)
        );
        assert!(
            html.len() > QID_SCAN_BYTES,
            "page must exceed the window to be truncated"
        );
        assert_eq!(extract_qid_windowed(&html), Some(7));
    }

    #[test]
    fn extract_qid_windowed_beyond_scan_window_returns_none() {
        // A Q-ID placed beyond the 512 KiB scan window is not found (P2: we do
        // not scan the whole page). The unwindowed extractor still finds it,
        // proving the window is the cause of the `None`.
        let pad = "x".repeat(QID_SCAN_BYTES);
        let html =
            format!("{pad}<a href=\"https://www.wikidata.org/wiki/Q4242#identifiers\">own</a>");
        assert_eq!(extract_qid_windowed(&html), None);
        assert_eq!(extract_qid(&html), Some(4242));
    }

    #[test]
    fn escape_copy_text_table() {
        let cases = [
            ("hello", "hello"),
            ("", ""),
            ("back\\slash", "back\\\\slash"),
            ("line\nbreak", "line\\nbreak"),
            ("carriage\rreturn", "carriage\\rreturn"),
            ("tab\there", "tab\\there"),
            ("mixed\n\t\\end", "mixed\\n\\t\\\\end"),
            ("café résumé", "café résumé"),
            ("nul\0byte", "nul\\0byte"),
        ];
        for (input, expected) in cases {
            let mut out = String::new();
            escape_copy_text_into(&mut out, input);
            assert_eq!(out, expected, "input: {input:?}");
        }
    }

    // ── H3: mark_index_error per-ZIM isolation (DB-gated) ───────────────────

    /// H3: `mark_index_error` must mark only the failed ZIM's row
    /// (`index_status` → `error`, `updated_at` bumped) and leave every other
    /// ZIM row untouched — one corrupt archive must not dirty the rest of the
    /// library. DB-gated exactly like the poller's `test_pool`/startup smoke
    /// tests: skips cleanly when the DB is unreachable unless
    /// `ZIMSERVICE_REQUIRE_DB` is set (then a skip is a hard failure).
    #[tokio::test]
    async fn db_mark_index_error_only_touches_that_zim_row() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        let config = crate::config::Config {
            database_url: url.clone(),
            db_pool_size: 4,
            ..Default::default()
        };
        let pool = match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            crate::db::pool::create_pool(&config),
        )
        .await
        {
            Ok(Ok(pool)) => pool,
            Ok(Err(e)) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB is set but cannot reach {url}: {e}");
                }
                eprintln!("skipping db_mark_index_error_only_touches_that_zim_row: cannot reach {url} ({e})");
                return;
            }
            Err(_) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB is set but timed out reaching {url}");
                }
                eprintln!("skipping db_mark_index_error_only_touches_that_zim_row: timed out reaching {url}");
                return;
            }
        };
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");

        // Two ZIM rows in distinct states (a corrupt/in-flight one and a
        // healthy bystander) so a leaky UPDATE cannot be masked by a row
        // that happens to look unchanged. Idempotent clean slate first so a
        // rerun (or a crashed prior run) cannot collide on `name`.
        const A: &str = "h3_corrupt_zim";
        const B: &str = "h3_bystander_zim";
        {
            let mut conn = pool.acquire().await.expect("connection");
            for name in [A, B] {
                crate::db::raw::execute(&mut *conn, "DELETE FROM zims WHERE name = $1", |q| {
                    q.bind(name)
                })
                .await
                .unwrap();
            }
            crate::db::raw::execute(
                &mut *conn,
                "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime, index_status, indexed_entries, article_count)
                 VALUES ($1, $1, $1, 2048, now(), 'indexing', 1, 1)",
                |q| q.bind(A),
            )
            .await
            .unwrap();
            crate::db::raw::execute(
                &mut *conn,
                "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime, index_status, indexed_entries, article_count)
                 VALUES ($1, $1, $1, 2048, now(), 'ready', 5, 5)",
                |q| q.bind(B),
            )
            .await
            .unwrap();
        }

        // The columns the assertions read back (a raw fetch of exactly those).
        #[derive(Debug, PartialEq)]
        struct ZimRow {
            name: String,
            index_status: String,
            index_progress: f32,
            indexed_entries: i64,
            indexed_at: Option<chrono::DateTime<chrono::Utc>>,
            updated_at: chrono::DateTime<chrono::Utc>,
        }
        async fn fetch_row(pool: &crate::db::pool::Pool, name: &str) -> ZimRow {
            let (name, index_status, index_progress, indexed_entries, indexed_at, updated_at) =
                crate::db::raw::fetch_optional(
                    pool,
                    "SELECT name, index_status, index_progress, indexed_entries, indexed_at, \
                     updated_at FROM zims WHERE name = $1",
                    |q| q.bind(name),
                )
                .await
                .expect("select zims row")
                .expect("row must exist");
            ZimRow {
                name,
                index_status,
                index_progress,
                indexed_entries,
                indexed_at,
                updated_at,
            }
        }
        let before_a = fetch_row(&pool, A).await;
        let before_b = fetch_row(&pool, B).await;

        // The corrupt ZIM's failure is recorded against its own name only.
        let meta = crate::zim::ZimMeta::stub(A.to_string(), format!("/nonexistent/{A}.zim"), 2048);
        mark_index_error(&pool, &meta).await;

        let after_a = fetch_row(&pool, A).await;
        let after_b = fetch_row(&pool, B).await;

        // The corrupt ZIM's row is marked: status flipped to `error`, the
        // timestamp bumped (>=: Postgres `now()` may land on the same
        // microsecond), and no other column touched.
        assert_eq!(
            after_a.index_status, "error",
            "corrupt ZIM must be marked error"
        );
        assert!(
            after_a.updated_at >= before_a.updated_at,
            "updated_at must not move backwards on the marked row"
        );
        assert_eq!(after_a.name, before_a.name);
        assert_eq!(after_a.index_progress, before_a.index_progress);
        assert_eq!(after_a.indexed_entries, before_a.indexed_entries);
        assert_eq!(after_a.indexed_at, before_a.indexed_at);

        // ...and the other ZIM's row is completely untouched (no cross-ZIM
        // leak: the UPDATE is scoped by `name`, not a blanket status sweep).
        assert_eq!(after_b, before_b, "bystander ZIM row must be untouched");

        // Cleanup (best-effort; the clean-slate DELETE above handles reruns).
        let mut conn = pool.acquire().await.expect("connection");
        for name in [A, B] {
            let _ = crate::db::raw::execute(&mut *conn, "DELETE FROM zims WHERE name = $1", |q| {
                q.bind(name)
            })
            .await;
        }
    }
}
