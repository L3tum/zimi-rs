//! ZIM archive manager: on-disk discovery, index state, and DB reconciliation.
//!
//! `ZimManager` caches open archives, tracks per-file (mtime, size) snapshots
//! for a cheap resync early-out, and reconciles the `zims` table with the files
//! on disk.
pub mod discovery;
pub mod index;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::SystemTime;

use crate::db::{pool::Pool, raw};
use crate::error::{Error, Result};

/// Max simultaneously-open ZIM handles. Each open ZIM holds one mmap fd, so this
/// bounds fd usage. Oldest (by insertion order) is evicted when the cap is hit.
const MAX_OPEN_ZIMS: usize = 16;
/// How long a cached handle's stat (mtime/size) is trusted before
/// re-statting the file. Bounds a fresh download's visibility to this
/// window while avoiding a `stat` syscall on every open of a hot ZIM.
const STAT_TTL_MS: u64 = 30_000;

/// Manages ZIM file discovery, metadata caching, and open handles.
pub struct ZimManager {
    /// Base directory to scan for .zim files
    pub zim_dir: PathBuf,
    /// Cache of ZIM metadata keyed by name
    cache: RwLock<HashMap<String, ZimMeta>>,
    /// Postgres pool for persisting metadata
    db: Pool,
    /// Cache of open ZIM handles, invalidated when the file's mtime changes.
    /// `zim::Zim` is Send + Sync (mmap-backed), so handles are shared across requests.
    open_handles: RwLock<HashMap<String, CachedZim>>,
    /// Monotonic insertion counter for LRU-ish eviction of open handles
    next_seq: AtomicU64,
    /// Snapshot of (name -> (mtime, size, real path)) from the last scan, used
    /// for early-out in `resync` to skip the full reconcile when nothing on
    /// disk has changed. The path element makes an extension-case rename
    /// (`a.zim` → `a.ZIM`, same stem/mtime/size) reach reconcile too.
    scan_snapshot: RwLock<HashMap<String, (SystemTime, u64, String)>>,
}

/// A cached open ZIM archive with the mtime + size it was opened at, and a
/// monotonic last-access counter for LRU eviction (bumped under a read lock,
/// so the fast path never takes a write lock).
struct CachedZim {
    zim: Arc<zim::Zim>,
    mtime: Option<SystemTime>,
    size: u64,
    last_access: AtomicU64,
    /// Epoch-ms of the last `stat` for this handle. A recently-stat'd
    /// handle is trusted without re-statting (see `STAT_TTL_MS`).
    last_stat: AtomicU64,
}

/// One `zims` row as decoded by [`ZimManager::load_from_db`]. A named struct
/// (decoded by column name) rather than a tuple: the SELECT lists 17
/// columns, past sqlx's 16-tuple `FromRow` cap, and this sqlx build has the
/// `derive` feature off.
struct ZimRow {
    id: i32,
    name: String,
    display_title: String,
    description: Option<String>,
    language: String,
    creator: Option<String>,
    publisher: Option<String>,
    date: Option<chrono::NaiveDate>,
    entry_count: i64,
    article_count: i64,
    file_path: String,
    file_size: i64,
    category: Option<String>,
    index_status: String,
    index_progress: f32,
    indexed_entries: i64,
    embed_enabled: bool,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for ZimRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        use sqlx::Row as _;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            display_title: row.try_get("display_title")?,
            description: row.try_get("description")?,
            language: row.try_get("language")?,
            creator: row.try_get("creator")?,
            publisher: row.try_get("publisher")?,
            date: row.try_get("date")?,
            entry_count: row.try_get("entry_count")?,
            article_count: row.try_get("article_count")?,
            file_path: row.try_get("file_path")?,
            file_size: row.try_get("file_size")?,
            category: row.try_get("category")?,
            index_status: row.try_get("index_status")?,
            index_progress: row.try_get("index_progress")?,
            indexed_entries: row.try_get("indexed_entries")?,
            embed_enabled: row.try_get("embed_enabled")?,
        })
    }
}

/// Pure staleness predicate for a cached ZIM handle (WI-38 / PERF-6): a handle
/// is still valid iff its recorded `(mtime, size)` matches the file's current
/// `(mtime, size)`. A missing stat is normalized to `None`/`0` at the call
/// sites, so two absent stats compare equal. Extracted from the two inline
/// compares in `open_zim` so the staleness rule is unit-tested in one place.
fn handle_still_valid(
    cached_mtime: Option<SystemTime>,
    cached_size: u64,
    cur_mtime: Option<SystemTime>,
    cur_size: u64,
) -> bool {
    cached_mtime == cur_mtime && cached_size == cur_size
}

/// Evict the least-recently-used open handle when the cache is at capacity
/// (WI-38 seam). Called with the `open_handles` write guard; removes the
/// min-`last_access` key so a subsequent insert lands back at `MAX_OPEN_ZIMS`.
fn evict_lru_if_full(handles: &mut HashMap<String, CachedZim>) {
    if handles.len() >= MAX_OPEN_ZIMS {
        let oldest = handles
            .iter()
            .min_by_key(|(_, v)| v.last_access.load(Ordering::Relaxed))
            .map(|(k, _)| k.clone());
        if let Some(k) = oldest {
            handles.remove(&k);
        }
    }
}

/// Metadata for a single ZIM archive.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct ZimMeta {
    /// DB row id (`None` for a stub not yet persisted).
    pub id: Option<i32>,
    /// ZIM name (catalog/filename name).
    pub name: String,
    /// Human-readable title shown in the UI.
    pub display_title: String,
    /// Archive description (from ZIM metadata, when present).
    pub description: Option<String>,
    /// Language code of the archive content.
    pub language: String,
    /// Creator from ZIM metadata (when present).
    pub creator: Option<String>,
    /// Publisher from ZIM metadata (when present).
    pub publisher: Option<String>,
    /// Date from ZIM metadata (when present).
    pub date: Option<String>,
    /// Total number of entries in the archive.
    pub entry_count: u64,
    /// Number of articles in the archive.
    pub article_count: u64,
    /// On-disk path of the `.zim` file.
    pub file_path: String,
    /// File size in bytes.
    pub file_size: u64,
    /// Catalog category (when set).
    pub category: Option<String>,
    /// Full-text index status (e.g. `pending` for a fresh stub).
    pub index_status: String,
    /// Full-text index progress in `0.0..=1.0`.
    pub index_progress: f64,
    /// Entries indexed so far (counts toward the aggregate index progress).
    pub indexed_entries: u64,
    /// Whether the embedding index is enabled for this ZIM.
    pub embed_enabled: bool,
}

impl ZimMeta {
    /// Create a stub `ZimMeta` for a newly-discovered ZIM file on disk
    /// that has no DB row yet.
    pub fn stub(name: String, file_path: String, file_size: u64) -> Self {
        Self {
            id: None,
            display_title: name.replace('_', " "),
            name,
            description: None,
            language: "en".into(),
            creator: None,
            publisher: None,
            date: None,
            entry_count: 0,
            article_count: 0,
            file_path,
            file_size,
            category: None,
            index_status: "pending".into(),
            index_progress: 0.0,
            indexed_entries: 0,
            embed_enabled: true,
        }
    }
}

/// Compute a strong ETag from a file's `mtime` (ms since epoch) and `size`
/// (W6.1). Extracted so `ZimManager::file_etag` and the blocking raw-content
/// read share the exact tag format — a format drift between the two would 304
/// a stale entry after an in-place replace.
pub(crate) fn etag_from_metadata(md: &std::fs::Metadata) -> Option<String> {
    let mtime_ms = md
        .modified()
        .ok()
        .and_then(|m| m.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Some(format!("\"{}-{}\"", mtime_ms, md.len()))
}

/// Read a non-negative count column as `u64`; a negative (corrupt) value
/// clamps to 0 rather than wrapping to a huge number.
fn count_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

impl ZimManager {
    /// Create a new ZimManager and scan for ZIM files.
    ///
    /// ```no_run
    /// # async fn example(pool: sqlx::postgres::PgPool) {
    /// let mgr = zimservice::zim::ZimManager::new(
    ///     std::path::PathBuf::from("/tmp/zims"),
    ///     pool,
    /// );
    /// # }
    /// ```
    pub fn new(zim_dir: PathBuf, db: Pool) -> Arc<Self> {
        Arc::new(Self {
            zim_dir,
            cache: RwLock::new(HashMap::new()),
            db,
            open_handles: RwLock::new(HashMap::new()),
            next_seq: AtomicU64::new(0),
            scan_snapshot: RwLock::new(HashMap::new()),
        })
    }

    /// Scan the ZIM directory for .zim files and return found names.
    ///
    /// Blocking scan+stat runs off the async worker (as in `resync`); each
    /// new entry is stubbed with its **real** on-disk path, so mixed-case
    /// files (`WIKIPEDIA.ZIM`) get a usable `file_path` on case-sensitive
    /// filesystems instead of a rebuilt lowercase `name.zim`.
    pub async fn scan(&self) -> Result<Vec<String>> {
        let dir = self.zim_dir.clone();
        let entries = tokio::task::spawn_blocking(move || discovery::scan_snapshot_blocking(&dir))
            .await
            .map_err(|e| Error::Internal(e.into()))??;
        let found: Vec<String> = entries.iter().map(|(name, _, _)| name.clone()).collect();

        let mut cache = self.cache.write().expect("zim cache lock poisoned");
        for (name, path, meta) in &entries {
            if !cache.contains_key(name) {
                let file_size = meta.map(|(_, size)| size).unwrap_or(0);

                cache.insert(
                    name.clone(),
                    ZimMeta::stub(name.clone(), path.display().to_string(), file_size),
                );
            }
        }

        Ok(found)
    }

    /// Get all cached ZIM metadata.
    pub fn list(&self) -> Vec<ZimMeta> {
        self.cache
            .read()
            .expect("zim cache lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Count of ZIMs and total indexed articles, computed under a single read
    /// lock without cloning the metadata `Vec` (used by `/health`).
    pub fn summary(&self) -> (usize, i64) {
        let cache = self.cache.read().expect("zim cache lock poisoned");
        let count = cache.len();
        let total: i64 = cache.values().map(|z| z.indexed_entries as i64).sum();
        (count, total)
    }

    /// Get a single ZIM's metadata by name.
    pub fn get(&self, name: &str) -> Option<ZimMeta> {
        self.cache
            .read()
            .expect("zim cache lock poisoned")
            .get(name)
            .cloned()
    }

    /// Strong ETag for the ZIM file: `"{mtime_ms}-{size}"` — the same
    /// (mtime, size) pair `open_zim` trusts as its file-change signal. File-level,
    /// so a matching tag implies every entry is byte-identical, and it is
    /// computable without reading any entry. Uses a **fresh** stat per request
    /// (not the `CachedZim` snapshot, whose `STAT_TTL_MS` could serve stale 304s
    /// after an in-place replace). `None` if the ZIM is unknown or the stat
    /// fails (i.e. not cacheable).
    #[cfg(test)]
    pub fn file_etag(&self, name: &str) -> Option<String> {
        let meta = self.get(name)?;
        let md = std::fs::metadata(Path::new(&meta.file_path)).ok()?;
        etag_from_metadata(&md)
    }

    /// Persist ZIM metadata to the database (upsert).
    ///
    /// Raw SQL: the statement binds `meta.date` as a loose `Option<String>`
    /// straight into the `DATE` column, keeps `file_mtime = now()` /
    /// `updated_at = now()` server-side, and the `ON CONFLICT (name) DO
    /// UPDATE` arm re-writes `date = $7` — an explicit `NULL` overwrite that
    /// needs raw SQL (as does `col = now()` in the DO UPDATE arm).
    async fn persist_to_db(&self, meta: &ZimMeta) -> Result<()> {
        let entry_count = meta.entry_count as i64;
        let article_count = meta.article_count as i64;
        let file_size = meta.file_size as i64;
        let indexed_entries = meta.indexed_entries as i64;

        let id: Option<i32> = raw::fetch_scalar_optional(
            &self.db,
            "INSERT INTO zims (
                name, display_title, description, language, creator, publisher, date,
                entry_count, article_count, file_path, file_size, category,
                index_status, index_progress, indexed_entries, embed_enabled, file_mtime
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,now())
            ON CONFLICT (name) DO UPDATE SET
                display_title = $2,
                description = $3,
                language = $4,
                creator = $5,
                publisher = $6,
                date = $7,
                entry_count = $8,
                article_count = $9,
                file_path = $10,
                file_size = $11,
                category = $12,
                index_status = $13,
                index_progress = $14,
                indexed_entries = $15,
                embed_enabled = $16,
                file_mtime = now(),
                updated_at = now()
            RETURNING id",
            |q| {
                q.bind(&meta.name)
                    .bind(&meta.display_title)
                    .bind(&meta.description)
                    .bind(&meta.language)
                    .bind(&meta.creator)
                    .bind(&meta.publisher)
                    .bind(&meta.date)
                    .bind(entry_count)
                    .bind(article_count)
                    .bind(&meta.file_path)
                    .bind(file_size)
                    .bind(&meta.category)
                    .bind(&meta.index_status)
                    .bind(meta.index_progress)
                    .bind(indexed_entries)
                    .bind(meta.embed_enabled)
            },
        )
        .await?;

        // Write the DB id back into the in-memory cache. New ZIMs (added via
        // resync at runtime) start with id=None; without this, id-based lookups
        // (collections, per-ZIM settings) silently miss for them.
        if let Some(id) = id {
            let mut cache = self.cache.write().expect("zim cache lock poisoned");
            if let Some(cached) = cache.get_mut(&meta.name) {
                cached.id = Some(id);
            }
        }

        Ok(())
    }

    /// Open a ZIM file for reading, reusing a cached handle when the file is unchanged.
    ///
    /// `zim::Zim` is `Send + Sync` (mmap-backed, read-only access), so the shared
    /// handle is safe across threads and async tasks. The cache entry is invalidated
    /// when the file's mtime changes (e.g. a fresh download replaced the file).
    ///
    /// Async because the cold-path `zim::Zim::new` (multi-GB central-dir parse)
    /// runs on the blocking pool; both fast paths (stat-TTL, mtime compare)
    /// stay sync.
    pub async fn open_zim(&self, name: &str) -> Result<Arc<zim::Zim>> {
        let meta = self
            .get(name)
            .ok_or_else(|| Error::NotFound(format!("ZIM '{name}' not found")))?;
        let path = Path::new(&meta.file_path);
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Fast path 1: a recently-stat'd cached handle is trusted without a
        // re-stat (the 5s TTL) — the common case for a hot ZIM. This skips
        // the `stat` syscall entirely on the hot path.
        {
            let handles = self
                .open_handles
                .read()
                .expect("open-handles lock poisoned");
            if let Some(cached) = handles.get(name) {
                let age_ms = now_ms.saturating_sub(cached.last_stat.load(Ordering::Relaxed));
                if age_ms < STAT_TTL_MS {
                    cached.last_access.fetch_add(1, Ordering::Relaxed);
                    return Ok(cached.zim.clone());
                }
            }
        }

        // Stat the file (cheap local syscall) and compare mtime + size.
        let md = std::fs::metadata(path).ok();
        let file_mtime = md.as_ref().and_then(|m| m.modified().ok());
        let file_size = md.as_ref().map(|m| m.len()).unwrap_or(0);

        // Fast path 2: a read lock is enough to return a cached, unchanged
        // handle, so concurrent reads don't serialize on a write lock.
        {
            let handles = self
                .open_handles
                .read()
                .expect("open-handles lock poisoned");
            if let Some(cached) = handles.get(name) {
                if handle_still_valid(cached.mtime, cached.size, file_mtime, file_size) {
                    cached.last_access.fetch_add(1, Ordering::Relaxed);
                    cached.last_stat.store(now_ms, Ordering::Relaxed);
                    return Ok(cached.zim.clone());
                }
            }
        }

        // Slow path: build the Zim handle OUTSIDE the write lock so the
        // multi-GB central-dir parse doesn't block every other ZIM's requests,
        // and a panic in Zim::new can't poison the map lock. The parse itself
        // runs on the blocking pool (H1): it must not stall an async worker.
        let path2 = path.to_path_buf();
        let zim = tokio::task::spawn_blocking(move || zim::Zim::new(path2.as_path()))
            .await
            .map_err(|e| Error::Internal(anyhow::anyhow!("zim parse task: {e}")))?
            .map_err(|e| Error::Zim(e.to_string()))?;
        let arc = Arc::new(zim);

        // Acquire the write lock and re-check (another task may have inserted
        // a handle for this ZIM while we were parsing). Two concurrent cold
        // opens of the same ZIM both parse; the second insert is discarded
        // (first-wins) — an accepted tradeoff to keep the hot-path lock short.
        let mut handles = self
            .open_handles
            .write()
            .expect("open-handles lock poisoned");
        if let Some(cached) = handles.get(name) {
            if handle_still_valid(cached.mtime, cached.size, file_mtime, file_size) {
                cached.last_access.fetch_add(1, Ordering::Relaxed);
                cached.last_stat.store(now_ms, Ordering::Relaxed);
                return Ok(cached.zim.clone());
            }
        }

        let access = self.next_seq.fetch_add(1, Ordering::Relaxed);

        // Evict the least-recently-used handle when at capacity.
        evict_lru_if_full(&mut handles);

        handles.insert(
            name.to_string(),
            CachedZim {
                zim: arc.clone(),
                mtime: file_mtime,
                size: file_size,
                last_access: AtomicU64::new(access),
                last_stat: AtomicU64::new(now_ms),
            },
        );
        Ok(arc)
    }

    /// Load persisted ZIM metadata from Postgres into the cache.
    ///
    /// Called after `scan()` at startup so indexing state (status, counts,
    /// metadata) survives server restarts. DB values take precedence over
    /// cache entries; an optional `resync()` then reconciles against the
    /// actual files (see `populate_zims` in main.rs).
    pub async fn load_from_db(&self) -> Result<()> {
        let rows: Vec<ZimRow> = raw::fetch_all(
            &self.db,
            "SELECT id, name, display_title, description, language, creator, publisher, date, \
             entry_count, article_count, file_path, file_size, category, index_status, \
             index_progress, indexed_entries, embed_enabled \
             FROM zims ORDER BY name",
            |q| q,
        )
        .await?;

        let mut cache = self.cache.write().expect("zim cache lock poisoned");
        for m in rows {
            // `date` comes back as `Option<NaiveDate>` (`NaiveDate::to_string`
            // is the identical `YYYY-MM-DD` text) and `index_progress` as
            // `REAL`/`f32` (exact when widened to the `f64` ZimMeta carries).
            let meta = ZimMeta {
                id: Some(m.id),
                name: m.name.clone(),
                display_title: m.display_title,
                description: m.description,
                language: m.language,
                creator: m.creator,
                publisher: m.publisher,
                date: m.date.map(|d| d.to_string()),
                entry_count: count_u64(m.entry_count),
                article_count: count_u64(m.article_count),
                file_path: m.file_path,
                file_size: count_u64(m.file_size),
                category: m.category,
                index_status: m.index_status,
                index_progress: m.index_progress as f64,
                indexed_entries: count_u64(m.indexed_entries),
                embed_enabled: m.embed_enabled,
            };
            cache.insert(m.name, meta);
        }
        Ok(())
    }

    /// Synchronously reconcile the in-memory cache with files found on disk.
    ///
    /// Pure cache operation (no DB access, lock not held across an await):
    /// vanished entries are dropped, new entries added, changed entries
    /// (size or path) updated. Returns the added / removed / changed names.
    /// `found` entries carry each file's real on-disk path, so the stored
    /// `file_path` is never rebuilt from the name (mixed-case files like
    /// `WIKIPEDIA.ZIM` stay openable on case-sensitive filesystems).
    fn reconcile(&self, found: &[discovery::ScanEntry]) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut cache = self.cache.write().expect("zim cache lock poisoned");
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut changed = Vec::new();

        // Use a HashSet for O(1) lookups instead of O(n) Vec::contains
        let found_set: std::collections::HashSet<&str> =
            found.iter().map(|(name, _, _)| name.as_str()).collect();

        for name in cache.keys().cloned().collect::<Vec<_>>() {
            if !found_set.contains(name.as_str()) {
                cache.remove(&name);
                removed.push(name);
            }
        }

        for (name, path, meta) in found {
            // Prefer the snapshot's size (already stat'd during the scan);
            // fall back to a fresh stat only when the snapshot is `None`
            // (stat raced the file disappearing).
            let file_size = meta
                .map(|(_, size)| size)
                .unwrap_or_else(|| std::fs::metadata(path).map(|m| m.len()).unwrap_or(0));
            let file_path = path.display().to_string();
            match cache.get(name) {
                Some(meta) if meta.file_size == file_size && meta.file_path == file_path => {}
                Some(meta) => {
                    let mut meta = meta.clone();
                    let size_changed = meta.file_size != file_size;
                    meta.file_path = file_path;
                    meta.file_size = file_size;
                    // A different-size file means different content — a
                    // previous index is stale. A pure move (same size) keeps
                    // its index valid.
                    if size_changed
                        && (meta.index_status == "ready" || meta.index_status == "indexing")
                    {
                        meta.index_status = "pending".into();
                        meta.index_progress = 0.0;
                    }
                    cache.insert(name.clone(), meta);
                    changed.push(name.clone());
                }
                None => {
                    cache.insert(
                        name.clone(),
                        ZimMeta::stub(name.clone(), file_path.clone(), file_size),
                    );
                    added.push(name.clone());
                }
            }
        }
        (added, removed, changed)
    }

    /// Re-scan the directory and reconcile the cache and DB with the filesystem.
    ///
    /// - Files that disappeared are removed from the cache and DB.
    /// - New files are added (status `pending`) and persisted.
    /// - Files whose size changed refresh metadata, drop any cached open handle,
    ///   and reset indexing state to `pending` (the old index is stale).
    ///
    /// Returns a human-readable report of what changed (empty when up to date).
    pub async fn resync(&self) -> Result<Vec<String>> {
        // Never reconcile against a missing directory (e.g. a volume that
        // hasn't mounted yet) — that would look like every ZIM was deleted.
        if !self.zim_dir.exists() {
            tracing::warn!(
                "zim dir {} does not exist — skipping resync",
                self.zim_dir.display()
            );
            return Ok(Vec::new());
        }

        // The directory scan + per-file stat is blocking I/O; run it off the
        // async worker (M4-perf) so a large ZIM dir never blocks the runtime.
        // Each entry is a `.zim` name + real path + `(mtime, size)`, or `None`
        // metadata when the file vanished between `read_dir` and stat (a
        // transient failure that must not look like a deletion, so the name
        // still reaches `reconcile`).
        let scan_dir = self.zim_dir.clone();
        let scan =
            tokio::task::spawn_blocking(move || discovery::scan_snapshot_blocking(&scan_dir))
                .await
                .map_err(|e| Error::Internal(e.into()))?;
        let scan = scan?;

        // Early-out: if the set of files and their (mtime, size, path) match
        // the last snapshot, skip the (potentially expensive) reconcile + DB
        // persist. Size is part of the key (B6): a same-mtime content change
        // (e.g. an rsync --size-only rewrite, or a clock-coarse re-download
        // that preserves mtime) is still detected; the path is part of the
        // key so an extension-case rename (`a.zim` → `a.ZIM`) with unchanged
        // mtime+size still reaches reconcile to refresh `file_path`.
        let current_snapshot: HashMap<String, (SystemTime, u64, String)> = scan
            .iter()
            .filter_map(|(name, path, entry)| {
                entry.map(|(mtime, size)| (name.clone(), (mtime, size, path.display().to_string())))
            })
            .collect();

        // W6.6: compare the guard directly instead of cloning the whole map
        // every tick — the O(n) compare is negligible, the per-tick full-map
        // allocation was the cost (len+spot-check is NOT equivalent: it would
        // miss same-count/size in-place replaces, exactly what resync catches).
        // Scoped block so the non-`Send` read guard is dropped before any await.
        let snapshot_changed = {
            let guard = self
                .scan_snapshot
                .read()
                .expect("scan-snapshot lock poisoned");
            *guard != current_snapshot
        };
        if snapshot_changed {
            *self
                .scan_snapshot
                .write()
                .expect("scan-snapshot lock poisoned") = current_snapshot;
        } else {
            return Ok(Vec::new());
        }

        let (added, removed, changed) = self.reconcile(&scan);

        if added.is_empty() && removed.is_empty() && changed.is_empty() {
            return Ok(Vec::new());
        }

        // ── Persist to DB (no in-memory locks held across awaits) ──
        let mut report = Vec::new();

        for name in &removed {
            self.open_handles
                .write()
                .expect("open-handles lock poisoned")
                .remove(name);
            raw::execute(&self.db, "DELETE FROM zims WHERE name = $1", |q| {
                q.bind(name)
            })
            .await?;
            tracing::info!("resync: removed ZIM {name}");
            report.push(format!("{name} (removed)"));
        }

        for name in added.iter().chain(changed.iter()) {
            let kind = if added.contains(name) {
                "added"
            } else {
                "changed"
            };
            // Force the next open to re-mmap the (new) file.
            self.open_handles
                .write()
                .expect("open-handles lock poisoned")
                .remove(name);
            if let Some(meta) = self.get(name) {
                self.persist_to_db(&meta).await?;
                tracing::info!("resync: {kind} ZIM {name}");
                report.push(format!("{name} ({kind})"));
            }
        }

        Ok(report)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Per-test temp dir under the system temp (unique per process + tag).
    fn temp_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("zimi-reconcile-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Build a ZimManager with a pool that never connects (sqlx is lazy with
    /// `connect_lazy`, and `reconcile`/`resync`-guard paths never touch the
    /// DB). The short acquire timeout keeps a hypothetical DB touch a fast
    /// failure instead of a 30 s hang.
    fn manager(dir: &Path) -> Arc<ZimManager> {
        // `dead_pool()` carries the 250 ms acquire timeout plus a runtime
        // fallback for sync `#[test]` bodies (sqlx 0.8 pool creation
        // requires a current Tokio runtime even for `connect_lazy`).
        ZimManager::new(dir.to_path_buf(), crate::testing::dead_pool())
    }

    fn write_zim(dir: &Path, name: &str, size: usize) {
        let mut buf = vec![0u8; size];
        if size > 0 {
            buf[0] = b'Z';
        }
        std::fs::write(dir.join(format!("{name}.zim")), buf).unwrap();
    }

    fn found(dir: &Path) -> Vec<discovery::ScanEntry> {
        discovery::scan_snapshot_blocking(dir).unwrap()
    }

    fn set_status(m: &ZimManager, name: &str, status: &str) {
        m.cache.write().unwrap().get_mut(name).unwrap().index_status = status.into();
    }

    #[test]
    fn reconcile_adds_new_files() {
        let dir = temp_dir("add");
        let m = manager(&dir);
        write_zim(&dir, "alpha", 100);
        write_zim(&dir, "beta", 200);

        let (added, removed, changed) = m.reconcile(&found(&dir));

        assert_eq!(added, vec!["alpha".to_string(), "beta".to_string()]);
        assert!(removed.is_empty());
        assert!(changed.is_empty());
        assert!(m.get("alpha").is_some());
        assert_eq!(m.get("alpha").unwrap().file_size, 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reconcile_removes_missing_files() {
        let dir = temp_dir("remove");
        let m = manager(&dir);
        write_zim(&dir, "alpha", 100);
        write_zim(&dir, "beta", 200);
        m.reconcile(&found(&dir));

        std::fs::remove_file(dir.join("beta.zim")).unwrap();
        let (added, removed, changed) = m.reconcile(&found(&dir));

        assert!(added.is_empty());
        assert_eq!(removed, vec!["beta".to_string()]);
        assert!(changed.is_empty());
        assert!(m.get("beta").is_none());
        assert!(m.get("alpha").is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reconcile_size_change_resets_ready_index() {
        let dir = temp_dir("sizechange");
        let m = manager(&dir);
        write_zim(&dir, "alpha", 100);
        m.reconcile(&found(&dir));
        set_status(&m, "alpha", "ready");

        // Replace with different-sized content (simulates an updated ZIM).
        write_zim(&dir, "alpha", 300);
        let (added, removed, changed) = m.reconcile(&found(&dir));

        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert_eq!(changed, vec!["alpha".to_string()]);
        let meta = m.get("alpha").unwrap();
        assert_eq!(meta.file_size, 300);
        assert_eq!(meta.index_status, "pending");
        assert_eq!(meta.index_progress, 0.0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reconcile_same_size_no_change() {
        let dir = temp_dir("same");
        let m = manager(&dir);
        write_zim(&dir, "alpha", 100);
        m.reconcile(&found(&dir));
        set_status(&m, "alpha", "ready");

        // Rewrite with identical size and content → up to date.
        write_zim(&dir, "alpha", 100);
        let (added, removed, changed) = m.reconcile(&found(&dir));

        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert!(changed.is_empty());
        assert_eq!(m.get("alpha").unwrap().index_status, "ready");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reconcile_ignores_non_zim_files() {
        let dir = temp_dir("other");
        let m = manager(&dir);
        write_zim(&dir, "alpha", 100);
        std::fs::write(dir.join("notes.txt"), b"hello").unwrap();
        std::fs::write(dir.join("partial.zimaa"), b"chunk").unwrap();

        let (added, _, _) = m.reconcile(&found(&dir));

        assert_eq!(added, vec!["alpha".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Mixed-case file (`TINY.ZIM`): the stored `file_path` must be the real
    /// on-disk path — the old `format!("{name}.zim")` rebuild produced a
    /// nonexistent lowercase path on case-sensitive filesystems, leaving
    /// `file_size = 0` and an unopenable ZIM.
    #[tokio::test]
    async fn reconcile_mixed_case_real_path_and_openable() {
        let dir = temp_dir("mixedcase");
        let m = manager(&dir);
        std::fs::copy("tests/fixtures/tiny.zim", dir.join("TINY.ZIM")).unwrap();

        let (added, removed, changed) = m.reconcile(&found(&dir));

        assert_eq!(added, vec!["TINY".to_string()]);
        assert!(removed.is_empty());
        assert!(changed.is_empty());
        let meta = m.get("TINY").unwrap();
        let real_len = std::fs::metadata(dir.join("TINY.ZIM")).unwrap().len();
        assert_eq!(meta.file_path, dir.join("TINY.ZIM").display().to_string());
        assert_eq!(meta.file_size, real_len);
        // The real on-disk path must be openable (the dead DB pool is never
        // touched by `open_zim`).
        assert!(m.open_zim("TINY").await.is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Extension-case rename (`alpha.zim` → `alpha.ZIM`): the key is the stem
    /// (unchanged), so this is a `changed` entry — `file_path` is updated to
    /// the real path and the existing index stays valid (same content).
    #[test]
    fn reconcile_extension_case_rename_updates_file_path() {
        let dir = temp_dir("extcase");
        let m = manager(&dir);
        write_zim(&dir, "alpha", 100);
        m.reconcile(&found(&dir));
        set_status(&m, "alpha", "ready");

        std::fs::rename(dir.join("alpha.zim"), dir.join("alpha.ZIM")).unwrap();
        let (added, removed, changed) = m.reconcile(&found(&dir));

        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert_eq!(changed, vec!["alpha".to_string()]);
        let meta = m.get("alpha").unwrap();
        assert_eq!(meta.file_path, dir.join("alpha.ZIM").display().to_string());
        assert_eq!(meta.index_status, "ready");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resync_skips_missing_dir() {
        let dir =
            std::env::temp_dir().join(format!("zimi-reconcile-{}-missing", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let m = manager(&dir); // dir does not exist
        let report = m.resync().await.unwrap();
        assert!(report.is_empty());
    }

    #[tokio::test]
    async fn resync_no_changes_is_noop() {
        let dir = temp_dir("noop");
        let m = manager(&dir);
        write_zim(&dir, "alpha", 100);
        m.reconcile(&found(&dir)); // populate cache only

        // A resync with nothing new must not touch the DB (which would fail:
        // the pool is unroutable) and report no changes.
        let report = m.resync().await.unwrap();
        assert!(report.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Step 5.6 (T9c): once the directory and cache are stable, repeated
    /// rescans must be cheap no-ops (snapshot early-out) and never touch the
    /// DB. The pool is unroutable, so any accidental DB write would panic.
    #[tokio::test]
    async fn resync_is_idempotent_repeated_noop() {
        let dir = temp_dir("idempotent");
        let m = manager(&dir);
        write_zim(&dir, "alpha", 100);
        m.reconcile(&found(&dir)); // seed the in-memory cache

        // First resync: populates the snapshot; reconcile finds no DB diff,
        // so no DB writes occur.
        let r1 = m.resync().await.unwrap();
        // Second / third: snapshot matches → early-out, guaranteed no-ops.
        let r2 = m.resync().await.unwrap();
        let r3 = m.resync().await.unwrap();

        assert!(r1.is_empty(), "first resync should report nothing: {r1:?}");
        assert!(r2.is_empty(), "second resync should be a no-op: {r2:?}");
        assert!(r3.is_empty(), "third resync should be a no-op: {r3:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── WI-38 (PERF-6): staleness predicate + LRU eviction ─────────────────

    #[test]
    fn handle_still_valid_matrix() {
        let t = SystemTime::UNIX_EPOCH;
        // Same (mtime, size) → still valid.
        assert!(handle_still_valid(Some(t), 100, Some(t), 100));
        // Size-only change → stale.
        assert!(!handle_still_valid(Some(t), 100, Some(t), 101));
        // mtime-only change → stale.
        assert!(!handle_still_valid(
            Some(t),
            100,
            Some(t + std::time::Duration::from_secs(1)),
            100
        ));
        // Both absent (None mtime, 0 size) → valid (matches the call sites
        // where a missing stat is normalized to None/0).
        assert!(handle_still_valid(None, 0, None, 0));
        // Present vs absent mtime → stale.
        assert!(!handle_still_valid(Some(t), 100, None, 100));
    }

    #[test]
    fn evict_lru_if_full_drops_min_last_access_at_capacity() {
        // Parse one real ZIM and share its Arc across every slot: the eviction
        // logic only inspects `last_access`, never the Zim itself.
        let zim =
            Arc::new(zim::Zim::new(Path::new("tests/fixtures/tiny.zim")).expect("tiny.zim parses"));
        let mut handles: HashMap<String, CachedZim> = HashMap::new();
        for i in 0..MAX_OPEN_ZIMS {
            handles.insert(
                format!("zim-{i}"),
                CachedZim {
                    zim: zim.clone(),
                    mtime: None,
                    size: 0,
                    last_access: AtomicU64::new(i as u64),
                    last_stat: AtomicU64::new(0),
                },
            );
        }
        // At capacity (== MAX_OPEN_ZIMS) → evict the min-`last_access` key.
        evict_lru_if_full(&mut handles);
        assert!(
            !handles.contains_key("zim-0"),
            "min-last_access key must be evicted at capacity"
        );
        assert_eq!(handles.len(), MAX_OPEN_ZIMS - 1);
        assert!(handles.contains_key("zim-1"));
        assert!(handles.contains_key(&format!("zim-{}", MAX_OPEN_ZIMS - 1)));

        // Below capacity → no eviction.
        evict_lru_if_full(&mut handles);
        assert_eq!(handles.len(), MAX_OPEN_ZIMS - 1);
    }

    #[tokio::test]
    async fn file_etag_reports_mtime_and_size() {
        let dir = temp_dir("etag");
        write_zim(&dir, "x", 5);
        let m = manager(&dir);
        m.scan().await.unwrap();
        let etag = m.file_etag("x").expect("etag for a present ZIM");
        assert!(
            etag.ends_with("-5\""),
            "etag must carry the 5-byte size: {etag}"
        );
        assert!(etag.starts_with('"'), "strong tag is quoted: {etag}");
        assert_eq!(m.file_etag("missing"), None, "unknown ZIM → no etag");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn etag_from_metadata_matches_file_etag() {
        // W6.1: the extracted `etag_from_metadata` must produce a byte-identical
        // tag to `file_etag` for the same file — a format drift between the two
        // would 304 a stale entry after an in-place replace.
        let dir = temp_dir("etag2");
        write_zim(&dir, "x", 5);
        let m = manager(&dir);
        m.scan().await.unwrap();
        let meta = m.get("x").expect("meta");
        let md = std::fs::metadata(meta.file_path).unwrap();
        let from_meta = etag_from_metadata(&md).expect("etag from metadata");
        let via_accessor = m.file_etag("x").expect("etag via file_etag");
        assert_eq!(
            from_meta, via_accessor,
            "etag_from_metadata must match file_etag"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
