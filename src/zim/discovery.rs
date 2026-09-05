//! ZIM directory discovery: periodic scan + blocking snapshot for resync.
//!
//! `watch_zim_dir` rescans every 10 s as a safety net for new/removed files;
//! `scan_snapshot_blocking` does the blocking scan+stat (run via
//! `spawn_blocking`) and `scan_directory` returns just the names.
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::error::{Error, Result};
use crate::zim::ZimManager;

/// One scanned ZIM: the file's name (stem, no extension), its **real
/// on-disk path**, and its `(mtime, size)` metadata — `None` when the file
/// vanished between `read_dir` and stat. Carrying the real path (rather than
/// rebuilding `name.zim` from the stem) is what makes mixed-case files like
/// `WIKIPEDIA.ZIM` usable on case-sensitive filesystems. A `Vec` of these is
/// the return of [`scan_snapshot_blocking`].
pub(crate) type ScanEntry = (String, PathBuf, Option<(SystemTime, u64)>);

/// Interval between directory rescans.
///
/// 10 s is the safety-net cadence for files dropped directly into the ZIM
/// directory (bypassing a download). The real-time path is unaffected: the
/// download poller calls `resync()` explicitly the moment a file lands, so
/// this interval only bounds how long a manually-placed ZIM waits to be picked
/// up. (P4: was 2 s — too chatty for a directory that changes rarely.)
const RESYNC_INTERVAL: Duration = Duration::from_secs(10);

/// Periodically rescan the ZIM directory (every 10 s) and reconcile with the
/// database. Downloads are detected by the poller; this is the safety net.
///
/// Runs until the process exits; rescan failures are logged and the loop
/// continues (the service keeps working, just without auto-detection).
pub async fn watch_zim_dir(zims: &std::sync::Arc<ZimManager>) {
    loop {
        tokio::time::sleep(RESYNC_INTERVAL).await;
        if let Err(e) = zims.resync().await {
            tracing::warn!("ZIM directory rescan failed: {e}");
        }
    }
}

/// Scan a directory for .zim files and return their names (without extension).
pub fn scan_directory(dir: &Path) -> Result<Vec<String>> {
    scan_snapshot_blocking(dir)
        .map(|entries| entries.into_iter().map(|(name, _, _)| name).collect())
}

/// `(mtime, size)` snapshot of a single file, or `None` when the file can't
/// be stat'd or has no mtime. Size is part of the key so a same-mtime content
/// change is still detected — mtime resolution is coarse on some filesystems
/// and `rsync`/re-downloads can preserve it while the bytes (and length)
/// change.
pub(crate) fn snapshot_entry(path: &Path) -> Option<(SystemTime, u64)> {
    let m = fs::metadata(path).ok()?;
    let mt = m.modified().ok()?;
    Some((mt, m.len()))
}

/// Synchronous (blocking) scan of a ZIM directory: every `.zim` file name
/// (case-insensitive extension, no extension, sorted) paired with its real
/// on-disk path and its `(mtime, size)` metadata. `None` marks a name whose
/// stat failed (the file vanished between `read_dir` and stat, or is
/// unstat-able) — callers keep such names in their reconcile input so a
/// transient stat failure never looks like a deletion.
///
/// This does blocking I/O; call it from `tokio::task::spawn_blocking` (see
/// `ZimManager::resync`).
pub(crate) fn scan_snapshot_blocking(dir: &Path) -> Result<Vec<ScanEntry>> {
    let mut entries = Vec::new();

    if !dir.exists() {
        return Ok(entries);
    }

    for entry in fs::read_dir(dir).map_err(Error::Io)? {
        let entry = entry.map_err(Error::Io)?;
        let path = entry.path();

        if path.is_file()
            && path
                .extension()
                .map(|e| e.eq_ignore_ascii_case("zim"))
                .unwrap_or(false)
        {
            if let Some(stem) = path.file_stem() {
                entries.push((
                    stem.to_string_lossy().to_string(),
                    path.clone(),
                    snapshot_entry(&path),
                ));
            }
        }
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(entries)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(dir: &Path, name: &str) {
        let mut f = fs::File::create(dir.join(name)).expect("create test file");
        f.write_all(b"x").expect("write test file");
    }

    #[test]
    fn scan_directory_is_case_insensitive() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_file(dir.path(), "WIKIPEDIA.ZIM");
        write_file(dir.path(), "x.Zim");
        write_file(dir.path(), "lower.zim");
        write_file(dir.path(), "notazim.txt");
        write_file(dir.path(), "zim"); // no dot extension

        let names = scan_directory(dir.path()).expect("scan");
        // Unicode codepoint sort: uppercase (W=0x57) < lowercase (l=0x6C, x=0x78)
        assert_eq!(names, vec!["WIKIPEDIA", "lower", "x"], "got: {names:?}");
    }

    #[test]
    fn scan_directory_missing_dir_is_empty_ok() {
        let names = scan_directory(Path::new("/nonexistent/zim/dir")).expect("scan");
        assert!(names.is_empty());
    }

    #[test]
    fn scan_snapshot_blocking_missing_dir_empty() {
        let entries = scan_snapshot_blocking(Path::new("/nonexistent/zim/dir")).expect("scan");
        assert!(entries.is_empty());
    }

    #[test]
    fn scan_snapshot_blocking_lists_and_orders() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_file(dir.path(), "ZULU.ZIM"); // uppercase extension
        write_file(dir.path(), "alpha.zim");
        write_file(dir.path(), "mid.Zim");
        write_file(dir.path(), "ignore.txt"); // not a .zim
        write_file(dir.path(), "zim"); // no dot extension

        let entries = scan_snapshot_blocking(dir.path()).expect("scan");
        let names: Vec<&str> = entries.iter().map(|(n, _, _)| n.as_str()).collect();
        // Unicode codepoint sort: uppercase (Z) < lowercase (a, m)
        assert_eq!(names, vec!["ZULU", "alpha", "mid"], "got: {names:?}");
        // Every listed file stat'd fine → every entry has metadata
        assert!(
            entries.iter().all(|(_, _, e)| e.is_some()),
            "got: {entries:?}"
        );
    }

    #[test]
    fn snapshot_entry_differs_on_size() {
        // mtime resolution is coarse (two quick writes often share an mtime),
        // so size must be what distinguishes a same-mtime rewrite — this is
        // the exact case the mtime-only snapshot used to miss (B6).
        let dir = tempfile::tempdir().expect("tempdir");
        let p = dir.path().join("a.zim");
        std::fs::write(&p, vec![0u8; 100]).expect("write");
        let e1 = snapshot_entry(&p).expect("first entry");
        assert_eq!(e1.1, 100);
        std::fs::write(&p, vec![0u8; 101]).expect("write");
        let e2 = snapshot_entry(&p).expect("second entry");
        assert_eq!(e2.1, 101);
        assert_ne!(
            e1, e2,
            "same-mtime size change must produce a different snapshot entry"
        );
    }

    #[test]
    fn snapshot_entry_missing_file_is_none() {
        assert!(snapshot_entry(Path::new("/nonexistent/a.zim")).is_none());
    }
}
