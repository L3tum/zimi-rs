//! Post-download file handling: install ZIMs into the ZIM directory via
//! hardlink (preferred: zero extra space) with copy fallback, and locate
//! `.zim` files inside a torrent's content directory.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::error::{Error, Result, TorrentKind};

/// Recursively find `.zim` files under `root` (case-insensitive extension).
///
/// Depth- and result-capped so a misbehaving save path (e.g. pointing at `/`)
/// cannot produce a runaway scan. Returns paths sorted for determinism.
pub fn find_zim_files(root: &Path, max_files: usize) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(root, 0, 8, &mut out, max_files);
    out.sort();
    out
}

fn walk(dir: &Path, depth: usize, max_depth: usize, out: &mut Vec<PathBuf>, cap: usize) {
    if out.len() >= cap || depth > max_depth {
        return;
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        if out.len() >= cap {
            return;
        }
        let path = entry.path();
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        if is_dir {
            walk(&path, depth + 1, max_depth, out, cap);
        } else if is_zim_path(&path) {
            out.push(path);
        }
    }
}

/// Is this path a ZIM archive by extension (case-insensitive)?
pub fn is_zim_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("zim"))
        .unwrap_or(false)
}

/// Monotonic per-process counter making install tmp names unique per call
/// (B5: two concurrent installs of the same ZIM name must never share — and
/// thus corrupt or delete — a tmp path).
static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Install tmp files older than this are considered stale (crashed runs).
const STALE_TMP_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Whether `path` is a stale `*.zim.tmp-*` install file that cleanup should
/// remove: it is not the current call's tmp, it matches the tmp name shape,
/// and its mtime is more than `STALE_TMP_AGE` before `now`.
///
/// Hardlink mtime edge (documented): a hardlinked tmp inherits the *source*
/// file's mtime (typically hours old for a downloaded ZIM), so an in-flight
/// tmp can look stale. With per-call unique names the only exposure is the
/// microsecond hardlink→rename window of a same-name concurrent install —
/// the scan skips `current_tmp` and accepts that residual.
fn is_stale_tmp(path: &Path, current_tmp: &Path, now: SystemTime) -> bool {
    if path == current_tmp {
        return false;
    }
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    if !name.contains(".zim.tmp-") {
        return false;
    }
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    let Ok(mtime) = meta.modified() else {
        return false;
    };
    let epoch = SystemTime::UNIX_EPOCH;
    let Ok(now_secs) = now.duration_since(epoch) else {
        return false;
    };
    let Ok(mtime_secs) = mtime.duration_since(epoch) else {
        return false;
    };
    now_secs >= STALE_TMP_AGE && mtime_secs < now_secs - STALE_TMP_AGE
}

/// Remove stale install tmps (`*.zim.tmp-*` older than `STALE_TMP_AGE`) from
/// `dir`, skipping `current_tmp` (the in-flight file of this install — it is
/// about to be written and must never be deleted under itself). Best-effort:
/// failures are logged at debug level, never fatal.
fn remove_stale_tmps(dir: &Path, current_tmp: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let path = entry.path();
        if is_stale_tmp(&path, current_tmp, now) {
            tracing::debug!("removing stale install tmp {}", path.display());
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Install a downloaded ZIM into `zim_dir`.
///
/// - `strategy == "hardlink"`: hardlink when possible (same filesystem, no
///   extra space used); falls back to a full copy across devices.
/// - any other strategy (or "copy"): full copy.
///
/// An existing file with the same name in `zim_dir` is replaced.
/// Returns the final path inside `zim_dir`.
pub fn install_zim(src: &Path, zim_dir: &Path, strategy: &str) -> Result<PathBuf> {
    let file_name = src
        .file_name()
        .ok_or_else(|| Error::InvalidInput(format!("no file name in {}", src.display())))?;
    let dst = zim_dir.join(file_name);

    // Atomic install: write to a tmp path in the same directory, then rename.
    // This avoids a window where dst is missing (data-loss risk) or half-written.
    // The tmp name is unique per call (pid + counter): a deterministic
    // `tmp-<pid>` name let a second concurrent install of the same ZIM delete
    // the first install's in-flight tmp (B5).
    let n = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dst.with_extension(format!("zim.tmp-{}-{}", std::process::id(), n));

    // Clean up stale tmps from crashed runs (mtime-gated; never touches the
    // current call's tmp).
    remove_stale_tmps(zim_dir, &tmp);

    let install_result: Result<()> = if strategy == "hardlink" {
        match std::fs::hard_link(src, &tmp) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
                match std::fs::copy(src, &tmp) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Error::Io(e)),
                }
            }
            Err(e) => Err(Error::Io(e)),
        }
    } else {
        match std::fs::copy(src, &tmp) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Io(e)),
        }
    };

    if let Err(e) = install_result {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }

    std::fs::rename(&tmp, &dst)?;
    Ok(dst)
}

/// Locate the single `.zim` a completed torrent produced.
///
/// qBittorrent reports `content_path` as the *first file* of the torrent:
/// for a single-file torrent that is the `.zim` itself (a file, not a
/// directory), for a multi-file torrent it is the path of the first entry.
///
/// - `path` is a file → it must be a `.zim`, that is the archive.
/// - `path` is a directory → exactly one `.zim` must be found underneath.
///   Zero → "no .zim found"; more than one → explicit rejection: a multi-
///   file torrent with several ZIMs is ambiguous and must not be silently
///   installed.
pub fn locate_torrent_zim(path: &Path) -> Result<PathBuf> {
    if path.is_file() {
        if !is_zim_path(path) {
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg: format!("expected a .zim file, got {}", path.display()),
            });
        }
        return Ok(path.to_path_buf());
    }

    let mut found = find_zim_files(path, 10);
    match found.len() {
        1 => Ok(found.pop().expect("found.len()==1 guarantees pop succeeds")),
        0 => Err(Error::Torrent {
            kind: TorrentKind::Other,
            msg: format!("no .zim file found in {}", path.display()),
        }),
        n => {
            let names: Vec<String> = found
                .iter()
                .map(|p| {
                    p.file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string()
                })
                .collect();
            Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg: format!(
                    "torrent produced {n} .zim files, expected exactly one ({}); \
                     multi-file torrents with several ZIMs are not supported",
                    names.join(", ")
                ),
            })
        }
    }
}

/// Verify a file is a readable ZIM archive (parses the header + central dir).
/// Fails for truncated/corrupt files.
///
/// **Integrity scope (SEC M3b):** this verifies *structural* integrity only
/// (the `zim` 0.5 crate parses the header + central directory; there is no
/// checksum or signature API). It does **not** authenticate the bytes — a
/// structurally valid but tampered or malicious ZIM still parses **and is
/// served**. There is no end-to-end byte authentication anywhere in the
/// pipeline: trust comes from pinning trusted OPDS/torrent sources plus
/// (https) TLS, not from this check.
///
/// # Trust model
///
/// This check verifies **structural integrity only** (the file parses as a
/// ZIM with a valid header, article index, and cluster layout). It does NOT
/// verify content provenance or integrity:
///
/// - No cryptographic signature is checked. A ZIM produced by a compromised
///   source (or a malicious peer in a torrent swarm) will pass this check if
///   it is structurally valid.
/// - The operator's trust boundary is the **source** (the OPDS feed URL or
///   the torrent tracker), not the file itself.
/// - Content served by zimservice is trusted by its consumers (the web UI,
///   the OPDS client) exactly as much as the ZIM source is trusted by the
///   operator.
///
/// For high-trust deployments, pin torrents by hash and/or verify the OPDS
/// feed over TLS with a known-good endpoint.
pub fn verify_zim(path: &Path) -> Result<()> {
    let path_str = path.display().to_string();
    zim::Zim::new(&path_str).map_err(|e| Error::Zim(format!("{}: {e}", path.display())))?;
    Ok(())
}

/// Validate a user-supplied download name.
///
/// The name is spliced directly into a file path under the ZIM directory
/// (`{name}.part`), so anything with a path separator, `..`, or a leading
/// dot is rejected. Length is capped at 255 bytes (ext4 NAME_MAX).
pub fn validate_download_name(name: &str) -> Result<()> {
    let name = name.trim();
    if name.is_empty() {
        return Err(Error::InvalidInput("download name is empty".into()));
    }
    if name.len() > 255 {
        return Err(Error::InvalidInput(
            "download name exceeds 255 characters".into(),
        ));
    }
    if name.contains(['/', '\\', '\0']) {
        return Err(Error::InvalidInput(
            "download name must not contain path separators".into(),
        ));
    }
    if name.contains("..") {
        return Err(Error::InvalidInput(
            "download name must not contain `..`".into(),
        ));
    }
    if name.starts_with('.') {
        return Err(Error::InvalidInput(
            "download name must not start with a dot".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zimi-files-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(path: &Path, data: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(data).unwrap();
    }

    #[test]
    fn is_zim_path_checking() {
        assert!(is_zim_path(Path::new("wikipedia_en_all.zim")));
        assert!(is_zim_path(Path::new("x/Y.ZIM")));
        assert!(!is_zim_path(Path::new("x.zimaa")));
        assert!(!is_zim_path(Path::new("zim")));
    }

    #[test]
    fn is_stale_tmp_predicate() {
        let base = tmp("stale");
        let current = base.join(format!("a.zim.tmp-{}-0", std::process::id()));
        let other = base.join("a.zim.tmp-1-2");
        let notmp = base.join("a.zim");
        write(&other, b"x");
        write(&notmp, b"x");

        let real_now = SystemTime::now();
        // A "now" two hours in the future makes the just-written tmp look
        // older than STALE_TMP_AGE (1h) — no mtime manipulation needed.
        let old_now = real_now + std::time::Duration::from_secs(2 * 3600);

        assert!(
            is_stale_tmp(&other, &current, old_now),
            "1h-old tmp is stale"
        );
        assert!(
            !is_stale_tmp(&other, &current, real_now),
            "fresh tmp is not stale"
        );
        assert!(
            !is_stale_tmp(&notmp, &current, old_now),
            "non-tmp file is never stale"
        );
        // The current tmp is never stale — the equality check short-circuits
        // before any metadata read (so even a nonexistent one is skipped).
        assert!(!is_stale_tmp(&current, &current, old_now));
        // A tmp whose own name is passed as `current_tmp` is also skipped.
        assert!(!is_stale_tmp(&other, &other, old_now));
        // Missing tmp file → not stale (nothing to remove).
        assert!(!is_stale_tmp(
            &base.join("a.zim.tmp-1-9"),
            &current,
            old_now
        ));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn find_zim_files_nested() {
        let dir = tmp("find");
        write(&dir.join("a.zim"), b"x");
        write(&dir.join("sub/b.zim"), b"x");
        write(&dir.join("sub/deep/c.ZIM"), b"x");
        write(&dir.join("notes.txt"), b"x");
        write(&dir.join("sub/d.zimaa"), b"x");

        let found = find_zim_files(&dir, 100);
        let names: Vec<String> = found
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_lowercase())
            .collect();
        assert_eq!(names, vec!["a.zim", "b.zim", "c.zim"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn find_zim_files_capped() {
        let dir = tmp("cap");
        for i in 0..10 {
            write(&dir.join(format!("z{i}.zim")), b"x");
        }
        let found = find_zim_files(&dir, 3);
        assert_eq!(found.len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn install_hardlink_same_device() {
        let base = tmp("hl");
        let src = base.join("src/a.zim");
        write(&src, b"zimdata");
        let zim_dir = base.join("zim");
        std::fs::create_dir_all(&zim_dir).unwrap();

        let dst = install_zim(&src, &zim_dir, "hardlink").unwrap();
        assert!(dst.exists());
        assert_eq!(dst, zim_dir.join("a.zim"));

        // Same device → real hardlink: identical inode, link count 2.
        let m1 = std::fs::metadata(&src).unwrap();
        let m2 = std::fs::metadata(&dst).unwrap();
        use std::os::unix::fs::MetadataExt;
        assert_eq!(m1.ino(), m2.ino());
        assert_eq!(m2.nlink(), 2);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn install_copy_strategy_independent_file() {
        let base = tmp("copy");
        let src = base.join("src/a.zim");
        write(&src, b"zimdata");
        let zim_dir = base.join("zim");
        std::fs::create_dir_all(&zim_dir).unwrap();

        let dst = install_zim(&src, &zim_dir, "copy").unwrap();
        let m1 = std::fs::metadata(&src).unwrap();
        let m2 = std::fs::metadata(&dst).unwrap();
        use std::os::unix::fs::MetadataExt;
        assert_ne!(m1.ino(), m2.ino());
        assert_eq!(std::fs::read(&dst).unwrap(), b"zimdata");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn install_replaces_existing() {
        let base = tmp("replace");
        let src = base.join("src/a.zim");
        write(&src, b"new-content");
        let zim_dir = base.join("zim");
        std::fs::create_dir_all(&zim_dir).unwrap();
        write(&zim_dir.join("a.zim"), b"old-content");

        let dst = install_zim(&src, &zim_dir, "copy").unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"new-content");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn install_rejected_without_file_name() {
        let base = tmp("noname");
        std::fs::create_dir_all(&base).unwrap();
        // "/" has no file_name → must be an error, not a panic.
        assert!(install_zim(Path::new("/"), &base, "copy").is_err());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn locate_single_file_torrent() {
        let base = tmp("locate-file");
        let zim = base.join("wiki.zim");
        write(&zim, b"x");
        let other = base.join("notes.txt");
        write(&other, b"x");

        // content_path is the file itself (single-file torrent)
        assert_eq!(locate_torrent_zim(&zim).unwrap(), zim);
        // a non-zim file is rejected
        assert!(locate_torrent_zim(&other).is_err());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn locate_dir_with_exactly_one_zim() {
        let base = tmp("locate-one");
        write(&base.join("root/a.zim"), b"x");
        write(&base.join("root/readme.txt"), b"x");

        assert_eq!(
            locate_torrent_zim(&base.join("root")).unwrap(),
            base.join("root/a.zim")
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn locate_dir_empty_or_multi_rejected() {
        let base = tmp("locate-bad");
        write(&base.join("empty/readme.txt"), b"x");
        assert!(locate_torrent_zim(&base.join("empty")).is_err());

        write(&base.join("multi/a.zim"), b"x");
        write(&base.join("multi/b.zim"), b"x");
        let err = locate_torrent_zim(&base.join("multi")).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("2 .zim files"), "got: {msg}");
        assert!(msg.contains("a.zim") && msg.contains("b.zim"), "got: {msg}");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn validate_download_name_rejects_traversal() {
        // traversal attempts
        assert!(validate_download_name("../../etc/passwd").is_err());
        assert!(validate_download_name("..\\..\\evil").is_err());
        assert!(validate_download_name("..").is_err());
        assert!(validate_download_name("a/b.zim").is_err());
        assert!(validate_download_name("a\\b.zim").is_err());
        assert!(validate_download_name("a\0b.zim").is_err());
        assert!(validate_download_name("a..b.zim").is_err());
        // leading dot (hidden files, and `.part`-suffix confusion)
        assert!(validate_download_name(".hidden.zim").is_err());
        // empty / whitespace / too long
        assert!(validate_download_name("").is_err());
        assert!(validate_download_name("   ").is_err());
        assert!(validate_download_name(&"a".repeat(256)).is_err());
        // valid names (whitespace is trimmed, not an error)
        assert!(validate_download_name("  wikipedia_en_all.zim  ").is_ok());
        assert!(validate_download_name("zim with spaces.zim").is_ok());
        assert!(validate_download_name("a.b.zim").is_ok());
        assert!(validate_download_name(&"a".repeat(255)).is_ok());
    }
}
