//! Startup reconciliation: rows vs live qBittorrent state, orphan `.part`
//! sweep, and adoption of untracked category torrents.

use crate::torrent::TorrentInfo;

use super::*;

/// Download row snapshot used by reconcile:
/// `(id, name, url, hash, status, file_path, updated_at)`.
type TorrentRow = (
    i32,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    chrono::DateTime<chrono::Utc>,
);

impl DownloadPoller {
    pub(super) async fn reconcile(&self, qbit: Option<Arc<QbitClient>>) -> Result<()> {
        // PERF-12: snapshot all poller settings once per reconcile.
        let p = self.settings.poller_params_snapshot();

        // PERF-11 sweep protect-set, collected BEFORE the retry below:
        // `retry_interrupted_directs` clears `file_path` on interrupted
        // `downloading` direct rows (so they can be re-queued to resume),
        // and the orphan sweep deletes every `.part` not in the protect-set.
        // Building the set after the retry would drop those rows' `.part`
        // and the sweep would delete it milliseconds later — silently
        // defeating restart-resume (full re-download of a partially
        // downloaded multi-GB file on every restart). The set also derives
        // each row's `zim_dir/{name}.part` resume target, so a row the
        // retry is about to flip is protected even before it re-claims.
        let active_paths = self.fetch_resumable_part_paths().await?;

        // Interrupted direct downloads (`.zim` rows after stripping query
        // and fragment, see `is_direct_zim_url`): retry — resumes from a
        // surviving `.part` when one exists (PERF-11).
        let n = crate::db::downloads_lifecycle::retry_interrupted_directs(&self.db).await?;
        if n > 0 {
            tracing::info!("reconcile: retrying {n} interrupted direct download(s)");
        }

        // PERF-11 sweep: reclaim `.part` files whose download row will not
        // resume (a terminal-state row never resumes). `error` rows ARE
        // included in the protect-set: they keep `file_path` (the `.part`)
        // and the bounded 10-min retry re-queues them, so deleting the
        // `.part` would force a full multi-GB re-download (B1). Only truly
        // orphaned `.part`s (no live row, or a terminal `cancelled`/`complete`
        // row) are reclaimed.
        if let Ok(rd) = std::fs::read_dir(&self.zims.zim_dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.extension().and_then(|s| s.to_str()) == Some("part")
                    && !active_paths.contains(&p.display().to_string())
                {
                    tracing::debug!("reconcile: removing orphan .part file {}", p.display());
                    let _ = std::fs::remove_file(&p);
                }
            }
        }

        let Some(q) = qbit.as_ref() else {
            return Ok(());
        };
        let torrents = q.get_torrents("all").await?;
        let by_hash: HashMap<&str, &TorrentInfo> =
            torrents.iter().map(|t| (t.hash.as_str(), t)).collect();
        let by_name: HashMap<String, &TorrentInfo> = torrents
            .iter()
            .map(|t| (t.name.to_lowercase(), t))
            .collect();

        // 1) Rows: rebind hashes, recover finished torrents, flag orphans.
        let rows: Vec<TorrentRow> = crate::db::raw::fetch_all(
            &self.db,
            "SELECT id, name, url, hash, status, file_path, updated_at FROM downloads \
             WHERE status IN ($1, $2, $3)",
            |q| {
                q.bind(crate::torrent::DownloadStatus::Downloading.as_str())
                    .bind(crate::torrent::DownloadStatus::Complete.as_str())
                    .bind(crate::torrent::DownloadStatus::Seeding.as_str())
            },
        )
        .await?;

        let mut known_hashes = HashSet::new();
        // Inflight-2: qB entries already matched by an earlier row this pass.
        // `bind_hash` does not update the local snapshot maps, and the arms
        // that do not bind (complete-reinstall, seeding refresh) leave the
        // row's hash NULL — so without this set, two same-name rows could
        // double-match one torrent (double `handle_complete`), and a torrent
        // owned by a NULL-hash row could be re-adopted as a duplicate row
        // (the adoption's `NOT EXISTS (hash = …)` guard cannot see it).
        let mut consumed: HashSet<String> = HashSet::new();
        for (id, name, url, hash, status, file_path, updated_at) in &rows {
            let id = *id;
            if crate::torrent::is_direct_zim_url(url) {
                continue; // direct — handled above
            }
            if let Some(h) = &hash {
                known_hashes.insert(h.clone());
            }

            let t = hash
                .as_deref()
                .and_then(|h| by_hash.get(h))
                .or_else(|| {
                    by_name
                        .get(name.to_lowercase().as_str())
                        .or_else(|| by_name.get(url.to_lowercase().as_str()))
                        .filter(|t| !consumed.contains(&super::inflight::torrent_identity(t)))
                })
                .copied();
            if let Some(t) = t {
                consumed.insert(super::inflight::torrent_identity(t));
            }

            match (status.as_str(), t) {
                ("downloading", Some(t)) => {
                    if hash.as_deref() != Some(t.hash.as_str()) {
                        crate::db::downloads_lifecycle::bind_hash(&self.db, id, &t.hash).await?;
                    }
                    if t.is_complete() {
                        // We died between qB finishing and the file install.
                        if let Err(e) = self
                            .handle_complete(id, name, t, qbit.clone(), &p, file_path.clone())
                            .await
                        {
                            let marked = crate::db::downloads_lifecycle::mark_fatal_error(
                                &self.db,
                                id,
                                &e.to_string(),
                            )
                            .await?;
                            if marked == 0 {
                                tracing::info!(
                                    download_id = id,
                                    "skipping fatal error mark: row no longer queued/downloading"
                                );
                            }
                        }
                    }
                }
                ("downloading", None) => {
                    // Same grace as tick: a torrent paused mid-download or a
                    // slow qB at boot must not be mass-errored at startup.
                    // reconcile runs once, so a genuinely deleted torrent that
                    // we skip here surfaces via tick's MISSING_TORRENT_GRACE
                    // arm once `updated_at` ages out (the row is no longer
                    // refreshed while invisible).
                    if chrono::Utc::now() - updated_at > MISSING_TORRENT_GRACE {
                        let marked = crate::db::downloads_lifecycle::mark_fatal_error(
                            &self.db,
                            id,
                            "torrent missing from qBittorrent",
                        )
                        .await?;
                        if marked == 0 {
                            tracing::info!(
                                download_id = id,
                                "skipping fatal error mark: row no longer queued/downloading"
                            );
                        }
                        tracing::warn!(
                            "reconcile: download {id} ({name}) has no matching torrent in qBittorrent"
                        );
                    } else {
                        tracing::debug!(
                            "reconcile: download {id} ({name}) torrent missing but within grace — re-check on next tick"
                        );
                    }
                }
                ("complete", Some(t)) => {
                    let has_file = file_path
                        .as_deref()
                        .map(|p| Path::new(p).exists())
                        .unwrap_or(false);
                    if !has_file {
                        // File was deleted but the torrent is still around
                        // (seeding) — reinstall from it.
                        if let Err(e) = self
                            .handle_complete(id, name, t, qbit.clone(), &p, file_path.clone())
                            .await
                        {
                            tracing::warn!("reconcile: re-install of download {id} failed: {e}");
                        }
                    }
                }
                ("seeding", Some(t)) => {
                    if t.is_fatal() {
                        let msg = t
                            .err_str
                            .clone()
                            .unwrap_or_else(|| format!("torrent state: {}", t.state));
                        crate::db::downloads_lifecycle::settle_seeding(&self.db, id, Some(&msg))
                            .await?;
                    } else {
                        crate::db::downloads_lifecycle::refresh_seeding_stats(
                            &self.db,
                            id,
                            t.ratio as f32,
                            kibs_to_bps(t.upspeed),
                            t.num_seeds,
                            kibs_to_bps(t.dlspeed),
                        )
                        .await?;
                    }
                }
                ("seeding", None) => {
                    // Torrent no longer in qB — the file is installed, settle to complete.
                    crate::db::downloads_lifecycle::settle_seeding(&self.db, id, None).await?;
                }
                ("complete", None) => {
                    // Torrent already removed from qB and the file is
                    // presumably in the library — nothing to do.
                }
                _ => {}
            }
        }

        // 2) Category torrents with no tracking row (e.g. after a DB reset,
        //    or added manually in the qB UI) → adopt them. Inflight-2: a
        //    torrent an earlier row already matched this pass (bound or not)
        //    is owned — adopting it would create a duplicate row for the
        //    same torrent, which `known_hashes` (built from the pre-pass
        //    hash values) cannot always catch.
        let category = p.category.clone();
        for t in torrents
            .iter()
            .filter(|t| t.category.as_deref() == Some(category.as_str()))
        {
            if known_hashes.contains(&t.hash) || consumed.contains(&t.hash) {
                continue;
            }
            let status = if t.is_complete() {
                crate::torrent::DownloadStatus::Complete
            } else {
                crate::torrent::DownloadStatus::Downloading
            };
            crate::db::downloads_lifecycle::adopt_torrent(
                &self.db,
                &t.name,
                &t.hash,
                status,
                t.progress as f32,
            )
            .await?;
            tracing::info!(
                "reconcile: adopted qBittorrent torrent '{}' ({})",
                t.name,
                t.hash
            );
        }

        Ok(())
    }

    /// The `.part` files the orphan sweep must NOT delete, selected BEFORE
    /// [`retry_interrupted_directs`](crate::db::downloads_lifecycle::retry_interrupted_directs)
    /// — see the call site in [`Self::reconcile`]. Propagates a failed query
    /// — never default to an empty protect-set: an empty set makes the sweep
    /// delete the `.part` files of *live* downloads, so a poisoned query must
    /// abort the reconcile (same propagation-instead-of-unwrap_or_default
    /// convention as the direct-download client builder in direct.rs).
    async fn fetch_resumable_part_paths(&self) -> Result<HashSet<String>> {
        // The predicate fragment ([`ZIM_URL_PREDICATE`]) splices in verbatim.
        let pred = ZIM_URL_PREDICATE;
        let rows: Vec<(String, Option<String>)> = crate::db::raw::fetch_all(
            &self.db,
            &format!(
                "SELECT name, file_path FROM downloads \
                 WHERE status IN ($1, $2, $3) \
                   AND ({pred}) LIKE '%.zim'"
            ),
            |q| {
                q.bind(crate::torrent::DownloadStatus::Queued.as_str())
                    .bind(crate::torrent::DownloadStatus::Downloading.as_str())
                    .bind(crate::torrent::DownloadStatus::Error.as_str())
            },
        )
        .await?;
        Ok(resumable_part_paths(&rows, &self.zims.zim_dir))
    }
}

/// PERF-11 (pure): the `.part` files the reconcile orphan sweep must NOT
/// delete, derived from the direct `.zim` rows that will resume (`queued` /
/// `downloading` / `error`).
///
/// Each row contributes two entries: its live `file_path` (the claimed
/// `.part`, protected as-is) and its expected resume target
/// `zim_dir/{name}.part` (the exact path `claim_direct` writes on the
/// re-claim). The derived entry is what covers rows that
/// `retry_interrupted_directs` is about to flip `downloading → queued` (the
/// retry clears `file_path`), and re-queued rows whose `file_path` was
/// already cleared by an earlier retry — without it the sweep would delete
/// the `.part` the row resumes from.
fn resumable_part_paths(rows: &[(String, Option<String>)], zim_dir: &Path) -> HashSet<String> {
    let mut set = HashSet::new();
    for (name, file_path) in rows {
        if let Some(fp) = file_path {
            set.insert(fp.clone());
        }
        set.insert(zim_dir.join(format!("{name}.part")).display().to_string());
    }
    set
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::resumable_part_paths;

    /// PERF-11: the protect-set must include both a row's live `file_path`
    /// and the derived `zim_dir/{name}.part` resume target — the derived
    /// path is what survives `retry_interrupted_directs` clearing
    /// `file_path` (without it the orphan sweep would delete the `.part`
    /// the re-queued row resumes from).
    #[test]
    fn resumable_set_covers_rows_about_to_be_retried() {
        let dir = std::path::PathBuf::from("/zims");
        let rows: Vec<(String, Option<String>)> = vec![
            // Claimed, mid-download (about to be flipped to `queued`, which
            // clears `file_path`): the derived `.part` must still be protected.
            ("a.zim".into(), Some("/zims/a.zim.part".into())),
            // Re-queued by an earlier retry, `file_path` already NULL: only
            // the derived path protects the surviving `.part`.
            ("b.zim".into(), None),
            // Live `file_path` outside the ZIM dir (legacy): protected as-is,
            // plus the derived resume target.
            ("c.zim".into(), Some("/elsewhere/c.zim.part".into())),
        ];
        assert_eq!(
            resumable_part_paths(&rows, &dir),
            std::collections::HashSet::from([
                "/zims/a.zim.part".into(),
                "/zims/b.zim.part".into(),
                "/elsewhere/c.zim.part".into(),
                "/zims/c.zim.part".into(),
            ])
        );
    }

    /// PERF-11: no rows → empty protect-set (the sweep may reclaim every
    /// `.part` in the ZIM dir).
    #[test]
    fn resumable_set_empty_without_rows() {
        let dir = std::path::PathBuf::from("/zims");
        assert!(resumable_part_paths(&[], &dir).is_empty());
    }
}
