//! Startup reconciliation: rows vs live qBittorrent state, orphan `.part`
//! sweep, and adoption of untracked category torrents.

use crate::torrent::TorrentInfo;

use super::*;
use crate::db::entities::downloads::{Column, Entity};
use sea_orm::sea_query::Expr;
use sea_orm::{EntityTrait, QueryFilter};

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
        // Interrupted direct downloads (`.zim` rows after stripping query
        // and fragment, see `is_direct_zim_url`): retry — resumes from a
        // surviving `.part` when one exists (PERF-11).
        let n = crate::db::downloads_lifecycle::retry_interrupted_directs(&self.db).await?;
        if n > 0 {
            tracing::info!("reconcile: retrying {n} interrupted direct download(s)");
        }

        // PERF-11 sweep: reclaim `.part` files whose download row will not
        // resume (a terminal-state row never resumes). `error` rows ARE
        // included: they keep `file_path` (the `.part`) and the bounded 10-min
        // retry re-queues them, so deleting the `.part` would force a full
        // multi-GB re-download (B1). Only truly orphaned `.part`s (no live
        // row, or a terminal `cancelled`/`complete` row) are reclaimed.
        let active_paths: std::collections::HashSet<String> = {
            let db = crate::db::sea_orm_db(&self.db);
            // Propagate a failed query — never default to an empty active-set:
            // an empty set makes the sweep below delete the `.part` files of
            // *live* downloads, so a poisoned query must abort the tick
            // (same propagation-instead-of-unwrap_or_default convention as the
            // direct-download client builder in direct.rs).
            let rows: Vec<String> = Entity::find()
                .select_only()
                .column(Column::FilePath)
                .filter(Column::Status.is_in([
                    crate::torrent::DownloadStatus::Queued.as_str(),
                    crate::torrent::DownloadStatus::Downloading.as_str(),
                    crate::torrent::DownloadStatus::Error.as_str(),
                ]))
                .filter(Expr::cust(format!(
                    "({pred}) LIKE '%.zim'",
                    pred = ZIM_URL_PREDICATE
                )))
                .filter(Column::FilePath.is_not_null())
                .into_tuple()
                .all(&db)
                .await
                .map_err(Error::from)?;
            rows.into_iter().collect()
        };
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
        let rows: Vec<TorrentRow> = {
            let db = crate::db::sea_orm_db(&self.db);
            Entity::find()
                .select_only()
                .column(Column::Id)
                .column(Column::Name)
                .column(Column::Url)
                .column(Column::Hash)
                .column(Column::Status)
                .column(Column::FilePath)
                .column(Column::UpdatedAt)
                .filter(Column::Status.is_in([
                    crate::torrent::DownloadStatus::Downloading.as_str(),
                    crate::torrent::DownloadStatus::Complete.as_str(),
                    crate::torrent::DownloadStatus::Seeding.as_str(),
                ]))
                .into_tuple()
                .all(&db)
                .await
                .map_err(Error::from)?
        };

        let mut known_hashes = HashSet::new();
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
                .or_else(|| by_name.get(name.to_lowercase().as_str()))
                .or_else(|| by_name.get(url.to_lowercase().as_str()))
                .copied();

            match (status.as_str(), t) {
                ("downloading", Some(t)) => {
                    if hash.as_deref() != Some(t.hash.as_str()) {
                        crate::db::downloads_lifecycle::bind_hash(&self.db, id, &t.hash).await?;
                    }
                    if t.is_complete() {
                        // We died between qB finishing and the file install.
                        if let Err(e) = self
                            .handle_complete(id, t, qbit.clone(), &p, file_path.clone())
                            .await
                        {
                            crate::db::downloads_lifecycle::mark_fatal_error(
                                &self.db,
                                id,
                                &e.to_string(),
                            )
                            .await?;
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
                        crate::db::downloads_lifecycle::mark_fatal_error(
                            &self.db,
                            id,
                            "torrent missing from qBittorrent",
                        )
                        .await?;
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
                            .handle_complete(id, t, qbit.clone(), &p, file_path.clone())
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
        //    or added manually in the qB UI) → adopt them.
        let category = p.category.clone();
        for t in torrents
            .iter()
            .filter(|t| t.category.as_deref() == Some(category.as_str()))
        {
            if known_hashes.contains(&t.hash) {
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
}
