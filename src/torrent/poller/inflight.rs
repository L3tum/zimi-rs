//! In-flight tick processing and per-tick stats flush for active download rows.

use crate::torrent::TorrentInfo;

use super::*;
use crate::db::downloads::DownloadRecord;

impl DownloadPoller {
    /// LINT-2 extraction (verbatim `tick()` step-3 block 2): the in-flight
    /// loop body — direct-URL skip, hash-first/name-fallback match, seeding
    /// branch, hash rebind, fatal, `handle_complete`, `stats_changed` push
    /// into `changed`, missing-torrent grace expiry. Returns the collected
    /// `changed` rows (the flush is `flush_stats`).
    pub(super) async fn process_inflight(
        &self,
        by_hash: &HashMap<&str, &TorrentInfo>,
        by_name: &HashMap<String, &TorrentInfo>,
        qbit: Option<Arc<QbitClient>>,
        qb_available: bool,
        p: &crate::settings::PollerParams,
        rows: Vec<DownloadRecord>,
    ) -> Result<Vec<StatsRow>> {
        // In-progress rows whose stats changed enough to write this tick;
        // flushed after the loop as per-row UPDATEs (M-poller-n1).
        let mut changed: Vec<StatsRow> = Vec::new();
        for r in rows {
            let id = r.id;
            let name = r.name;
            let url = r.url;
            let hash = r.hash;
            let row_status = r.status;
            let updated = r.updated_at;
            // Previous stat values for the skip-no-change decision. Only the
            // three the decision reads are bound (the rest are selected for
            // parity). `ratio`/`num_seeds` are nullable columns → `Option`.
            let old_progress = r.progress;
            let old_ratio = r.ratio;
            let old_num_seeds = r.num_seeds;
            // BUG-20: the claimed install path — `handle_complete` skips
            // locate/verify/install when it already exists on disk.
            let file_path = r.file_path;

            // Direct downloads manage their own rows via spawned tasks.
            if crate::torrent::is_direct_zim_url(&url) {
                continue;
            }
            if qbit.is_none() {
                continue;
            }

            // Match by hash first, then by torrent name (qB names a
            // meta-torrent by its URL while fetching metadata). BUG-B4: the
            // name fallback is restricted to rows with `hash IS NULL` — a row
            // that already has a bound hash must match only by hash. Without
            // this guard, two rows sharing a display name could both match
            // the same torrent via the name fallback and double-
            // `handle_complete` (double multi-GB verify/copy + auto-index).
            let t = if let Some(h) = hash.as_deref() {
                by_hash.get(h).copied()
            } else {
                by_name
                    .get(name.to_lowercase().as_str())
                    .or_else(|| by_name.get(url.to_lowercase().as_str()))
                    .copied()
            };

            // Seeding rows: the ZIM is already installed — only refresh the
            // per-torrent seeding stats, or settle the row once the torrent
            // leaves qBittorrent. Never re-run handle_complete here (it would
            // re-verify / re-install / re-index the multi-GB file).
            if row_status == crate::torrent::DownloadStatus::Seeding.as_str() {
                // `grace_expired` folds in qB liveness: an unreachable qB must
                // not settle seeding rows, so the missing-torrent clock only
                // counts when the fetch itself succeeded this tick.
                let grace_expired = t.is_none()
                    && qb_available
                    && chrono::Utc::now() - updated > MISSING_TORRENT_GRACE;
                match seeding_row_action(t, grace_expired) {
                    SeedingAction::FatalDone => {
                        // The ZIM is installed and serving, so a fatal torrent
                        // state (e.g. missingFiles) is not a download failure —
                        // settle to complete with the qB error noted.
                        // FatalDone is only returned for a present, fatal torrent.
                        let t = t.expect("FatalDone implies the torrent is present");
                        let msg = t
                            .err_str
                            .clone()
                            .unwrap_or_else(|| format!("torrent state: {}", t.state));
                        crate::db::downloads_lifecycle::settle_seeding(&self.db, id, Some(&msg))
                            .await?;
                    }
                    SeedingAction::Refresh => {
                        // Refresh is only returned when the torrent is present.
                        let t = t.expect("Refresh implies the torrent is present");
                        // PERF-seed: skip the DB write when nothing
                        // meaningfully changed (same gate as the downloading
                        // branch). Previously seeding rows took an
                        // unconditional per-row UPDATE every tick.
                        if stats_changed(old_progress, old_ratio, old_num_seeds, t) {
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
                    SeedingAction::Done => {
                        // Torrent gone from qB after the grace period (user
                        // removed it, or the ratio cap triggered a removal).
                        crate::db::downloads_lifecycle::settle_seeding(&self.db, id, None).await?;
                    }
                    // Missing within the grace window, or qB unreachable this
                    // tick — re-check next cycle.
                    SeedingAction::Pending => {}
                }
                continue;
            }

            match t {
                Some(t) => {
                    if hash.as_deref() != Some(t.hash.as_str()) {
                        crate::db::downloads_lifecycle::bind_hash(&self.db, id, &t.hash).await?;
                    }
                    if t.is_fatal() {
                        let msg = t
                            .err_str
                            .clone()
                            .unwrap_or_else(|| format!("torrent state: {}", t.state));
                        crate::db::downloads_lifecycle::mark_fatal_error(&self.db, id, &msg)
                            .await?;
                    } else if t.is_complete() {
                        if let Err(e) = self
                            .handle_complete(id, t, qbit.clone(), p, file_path)
                            .await
                        {
                            crate::db::downloads_lifecycle::mark_fatal_error(
                                &self.db,
                                id,
                                &e.to_string(),
                            )
                            .await?;
                        }
                    } else {
                        // M-poller-n1: skip the DB write when nothing
                        // meaningfully changed; otherwise queue the row for
                        // the per-row UPDATE pass after the loop.
                        if stats_changed(old_progress, old_ratio, old_num_seeds, t) {
                            let eta: Option<i64> =
                                eta_secs(t.size.saturating_sub(t.downloaded), t.dlspeed);
                            changed.push((
                                id,
                                t.progress as f32,
                                kibs_to_bps(t.dlspeed),
                                eta,
                                Some(t.ratio as f32),
                                kibs_to_bps(t.upspeed),
                                t.num_seeds,
                            ));
                        }
                    }
                }
                // Torrent not visible yet. We only error after the grace
                // period — `updated_at` is refreshed every time the torrent
                // *is* visible, so a vanished torrent ages out naturally.
                // (Skipped entirely when qB itself was unreachable this tick.)
                None if qb_available && chrono::Utc::now() - updated > MISSING_TORRENT_GRACE => {
                    crate::db::downloads_lifecycle::mark_fatal_error(
                        &self.db,
                        id,
                        "torrent not found in qBittorrent",
                    )
                    .await?;
                }
                // qB was unreachable this tick — don't judge anything as
                // missing; re-check next cycle.
                None => {}
            }
        }

        Ok(changed)
    }

    /// LINT-2 extraction (verbatim `tick()` step-3 block 3): the flush tail.
    pub(super) async fn flush_stats(&self, changed: &[StatsRow]) {
        // Per-row UPDATE for every in-progress row that changed this tick
        // (M-poller-n1; PONY-S4 replaced the single VALUES statement).
        // Best-effort: warn only — an un-written row re-writes next tick
        // while its stats keep changing.
        if let Err(e) = apply_stats_batch(&self.db, changed).await {
            tracing::warn!("download stats write failed: {e}");
        }
    }
}
