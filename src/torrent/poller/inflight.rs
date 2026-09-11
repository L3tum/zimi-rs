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
    // LINT-3 (2026-09 sweep): invariant panic (SeedingAction variant implies torrent present) — grandfathered expect_used.
    #[allow(clippy::expect_used)]
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
        // Inflight-2: qB entries already consumed by an earlier row this
        // tick (identity = the torrent's hash, or its lowercased name while
        // metadata is still pending and the hash is empty). `bind_hash` does
        // not update the local snapshot maps, so this set is what stops two
        // same-name NULL-hash rows from both matching the same entry.
        let mut consumed: HashSet<String> = HashSet::new();
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
            // Inflight-2: the name fallback additionally skips qB entries an
            // earlier row already consumed this tick (two same-name NULL-hash
            // rows would otherwise both match the same entry and bind the
            // same hash).
            let t =
                match_inflight_torrent(hash.as_deref(), &name, &url, by_hash, by_name, &consumed);
            // The row owns its matched entry for the rest of this tick:
            // record the identity so a later same-name row skips it.
            if let Some(t) = t {
                consumed.insert(torrent_identity(t));
            }

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
                    // Inflight-1: the missing-torrent grace clock is measured
                    // from `updated_at`, which otherwise only advances on
                    // stats writes — a stalled torrent with unchanged stats
                    // would carry an exhausted grace clock the moment it
                    // vanished. While the torrent IS visible, refresh the
                    // clock when the row is stale (cheap: one guarded UPDATE
                    // at most once per `VISIBILITY_TOUCH_AFTER` window).
                    if visibility_touch_due(updated, chrono::Utc::now()) {
                        let touched =
                            crate::db::downloads_lifecycle::touch_downloading(&self.db, id).await?;
                        if touched == 0 {
                            tracing::debug!(
                                download_id = id,
                                "visibility touch: row no longer downloading"
                            );
                        }
                    }
                    if hash.as_deref() != Some(t.hash.as_str()) {
                        crate::db::downloads_lifecycle::bind_hash(&self.db, id, &t.hash).await?;
                    }
                    if t.is_fatal() {
                        let msg = t
                            .err_str
                            .clone()
                            .unwrap_or_else(|| format!("torrent state: {}", t.state));
                        let marked =
                            crate::db::downloads_lifecycle::mark_fatal_error(&self.db, id, &msg)
                                .await?;
                        if marked == 0 {
                            tracing::info!(
                                download_id = id,
                                "skipping fatal error mark: row no longer queued/downloading"
                            );
                        }
                    } else if t.is_complete() {
                        if let Err(e) = self
                            .handle_complete(id, &name, t, qbit.clone(), p, file_path)
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
                // *is* visible (the visibility touch: `touch_downloading`, at
                // most one per `VISIBILITY_TOUCH_AFTER` window, in addition
                // to the stats writes), so the grace window measures how long
                // the torrent has been invisible, and a vanished torrent ages
                // out naturally.
                // (Skipped entirely when qB itself was unreachable this tick.)
                None if qb_available && chrono::Utc::now() - updated > MISSING_TORRENT_GRACE => {
                    let marked = crate::db::downloads_lifecycle::mark_fatal_error(
                        &self.db,
                        id,
                        "torrent not found in qBittorrent",
                    )
                    .await?;
                    if marked == 0 {
                        tracing::info!(
                            download_id = id,
                            "skipping fatal error mark: row no longer queued/downloading"
                        );
                    }
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

// ── Pure decision helpers (unit-testable seams) ─────────────────────────────

/// Inflight-1: how stale a row's `updated_at` must be before the
/// visibility touch refreshes it. A no-op-cheap gate: while the row keeps
/// getting stats writes (or is fresh from a bind), the touch never fires.
pub(super) const VISIBILITY_TOUCH_AFTER: chrono::Duration = chrono::Duration::seconds(60);

/// Inflight-1 (pure): should a row whose torrent is visible this tick get
/// its `updated_at` refreshed? `true` once the row's last write is strictly
/// older than [`VISIBILITY_TOUCH_AFTER`]. The touch is what makes the
/// missing-torrent grace clock measure *invisibility* instead of the gap
/// since the last stats write. (Clock skew — `updated_at` in the future —
/// yields a negative age and never fires.)
pub(super) fn visibility_touch_due(
    updated_at: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    now.signed_duration_since(updated_at) > VISIBILITY_TOUCH_AFTER
}

/// Inflight-2 (pure): the per-tick identity of a qBittorrent entry for the
/// consumed set — the torrent's hash when known, else a lowercased-name key
/// (metadata-pending entries have no hash yet).
pub(super) fn torrent_identity(t: &TorrentInfo) -> String {
    if t.hash.is_empty() {
        format!("name:{}", t.name.to_lowercase())
    } else {
        t.hash.clone()
    }
}

/// Inflight-2 (pure): the hash-first / name-fallback match decision for one
/// in-flight row. A row with a bound hash matches **only** by hash (BUG-B4);
/// a `hash IS NULL` row falls back to the torrent name (or URL), skipping
/// any qB entry in `consumed` — the entries an earlier row already matched
/// this tick (the snapshot maps are not updated by `bind_hash`).
pub(super) fn match_inflight_torrent<'a>(
    hash: Option<&str>,
    name: &str,
    url: &str,
    by_hash: &'a HashMap<&str, &TorrentInfo>,
    by_name: &'a HashMap<String, &TorrentInfo>,
    consumed: &HashSet<String>,
) -> Option<&'a TorrentInfo> {
    if let Some(h) = hash {
        by_hash.get(h).copied()
    } else {
        by_name
            .get(name.to_lowercase().as_str())
            .or_else(|| by_name.get(url.to_lowercase().as_str()))
            .filter(|t| !consumed.contains(&torrent_identity(t)))
            .copied()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::super::tests::state::tinfo;

    use super::*;

    /// A downloadable-state torrent with explicit hash/name.
    fn ti(hash: &str, name: &str) -> TorrentInfo {
        let mut t = tinfo("downloading", 0.5);
        t.hash = hash.into();
        t.name = name.into();
        t
    }

    fn maps(t: &TorrentInfo) -> (HashMap<&str, &TorrentInfo>, HashMap<String, &TorrentInfo>) {
        let hash_key = t.hash.as_str();
        (
            HashMap::from([(hash_key, t)]),
            HashMap::from([(t.name.to_lowercase(), t)]),
        )
    }

    #[test]
    fn visibility_touch_due_only_when_stale() {
        let now = chrono::Utc::now();
        assert!(!visibility_touch_due(
            now - chrono::Duration::seconds(59),
            now
        ));
        // The boundary is strict: exactly 60 s old is not yet due.
        assert!(!visibility_touch_due(
            now - chrono::Duration::seconds(60),
            now
        ));
        assert!(visibility_touch_due(
            now - chrono::Duration::seconds(61),
            now
        ));
        // Clock skew (updated_at in the future) must not panic or fire.
        assert!(!visibility_touch_due(
            now + chrono::Duration::seconds(5),
            now
        ));
    }

    #[test]
    fn match_hash_bound_rows_by_hash_only() {
        let t = ti("h1", "Dup Name");
        let (by_hash, by_name) = maps(&t);
        let consumed = HashSet::new();
        // A bound hash matches by hash (even when the name would match too).
        assert!(matches!(
            match_inflight_torrent(Some("h1"), "Dup Name", "u", &by_hash, &by_name, &consumed),
            Some(x) if x.hash == "h1"
        ));
        // A stale bound hash must NOT fall back to the name (BUG-B4).
        assert!(match_inflight_torrent(
            Some("stale"),
            "Dup Name",
            "u",
            &by_hash,
            &by_name,
            &consumed
        )
        .is_none());
    }

    #[test]
    fn match_null_hash_same_name_skips_consumed() {
        let t = ti("h1", "Dup Name");
        let (by_hash, by_name) = maps(&t);
        let mut consumed = HashSet::new();
        // The first same-name NULL-hash row matches the entry…
        let first = match_inflight_torrent(None, "Dup Name", "u", &by_hash, &by_name, &consumed)
            .expect("first same-name row must match");
        consumed.insert(torrent_identity(first));
        // …and the second same-name row must skip it within the same tick.
        assert!(
            match_inflight_torrent(None, "Dup Name", "u", &by_hash, &by_name, &consumed).is_none(),
            "second same-name NULL-hash row must not re-match the consumed entry"
        );
        // A row bound to the consumed entry's hash still matches by hash
        // (hash match is authoritative; only the name fallback is guarded).
        assert!(
            match_inflight_torrent(Some("h1"), "Other", "u", &by_hash, &by_name, &consumed)
                .is_some()
        );
    }

    #[test]
    fn match_consumed_entry_found_via_url_is_skipped_too() {
        let t = ti("h1", "Dup Name");
        let (by_hash, by_name) = maps(&t);
        let mut consumed = HashSet::new();
        // Name miss, URL hit: the URL fallback matches the same entry…
        let via_url =
            match_inflight_torrent(None, "other", "Dup Name", &by_hash, &by_name, &consumed)
                .expect("url fallback must match");
        assert_eq!(via_url.hash, "h1");
        consumed.insert(torrent_identity(via_url));
        // …so a later row hitting the entry by name now skips it.
        assert!(
            match_inflight_torrent(None, "Dup Name", "u", &by_hash, &by_name, &consumed).is_none()
        );
    }

    #[test]
    fn identity_uses_name_when_hash_pending() {
        // Metadata-pending: empty hash → name key.
        let pending = ti("", "Pending Name");
        assert_eq!(torrent_identity(&pending), "name:pending name");
        // Known hash → hash key.
        let known = ti("h1", "Pending Name");
        assert_eq!(torrent_identity(&known), "h1");
    }
}
