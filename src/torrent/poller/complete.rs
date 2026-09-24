//! Completion path for finished downloads: verify → install → resync →
//! index, the direct-download spawn, and the shared row helpers (error
//! marking, cancel-check) that feed it.

use crate::torrent::TorrentInfo;

use super::direct::discard_if_unowned;
use super::{
    client_from_option, direct_download, index, install_zim, kibs_to_bps, locate_torrent_zim,
    mark_error, verify_zim, Arc, DownloadPoller, Error, Path, QbitClient, Result,
};

impl DownloadPoller {
    // ── Completion: verify → install → resync → index ───────────────────────

    // LINT-3 (2026-09 sweep): invariant panic (skip path implies file_path present) —
    // grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub(super) async fn handle_complete(
        &self,
        id: i32,
        name: &str,
        t: &TorrentInfo,
        qbit: Option<Arc<QbitClient>>,
        p: &crate::settings::PollerParams,
        file_path: Option<String>,
    ) -> Result<()> {
        // BUG-20: the claimed install may already be on disk. A requeued
        // completion re-enters here with the file present (resync/final-
        // UPDATE errors that match REQUEUE_ERROR_PATTERN are requeued;
        // anything else already marks `error` and stops), so reuse it — the
        // retry is O(1) instead of a multi-GB re-verify + copy.
        let skipped_existing = file_path
            .as_deref()
            .map(|p| Path::new(p).exists())
            .unwrap_or(false);
        // BUGS-2: resolve the expected install destination up front (cheap —
        // locate only, no verify). The install destination is the torrent's
        // CONTENT file name — `zim_dir/{content_file_name}` — or the claimed
        // `file_path` itself on the skip path. A locate failure yields
        // `None` and defers to the install error path below (the guard runs
        // only on success).
        let expected_dst: Option<std::path::PathBuf> = if skipped_existing {
            file_path.clone().map(std::path::PathBuf::from)
        } else {
            let content = t
                .content_path
                .as_deref()
                .or(t.save_path.as_deref())
                .unwrap_or("");
            locate_torrent_zim(Path::new(content))
                .ok()
                .and_then(|zim_file| zim_file.file_name().map(|n| self.zims.zim_dir.join(n)))
        };

        // Install-collision guard: dedup at enqueue only covers live rows, so
        // an older terminal row with the same name can already own
        // `zim_dir/{name}.zim` — an install on this row would overwrite it
        // (and the `updated == 0` cancel-race discard below could delete it).
        // Refuse to install over a live same-named row: mark this row
        // `error` (status-guarded, so a cancel keeps winning) and return
        // WITHOUT touching the destination file. A query failure propagates
        // (fail closed — a swallowed blip could mask the collision and
        // overwrite the other row's file).
        if let Some(other) =
            crate::db::downloads::find_same_name_live_row(&self.db, name, id).await?
        {
            let msg = format!(
                "install skipped: same-named download already active/installed (row {other})"
            );
            tracing::info!("download {id}: {msg}");
            mark_error(&self.db, id, &msg).await;
            return Ok(());
        }
        // BUGS-2: the guard above keys on the row DISPLAY name, but the
        // install destination is the CONTENT file name (`expected_dst`) — a
        // differently-named row can still own that path. Guard on the
        // resolved destination itself and refuse the install the same way —
        // mark this row `error`, return WITHOUT touching the destination
        // file. A query failure propagates (fail closed — same rationale as
        // the same-name guard above).
        if let Some(expected_dst) = &expected_dst {
            if let Some(other) = crate::db::downloads::find_live_row_owns_path(
                &self.db,
                &expected_dst.display().to_string(),
                id,
            )
            .await?
            {
                let msg = format!("install skipped: destination already owned by live row {other}");
                tracing::info!("download {id}: {msg}");
                mark_error(&self.db, id, &msg).await;
                return Ok(());
            }
        }
        // Seeding: cap the ratio; the torrent stays in qBittorrent
        // (keep_completed means we simply never delete it).
        if let Some(q) = qbit.as_ref() {
            if let Some(ratio) = p.seed_ratio {
                if ratio > 0.0 {
                    let _ = q.set_ratio_limit(&t.hash, ratio).await;
                }
            }
        }

        let dst: std::path::PathBuf = if skipped_existing {
            tracing::info!(
                download_id = id,
                "install already present on disk, skipping re-verify (BUG-20)"
            );
            std::path::PathBuf::from(file_path.expect("file_path present when skipped"))
        } else {
            // Locate the single .zim this torrent produced (file or dir;
            // multi-file torrents with several ZIMs are rejected with a clear
            // error instead of silently installing an arbitrary one).
            let content = t
                .content_path
                .as_deref()
                .or(t.save_path.as_deref())
                .unwrap_or("");
            let zim_file = locate_torrent_zim(Path::new(content))?;

            // Verify (multi-GB central-dir parse) + install (cross-device
            // copy) are blocking — run them off the single poller task so
            // all download polling stays responsive.
            let strategy = p.file_strategy.clone();
            let zim_dir = self.zims.zim_dir.clone();
            let install_res = tokio::task::spawn_blocking(move || {
                verify_zim(&zim_file)?;
                install_zim(&zim_file, &zim_dir, &strategy)
            })
            .await
            .map_err(|e| Error::Internal(anyhow::anyhow!("install task failed: {e}")))?;
            install_res?
        };
        // Register the new/updated archives in cache + DB (status → pending).
        self.zims.resync().await?;
        // A skipped call (file already present) must never delete a file it
        // did not write — only the install path owns the `updated == 0`
        // discard.
        let installed_here = !skipped_existing;

        let updated = {
            // The completion settlement and its transition guard
            // (`status IN ('downloading', 'complete')` —
            // `completion_guard_statuses()` in the lifecycle module) are
            // owned by `mark_complete`. A `downloading` row is the normal
            // fresh-download path (a cancel landing while we installed takes
            // the `updated == 0` branch below); a `complete` row is the
            // startup-recovery path (reinstalling a deleted file); `seeding`
            // rows are settled by their own arms and `cancelled` is excluded
            // so a cancel always wins.
            let keep_seeding = p.keep_completed && qbit.is_some();
            let new_status = if keep_seeding {
                crate::torrent::DownloadStatus::Seeding
            } else {
                crate::torrent::DownloadStatus::Complete
            };
            let ratio_val: Option<f32> = keep_seeding.then_some(t.ratio as f32);
            let up_speed: Option<i64> = keep_seeding.then_some(kibs_to_bps(t.upspeed));
            let num_seeds: Option<i64> = keep_seeding.then_some(t.num_seeds);
            crate::db::downloads_lifecycle::mark_complete(
                &self.db,
                id,
                &dst.display().to_string(),
                new_status,
                ratio_val,
                up_speed,
                num_seeds,
            )
            .await?
        };
        if updated == 0 && installed_here {
            // Only reachable now for a genuine cancel race: a `cancelled`
            // (or otherwise terminal) row landed between the install and
            // this UPDATE (a skipped call is gated out by `installed_here`).
            tracing::info!(
                "download {id} was cancelled during install — discarding {}",
                dst.display()
            );
            discard_if_unowned(&self.db, &self.zims, id, &dst).await;
            return Ok(());
        }

        tracing::info!(
            "download {id} complete: 1 installed into {}",
            self.zims.zim_dir.display()
        );

        // Content provenance (SEC): record the OBSERVED SHA-256 of the
        // installed file. The torrent's content identity is its BitTorrent
        // info-hash — qBittorrent verified every piece against it before the
        // torrent could reach a post-download state (the acceptance
        // precondition; see `TorrentInfo::is_complete`), so this is a
        // provenance record + drift baseline, not a re-verification (and
        // never a rejection). Hashing is blocking (multi-GB) — off the
        // single poller task. The skip path (BUG-20 reuse) hashes only for
        // legacy rows with no recorded digest yet; the fresh path always
        // records (a pre-settlement row digest, when present, is the catalog
        // claim — noted on mismatch, then replaced by the observed value).
        let row_digest = crate::db::downloads_lifecycle::claim_digest(&self.db, id).await?;
        let needs_digest = if skipped_existing {
            row_digest.is_none()
        } else {
            true
        };
        if needs_digest {
            let digest_path = dst.clone();
            let hash = t.hash.clone();
            let digest_res =
                tokio::task::spawn_blocking(move || super::direct::sha256_file(&digest_path))
                    .await
                    .map_err(|e| Error::Internal(anyhow::anyhow!("digest task failed: {e}")))?;
            match digest_res {
                Ok(digest) => {
                    // A catalog-driven torrent row (rare — the live catalog
                    // declares no digests) may carry a claim: note, don't
                    // reject (the info-hash is the content gate here).
                    if let Some(c) = &row_digest {
                        if !c.eq_ignore_ascii_case(&digest) {
                            tracing::warn!(
                                "download {id}: catalog claim {c} differs from observed \
                                 digest {digest} (torrent path — info-hash-pinned, noted \
                                 not rejected)"
                            );
                        }
                    }
                    // Provenance on the settled row (status-guarded; a
                    // racing settle/cancel wins — a no-op then, harmless).
                    let _ = crate::db::downloads_lifecycle::record_observed_digest(
                        &self.db, id, &digest,
                    )
                    .await;
                    // Drift (SEC): same info-hash, different bytes than the
                    // previously observed digest for this torrent. Flag,
                    // never reject — a re-torrent of the same name is a
                    // human judgment call. Best-effort: a blip degrades to
                    // "no drift flagged", the same state as missing history.
                    let drift = match crate::db::downloads_lifecycle::prior_observed_digest_by_hash(
                        &self.db, id, &hash,
                    )
                    .await
                    {
                        Ok(Some(prev))
                            if crate::db::downloads_lifecycle::drift_decision(
                                Some(&prev),
                                &digest,
                            ) =>
                        {
                            tracing::warn!(
                                "torrent {hash} digest DRIFT: previously observed {prev}, \
                                 now {digest} — flagged (not rejected)"
                            );
                            true
                        }
                        Ok(_) => false,
                        Err(e) => {
                            tracing::warn!("drift check for download {id} failed: {e}");
                            false
                        }
                    };
                    let zname = dst
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    if !zname.is_empty() {
                        if let Err(e) = self
                            .zims
                            .set_content_sha256(&zname, &digest, None, drift)
                            .await
                        {
                            tracing::error!("recording content digest for {zname} failed: {e}");
                        }
                    }
                }
                // Best-effort provenance: a hashing failure degrades to
                // "digest unknown" (the next settlement retries), it never
                // fails an already-settled install.
                Err(e) => {
                    tracing::warn!("observed-digest hashing for download {id} failed: {e}");
                }
            }
        }

        // If we don't keep completed torrents, drop them from qBittorrent now
        // that the file is safely in the ZIM dir.
        if !p.keep_completed {
            if let Some(q) = qbit.as_ref() {
                match q.delete(&t.hash, true).await {
                    Ok(()) => {
                        tracing::info!("removed completed torrent {} from qBittorrent", t.hash)
                    }
                    Err(e) => tracing::warn!("failed to remove completed torrent {}: {e}", t.hash),
                }
            }
        }

        // Index the new ZIM in the background.
        let zims = self.zims.clone();
        let db = self.db.clone();
        let name = dst
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .filter(|n| !n.is_empty());
        if let Some(name) = name {
            tokio::spawn(async move {
                if let Err(e) = index::index_zims(&zims, &db, Some(&name)).await {
                    tracing::error!("auto-index of {name} failed: {e}");
                }
            });
        }

        Ok(())
    }

    // ── Direct (non-torrent) .zim downloads ──────────────────────────────────

    pub(super) fn spawn_direct(&self, id: i32, url: &str, part: &Path) {
        let db = self.db.clone();
        let http = match client_from_option(self.http.as_ref()) {
            Ok(c) => c.clone(),
            Err(e) => {
                // The row was already claimed `downloading` — mark the error
                // instead of leaving it stuck and eating the direct_active
                // budget until a restart.
                let msg = e.to_string();
                tokio::spawn(async move {
                    mark_error(&db, id, &msg).await;
                    tracing::warn!("direct download {id} failed: {msg}");
                });
                return;
            }
        };
        let zims = self.zims.clone();
        let settings = self.settings.clone();
        let url = url.to_string();
        let part = part.to_path_buf();
        tracing::info!("direct download {id}: {}", crate::redact_url(&url));
        tokio::spawn(async move {
            if let Err(e) = direct_download(&http, &db, &zims, &settings, id, &url, &part).await {
                // Mark every failure, not just the early returns: otherwise a
                // failed row stays `downloading` (with file_path set) and
                // silently eats the direct_active budget until a process
                // restart. mark_error's terminal-state guard means a
                // concurrent cancel still wins.
                mark_error(&db, id, &e.to_string()).await;
                tracing::warn!("direct download {id} failed: {e}");
            }
        });
    }
}

// The shared row helpers `status_no_longer_downloading` and `mark_error` now
// live in `crate::db::downloads_lifecycle` (ARCH M-2/M-3) and are re-exported
// at the top of `mod.rs` (reachable here via `use super::*`).

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::tests::download::{download_settings, test_pool};

    use super::*;

    /// Wait (bounded) for the fire-and-forget auto-index spawned at the end
    /// of `handle_complete` to settle: the `zims` row's `index_status`
    /// reaching a terminal state (`ready`/`error`). Without this the
    /// spawned task outlives the test — the tempdir drops and the row
    /// cleanup below races the index's article inserts, leaking work into
    /// the next test (the suite DB is shared; only the DbExclusiveGuard
    /// serializes the tests themselves, not their spawns).
    async fn wait_index_settled(pool: &crate::db::Pool, name: &str) -> String {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let status: Option<String> = crate::db::raw::fetch_scalar_optional(
                pool,
                "SELECT index_status FROM zims WHERE name = $1",
                |q| q.bind(name),
            )
            .await
            .expect("read index_status");
            if matches!(status.as_deref(), Some("ready" | "error")) {
                return status.unwrap();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "auto-index of '{name}' did not settle within 30 s (status: {status:?})"
            );
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    /// TEST-5 (CI-DB): completion with no qBittorrent — the staged content
    /// file is installed per `torrent.file_strategy` and the row settles to
    /// `complete` with `file_path` and `ratio` NULL (`keep_seeding` is false
    /// when `qbit = None`).
    #[tokio::test]
    async fn handle_complete_no_qbit_installs_and_marks_complete() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let tmp = tempfile::tempdir().expect("tempdir");
        let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
        let settings = download_settings();
        let poller = super::super::DownloadPoller::new(
            pool.clone(),
            settings.clone(),
            zims.clone(),
            crate::torrent::QbitClientCache::new(),
            None,
            String::new(),
            String::new(),
        );
        let p = poller.settings.poller_params_snapshot();

        // Pre-stage the torrent content: a valid utiny.zim at content_path.
        let content_dir = tmp.path().join("qb-content");
        std::fs::create_dir_all(&content_dir).unwrap();
        let content_file = content_dir.join("utiny.zim");
        std::fs::copy("tests/fixtures/tiny.zim", &content_file).unwrap();

        // Pre-clean (re-runnability): a failed run leaves a live row on the
        // shared suite DB that would hit the partial unique index.
        crate::db::raw::execute(
            &pool,
            "DELETE FROM downloads WHERE url = 'magnet:?xt=urn:btih:it'",
            |q| q,
        )
        .await
        .unwrap();
        let id: i32 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "INSERT INTO downloads (name, url, status)
                 VALUES ('utiny', 'magnet:?xt=urn:btih:it', 'downloading') RETURNING id",
            |q| q,
        )
        .await
        .expect("insert downloading row")
        .unwrap();

        let t = TorrentInfo {
            hash: "it".into(),
            name: "utiny".into(),
            progress: 1.0,
            state: "uploading".into(),
            dlspeed: 0,
            upspeed: 0,
            ratio: 0.0,
            category: None,
            save_path: Some(content_dir.to_string_lossy().into_owned()),
            content_path: Some(content_file.to_string_lossy().into_owned()),
            size: 0,
            downloaded: 0,
            num_seeds: 0,
            err_str: None,
        };

        poller
            .handle_complete(id, "utiny", &t, None, &p, None)
            .await
            .expect("handle_complete");

        let status: String = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT status FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .unwrap();
        let file_path: Option<String> = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT file_path FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .flatten();
        let ratio: Option<f32> = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT ratio FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .flatten();
        assert_eq!(status, "complete");
        assert_eq!(
            ratio, None,
            "qbit = None ⇒ keep_seeding false ⇒ ratio cleared"
        );
        let fp = file_path.expect("file_path must be set");
        let installed = std::path::Path::new(&fp);
        assert!(installed.exists(), "installed file must exist");
        assert!(
            installed.starts_with(&zims.zim_dir),
            "file installed into zim_dir, got {fp}"
        );

        // SEC provenance (torrent path): the settled row and the ZIM row
        // carry the MEASURED digest of the installed bytes. Torrents are
        // pinned by the info-hash (the magnet), so no publisher column is
        // set and there is nothing to compare against — no drift flag.
        let measured =
            crate::torrent::poller::direct::sha256_file(installed).expect("hash installed");
        let sha: Option<String> = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT sha256 FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .unwrap();
        assert_eq!(
            sha.as_deref(),
            Some(measured.as_str()),
            "settled torrent row must carry the measured digest"
        );
        let (content, publisher, drift): (Option<String>, Option<String>, bool) =
            crate::db::raw::fetch_optional(
                &pool,
                "SELECT content_sha256, publisher_sha256, digest_drift FROM zims WHERE name = $1",
                |q| q.bind("utiny"),
            )
            .await
            .expect("read zims row")
            .expect("zims row must exist");
        assert_eq!(
            content.as_deref(),
            Some(measured.as_str()),
            "zims row carries the observed digest"
        );
        assert!(
            publisher.is_none(),
            "torrent installs record no publisher column (info-hash pinned)"
        );
        assert!(!drift, "first observation is never drift");

        // Let the fire-and-forget auto-index settle BEFORE the cleanup: it
        // indexes the file just installed, and the row deletes below would
        // race its article inserts (and the tempdir drop would kill its
        // open mid-run).
        let status = wait_index_settled(&pool, "utiny").await;
        assert_eq!(
            status, "ready",
            "the auto-index of the installed fixture must succeed"
        );

        // Cleanup (shared single-DB suite; articles cascade off zims).
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE id = $1", |q| q.bind(id))
            .await
            .unwrap();
        crate::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| {
            q.bind("utiny")
        })
        .await
        .unwrap();
    }

    /// CI-DB (BUG-20): `handle_complete` is idempotent when the claimed
    /// install already exists on disk — with a small fixture file
    /// pre-placed at `file_path`, the completion path (driven twice, as a
    /// requeued completion would re-enter it) must skip
    /// locate/verify/install entirely: no rename (inode/mtime/size of the
    /// on-disk file unchanged after the second pass) and the row stays
    /// `complete` with the same `file_path`.
    #[tokio::test]
    async fn handle_complete_second_call_is_noop() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let tmp = tempfile::tempdir().expect("tempdir");
        let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
        let settings = download_settings();
        let poller = super::super::DownloadPoller::new(
            pool.clone(),
            settings.clone(),
            zims.clone(),
            crate::torrent::QbitClientCache::new(),
            None,
            String::new(),
            String::new(),
        );
        let p = poller.settings.poller_params_snapshot();

        // Pre-place the "installed" file in the zims dir: any
        // locate/verify/install would replace it (fresh inode), so the
        // no-rename assertion observes a regression to the re-verify path.
        let installed = zims.zim_dir.join("utiny.zim");
        std::fs::copy("tests/fixtures/tiny.zim", &installed).expect("pre-place utiny.zim");

        // Pre-clean (re-runnability): a failed run leaves a live row on the
        // shared suite DB that would hit the partial unique index.
        crate::db::raw::execute(
            &pool,
            "DELETE FROM downloads WHERE url = 'magnet:?xt=urn:btih:it'",
            |q| q,
        )
        .await
        .unwrap();

        let id: i32 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "INSERT INTO downloads (name, url, status, file_path) \
                 VALUES ('utiny', 'magnet:?xt=urn:btih:it', 'downloading', $1) RETURNING id",
            |q| q.bind(installed.display().to_string()),
        )
        .await
        .expect("insert downloading row with claimed file_path")
        .unwrap();

        // The torrent content (a valid utiny.zim) — only reachable if the
        // skip regresses, which the inode assertion then fails on.
        let content_dir = tmp.path().join("qb-content");
        std::fs::create_dir_all(&content_dir).unwrap();
        let content_file = content_dir.join("utiny.zim");
        std::fs::copy("tests/fixtures/tiny.zim", &content_file).unwrap();

        let t = TorrentInfo {
            hash: "it".into(),
            name: "utiny".into(),
            progress: 1.0,
            state: "uploading".into(),
            dlspeed: 0,
            upspeed: 0,
            ratio: 0.0,
            category: None,
            save_path: Some(content_dir.to_string_lossy().into_owned()),
            content_path: Some(content_file.to_string_lossy().into_owned()),
            size: 0,
            downloaded: 0,
            num_seeds: 0,
            err_str: None,
        };

        fn fingerprint(path: &std::path::Path) -> (u64, std::time::SystemTime, u64) {
            use std::os::unix::fs::MetadataExt;
            let m = std::fs::metadata(path).expect("stat installed file");
            (m.ino(), m.modified().expect("mtime"), m.len())
        }
        let claimed = installed.display().to_string();
        let before = fingerprint(&installed);

        poller
            .handle_complete(id, "utiny", &t, None, &p, Some(claimed.clone()))
            .await
            .expect("first handle_complete");
        let after_first = fingerprint(&installed);
        assert_eq!(
            before, after_first,
            "file present on disk ⇒ first call must not re-verify/install either"
        );

        // The second pass — a requeued completion re-entering with the
        // file present: still no rename, row stable.
        poller
            .handle_complete(id, "utiny", &t, None, &p, Some(claimed))
            .await
            .expect("second handle_complete");
        let after_second = fingerprint(&installed);
        assert_eq!(
            after_first, after_second,
            "second call must be a no-op: no rename (inode/mtime/size unchanged)"
        );

        let (status, file_path) = crate::db::raw::fetch_optional::<(String, Option<String>), _, _>(
            &pool,
            "SELECT status, file_path FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .expect("row must exist");
        assert_eq!(status, "complete");
        assert_eq!(file_path, Some(installed.display().to_string()));

        // Both handle_complete passes spawned a fire-and-forget auto-index
        // of the pre-placed fixture; let the last one settle before the
        // cleanup (the row deletes would race its article inserts, and the
        // tempdir drop would kill its open mid-run).
        let status = wait_index_settled(&pool, "utiny").await;
        assert_eq!(
            status, "ready",
            "the auto-index of the installed fixture must succeed"
        );

        // Cleanup (shared single-DB suite; articles cascade off zims).
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE id = $1", |q| q.bind(id))
            .await
            .unwrap();
        crate::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| {
            q.bind("utiny")
        })
        .await
        .unwrap();
    }

    /// BUGS-2 (CI-DB): the destination-ownership guard — row A is `complete`
    /// and owns `<zim_dir>/utiny.zim`; row B has a DIFFERENT display name,
    /// but its torrent's CONTENT file is also named `utiny.zim`, so the
    /// name-based same-name guard cannot see the collision. `handle_complete`
    /// on B must refuse the install (B → `error`, message names the owning
    /// row) and must leave A's file (inode + bytes) and A's row untouched.
    #[tokio::test]
    async fn handle_complete_refuses_install_when_destination_owned_by_other_live_row() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let tmp = tempfile::tempdir().expect("tempdir");
        let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
        let settings = download_settings();
        let poller = super::super::DownloadPoller::new(
            pool.clone(),
            settings.clone(),
            zims.clone(),
            crate::torrent::QbitClientCache::new(),
            None,
            String::new(),
            String::new(),
        );
        let p = poller.settings.poller_params_snapshot();

        // Row A: `complete`, owns <zim_dir>/utiny.zim (the fixture is there).
        let a_file = zims.zim_dir.join("utiny.zim");
        std::fs::copy("tests/fixtures/tiny.zim", &a_file).expect("place A's file");
        // Pre-clean (re-runnability): a failed run leaves a live row on the
        // shared suite DB that would hit the partial unique index.
        crate::db::raw::execute(
            &pool,
            "DELETE FROM downloads WHERE name IN ('bugs2-a', 'bugs2-b')",
            |q| q,
        )
        .await
        .unwrap();

        let a_id: i32 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "INSERT INTO downloads (name, url, status, file_path)
                 VALUES ('bugs2-a', 'magnet:?xt=urn:btih:bugs2a', 'complete', $1) RETURNING id",
            |q| q.bind(a_file.display().to_string()),
        )
        .await
        .expect("insert complete row A")
        .unwrap();

        // Row B: different display name, `downloading`; its torrent content
        // file is named utiny.zim — the SAME installed file A owns, a
        // collision only the destination-keyed guard can see.
        let content_dir = tmp.path().join("qb-content");
        std::fs::create_dir_all(&content_dir).unwrap();
        let content_file = content_dir.join("utiny.zim");
        std::fs::copy("tests/fixtures/tiny.zim", &content_file).unwrap();
        let b_id: i32 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "INSERT INTO downloads (name, url, status)
                 VALUES ('bugs2-b', 'magnet:?xt=urn:btih:bugs2b', 'downloading') RETURNING id",
            |q| q,
        )
        .await
        .expect("insert downloading row B")
        .unwrap();

        let t = TorrentInfo {
            hash: "bugs2b".into(),
            name: "bugs2-b".into(),
            progress: 1.0,
            state: "uploading".into(),
            dlspeed: 0,
            upspeed: 0,
            ratio: 0.0,
            category: None,
            save_path: Some(content_dir.to_string_lossy().into_owned()),
            content_path: Some(content_file.to_string_lossy().into_owned()),
            size: 0,
            downloaded: 0,
            num_seeds: 0,
            err_str: None,
        };

        fn fingerprint(path: &std::path::Path) -> (u64, std::time::SystemTime, u64) {
            use std::os::unix::fs::MetadataExt;
            let m = std::fs::metadata(path).expect("stat A's file");
            (m.ino(), m.modified().expect("mtime"), m.len())
        }
        let before = fingerprint(&a_file);
        let before_bytes = std::fs::read(&a_file).expect("read A's file");

        poller
            .handle_complete(b_id, "bugs2-b", &t, None, &p, None)
            .await
            .expect("handle_complete");

        // B refused the install: `error` with the ownership message.
        let (status, error): (String, Option<String>) = crate::db::raw::fetch_optional(
            &pool,
            "SELECT status, error FROM downloads WHERE id = $1",
            |q| q.bind(b_id),
        )
        .await
        .expect("read B")
        .expect("row B present");
        assert_eq!(status, "error", "B must be refused, not installed");
        let msg = error.expect("B must carry the refusal message");
        assert!(
            msg.contains("owned by live row"),
            "refusal must name the ownership guard: {msg}"
        );
        assert!(
            msg.contains(&a_id.to_string()),
            "refusal must name the owning row: {msg}"
        );

        // A's file is inode- and byte-identical; A's row is untouched.
        assert_eq!(
            before,
            fingerprint(&a_file),
            "A's file must be untouched (inode/mtime/size)"
        );
        assert_eq!(
            std::fs::read(&a_file).expect("read A's file"),
            before_bytes,
            "A's bytes must be untouched"
        );
        let (a_status, a_fp): (String, Option<String>) = crate::db::raw::fetch_optional(
            &pool,
            "SELECT status, file_path FROM downloads WHERE id = $1",
            |q| q.bind(a_id),
        )
        .await
        .expect("read A")
        .expect("row A present");
        assert_eq!(a_status, "complete", "row A must stay complete");
        assert_eq!(
            a_fp,
            Some(a_file.display().to_string()),
            "row A's file_path must be intact"
        );

        // Cleanup (shared single-DB suite; articles cascade off zims).
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE id IN ($1, $2)", |q| {
            q.bind(a_id).bind(b_id)
        })
        .await
        .unwrap();
        crate::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| {
            q.bind("utiny")
        })
        .await
        .unwrap();
    }
}
