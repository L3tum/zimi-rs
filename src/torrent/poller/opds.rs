//! OPDS catalog auto-update for the download poller.
//!
//! The pure OPDS parsing/fetching lives in `crate::torrent::opds`; this module
//! hosts only the poller's side of the feature — the periodic `opds_check`
//! (called from the poller's run loop every `OPDS_EVERY_N_TICKS` ticks) and
//! the DB queue insert — so the poller's method lives in the poller's own
//! directory.

use crate::db::Pool;
use crate::error::{Error, Result};
use crate::netguard::validate_download_url;
use crate::torrent::opds::{check_updates, OpdsUpdate};

use super::DownloadPoller;

impl DownloadPoller {
    // ── OPDS auto-update ─────────────────────────────────────────────────────

    pub(super) async fn opds_check(&self) -> Result<()> {
        // PERF-12: snapshot all poller settings once per opds_check.
        let p = self.settings.poller_params_snapshot();
        if !p.opds_auto_update {
            return Ok(());
        }
        let url = p.opds_url.clone();
        if url.is_empty() {
            return Ok(());
        }
        // SSRF guard: validate the OPDS URL before fetching, same rules as
        // direct downloads. Redirects are then followed manually with
        // per-hop validate + resolve + pin by `fetch_catalog` (SEC-1).
        let allow_private = p.allow_private_networks;
        validate_download_url(&url, allow_private)?;
        let updates = check_updates(&self.settings, &url, &self.zims).await?;
        queue_opds_updates(&self.db, &updates).await
    }
}

/// Queue OPDS auto-updates (TEST-5 seam, extracted from
/// [`DownloadPoller::opds_check`]).
///
/// Version-key semantics (SEC content integrity): a URL is skipped only
/// while a row for it is LIVE (`queued`/`downloading`). Terminal rows
/// (`complete`/`seeding`) are history, not dedup — they skip only when they
/// MATCH the catalog's CURRENT digest claim:
/// - the catalog declares digest D: skip iff some terminal row recorded
///   `sha256 = D` (this exact version is already installed). A different
///   (or newly declared) digest is a NEW version, not a violation: the old
///   row stays as history and a fresh `queued` row is inserted carrying the
///   new claim, which the direct-download finalize path verifies against.
/// - the catalog declares nothing (the current Kiwix catalog shape): the
///   legacy status-based skip applies — any terminal row means the current
///   file is the one we installed.
///
/// Each inserted row records `sha256 = <normalized catalog claim>` (NULL
/// when the source declared nothing), so the settled row doubles as the
/// provenance/version record for that URL.
///
/// Loopback URLs never reach here — the SSRF gate in `opds_check`
/// (`validate_download_url`) fires first.
async fn queue_opds_updates(db: &Pool, updates: &[OpdsUpdate]) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    for u in updates {
        // First line (B10): a LIVE row for this URL — queued or downloading
        // — means the fetch is already in flight; never re-queue it.
        let active: Option<i32> = crate::db::raw::fetch_scalar_optional(
            db,
            "SELECT id FROM downloads WHERE url = $1 \
             AND status IN ($2, $3) LIMIT 1",
            |q| {
                q.bind(&u.download_url)
                    .bind(crate::torrent::DownloadStatus::Queued.as_str())
                    .bind(crate::torrent::DownloadStatus::Downloading.as_str())
            },
        )
        .await?;
        if active.is_some() {
            continue;
        }
        // Second line (version key): does the terminal history for this URL
        // already match the catalog's CURRENT claim?
        let terminal: Vec<Option<String>> = crate::db::raw::fetch_scalar_all(
            db,
            "SELECT sha256 FROM downloads WHERE url = $1 \
             AND status IN ($2, $3)",
            |q| {
                q.bind(&u.download_url)
                    .bind(crate::torrent::DownloadStatus::Complete.as_str())
                    .bind(crate::torrent::DownloadStatus::Seeding.as_str())
            },
        )
        .await?;
        let claim: Option<String> = u.digest.as_ref().map(|d| d.value.clone());
        let matches_current = match claim.as_deref() {
            Some(c) => terminal.iter().any(|d| d.as_deref() == Some(c)),
            None => !terminal.is_empty(),
        };
        if matches_current {
            continue;
        }
        match crate::db::raw::execute(
            db,
            "INSERT INTO downloads (name, url, status, sha256) VALUES ($1, $2, $3, $4)",
            |q| {
                q.bind(&u.catalog_name)
                    .bind(&u.download_url)
                    .bind(crate::torrent::DownloadStatus::Queued.as_str())
                    .bind(&claim)
            },
        )
        .await
        {
            Ok(_) => {}
            // 23505 unique_violation (concurrent insert, e.g. manual POST
            // /downloads) — safe to skip.
            Err(Error::Database(e)) if crate::db::collections::is_unique_violation(&e) => {
                continue;
            }
            Err(e) => return Err(e),
        }
        tracing::info!(
            "OPDS: queued auto-update for {} → {} ({}) digest={}",
            u.local_name,
            u.catalog_name,
            u.download_url,
            claim.as_deref().unwrap_or("<none declared>")
        );
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{queue_opds_updates, DownloadPoller, OpdsUpdate};
    use crate::settings::{KEY_TORRENT_AUTO_UPDATE, KEY_TORRENT_OPDS_URL};
    use crate::torrent::opds::ZimDigest;
    use crate::torrent::poller::test_pool;

    /// A normalized 64-char lowercase hex claim for tests.
    fn digest(s: &str) -> ZimDigest {
        ZimDigest { value: s.into() }
    }

    /// A normalized 64-char lowercase hex claim for tests (64 × `a`).
    const DIGEST_OLD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// TEST-5: wiremock binds 127.0.0.1 and netguard hard-blocks loopback,
    /// so a loopback OPDS URL must be rejected by the SSRF gate BEFORE any
    /// HTTP I/O (DB-less: the dead pool never gets used).
    #[tokio::test]
    async fn opds_check_rejects_loopback_url_before_http() {
        use wiremock::MockServer;
        let server = MockServer::start().await;
        let pool = crate::testing::dead_pool();
        let mut map = crate::settings::default_settings();
        map.insert(KEY_TORRENT_AUTO_UPDATE.into(), serde_json::json!(true));
        map.insert(KEY_TORRENT_OPDS_URL.into(), serde_json::json!(server.uri()));
        let settings = crate::settings::SettingsCache::new_with_map(
            pool.clone(),
            map,
            std::collections::HashMap::new(),
        );
        let tmp = tempfile::tempdir().unwrap();
        let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
        let poller = DownloadPoller::new(
            pool,
            settings,
            zims,
            crate::torrent::QbitClientCache::new(),
            None,
            String::new(),
            String::new(),
        );
        poller
            .opds_check()
            .await
            .expect_err("loopback OPDS URL must be rejected by the SSRF gate");
        assert!(
            server
                .received_requests()
                .await
                .unwrap_or_default()
                .is_empty(),
            "SSRF gate must fire before any HTTP I/O"
        );
    }

    /// TEST-5 (CI-DB): B10 guard — an update whose URL already has a queued
    /// row is skipped; a new URL is inserted as `queued`.
    #[tokio::test]
    async fn queue_opds_updates_queues_new_skips_pending() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let x = "http://it.example/x.zim";
        let y = "http://it.example/y.zim";
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url IN ($1, $2)", |q| {
            q.bind(x).bind(y)
        })
        .await
        .unwrap();
        crate::db::raw::execute(
            &pool,
            "INSERT INTO downloads (name, url, status) VALUES ('it-x', $1, 'queued')",
            |q| q.bind(x),
        )
        .await
        .unwrap();

        let updates = vec![
            OpdsUpdate {
                local_name: "x".into(),
                catalog_name: "x2".into(),
                title: None,
                download_url: x.into(),
                digest: None,
            },
            OpdsUpdate {
                local_name: "y".into(),
                catalog_name: "y2".into(),
                title: None,
                download_url: y.into(),
                digest: None,
            },
        ];
        queue_opds_updates(&pool, &updates)
            .await
            .expect("queueing runs");

        let x_count: i64 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM downloads WHERE url = $1",
            |q| q.bind(x),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(x_count, 1, "B10 guard: pending URL must not be re-queued");
        let y_status: Option<String> = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT status FROM downloads WHERE url = $1",
            |q| q.bind(y),
        )
        .await
        .unwrap();
        assert_eq!(
            y_status.as_deref(),
            Some("queued"),
            "new URL must be queued exactly once"
        );

        // Cleanup (shared single-DB suite).
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url IN ($1, $2)", |q| {
            q.bind(x).bind(y)
        })
        .await
        .unwrap();
    }

    /// Version-key (CI-DB): a `complete` row whose recorded digest EQUALS the
    /// catalog's current claim means this exact version is already installed
    /// → the URL is skipped (no re-queue, no duplicate row).
    #[tokio::test]
    async fn queue_opds_updates_skips_complete_row_matching_claim() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let x = "http://it.example/vk-match.zim";
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url = $1", |q| q.bind(x))
            .await
            .unwrap();
        crate::db::raw::execute(
            &pool,
            "INSERT INTO downloads (name, url, status, sha256) VALUES ('vk-match', $1, 'complete', $2)",
            |q| q.bind(x).bind(DIGEST_OLD),
        )
        .await
        .unwrap();

        queue_opds_updates(
            &pool,
            &[OpdsUpdate {
                local_name: "m".into(),
                catalog_name: "m2".into(),
                title: None,
                download_url: x.into(),
                digest: Some(digest(DIGEST_OLD)),
            }],
        )
        .await
        .expect("queueing runs");

        let count: i64 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM downloads WHERE url = $1",
            |q| q.bind(x),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count, 1, "matching digest ⇒ already installed ⇒ skip");

        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url = $1", |q| q.bind(x))
            .await
            .unwrap();
    }

    /// Version-key (CI-DB): a `complete` row with a DIFFERENT digest than the
    /// catalog's current claim is a NEW version, not a violation — a fresh
    /// `queued` row is inserted carrying the NEW claim, and the old row stays
    /// untouched as history.
    #[tokio::test]
    async fn queue_opds_updates_requeues_when_claim_changes() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let x = "http://it.example/vk-change.zim";
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url = $1", |q| q.bind(x))
            .await
            .unwrap();
        crate::db::raw::execute(
            &pool,
            "INSERT INTO downloads (name, url, status, sha256) VALUES ('vk-change', $1, 'complete', $2)",
            |q| q.bind(x).bind(DIGEST_OLD),
        )
        .await
        .unwrap();

        let new_claim = digest(&"b".repeat(64));
        queue_opds_updates(
            &pool,
            &[OpdsUpdate {
                local_name: "c".into(),
                catalog_name: "c2".into(),
                title: None,
                download_url: x.into(),
                digest: Some(new_claim.clone()),
            }],
        )
        .await
        .expect("queueing runs");

        let count: i64 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM downloads WHERE url = $1 AND status = 'queued'",
            |q| q.bind(x),
        )
        .await
        .expect("count new rows")
        .unwrap();
        let (new_status, new_sha): (String, Option<String>) = crate::db::raw::fetch_optional(
            &pool,
            "SELECT status, sha256 FROM downloads WHERE url = $1 AND status = 'queued'",
            |q| q.bind(x),
        )
        .await
        .expect("read new row")
        .expect("a fresh queued row must exist for the new claim");
        assert_eq!(count, 1);
        assert_eq!(new_status, "queued");
        assert_eq!(
            new_sha.as_deref(),
            Some(new_claim.value.as_str()),
            "the new row must carry the catalog's current claim"
        );
        // The old completed row is preserved as history.
        let old: Option<(String, Option<String>)> = crate::db::raw::fetch_optional(
            &pool,
            "SELECT status, sha256 FROM downloads \
             WHERE url = $1 AND status = 'complete'",
            |q| q.bind(x),
        )
        .await
        .expect("read old row");
        assert_eq!(old, Some(("complete".into(), Some(DIGEST_OLD.into()))));

        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url = $1", |q| q.bind(x))
            .await
            .unwrap();
    }

    /// Version-key (CI-DB): a `complete` row with NO recorded claim (the URL
    /// was fetched before the catalog started declaring digests) must be
    /// re-queued once the catalog declares one — the claim was never
    /// verified for that install.
    #[tokio::test]
    async fn queue_opds_updates_requeues_unclaimed_row_when_claim_appears() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let x = "http://it.example/vk-appear.zim";
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url = $1", |q| q.bind(x))
            .await
            .unwrap();
        crate::db::raw::execute(
            &pool,
            "INSERT INTO downloads (name, url, status) VALUES ('vk-appear', $1, 'complete')",
            |q| q.bind(x),
        )
        .await
        .unwrap();

        queue_opds_updates(
            &pool,
            &[OpdsUpdate {
                local_name: "a".into(),
                catalog_name: "a2".into(),
                title: None,
                download_url: x.into(),
                digest: Some(digest(DIGEST_OLD)),
            }],
        )
        .await
        .expect("queueing runs");

        let count: i64 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM downloads WHERE url = $1 AND status = 'queued'",
            |q| q.bind(x),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count, 1, "unclaimed history + new claim ⇒ re-queue");

        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url = $1", |q| q.bind(x))
            .await
            .unwrap();
    }

    /// Version-key (CI-DB): when the catalog declares NOTHING (the current
    /// Kiwix catalog shape), the legacy status-based dedup applies — a
    /// `complete` row (claim or not) still skips the URL, preserving the
    /// pre-digest behavior for digest-less sources.
    #[tokio::test]
    async fn queue_opds_updates_undeclared_claim_keeps_status_dedup() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let x = "http://it.example/vk-legacy.zim";
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url = $1", |q| q.bind(x))
            .await
            .unwrap();
        crate::db::raw::execute(
            &pool,
            "INSERT INTO downloads (name, url, status) VALUES ('vk-legacy', $1, 'complete')",
            |q| q.bind(x),
        )
        .await
        .unwrap();

        queue_opds_updates(
            &pool,
            &[OpdsUpdate {
                local_name: "l".into(),
                catalog_name: "l2".into(),
                title: None,
                download_url: x.into(),
                digest: None,
            }],
        )
        .await
        .expect("queueing runs");

        let count: i64 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM downloads WHERE url = $1",
            |q| q.bind(x),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count, 1, "digest-less catalog + complete row ⇒ legacy skip");

        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE url = $1", |q| q.bind(x))
            .await
            .unwrap();
    }
}
