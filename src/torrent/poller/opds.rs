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
/// [`DownloadPoller::opds_check`]): skip an update whose URL already has a
/// queued/in-flight/installed row (B10), insert the rest as `queued`.
///
/// Loopback URLs never reach here — the SSRF gate in `opds_check`
/// (`validate_download_url`) fires first.
async fn queue_opds_updates(db: &Pool, updates: &[OpdsUpdate]) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    for u in updates {
        // Second line of defense (B10): `find_updates` already dedups to
        // one (newest) update per local ZIM; this per-URL guard prevents
        // re-queueing while an older version of the same catalog is still
        // queued/downloading, so the update lands cleanly on completion.
        let pending: Option<i32> = crate::db::raw::fetch_scalar_optional(
            db,
            "SELECT id FROM downloads WHERE url = $1 \
             AND status IN ($2, $3, $4, $5) LIMIT 1",
            |q| {
                q.bind(&u.download_url)
                    .bind(crate::torrent::DownloadStatus::Queued.as_str())
                    .bind(crate::torrent::DownloadStatus::Downloading.as_str())
                    .bind(crate::torrent::DownloadStatus::Complete.as_str())
                    .bind(crate::torrent::DownloadStatus::Seeding.as_str())
            },
        )
        .await?;
        if pending.is_some() {
            continue;
        }
        match crate::db::raw::execute(
            db,
            "INSERT INTO downloads (name, url, status) VALUES ($1, $2, $3)",
            |q| {
                q.bind(&u.catalog_name)
                    .bind(&u.download_url)
                    .bind(crate::torrent::DownloadStatus::Queued.as_str())
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
            "OPDS: queued auto-update for {} → {} ({})",
            u.local_name,
            u.catalog_name,
            u.download_url
        );
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{queue_opds_updates, DownloadPoller, OpdsUpdate};

    use crate::settings::{KEY_TORRENT_AUTO_UPDATE, KEY_TORRENT_OPDS_URL};
    use crate::torrent::poller::test_pool;

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
            },
            OpdsUpdate {
                local_name: "y".into(),
                catalog_name: "y2".into(),
                title: None,
                download_url: y.into(),
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
}
