//! Background download lifecycle.
//!
//! One task, polled every `torrent.poll_secs`:
//!
//! 1. **Startup reconciliation** — rows vs live qBittorrent state (rebind
//!    hashes, recover completed torrents whose files were never installed,
//!    adopt category torrents that have no tracking row, retry interrupted
//!    direct downloads).
//! 2. **Queue** — `queued` rows become qBittorrent torrents (subject to
//!    `torrent.max_active`) or direct HTTP downloads when the URL is a plain
//!    `.zim` (works even without qBittorrent).
//! 3. **Poll** — progress/speed/ETA refresh; completion detection.
//! 4. **Complete** — seed-ratio cap, locate `.zim` file(s) in the torrent's
//!    content dir, verify via libzim, install into `zim_dir` (hardlink or
//!    copy), resync the library, then index in the background.
//! 5. **OPDS** — periodically check the Kiwix catalog for newer versions and
//!    queue auto-updates when `torrent.auto_update` is enabled.
//!
//! **Master switch:** while `torrent.enabled` is `false`, the poller is fully
//! inert — no qBittorrent connect, no queued-row claims (direct or torrent),
//! no requeue, no in-flight processing, no OPDS auto-seed. The switch is
//! read live from the settings cache each cycle, so an operator toggle
//! takes effect within one poll interval (no restart).

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::time::sleep;

use crate::db::downloads::DownloadRecord;
use crate::db::Pool;
use crate::error::{Error, Result, TorrentKind};
use crate::netguard::{
    assert_host_not_blocked, follow_pinned_get, validate_download_url, PinnedResponse,
};
use crate::settings::SettingsCache;
use crate::torrent::client::{build_download_client, ClientProfile};
use crate::torrent::files::{install_zim, locate_torrent_zim, validate_download_name, verify_zim};
use crate::torrent::{
    connect_qbit, qbit_fingerprint, resolve_qbit_inputs, QbitClient, QbitClientCache,
};

mod complete;
mod direct;
mod inflight;
mod opds;
mod reconcile;
mod requeue;
mod stats;

use self::direct::direct_download;
pub(crate) use self::requeue::{
    filter_requeue_name_collisions, requeue_guard_step, GuardEntry, RequeueDecision,
    REQUEUE_ERROR_PATTERN,
};
pub use self::stats::{apply_stats_batch, StatsRow};
use self::stats::{eta_secs, kibs_to_bps, stats_changed};
// The download lifecycle state machine lives in `db::downloads_lifecycle`
// (ARCH M-2/M-3); the poller drives it. Re-exported here so the poller's
// child modules reach the shared row helpers via `use super::*`.
pub(crate) use crate::db::downloads_lifecycle::{
    mark_error, status_checked, status_if_changed, status_no_longer_downloading,
};
#[cfg(test)]
pub(crate) use tests::download::test_pool;

use crate::zim::{index, ZimManager};

/// The shared transfer client, or a clear error when the startup build failed.
/// Free function (not a method) so the `None` path is unit-testable without
/// constructing a full `DownloadPoller`.
fn client_from_option(http: Option<&reqwest::Client>) -> Result<&reqwest::Client> {
    http.ok_or_else(|| {
        Error::Internal(anyhow::anyhow!(
            "download HTTP client unavailable (client build failed at startup)"
        ))
    })
}

/// Check the OPDS catalog every N poll ticks — i.e. every `180 × torrent_poll_secs`
/// (the poll interval is user-settable, so the real cadence varies).
const OPDS_EVERY_N_TICKS: u64 = 180;
/// A download row that can't find its torrent after this long is an error.
const MISSING_TORRENT_GRACE: chrono::Duration = chrono::Duration::minutes(10);
/// Max queued rows to consider per tick (bound, not hardcoded, in the SQL).
const MAX_QUEUED_PER_TICK: i32 = 50;
/// Re-exported from the db layer (the predicate belongs to the `downloads`
/// table, not the poller). See `crate::db::downloads::ZIM_URL_PREDICATE`.
pub use crate::db::downloads::ZIM_URL_PREDICATE;

/// Background poller that reconciles the `downloads` table with qBittorrent
/// state, direct-download progress, and the ZIM library on each tick.
///
/// **KNOWN behavior (restart-reset):** the bounded-retry requeue guard
/// (`last_error`) and its pass clock (`requeue_passes`) are process-local;
/// both reset when the process restarts, so the give-up-after-N-consecutive-
/// transient-failures policy does not survive a restart. The `downloads`
/// table persists the last error message but not the consecutive-failure
/// count — a row that had almost given up before a restart starts its retry
/// count over again after one.
pub struct DownloadPoller {
    pub(crate) db: Pool,
    /// Background (small, capped) pool — poller-triggered ZIM reindexing
    /// runs on it so a long COPY-holding reindex can't starve foreground
    /// search connections (the db/db_bg split in src/startup.rs build_state /
    /// ARCHITECTURE.md "Persistence layer"). Short row writes deliberately
    /// stay on `db` (2026-09-26 review fix).
    pub(crate) db_bg: Pool,
    pub(crate) settings: SettingsCache,
    pub(crate) zims: Arc<ZimManager>,
    /// Runtime qBittorrent client cache (ARCH M3) — rebuilds when the
    /// effective `torrent.url` changes.
    torrent: QbitClientCache,
    /// Config (env) qBittorrent URL override; when `Some` it wins over the
    /// runtime `torrent.url` setting. User/pass are config-level (env) and
    /// stable for the process.
    torrent_url_override: Option<String>,
    torrent_username: String,
    torrent_password: String,
    http: Option<reqwest::Client>,
    /// Cooperative shutdown for [`run`](Self::run): set via
    /// [`cancel`](Self::cancel) and polled between ticks.
    stopping: Arc<AtomicBool>,
    /// Bounded-retry requeue guard, keyed by row id: the last transient
    /// error message, its consecutive count, and the pass on which the row
    /// was last observed as an error row ([`GuardEntry`]). Replaced (count
    /// reset) when the message changes; on give-up the entry is RETAINED
    /// (sticky — give-up keeps re-asserting on later passes) and is only
    /// reclaimed by lazy eviction once the row leaves `error` by other
    /// means (e.g. a manual re-queue); capped at `MAX_LAST_ERROR_TRACKED`.
    ///
    /// **KNOWN behavior (restart-reset):** process-local — the map is empty
    /// at startup, so a row's consecutive transient-failure count resets to
    /// zero on process restart and the give-up-after-N policy does not
    /// survive it (only the error message persists, in `downloads.error`).
    last_error: Arc<std::sync::Mutex<HashMap<i32, GuardEntry>>>,
    /// Requeue pass counter (the lazy-eviction clock for the guard map):
    /// incremented once per `requeue_stale_errors` pass and compared
    /// against [`GuardEntry::last_seen`] by the cap sweep.
    ///
    /// **KNOWN behavior (restart-reset):** process-local — starts at zero
    /// on every startup (with an empty guard map, see
    /// [`last_error`](Self::last_error)); see the struct doc for the
    /// resulting restart-reset of the requeue give-up policy.
    requeue_passes: AtomicU64,
}

impl DownloadPoller {
    /// Create a new poller wired to the given DB pools (foreground +
    /// background — see the `db_bg` field), settings, and ZIM manager.
    // one parameter per externally-wired poller field (db/db_bg/settings/zims/qb×4)
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Pool,
        db_bg: Pool,
        settings: SettingsCache,
        zims: Arc<ZimManager>,
        torrent: QbitClientCache,
        torrent_url_override: Option<String>,
        torrent_username: String,
        torrent_password: String,
    ) -> Self {
        // Transfer profile: this shared client streams `.zim` bodies when the
        // host is an IP literal (pin = None → no DNS, nothing to pin in
        // direct_download). A builder
        // failure must not crash serve startup (rare and recoverable-per-tick)
        // — but the client stays `None`, and every use site fails with a clear
        // error instead of silently falling back to an unguarded default
        // client (which would drop the SSRF redirect policy and timeouts).
        let http = match build_download_client(ClientProfile::Transfer, None) {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(
                    "download HTTP client build failed: {e}; direct downloads and OPDS \
                checks will fail until restart"
                );
                None
            }
        };
        Self {
            db,
            db_bg,
            settings,
            zims,
            torrent,
            torrent_url_override,
            torrent_username,
            torrent_password,
            http,
            stopping: Arc::new(AtomicBool::new(false)),
            last_error: Arc::new(std::sync::Mutex::new(HashMap::new())),
            requeue_passes: AtomicU64::new(0),
        }
    }

    /// Request a cooperative stop: [`run`](Self::run) breaks out of its loop
    /// at the next between-ticks poll. No tokio-util `CancellationToken`
    /// — a process-lifetime flag is all the run loop needs.
    // No production caller yet — main.rs aborts the task on shutdown
    // instead; the `run_stops_on_cancel_between_ticks` test exercises the
    // cooperative-stop path.
    #[allow(dead_code)]
    pub(crate) fn cancel(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    /// Whether [`cancel`](Self::cancel) has been called.
    fn cancelled(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Resolve the effective qBittorrent client for this cycle (ARCH M3).
    /// Merges the config URL override with the runtime `torrent.url` setting,
    /// and rebuilds the cached client only when the connection fingerprint
    /// changed (e.g. after a `PUT /settings`). Connect + auth run outside the
    /// cache's short lock. Returns `None` when qBittorrent is effectively
    /// disabled (no URL) or the current connect failed — the cache stores
    /// nothing on a failed connect, so `None` is NOT by itself "not
    /// configured": callers needing the split use
    /// [`Self::qbit_configured`].
    async fn resolve_torrent(&self, p: &crate::settings::PollerParams) -> Option<Arc<QbitClient>> {
        let inputs = resolve_qbit_inputs(
            p.qbit_url.as_deref(),
            &self.torrent_url_override,
            &self.torrent_username,
            &self.torrent_password,
        )?;
        // SEC M-1: the private-network opt-in is part of the fingerprint, so a
        // runtime flip of `torrent.allow_private_networks` rebuilds the client
        // with the correct netguard policy (like a `torrent.url` change).
        let allow_private = p.qbit_allow_private;
        let fingerprint = qbit_fingerprint(&inputs.0, &inputs.1, &inputs.2, allow_private);
        let (url, user, pass) = inputs;
        self.torrent
            .ensure(&fingerprint, move || async move {
                connect_qbit(&url, &user, &pass, allow_private).await
            })
            .await
    }

    /// Whether qBittorrent is *configured* for this cycle: an effective URL
    /// (env override or the runtime `torrent.url` setting) is present and
    /// non-blank. Deliberately independent of [`Self::resolve_torrent`]
    /// returning `Some` — a configured-but-unreachable qB (wrong port, qB
    /// restarting, LAN blip) also resolves to `None`. Callers use this to
    /// keep the two apart: "not configured" is fatal for queued torrent
    /// rows and drains cancelled hash bindings; "down" is not — those rows
    /// stay put and the next tick retries once qB recovers.
    fn qbit_configured(&self, p: &crate::settings::PollerParams) -> bool {
        resolve_qbit_inputs(
            p.qbit_url.as_deref(),
            &self.torrent_url_override,
            &self.torrent_username,
            &self.torrent_password,
        )
        .is_some()
    }

    /// Atomically check whether this row is still `downloading`; if the
    /// status changed (cancel/finalize raced), log it, drop the qB item, and
    /// return `false` so the caller bails before any further mutation.
    ///
    /// The stale `hash` binding is cleared only once the qB removal is
    /// confirmed: when qB is unreachable (or the delete fails) the binding is
    /// kept so the next tick retries the removal instead of orphaning the
    /// torrent. Rows without a hash (direct downloads) skip qB entirely.
    async fn status_if_changed(&self, id: i32, torrent_hash: &str) -> bool {
        // The lifecycle helper takes a checked-out `&mut PgConnection` (it can
        // pair the read with a follow-up write on the same connection). A
        // failed acquire fails open like a failed status query: the check is
        // skipped and the download continues (the next periodic check re-runs).
        let mut conn = match self.db.acquire().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(
                    "status check for download {id} could not acquire a connection: {e}"
                );
                return true;
            }
        };
        if status_no_longer_downloading(&mut conn, id).await.is_none() {
            return true;
        }
        tracing::info!(
            download_id = id,
            "status changed since claim, stopping download"
        );
        if !torrent_hash.is_empty() {
            let p = self.settings.poller_params_snapshot();
            match self.resolve_torrent(&p).await {
                Some(q) => match q.delete(torrent_hash, true).await {
                    Ok(()) => {
                        crate::db::downloads_lifecycle::clear_hash(&self.db, id).await;
                    }
                    Err(e) => tracing::warn!(
                        "failed to remove cancelled torrent {torrent_hash}: {e} (hash kept for \
                        retry)"
                    ),
                },
                None => {
                    tracing::debug!(
                        "qBittorrent unavailable — keeping hash {torrent_hash} for the next tick"
                    );
                }
            }
        }
        false
    }

    /// Runs until the process exits.
    pub async fn run(self) {
        // Master switch (H2, 2026-09 review): a disabled poller must not
        // establish a qB session — `startup.rs`'s initial-connect gate
        // mirrors this, and without it the first tick's `resolve_torrent`
        // reconnects anyway (the cache is empty at startup). Read live, not
        // latched: the per-cycle gates in `tick()`/`opds_check()` re-read
        // `torrent.enabled` every cycle. `reconcile` is startup-only, so an
        // instance that starts disabled defers its adoption/orphan cleanup
        // to the next restart (accepted trade-off; a live enable does
        // everything else).
        if self.settings.torrent_enabled() {
            let p0 = self.settings.poller_params_snapshot();
            let qbit = self.resolve_torrent(&p0).await;
            if let Err(e) = self.reconcile(qbit).await {
                tracing::warn!("startup download reconciliation failed: {e}");
            }
        } else {
            tracing::info!(
                "torrent poller idle: torrent.enabled=false — set torrent.enabled=true \
                 to enable acquisition"
            );
        }
        let mut ticks: u64 = 0;
        loop {
            ticks += 1;
            if let Err(e) = self.tick().await {
                tracing::warn!("download poll failed: {e}");
            }
            if ticks.is_multiple_of(OPDS_EVERY_N_TICKS) {
                if let Err(e) = self.opds_check().await {
                    tracing::debug!("OPDS update check failed: {e}");
                }
            }
            // Poll for cancellation between ticks (no tokio::select! — keep
            // the loop shape simple; granularity = tick time).
            if self.cancelled() {
                tracing::info!("poller cancelled, stopping");
                break;
            }
            sleep(Duration::from_secs(self.settings.torrent_poll_secs())).await;
        }
    }

    // ── One poll cycle ───────────────────────────────────────────────────────

    async fn tick(&self) -> Result<()> {
        // Master switch (H2, 2026-09 review): read LIVE (a cheap RwLock read,
        // deliberately not via the PERF-12 snapshot below) so an operator's
        // settings toggle takes effect within one poll interval without a
        // restart. While off: no qB connect, no claims, no requeue, no
        // in-flight processing.
        if !self.settings.torrent_enabled() {
            return Ok(());
        }
        // PERF-12: snapshot all poller settings once per tick.
        let p = self.settings.poller_params_snapshot();
        // Resolve the effective qBittorrent client for this cycle (rebuilds
        // the cache only when the connection fingerprint changed — ARCH M3).
        let qbit = self.resolve_torrent(&p).await;
        // "Configured" is checked separately: `qbit == None` also happens
        // when qB IS configured but the connect failed this tick (wrong
        // port, qB restarting, LAN blip — the cache stores nothing on a
        // failed connect). See [`Self::qbit_configured`].
        let qb_configured = self.qbit_configured(&p);

        // Bounded error-row retry: re-queue stale connection-level
        // errors with a bounded retry guard. This runs BEFORE the early-exit
        // below on purpose: `should_skip_tick` does not count stale `error`
        // rows, so a single failed download with an otherwise-idle queue
        // would never be requeued if the skip ran first (the 10-minute
        // bounded retry would be dead in the most common case). The pass is
        // pure DB work (a stale-row SELECT + a guarded UPDATE) with no qB
        // fetch, so it is cheap when nothing matches.
        self.requeue_stale_errors().await?;

        // Cheap early-exit: skip the costly qBittorrent HTTP fetch when there
        // is nothing this tick actually processes (no queued rows, no in-flight
        // qB torrent rows, no cancellations to clean up). reconcile() and
        // opds_check() run on their own schedule and are unaffected. Rows
        // requeued above now count as `queued`, so a non-empty requeue
        // defeats the skip and they are enqueued in this same tick.
        if self.should_skip_tick().await? {
            return Ok(());
        }

        let (torrents, qb_available) = self.fetch_qb_state(qbit.as_ref()).await?;
        let by_hash: HashMap<&str, &super::TorrentInfo> =
            torrents.iter().map(|t| (t.hash.as_str(), t)).collect();
        let by_name: HashMap<String, &super::TorrentInfo> = torrents
            .iter()
            .map(|t| (t.name.to_lowercase(), t))
            .collect();

        // Each DB op below acquires its own short-lived pooled connection so
        // no connection is held across the qBittorrent HTTP calls (q.delete /
        // q.add_torrent) or handle_complete (file copy + resync + qB).
        // 1) Cancelled rows: drop the torrent from qBittorrent (files too).
        self.process_cancelled(qbit.as_ref(), qb_configured).await?;

        // 2) Enqueue queued rows, subject to the active-download budget.
        self.process_queued(qbit.as_ref(), qb_available, qb_configured, &p, &torrents)
            .await?;

        // 3) In-flight torrent rows (downloading) and seeding rows: bind
        //    hashes, refresh progress / seeding stats, detect completion /
        //    fatal states.
        let rows = self.fetch_inflight_rows().await?;
        let changed = self
            .process_inflight(&by_hash, &by_name, qbit, qb_available, &p, rows)
            .await?;
        self.flush_stats(&changed).await;

        Ok(())
    }

    /// LINT-2 extraction (verbatim `tick()` step-3 block 1): the in-flight
    /// rows the tick body processes (downloading + seeding). Returning owned
    /// [`DownloadRecord`] values is sound — no connection is involved.
    async fn fetch_inflight_rows(&self) -> Result<Vec<DownloadRecord>> {
        crate::db::downloads::fetch_downloads_by_statuses(
            &self.db,
            crate::torrent::DownloadStatus::Downloading.as_str(),
            crate::torrent::DownloadStatus::Seeding.as_str(),
        )
        .await
    }

    /// LINT-2 extraction (verbatim `tick()` early-exit block): whether this
    /// cycle can skip the costly qBittorrent HTTP fetch because there is
    /// nothing `tick` actually processes. Counts queued rows, in-flight qB
    /// torrent rows (`downloading` + non-`.zim` URL — direct downloads manage
    /// their own rows and are excluded), seeding rows, and cancellations with
    /// a live hash binding.
    async fn should_skip_tick(&self) -> Result<bool> {
        // `url NOT LIKE '%.zim'` (after stripping query **and** fragment) =
        // torrent rows (inverse of `is_direct_zim_url`). `count(id)` (id =
        // the non-null PK) is the skip-tick gate; the predicate fragment
        // ([`ZIM_URL_PREDICATE`]) splices in verbatim.
        let n: i64 = crate::db::raw::fetch_scalar_optional(
            &self.db,
            &format!(
                "SELECT count(id) FROM downloads \
                 WHERE status = $1 \
                    OR (status = $2 AND ({pred}) NOT LIKE '%.zim') \
                    OR status = $3 \
                    OR (status = $4 AND hash IS NOT NULL)",
                pred = ZIM_URL_PREDICATE
            ),
            |q| {
                q.bind(crate::torrent::DownloadStatus::Queued.as_str())
                    .bind(crate::torrent::DownloadStatus::Downloading.as_str())
                    .bind(crate::torrent::DownloadStatus::Seeding.as_str())
                    .bind(crate::torrent::DownloadStatus::Cancelled.as_str())
            },
        )
        .await?
        .unwrap_or(0);
        Ok(n == 0)
    }

    /// LINT-2 extraction (`tick()` requeue block): re-queue error rows that
    /// hit a connection-level failure more than 10 minutes ago, bounded by
    /// `REQUEUE_GIVE_UP_AFTER` so a persistently-failing row eventually stays
    /// `error` for manual intervention instead of cycling forever — and
    /// give-up is *sticky*: the guard entry is retained on give-up, so the row
    /// is not re-queued again on the next pass. A 401/403 session expiry is
    /// always exempt — the next re-login is the fix, not a human.
    ///
    /// The per-row guard decision is the pure [`requeue_guard_step`] and the
    /// collision pre-filter is the pure [`filter_requeue_name_collisions`], so
    /// both are unit-testable without Postgres.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    async fn requeue_stale_errors(&self) -> Result<()> {
        // Lazy-eviction clock: count every pass (also the timestamp stamped
        // on entries observed below), so "not seen for N passes" is measured
        // in real passes even during quiet stretches.
        let pass = self.requeue_passes.fetch_add(1, Ordering::SeqCst) + 1;
        // Raw SQL (db::raw): the case-insensitive regex match `error ~* $1`
        // has no `db::raw` helper shape (the guarded UPDATE lives in
        // `downloads_lifecycle::requeue_stale_errors`). `name` is fetched too
        // so the collision filter below can match it against active rows.
        let stale: Vec<(i32, String, String)> = crate::db::raw::fetch_all(
            &self.db,
            "SELECT id, name, error FROM downloads \
             WHERE status = $2 \
               AND updated_at < now() - interval '10 minutes' \
               AND error ~* $1",
            |q| {
                q.bind(REQUEUE_ERROR_PATTERN)
                    .bind(crate::torrent::DownloadStatus::Error.as_str())
            },
        )
        .await?;
        if stale.is_empty() {
            return Ok(());
        }
        // The partial unique `uq_downloads_active_name`
        // (name UNIQUE where status IN queued/downloading) covers active-vs-
        // active only, so an `error` row may share a `name` with an active one.
        // Re-queueing that row would trip 23505 — an error the guarded UPDATE
        // propagates and the tick treats as fatal — aborting the whole tick
        // (and, with `updated_at` never advancing, re-picking the same row
        // every tick). Fetch the active names once and skip the colliding
        // stale rows; they stay in `error` and retry once the active row goes
        // terminal.
        let active_names: HashSet<String> = crate::db::raw::fetch_scalar_all(
            &self.db,
            &format!(
                "SELECT name FROM downloads WHERE status IN {active}",
                active = crate::torrent::in_list([
                    crate::torrent::DownloadStatus::Queued,
                    crate::torrent::DownloadStatus::Downloading,
                ])
            ),
            |q| q,
        )
        .await?
        .into_iter()
        .collect();
        let candidates = filter_requeue_name_collisions(&stale, &active_names);
        if candidates.is_empty() {
            return Ok(());
        }
        // The guard's scope ends here (before the DB write) so the
        // `MutexGuard` is never held across an await.
        let requeue: Vec<i32> = {
            let mut requeue = Vec::new();
            let mut guard = self.last_error.lock().expect("last_error lock");
            for (id, _name, msg) in &candidates {
                if requeue_guard_step(&mut guard, *id, msg, pass) == RequeueDecision::Requeue {
                    requeue.push(*id);
                }
            }
            requeue
        };
        if !requeue.is_empty() {
            // `AND status = 'error'`: a row that moved on between the
            // SELECT and this UPDATE (e.g. manually cancelled) is never
            // clobbered (the guard lives in `requeue_stale_errors`).
            let n =
                crate::db::downloads_lifecycle::requeue_stale_errors(&self.db, &requeue).await?;
            tracing::info!("requeued {n} stale error row(s) for retry");
        }
        Ok(())
    }

    /// LINT-2 extraction (verbatim `tick()` qB-fetch block): fetch the current
    /// qBittorrent torrent list and whether qB itself is available.
    ///
    /// `qb_available` distinguishes "qB unreachable" from "torrent genuinely
    /// absent" — when qB is down, an empty list must NOT be treated as
    /// authoritative, or every in-flight row would age out to `error` during
    /// the outage. `filter=all` (not `active`): qB's `active` filter omits
    /// *paused* torrents, so a paused download would age out after the grace
    /// period even though it's still in qBittorrent. The classification fns
    /// handle the extra states correctly: `is_complete()` counts `pausedUP`
    /// as complete, and `is_active_download()` (used by the enqueue budget)
    /// still excludes paused torrents.
    async fn fetch_qb_state(
        &self,
        qbit: Option<&Arc<QbitClient>>,
    ) -> Result<(Vec<super::TorrentInfo>, bool)> {
        let (torrents, qb_available) = match qbit {
            Some(q) => match q.get_torrents("all").await {
                Ok(t) => (t, true),
                Err(e) => {
                    tracing::debug!("qBittorrent info fetch failed: {e}");
                    // Session expired/invalid (401/403): the in-memory cookie
                    // is stale, so drop the cached client. The next tick's
                    // resolve_torrent sees an empty cache and re-logins for
                    // free (connect_qbit). Ordinary network failures and other
                    // Error::Torrents still just fail soft (qb_available=false)
                    // without clearing — a transient blip shouldn't force a
                    // re-login.
                    if matches!(
                        &e,
                        Error::Torrent {
                            kind: TorrentKind::SessionExpired,
                            ..
                        }
                    ) {
                        self.torrent.invalidate();
                    }
                    (Vec::new(), false)
                }
            },
            None => (Vec::new(), false),
        };
        Ok((torrents, qb_available))
    }

    /// LINT-2 extraction (verbatim `tick()` cancelled-rows block): drop
    /// cancelled torrents from qBittorrent (files too), clearing the stale
    /// `hash` binding once removal is confirmed.
    ///
    /// BUG-21: with qBittorrent not configured nothing else clears `hash` on
    /// cancelled rows, so the early-exit count stays > 0 and every tick
    /// re-runs the full bookkeeping queries. Drain them: once the hash is
    /// NULL the row early-exits. A *configured-but-unreachable* qB is NOT
    /// drained (see the gate below) — the kept bindings are what make the
    /// per-tick removal retry visible.
    async fn process_cancelled(
        &self,
        qbit: Option<&Arc<QbitClient>>,
        qb_configured: bool,
    ) -> Result<()> {
        let cancelled: Vec<(i32, Option<String>)> = crate::db::raw::fetch_all(
            &self.db,
            "SELECT id, hash FROM downloads WHERE status = $1 AND hash IS NOT NULL",
            |q| q.bind(crate::torrent::DownloadStatus::Cancelled.as_str()),
        )
        .await?;
        // BUG-21: qB NOT configured → drain the hash bindings so the rows
        // early-exit. `qbit == None` also covers a configured-but-unreachable
        // qB (wrong port, qB restarting, LAN blip) — there the bindings are
        // KEPT: draining would orphan a still-alive torrent that the next
        // restart's reconcile re-adopts as a brand-new `downloading` row (a
        // cancelled download resurrects). The loop below retries the removal
        // each tick until qB is back.
        if qbit.is_none() && !qb_configured {
            let n = crate::db::downloads_lifecycle::drain_cancelled_hashes(&self.db)
                .await
                .unwrap_or(0);
            if n > 0 {
                tracing::info!(
                    rows = n,
                    "cleared stale hashes on cancelled rows (no qB configured)"
                );
            }
        }
        for (id, hash) in &cancelled {
            let id = *id;
            // qB down — keep the hash binding; the next tick retries the removal.
            if qbit.is_none() {
                continue;
            }
            // The `hash IS NOT NULL` filter above guarantees a binding
            // (belt & braces).
            let Some(hash) = hash.as_deref() else {
                continue;
            };
            // The helper owns the two-statement cleanup — confirm the
            // row left `downloading`, drop the torrent from qB, and clear the
            // stale `hash` binding (kept when the removal can't be confirmed).
            let _ = self.status_if_changed(id, hash).await;
        }
        Ok(())
    }

    /// LINT-2 extraction (verbatim `tick()` enqueue loop): claim queued rows
    /// as qBittorrent torrents or direct `.zim` HTTP downloads, subject to
    /// the active-download budget. Direct `.zim` URLs work without
    /// qBittorrent; torrent URLs require a live qB (`qb_available`) and share
    /// the budget with in-flight direct downloads. Per-row write failures are
    /// swallowed (the row stays `queued`/`error` and is retried next tick);
    /// only the initial SELECT and the direct-download claim propagate.
    ///
    /// A torrent URL requires qB to be *configured*; when it is configured
    /// but unreachable this tick the row is left `queued` (like
    /// `!qb_available`) instead of marked `error` — the "not configured"
    /// message does not match `REQUEUE_ERROR_PATTERN`, so a fatal mark there
    /// would stick the row in `error` even after qB recovers.
    async fn process_queued(
        &self,
        qbit: Option<&Arc<QbitClient>>,
        qb_available: bool,
        qb_configured: bool,
        p: &crate::settings::PollerParams,
        torrents: &[super::TorrentInfo],
    ) -> Result<()> {
        let queued: Vec<(i32, String, String)> = crate::db::raw::fetch_all(
            &self.db,
            "SELECT id, name, url FROM downloads WHERE status = $1 \
             ORDER BY id ASC LIMIT $2",
            |q| {
                q.bind(crate::torrent::DownloadStatus::Queued.as_str())
                    .bind(MAX_QUEUED_PER_TICK as i64)
            },
        )
        .await?;
        // Only torrents in OUR category count against the active budget — other
        // people's qBittorrent downloads must not starve ours.
        let category = p.category.clone();
        let active = torrents
            .iter()
            .filter(|t| t.category.as_deref() == Some(category.as_str()) && t.is_active_download())
            .count() as i32;
        // In-flight direct downloads also count against the active budget
        // (they're claimed with a non-NULL file_path marker). `url LIKE
        // '%.zim'` (after stripping query and fragment) is the SQL form of
        // `is_direct_zim_url`.
        let direct_active: i32 = {
            // The predicate fragment ([`ZIM_URL_PREDICATE`]) splices in
            // verbatim (byte-stable, see `retry_interrupted_directs`). Query
            // errors keep the budget whole: the count falls back to 0, as
            // before. `count(id)` (id = the non-null PK) is the gate.
            let n: i64 = crate::db::raw::fetch_scalar_optional(
                &self.db,
                &format!(
                    "SELECT count(id) FROM downloads \
                     WHERE status = $1 AND ({pred}) LIKE '%.zim' AND file_path IS NOT NULL",
                    pred = ZIM_URL_PREDICATE
                ),
                |q| q.bind(crate::torrent::DownloadStatus::Downloading.as_str()),
            )
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
            n as i32
        };
        let mut budget = (self
            .settings
            .torrent_max_active()
            .saturating_sub(active + direct_active))
        .max(0) as i64;

        for (id, name, url) in &queued {
            let id = *id;

            // SSRF guard (defense in depth — the API boundary also validates
            // before queueing). Direct .zim URLs must be http/https; torrent
            // URLs may be magnet: but http(s) ones are still host-checked.
            // `is_direct` is computed once and reused below (Ponytail S2).
            let is_direct = super::is_direct_zim_url(url);
            {
                let allow_private = p.allow_private_networks;
                let check = if is_direct {
                    validate_download_url(url, allow_private)
                } else {
                    assert_host_not_blocked(url, allow_private, false)
                };
                if let Err(e) = check {
                    tracing::warn!("download {id} rejected: {e}");
                    mark_error(&self.db, id, &format!("invalid URL: {e}")).await;
                    continue;
                }
            }

            if is_direct {
                // Direct HTTP download (no qBittorrent required), but it still
                // counts against the active-download budget.
                // Defense in depth: names are validated at insert time, but
                // a stale row with a bad name must not write outside the ZIM
                // dir (the name is spliced into the `.part` path).
                if let Err(e) = validate_download_name(name) {
                    tracing::warn!("direct download {id} rejected: {e}");
                    mark_error(&self.db, id, &format!("invalid name: {e}")).await;
                    continue;
                }
                if budget <= 0 {
                    continue; // picked up next tick
                }
                let part = self.zims.zim_dir.join(format!("{name}.part"));
                // Claim first so the next tick cannot double-spawn (the
                // `AND status = 'queued'` guard makes the claim atomic).
                let claimed = crate::db::downloads_lifecycle::claim_direct(
                    &self.db,
                    id,
                    &part.display().to_string(),
                )
                .await?;
                if claimed > 0 {
                    self.spawn_direct(id, url, &part);
                    budget -= 1;
                }
                continue;
            }

            let Some(q) = qbit else {
                if !qb_configured {
                    let msg = "qBittorrent not configured — only direct .zim URLs are supported"
                        .to_string();
                    let marked =
                        crate::db::downloads_lifecycle::mark_fatal_error(&self.db, id, &msg)
                            .await?;
                    if marked == 0 {
                        tracing::info!(
                            download_id = id,
                            "skipping fatal error mark: row no longer queued/downloading"
                        );
                    }
                    continue;
                }
                // Configured but the connect failed this tick (wrong port,
                // qB restarting, LAN blip): leave the row `queued` (same
                // treatment as `!qb_available`) so it enqueues normally
                // once qB recovers.
                tracing::warn!(
                    download_id = id,
                    "qBittorrent configured but unreachable — leaving row queued for the next tick"
                );
                continue;
            };

            if !qb_available {
                // qB down — leave row queued for next tick; direct downloads
                // handled above.
                continue;
            }

            if budget <= 0 {
                // No capacity for more active downloads this tick: skip the
                // enqueue, but KEEP processing the remaining queued rows —
                // an invalid URL must still get `mark_error`. A `break`
                // here would leave it sitting in `queued` forever (it never
                // enqueues while budget stays 0, and every queued row defeats
                // the skip-tick gate — a qB fetch every tick, indefinitely).
                // `continue` mirrors the direct-row budget check above.
                continue;
            }

            match q.add_torrent(url, &p.category, &p.save_path).await {
                Ok(added) => {
                    // Any Ok response from qB add_torrent is success (B5).
                    // Do NOT update the name column — keep the user-provided display name.
                    // The `AND status = 'queued'` guard makes the flip atomic
                    // (a cancel landing during the add_torrent round-trip keeps
                    // its terminal state).
                    let claimed =
                        crate::db::downloads_lifecycle::mark_downloading(&self.db, id).await?;
                    if claimed == 0 {
                        // The row changed state (a cancel won the race) — do NOT
                        // mark error (the row is cancelled or otherwise
                        // terminal) and do not consume budget. The torrent is
                        // already in qB: remove it, or the next reconcile
                        // adopts it as a fresh row.
                        tracing::info!(
                            download_id = id,
                            "row no longer queued after add_torrent (cancel won the race) — \
                            removing just-added torrent"
                        );
                        self.remove_just_added_torrent(q, &added, &p.category).await;
                        continue;
                    }
                    budget -= 1;
                }
                Err(e) => {
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
        Ok(())
    }

    /// Compensating removal after a
    /// [`mark_downloading`](crate::db::downloads_lifecycle::mark_downloading)
    /// guard miss in the enqueue path: the torrent was just added to qB but
    /// the row is no longer `queued` (a cancel won the race). `add_torrent`
    /// returns the qB torrent name (not a hash), so re-fetch the list and
    /// delete the first torrent [`is_just_added_candidate`] accepts — the
    /// returned name **and** the category this add assigned
    /// (`enqueue_category`) — so a pre-existing torrent that merely shares
    /// the name is never deleted. Best-effort: a failed lookup/removal is
    /// logged; a leftover is at worst re-adopted by the next reconcile.
    async fn remove_just_added_torrent(
        &self,
        q: &Arc<QbitClient>,
        added_name: &str,
        enqueue_category: &str,
    ) {
        let torrents = match q.get_torrents("all").await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "compensating removal: torrent list fetch failed: {e} (leftover may be \
                    re-adopted on next reconcile)"
                );
                return;
            }
        };
        match torrents
            .iter()
            .find(|t| is_just_added_candidate(t, added_name, enqueue_category))
        {
            Some(t) => match q.delete(&t.hash, true).await {
                Ok(()) => {
                    tracing::info!(
                        "removed just-added torrent {} from qBittorrent (row no longer queued)",
                        t.hash
                    )
                }
                Err(e) => {
                    tracing::warn!("compensating removal failed for torrent {}: {e}", t.hash)
                }
            },
            None => tracing::warn!(
                "compensating removal: no torrent matching {added_name:?} (category \
                {enqueue_category:?}) in qBittorrent (already gone?)"
            ),
        }
    }
}

/// Decide whether a torrent in the re-fetched qB list is the one the enqueue
/// loop just added (used by `remove_just_added_torrent`). Requires ALL of:
/// - **name**: case-insensitive match against the name `add_torrent`
///   returned (the same convention as the in-flight name fallback);
/// - **category**: the enqueue call always assigns the configured category,
///   so a candidate's `Some` category must equal `enqueue_category`
///   case-insensitively, and a `None` category is rejected whenever
///   `enqueue_category` is non-empty (our adds always carry the category).
///   An empty `enqueue_category` (operator left `torrent.category` unset)
///   falls back to the name-only match — a `Some` category still has to
///   equal it, so a pre-existing categorized same-named torrent is kept.
///
/// Pure by design (no qB/DB access) so the matching rules are unit-testable.
/// Note: `TorrentInfo` exposes no addition-date field, so no recency bound
/// is possible — the name+category match is the full candidate test.
fn is_just_added_candidate(
    t: &super::TorrentInfo,
    wanted_name: &str,
    enqueue_category: &str,
) -> bool {
    if t.name.to_lowercase() != wanted_name.to_lowercase() {
        return false;
    }
    match t.category.as_deref() {
        Some(cat) => cat.to_lowercase() == enqueue_category.to_lowercase(),
        None => enqueue_category.is_empty(),
    }
}

/// Decision for a seeding row based on the live qBittorrent state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeedingAction {
    /// Refresh ratio / up_speed_bps / num_seeds / speed_bps from the live torrent.
    Refresh,
    /// Torrent left qBittorrent after grace period — settle to complete.
    Done,
    /// qBittorrent reports a fatal state — settle to complete with error noted.
    FatalDone,
    /// Torrent missing but within grace period (or qB unreachable) — wait.
    Pending,
}

/// Compute the action for a seeding row given the torrent lookup result and whether
/// the missing-torrent grace period has expired.
pub(crate) fn seeding_row_action(
    t: Option<&super::TorrentInfo>,
    grace_expired: bool,
) -> SeedingAction {
    match t {
        Some(t) if t.is_fatal() => SeedingAction::FatalDone,
        Some(_) => SeedingAction::Refresh,
        None if grace_expired => SeedingAction::Done,
        None => SeedingAction::Pending,
    }
}

// The download lifecycle state machine (the `COMPLETION_GUARD_STATUSES` set
// and the shared row helpers `status_if_changed` / `status_checked` /
// `mark_error` / `status_no_longer_downloading`) lives in
// `crate::db::downloads_lifecycle` (ARCH M-2/M-3) and is re-exported at the
// top of this module.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
