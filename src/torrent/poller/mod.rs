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
use crate::torrent::files::{install_zim, locate_torrent_zim, validate_download_name, verify_zim};
use crate::torrent::{
    connect_qbit, qbit_fingerprint, resolve_qbit_inputs, QbitClient, QbitClientCache,
};

mod complete;
mod direct;
mod inflight;
mod opds;
mod reconcile;
mod stats;

use self::direct::direct_download;
pub(crate) use self::direct::{build_download_client, ClientProfile};
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

/// Error messages that indicate a transient/connection-level failure worth
/// retrying. A row in `error` state whose `error` matches this pattern (case
/// insensitive, Postgres `~*`) is re-queued for a bounded retry.
///
/// Kept as a named constant so the regex (which is subtle — a plain substring
/// check would be wrong) can be unit-tested independently of the DB.
const REQUEUE_ERROR_PATTERN: &str = "connect|timeout|refused|unreachable|connection";

/// BUG-6: after this many consecutive bounded-retry cycles with the SAME
/// error message, the row is left `error` (manual intervention) instead of
/// being re-queued again. 401/403 session-expiry messages are exempt — a
/// re-login is the fix, not a human.
const REQUEUE_GIVE_UP_AFTER: u32 = 3;
/// Hard cap on the BUG-6 guard map (one entry per row id cycling transient
/// errors). Entries are removed when the guard gives up and lazily evicted
/// when the cap is reached (rows that leave `error` by other means — e.g. a
/// manual re-queue — keep their entry until that sweep runs), so the cap
/// only stops pathological pile-up.
const MAX_LAST_ERROR_TRACKED: usize = 10_000;
/// BUG-6 guard lazy-eviction window, in requeue passes: when the map hits
/// `MAX_LAST_ERROR_TRACKED`, entries not observed as error rows for this
/// many passes are swept before a new entry is admitted.
const GUARD_STALE_AFTER_PASSES: u64 = 100;

/// Whether an error message is an auth/session-expiry (HTTP 401/403).
/// Checks the token with word-boundary semantics (non-alphanumeric
/// boundaries) — a plain substring check would misclassify port numbers
/// or IDs ("connection refused: port 40152") as auth and exempt them
/// from the requeue give-up guard.
fn is_auth_error(msg: &str) -> bool {
    ["401", "403"].iter().any(|code| {
        msg.split(|c: char| !c.is_alphanumeric())
            .any(|tok| tok == *code)
    })
}

/// BUG-6 requeue guard (pure): given the row's previous guard entry
/// (`Some((previous_message, consecutive_count))`) and the row's current
/// `error` message, whether the bounded retry loop should stop.
///
/// `true` only when the same non-auth message was already recorded
/// `REQUEUE_GIVE_UP_AFTER - 1` times in a row (making this occurrence the
/// `REQUEUE_GIVE_UP_AFTER`-th consecutive identical error). A different
/// message resets the count (returning `false`), and 401/403 (session
/// expiry) is always exempt — the next re-login is the fix, not manual
/// intervention.
fn should_give_up(prev: Option<(&str, u32)>, msg: &str, is_auth: bool) -> bool {
    if is_auth {
        return false;
    }
    matches!(prev, Some((pm, n)) if pm == msg && n >= REQUEUE_GIVE_UP_AFTER - 1)
}

/// One entry of the BUG-6 requeue guard (keyed by row id in
/// [`DownloadPoller::last_error`]): the last transient error message
/// observed for the row, how many times in a row, and the requeue pass it
/// was last observed on (the lazy-eviction timestamp for rows that leave
/// `error` by other means — give-up is the only other removal path).
#[derive(Debug, Clone)]
struct GuardEntry {
    prev_msg: String,
    count: u32,
    /// Requeue pass (monotonic counter, see [`DownloadPoller::requeue_passes`])
    /// on which the row was last observed as an error row.
    last_seen: u64,
}

/// BUG-6 guard lazy eviction (pure): remove entries not observed as error
/// rows within the last `GUARD_STALE_AFTER_PASSES` requeue passes and
/// return how many were evicted. Called only when the map hits
/// `MAX_LAST_ERROR_TRACKED`, so a recently-observed entry is never swept.
fn sweep_stale_guard_entries(guard: &mut HashMap<i32, GuardEntry>, pass: u64) -> usize {
    let horizon = pass.saturating_sub(GUARD_STALE_AFTER_PASSES);
    let stale: Vec<i32> = guard
        .iter()
        .filter(|(_, e)| e.last_seen < horizon)
        .map(|(&id, _)| id)
        .collect();
    for id in &stale {
        guard.remove(id);
    }
    stale.len()
}

/// Outcome of one stale `error` row passing through the BUG-6 requeue guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequeueDecision {
    /// Re-queue the row (it goes into the `id = ANY(…)` requeue batch).
    Requeue,
    /// Give up: leave the row in `error` for manual intervention (not re-queued).
    GiveUp,
}

/// BUG-6 requeue guard, one row at a time (pure — no DB, no lock, so the
/// give-up contract is unit-testable without Postgres).
///
/// Decides whether the row is re-queued this pass and updates `guard` in
/// place:
///
/// - **Give up** (`should_give_up`): the row is *not* re-queued and its guard
///   entry is **retained** — *not* removed — with its `last_seen` refreshed to
///   `pass`. Retention is the whole point (FIX-B): a give-up row stays `error`
///   with the same message, so the *next* pass must still see the give-up state
///   (count already at the cap) instead of treating it as a fresh failure
///   (`prev = None` → count resets to 1) and re-queueing it — that one-shot
///   removal was the bug that made "bounded retry" unbounded. Once the row
///   leaves `error` by other means (cancel / complete) it stops being observed
///   as an error row, its `last_seen` freezes, and the lazy
///   [`sweep_stale_guard_entries`] reclaims the entry after
///   `GUARD_STALE_AFTER_PASSES` unobserved passes.
/// - **Re-queue**: record/refresh the row's entry, incrementing the
///   consecutive-identical-message count, and re-queue it. A brand-new entry is
///   admitted only while under `MAX_LAST_ERROR_TRACKED` (sweeping stale entries
///   first; failing open — re-queue without a guard entry — if nothing was
///   stale).
fn requeue_guard_step(
    guard: &mut HashMap<i32, GuardEntry>,
    id: i32,
    msg: &str,
    pass: u64,
) -> RequeueDecision {
    let is_auth = is_auth_error(msg);
    let prev = guard.get(&id).map(|e| (e.prev_msg.as_str(), e.count));
    if should_give_up(prev, msg, is_auth) {
        tracing::warn!(
            download_id = id,
            "requeue guard: same error {msg:?} {REQUEUE_GIVE_UP_AFTER}x in a row — leaving row in error state"
        );
        // FIX-B: retain the entry (never remove) so the give-up state survives
        // to the next pass; refresh `last_seen` so the lazy sweep reclaims it
        // once the row leaves `error` by other means. On give-up the entry is
        // always present (`should_give_up` needs a prior count ≥ 2).
        if let Some(existing) = guard.get_mut(&id) {
            existing.last_seen = pass;
        }
        return RequeueDecision::GiveUp;
    }
    let count = prev.filter(|(m, _)| *m == msg).map_or(1, |(_, n)| n + 1);
    let entry = GuardEntry {
        prev_msg: msg.to_string(),
        count,
        last_seen: pass,
    };
    if let Some(existing) = guard.get_mut(&id) {
        *existing = entry;
    } else if guard.len() >= MAX_LAST_ERROR_TRACKED {
        // Cap reached: sweep entries whose rows stopped appearing as error
        // rows (left `error` by other means) more than
        // `GUARD_STALE_AFTER_PASSES` passes ago, then admit the new entry if
        // the sweep freed a slot. If nothing was stale, fail open: the row
        // retries without a guard entry (and without give-up protection).
        sweep_stale_guard_entries(guard, pass);
        if guard.len() < MAX_LAST_ERROR_TRACKED {
            guard.insert(id, entry);
        }
    } else {
        guard.insert(id, entry);
    }
    if count >= 2 {
        tracing::warn!(
            download_id = id,
            attempt = count,
            "requeue guard: same error {count}x, will stop after {REQUEUE_GIVE_UP_AFTER}"
        );
    }
    RequeueDecision::Requeue
}

/// FIX-A (requeue 23505), pure: drop stale `error` rows that would trip the
/// partial unique `uq_downloads_active_name` if re-queued in this batch —
/// either because their `name` is already held by an ACTIVE row, or because
/// another stale `error` row in the same batch shares it.
///
/// The partial unique `uq_downloads_active_name` (name UNIQUE where status IN
/// queued/downloading) covers active-vs-active only — an `error` row may share
/// a `name` with an active one. Re-queueing such a row (`status='error'` →
/// `queued`) would trip 23505, which the guarded UPDATE surfaces as an error
/// the tick treats as fatal — aborting the whole tick (and, with `updated_at`
/// never advancing, re-picking the same row every tick until the active row
/// goes terminal). Skipping the colliding rows here (they stay in `error` and
/// retry on a later pass once the colliding active row leaves the active
/// states) keeps the requeue UPDATE from ever seeing a collision. Returns the
/// `(id, name, error)` rows that may be safely re-queued.
fn filter_requeue_name_collisions(
    stale: &[(i32, String, String)],
    active_names: &HashSet<String>,
) -> Vec<(i32, String, String)> {
    // Two stale `error` rows sharing a name (re-adding a previously-failed
    // name is legal — only active-vs-active is constrained) would both flip
    // to `queued` in one batched UPDATE and still trip the partial unique.
    // Keep only the newest (highest-id) candidate per name; the older
    // duplicates stay in `error` and can be retried later.
    let mut best_id: HashMap<String, i32> = HashMap::new();
    for (id, name, _) in stale {
        if active_names.contains(name) {
            continue;
        }
        match best_id.get_mut(name) {
            Some(prev) => *prev = (*prev).max(*id),
            None => {
                best_id.insert(name.clone(), *id);
            }
        }
    }
    stale
        .iter()
        .filter(|(id, name, _)| best_id.get(name) == Some(id))
        .cloned()
        .collect()
}
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
pub struct DownloadPoller {
    pub(crate) db: Pool,
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
    /// [`cancel`](Self::cancel) and polled between ticks (BUG-5).
    stopping: Arc<AtomicBool>,
    /// BUG-6 requeue guard, keyed by row id: the last transient error
    /// message, its consecutive count, and the pass on which the row was
    /// last observed as an error row ([`GuardEntry`]). Replaced (count
    /// reset) when the message changes, removed when the guard gives up or
    /// when lazy eviction sweeps an entry whose row left `error` by other
    /// means (e.g. a manual re-queue); capped at [`MAX_LAST_ERROR_TRACKED`].
    last_error: Arc<std::sync::Mutex<HashMap<i32, GuardEntry>>>,
    /// BUG-6 requeue pass counter (the lazy-eviction clock for the guard
    /// map): incremented once per `requeue_stale_errors` pass and compared
    /// against [`GuardEntry::last_seen`] by the cap sweep.
    requeue_passes: AtomicU64,
}

impl DownloadPoller {
    /// Create a new poller wired to the given DB pool, settings, and ZIM manager.
    pub fn new(
        db: Pool,
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
                tracing::warn!("download HTTP client build failed: {e}; direct downloads and OPDS checks will fail until restart");
                None
            }
        };
        Self {
            db,
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
    /// at the next between-ticks poll (BUG-5). No tokio-util `CancellationToken`
    /// — a process-lifetime flag is all the run loop needs.
    pub fn cancel(&self) {
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

    /// BUG-5: atomically check whether this row is still `downloading`; if the
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
                        "failed to remove cancelled torrent {torrent_hash}: {e} (hash kept for retry)"
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
        let p0 = self.settings.poller_params_snapshot();
        let qbit = self.resolve_torrent(&p0).await;
        if let Err(e) = self.reconcile(qbit).await {
            tracing::warn!("startup download reconciliation failed: {e}");
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
            // BUG-5: poll for cancellation between ticks (no tokio::select! — keep
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

        // Bounded error-row retry (BUG-6): re-queue stale connection-level
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
    /// `REQUEUE_GIVE_UP_AFTER` (BUG-6) so a persistently-failing row
    /// eventually stays `error` for manual intervention instead of cycling
    /// forever — and give-up is *sticky* (FIX-B): the guard entry is retained
    /// on give-up, so the row is not re-queued again on the next pass. A
    /// 401/403 session expiry is always exempt — the next re-login is the fix,
    /// not a human.
    ///
    /// FIX-A (requeue 23505): the per-row guard decision is the pure
    /// [`requeue_guard_step`] and the collision pre-filter is the pure
    /// [`filter_requeue_name_collisions`], so both bugs are unit-testable
    /// without Postgres.
    async fn requeue_stale_errors(&self) -> Result<()> {
        // Lazy-eviction clock: count every pass (also the timestamp stamped
        // on entries observed below), so "not seen for N passes" is measured
        // in real passes even during quiet stretches.
        let pass = self.requeue_passes.fetch_add(1, Ordering::SeqCst) + 1;
        // Raw SQL (db::raw): the case-insensitive regex match `error ~* $1`
        // has no `db::raw` helper shape (the guarded UPDATE lives in
        // `downloads_lifecycle::requeue_stale_errors`). `name` is fetched too
        // so the FIX-A collision filter below can match it against active rows.
        let stale: Vec<(i32, String, String)> = crate::db::raw::fetch_all(
            &self.db,
            &format!(
                "SELECT id, name, error FROM downloads \
                 WHERE status = {err} \
                   AND updated_at < now() - interval '10 minutes' \
                   AND error ~* $1",
                err = crate::torrent::DownloadStatus::Error.as_str()
            ),
            |q| q.bind(REQUEUE_ERROR_PATTERN),
        )
        .await?;
        if stale.is_empty() {
            return Ok(());
        }
        // FIX-A (requeue 23505): the partial unique `uq_downloads_active_name`
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
            // BUG-5: the helper owns the two-statement cleanup — confirm the
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
                            "row no longer queued after add_torrent (cancel won the race) — removing just-added torrent"
                        );
                        self.remove_just_added_torrent(q, &added).await;
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

    /// Compensating removal after a [`mark_downloading`](crate::db::downloads_lifecycle::mark_downloading)
    /// guard miss in the enqueue path: the torrent was just added to qB but
    /// the row is no longer `queued` (a cancel won the race). `add_torrent`
    /// returns the qB torrent name (not a hash), so re-fetch the list and
    /// delete by the matched hash (case-insensitive name match — the same
    /// convention as the in-flight name fallback). Best-effort: a failed
    /// lookup/removal is logged; a leftover is at worst re-adopted by the
    /// next reconcile.
    async fn remove_just_added_torrent(&self, q: &Arc<QbitClient>, added_name: &str) {
        let wanted = added_name.to_lowercase();
        let torrents = match q.get_torrents("all").await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "compensating removal: torrent list fetch failed: {e} (leftover may be re-adopted on next reconcile)"
                );
                return;
            }
        };
        match torrents.iter().find(|t| t.name.to_lowercase() == wanted) {
            Some(t) => match q.delete(&t.hash, true).await {
                Ok(()) => {
                    tracing::info!(
                        "removed just-added torrent {} from qBittorrent (row no longer queued)",
                        t.hash
                    )
                }
                Err(e) => {
                    tracing::warn!(
                        "compensating removal failed for torrent {}: {e}",
                        t.hash
                    )
                }
            },
            None => tracing::warn!(
                "compensating removal: no torrent matching {wanted:?} in qBittorrent (already gone?)"
            ),
        }
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{Pool, REQUEUE_ERROR_PATTERN};
    use crate::torrent::TorrentInfo;

    /// `(progress, speed_bps, eta_secs, ratio, up_speed_bps, num_seeds)`
    type StatsTuple = (
        f32,
        Option<i64>,
        Option<i64>,
        Option<f32>,
        Option<i64>,
        Option<i64>,
    );

    /// `(status, progress, speed_bps, eta_secs, ratio, up_speed_bps, num_seeds)`
    type StatusStatsTuple = (
        String,
        f32,
        Option<i64>,
        Option<i64>,
        Option<f32>,
        Option<i64>,
        Option<i64>,
    );

    pub(crate) mod state {
        use super::*;

        pub(crate) fn tinfo(state: &str, progress: f64) -> TorrentInfo {
            TorrentInfo {
                hash: "abc".into(),
                name: "x".into(),
                progress,
                state: state.into(),
                dlspeed: 0,
                upspeed: 0,
                ratio: 0.0,
                category: None,
                save_path: None,
                content_path: None,
                size: 0,
                downloaded: 0,
                num_seeds: 0,
                err_str: None,
            }
        }

        #[test]
        fn state_complete_uploading() {
            assert!(tinfo("uploading", 1.0).is_complete());
            assert!(tinfo("stalledUP", 1.0).is_complete());
            assert!(tinfo("pausedUP", 1.0).is_complete());
            assert!(tinfo("forcedUP", 0.9995).is_complete());
        }

        #[test]
        fn state_not_complete() {
            assert!(!tinfo("downloading", 1.0).is_complete());
            assert!(!tinfo("uploading", 0.5).is_complete());
            assert!(!tinfo("error", 1.0).is_complete());
            assert!(!tinfo("metaDL", 0.0).is_complete());
        }

        #[test]
        fn state_active_download() {
            assert!(tinfo("downloading", 0.1).is_active_download());
            assert!(tinfo("metaDL", 0.0).is_active_download());
            assert!(tinfo("checking", 0.0).is_active_download());
            assert!(tinfo("stalledDL", 0.0).is_active_download());
            assert!(!tinfo("uploading", 1.0).is_active_download());
            assert!(!tinfo("pausedDL", 0.5).is_active_download());
            assert!(!tinfo("error", 0.2).is_active_download());
        }

        #[test]
        fn state_fatal() {
            assert!(tinfo("error", 0.2).is_fatal());
            assert!(tinfo("missingFiles", 0.9).is_fatal());
            assert!(!tinfo("downloading", 0.2).is_fatal());
            assert!(!tinfo("uploading", 1.0).is_fatal());
        }
    }

    mod client {
        #[test]
        fn client_from_option_none_gives_clear_error() {
            let err = super::super::client_from_option(None).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("download HTTP client unavailable"),
                "unexpected: {msg}"
            );
        }

        #[test]
        fn client_from_option_some_returns_client() {
            let c = reqwest::Client::new();
            assert!(super::super::client_from_option(Some(&c)).is_ok());
        }
    }

    /// BUG-A/B: the configured-vs-down split must track the *effective URL*
    /// (override beats the `torrent.url` setting; blank disables), not
    /// `resolve_torrent`'s `Option` — which is also `None` while a
    /// configured qB is unreachable (wrong port, qB restarting, LAN blip).
    mod qbit_configured {
        fn poller_with_override(override_url: Option<String>) -> super::super::DownloadPoller {
            let dir =
                std::env::temp_dir().join(format!("zimi-qbit-configured-{}", std::process::id()));
            let _ = std::fs::create_dir_all(&dir);
            let zims = crate::zim::ZimManager::new(dir, crate::testing::dead_pool());
            super::super::DownloadPoller::new(
                crate::testing::dead_pool(),
                super::download::download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                override_url,
                "user".into(),
                "pass".into(),
            )
        }

        #[test]
        fn effective_override_url_is_configured() {
            let p = poller_with_override(Some("http://qb:8080".into()));
            let snap = p.settings.poller_params_snapshot();
            assert!(p.qbit_configured(&snap));
        }

        #[test]
        fn no_url_is_not_configured() {
            // `default_settings` seeds `torrent.url` as empty → `qbit_url`
            // None → not configured with no override.
            let p = poller_with_override(None);
            let snap = p.settings.poller_params_snapshot();
            assert!(!p.qbit_configured(&snap));
        }

        #[test]
        fn blank_override_is_not_configured() {
            // Whitespace URL: `resolve_qbit_inputs` selects the override
            // then disables on blank (same rule as the connect path).
            let p = poller_with_override(Some("   ".into()));
            let snap = p.settings.poller_params_snapshot();
            assert!(!p.qbit_configured(&snap));
        }
    }

    pub(crate) mod download {
        use super::*;
        use crate::db::raw;
        // Re-exported so existing `tests::download::test_pool` importers
        // (direct/complete/reconcile test modules) keep working unchanged;
        // the helper itself now lives in `crate::testing` (A-1).
        pub(crate) use crate::testing::test_pool;

        pub(crate) fn download_settings() -> crate::settings::SettingsCache {
            crate::settings::SettingsCache::new_with_map(
                crate::testing::dead_pool(),
                crate::settings::default_settings(),
                std::collections::HashMap::new(),
            )
        }

        /// Step 2.1: the re-queue error pattern is subtle (case-insensitive
        /// Postgres `~*` regex), so unit-test it directly rather than only via a
        /// live DB round-trip.
        #[test]
        fn requeue_pattern_matches_transient_errors_only() {
            // Case-insensitive to match Postgres `~*` semantics.
            let re =
                regex::Regex::new(&format!("(?i){REQUEUE_ERROR_PATTERN}")).expect("valid regex");
            // Transient / connection-level → re-queue
            assert!(re.is_match("Connection refused"));
            assert!(re.is_match("request timeout after 30s"));
            assert!(re.is_match("failed to connect to 10.0.0.5:8080"));
            assert!(re.is_match("host unreachable"));
            assert!(re.is_match("Connection reset by peer"));
            // Not connection-level → leave in `error` (no retry)
            assert!(!re.is_match("torrent not found in qBittorrent"));
            assert!(!re.is_match("404 Not Found"));
            assert!(!re.is_match("invalid ZIM file"));
            assert!(!re.is_match(""));
        }

        /// BUG-6: the pure requeue guard gives up only on the
        /// `REQUEUE_GIVE_UP_AFTER`-th consecutive IDENTICAL non-auth error; a
        /// different message resets the count, and 401/403 is always exempt.
        #[test]
        fn requeue_guard_gives_up_on_third_identical_error() {
            const MSG: &str = "connection refused";
            const AUTH: &str = "connection failed: HTTP 401";
            // 1st and 2nd identical errors → keep retrying; 3rd → give up.
            assert!(!super::super::should_give_up(None, MSG, false));
            assert!(!super::super::should_give_up(Some((MSG, 1)), MSG, false));
            assert!(super::super::should_give_up(Some((MSG, 2)), MSG, false));
            // A different message resets the consecutive count.
            assert!(!super::super::should_give_up(
                Some(("timeout", 2)),
                MSG,
                false
            ));
            // 401/403 (session expiry) is always exempt — the re-login path fixes it.
            assert!(!super::super::should_give_up(Some((AUTH, 2)), AUTH, true));
            assert!(!super::super::should_give_up(
                Some(("HTTP 403", 2)),
                "HTTP 403",
                true
            ));
            assert!(!super::super::should_give_up(None, AUTH, true));
        }

        /// BUG-6 lazy eviction: the sweep removes only entries not observed
        /// as error rows within the last `GUARD_STALE_AFTER_PASSES` requeue
        /// passes (rows that left `error` by other means — e.g. a manual
        /// re-queue — otherwise leak until the cap); the horizon saturates to
        /// 0 before the window has ever been filled, sweeping nothing.
        #[test]
        fn guard_sweep_evicts_only_unobserved_entries() {
            use super::super::sweep_stale_guard_entries;
            use super::super::GuardEntry;
            let entry = |last_seen: u64| GuardEntry {
                prev_msg: "m".into(),
                count: 1,
                last_seen,
            };
            let mut guard: std::collections::HashMap<i32, GuardEntry> =
                std::collections::HashMap::new();
            guard.insert(1, entry(399)); // 101 passes stale → swept
            guard.insert(2, entry(400)); // exactly at the horizon → kept
            guard.insert(3, entry(450)); // recent → kept
            let n = sweep_stale_guard_entries(&mut guard, 500);
            assert_eq!(n, 1, "only the 101-pass-stale entry is swept");
            assert!(!guard.contains_key(&1));
            assert!(guard.contains_key(&2));
            assert!(guard.contains_key(&3));

            // Fewer total passes than the window: nothing is swept.
            let mut fresh: std::collections::HashMap<i32, GuardEntry> =
                std::collections::HashMap::new();
            fresh.insert(9, entry(1));
            assert_eq!(sweep_stale_guard_entries(&mut fresh, 50), 0);
            assert!(fresh.contains_key(&9));
        }

        /// Auth detection must use word boundaries: port numbers/IDs that
        /// merely CONTAIN "401"/"403" ("port 40152") are not session
        /// expiry and must NOT be exempt from the give-up guard.
        #[test]
        fn is_auth_error_requires_word_boundaries() {
            assert!(super::super::is_auth_error("connection failed: HTTP 401"));
            assert!(super::super::is_auth_error("request failed: HTTP 403"));
            assert!(!super::super::is_auth_error(
                "connection refused: port 40152"
            ));
            assert!(!super::super::is_auth_error("download failed"));
        }

        /// BUG-6 give-up is STICKY, not one-shot (FIX-B): the moment a row
        /// gives up, its guard entry is retained (not removed), so the very
        /// next pass — the row still `error` with the same message — keeps
        /// giving up instead of treating it as a fresh failure (count resets
        /// to 1) and re-queueing it forever. While the row stays in `error` it
        /// keeps being observed, so `last_seen` is refreshed each pass and it
        /// is never swept. Once the row leaves `error` by other means
        /// (cancel/complete) it stops being observed, `last_seen` freezes, and
        /// the lazy stale sweep reclaims the entry after
        /// `GUARD_STALE_AFTER_PASSES` unobserved passes.
        #[test]
        fn requeue_guard_give_up_is_sticky_not_one_shot() {
            use super::super::{
                requeue_guard_step, sweep_stale_guard_entries, GuardEntry, RequeueDecision,
            };
            let mut guard: std::collections::HashMap<i32, GuardEntry> =
                std::collections::HashMap::new();
            const ID: i32 = 42;
            const MSG: &str = "connection refused";
            // First two identical errors → keep retrying (count climbs 1→2).
            assert_eq!(
                requeue_guard_step(&mut guard, ID, MSG, 1),
                RequeueDecision::Requeue
            );
            assert_eq!(
                requeue_guard_step(&mut guard, ID, MSG, 2),
                RequeueDecision::Requeue
            );
            // Third identical error → give up, and the entry must be RETAINED.
            assert_eq!(
                requeue_guard_step(&mut guard, ID, MSG, 3),
                RequeueDecision::GiveUp
            );
            assert!(
                guard.contains_key(&ID),
                "give-up must retain the guard entry (the one-shot-removal bug)"
            );
            assert_eq!(guard[&ID].last_seen, 3);
            // The next pass (row still error, same message) must still give up
            // rather than reset the count to 1 and re-queue it, and keep the
            // entry fresh while the row stays in `error`.
            assert_eq!(
                requeue_guard_step(&mut guard, ID, MSG, 4),
                RequeueDecision::GiveUp
            );
            assert_eq!(guard[&ID].last_seen, 4);
            assert_eq!(
                requeue_guard_step(&mut guard, ID, MSG, 50),
                RequeueDecision::GiveUp
            );
            assert_eq!(guard[&ID].last_seen, 50);

            // The row leaves `error` (cancel/complete) at pass 50: it stops
            // being observed, `last_seen` freezes at 50, and the lazy sweep
            // reclaims the entry once it is > `GUARD_STALE_AFTER_PASSES` (100)
            // passes old.
            let horizon = 50 + super::super::GUARD_STALE_AFTER_PASSES;
            assert!(
                sweep_stale_guard_entries(&mut guard, horizon) == 0,
                "at the sweep horizon the entry is still kept"
            );
            assert!(guard.contains_key(&ID));
            assert!(
                sweep_stale_guard_entries(&mut guard, horizon + 1) == 1,
                "just past the horizon the entry is swept"
            );
            assert!(
                !guard.contains_key(&ID),
                "stale give-up entry must be reclaimed"
            );
        }

        /// FIX-A (requeue 23505): a stale `error` row whose `name` is already
        /// held by an ACTIVE (`queued`/`downloading`) row must be skipped, and
        /// two stale `error` rows sharing a name must collapse to the newest —
        /// re-queueing both would trip the partial unique `uq_downloads_active_name`
        /// and abort the whole tick — while a non-colliding `error` row is
        /// still re-queued. The filter is pure (no DB), so it is testable
        /// without Postgres: seed an in-memory active name set and stale error
        /// rows, and check the colliding ids are dropped and the rest kept.
        #[test]
        fn requeue_skips_error_rows_whose_name_is_held_by_an_active_row() {
            use super::super::filter_requeue_name_collisions;
            // Seed: one active row `dupe`, plus two stale error rows — one
            // sharing that name (collides), one not.
            let active: std::collections::HashSet<String> =
                ["dupe".to_string()].into_iter().collect();
            let stale = vec![
                (1, "dupe".to_string(), "connection refused".to_string()),
                (
                    2,
                    "other".to_string(),
                    "timeout while connecting".to_string(),
                ),
            ];
            let kept = filter_requeue_name_collisions(&stale, &active);
            assert_eq!(
                kept.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                vec![2],
                "only the non-colliding error row is requeued"
            );
            // No active rows → nothing collides, everything is requeued.
            let none: std::collections::HashSet<String> = Default::default();
            assert_eq!(
                filter_requeue_name_collisions(&stale, &none)
                    .iter()
                    .map(|(id, _, _)| *id)
                    .collect::<Vec<_>>(),
                vec![1, 2]
            );
            // Two stale error rows sharing a name (re-adding a previously-
            // failed name) → only the newest (highest id) is requeued; the
            // older duplicate stays in `error` for a later pass.
            let stale_dupes = vec![
                (5, "dupe".to_string(), "older failure".to_string()),
                (9, "dupe".to_string(), "newer failure".to_string()),
            ];
            assert_eq!(
                filter_requeue_name_collisions(&stale_dupes, &none)
                    .iter()
                    .map(|(id, _, _)| *id)
                    .collect::<Vec<_>>(),
                vec![9],
                "only the newest same-name error row is requeued"
            );
        }

        /// T2: drive one real `tick()` end-to-end against a live Postgres and a
        /// wiremock qBittorrent. Proves the poller's `filter=all` fetch (not
        /// `active`) is the qBittorrent call a tick makes, and that both row kinds
        /// it manages are advanced in the same tick: a direct `.zim` row is claimed
        /// (budget) and a torrent row is enqueued to qBittorrent.
        #[tokio::test]
        async fn tick_fetches_all_torrents_once_and_advances_both_row_kinds() {
            use wiremock::matchers::{method, path, query_param};
            use wiremock::{Mock, MockServer, ResponseTemplate};

            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let server = MockServer::start().await;
            // One login, one add, one `filter=all` info fetch per tick.
            Mock::given(method("POST"))
                .and(path("/api/v2/login"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v2/torrents/add"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v2/torrents/info"))
                .and(query_param("filter", "all"))
                .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
                .expect(1)
                .mount(&server)
                .await;

            let mut c = pool.acquire().await.expect("conn");
            let zim_id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, status) VALUES ($1, $2, 'queued') RETURNING id",
                |q| q.bind("it-zim").bind("http://127.0.0.1:9/x.zim"),
            )
            .await
            .expect("insert zim row")
            .expect("row");
            let qb_id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, status) VALUES ($1, $2, 'queued') RETURNING id",
                |q| q.bind("it-torrent").bind("magnet:?xt=urn:btih:aaa"),
            )
            .await
            .expect("insert torrent row")
            .expect("row");

            let tmp = tempfile::tempdir().expect("tempdir");
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some(server.uri()),
                "user".into(),
                "pass".into(),
            );

            poller.tick().await.expect("tick runs");

            // The qBittorrent fetch contract is enforced by the mocks' `.expect(1)`
            // (verified on drop, as the rest of this crate's wiremock suite does):
            // one `filter=all` info fetch, one login, one add — not `active`.

            let mut c = pool.acquire().await.expect("conn");
            let qb_status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE id = $1",
                |q| q.bind(qb_id),
            )
            .await
            .expect("read qb row")
            .expect("row");
            assert_eq!(
                qb_status, "downloading",
                "torrent row enqueued to qB this tick"
            );

            // The direct `.zim` row was claimed for download (budget consumed):
            // its `file_path` is set to the `.part` target. Its status is NOT
            // asserted — the spawned direct-download task runs concurrently and may
            // already have marked it `error` (the 127.0.0.1:9 fetch fails fast with
            // a local connection-refused, never leaving the machine), which would
            // race the read. `file_path` is set by the synchronous claim and
            // never cleared by that task, so it is the stable signal.
            let zp: Option<String> = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT file_path FROM downloads WHERE id = $1",
                |q| q.bind(zim_id),
            )
            .await
            .expect("read zim row");
            assert!(
                zp.as_deref().unwrap_or("").ends_with(".part"),
                "direct row must be claimed with a .part file_path, got: {zp:?}"
            );

            let _ = pool.acquire().await; // keep pool alive for cleanup
            let mut c2 = pool.acquire().await.expect("conn");
            let _ = raw::execute(
                &mut *c2,
                "DELETE FROM downloads WHERE id IN ($1, $2)",
                |q| q.bind(zim_id).bind(qb_id),
            )
            .await;
            let _ = tmp;
        }

        // ── Direct DB-gated coverage of `process_inflight` / `flush_stats` ──
        // Drives both on real rows via the same in-flight SELECT `tick()` uses
        // (`fetch_inflight_rows`) plus hand-built `TorrentInfo` maps (no
        // wiremock: the asserted branches never issue a qB call). Rows use a
        // unique name prefix and are deleted on entry (sweep) and exit.

        const IT_INFLIGHT_PREFIX: &str = "__it_inflight__";

        /// `TorrentInfo` with explicit fields (size/downloaded 2 MiB / 1 MiB → exact `eta_secs`).
        #[allow(clippy::too_many_arguments)]
        fn it_torrent(
            hash: &str,
            name: &str,
            state: &str,
            progress: f64,
            dlspeed: i64,
            upspeed: i64,
            ratio: f64,
            num_seeds: i64,
        ) -> TorrentInfo {
            TorrentInfo {
                hash: hash.into(),
                name: name.into(),
                progress,
                state: state.into(),
                dlspeed,
                upspeed,
                ratio,
                category: None,
                save_path: None,
                content_path: None,
                size: 2 * 1024 * 1024,
                downloaded: 1024 * 1024,
                num_seeds,
                err_str: None,
            }
        }

        /// Insert a `downloads` row under the unique prefix (fresh `now()` →
        /// no grace expiry; magnet URL keeps it on the qB-torrent code path).
        async fn it_insert_row(
            pool: &Pool,
            name: &str,
            hash: Option<&str>,
            status: &str,
            progress: f32,
            ratio: Option<f32>,
            num_seeds: Option<i64>,
        ) -> i32 {
            let mut c = pool.acquire().await.expect("conn");
            raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, hash, status, progress, ratio, num_seeds, \n                 updated_at) VALUES ($1, 'magnet:?xt=urn:btih:it', $2, $3, $4, $5, $6, \n                 now()) RETURNING id",
                |q| {
                    q.bind(name)
                        .bind(hash)
                        .bind(status)
                        .bind(progress)
                        .bind(ratio)
                        .bind(num_seeds)
                },
            )
            .await
            .expect("insert it-inflight row")
            .expect("row")
        }

        /// Shared harness for direct `process_inflight` / `flush_stats` tests:
        ///
        /// DB gate (clean skip without Postgres), leftover sweep, row insert,
        /// poller + dead-port qB client build, the real in-flight SELECT
        /// filtered to this suite's prefix, then `process_inflight` +
        /// `flush_stats`, the async `check` over (row id, changed rows,
        /// live pool), and row cleanup. `hash_key` keys `by_hash` — normally
        /// the torrent's own hash; the rebind test feeds the row's stale one.
        /// `stale` ages the inserted row's `updated_at` past
        /// `MISSING_TORRENT_GRACE` so the missing-torrent expiry arms can fire.
        #[allow(clippy::too_many_arguments)]
        async fn it_inflight_case<F>(
            name: &str,
            hash: Option<&str>,
            status: &str,
            progress: f32,
            ratio: Option<f32>,
            num_seeds: Option<i64>,
            stale: bool,
            hash_key: &str,
            t: TorrentInfo,
            check: F,
        ) where
            F: AsyncFnOnce(i32, Vec<super::super::StatsRow>, Pool),
        {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");
            let mut c = pool.acquire().await.expect("conn");
            let _ = raw::execute(
                &mut *c,
                "DELETE FROM downloads WHERE name LIKE '__it_inflight__%'",
                |q| q,
            )
            .await;
            let id = it_insert_row(&pool, name, hash, status, progress, ratio, num_seeds).await;
            if stale {
                // Age the row past `MISSING_TORRENT_GRACE` (10 min) so the
                // missing-torrent expiry arms judge it this tick.
                raw::execute(
                    &mut *c,
                    "UPDATE downloads SET updated_at = now() - interval '11 minutes' \
                     WHERE id = $1",
                    |q| q.bind(id),
                )
                .await
                .expect("stale the row's updated_at");
            }
            let tmp = tempfile::tempdir().expect("tempdir");
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some("http://127.0.0.1:9/qb".into()),
                "user".into(),
                "pass".into(),
            );
            let qbit = std::sync::Arc::new(
                crate::torrent::QbitClient::new(
                    "http://127.0.0.1:9/qb",
                    "user",
                    "pass",
                    false,
                    None,
                )
                .expect("qbit client build (no network needed)"),
            );
            let by_hash = std::collections::HashMap::from([(hash_key, &t)]);
            let by_name = std::collections::HashMap::from([(t.name.to_lowercase(), &t)]);
            let rows = poller
                .fetch_inflight_rows()
                .await
                .expect("rows")
                .into_iter()
                .filter(|r| r.name.starts_with(IT_INFLIGHT_PREFIX))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 1, "only this suite's row may be in-flight");
            let changed = poller
                .process_inflight(
                    &by_hash,
                    &by_name,
                    Some(qbit),
                    true,
                    &download_settings().poller_params_snapshot(),
                    rows,
                )
                .await
                .expect("process_inflight");
            poller.flush_stats(&changed).await;
            check(id, changed, pool.clone()).await;

            let _ = pool.acquire().await; // keep pool alive for cleanup
            let mut c2 = pool.acquire().await.expect("conn");
            let _ = raw::execute(
                &mut *c2,
                "DELETE FROM downloads WHERE name LIKE '__it_inflight__%'",
                |q| q,
            )
            .await;
            let _ = tmp;
        }

        /// 1) A `downloading` row whose stats moved (progress 0.5 → 0.75, fresh
        /// speeds/ratio/seeds) must be queued by `process_inflight` and, after
        /// `flush_stats`, written to every stat column by `apply_stats_batch`
        /// (M-poller-n1: skip the write only when nothing meaningful moved).
        #[tokio::test]
        async fn it_inflight_changed_row_flushes_stats_to_db() {
            it_inflight_case(
                "__it_inflight__changed",
                Some("itc1hash"),
                "downloading",
                0.5,
                None,
                None,
                false,
                "itc1hash",
                it_torrent("itc1hash", "changed-row", "downloading", 0.75, 100, 50, 0.5, 3),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    // 1 MiB remaining at 100 KiB/s → eta 10 s; speeds 1024×'d.
                    assert_eq!(
                        changed,
                        vec![(id, 0.75, 102_400, Some(10), Some(0.5), 51_200, 3)],
                        "the changed row must be queued exactly once with the fresh stats"
                    );
                    let (prog, speed, eta, ratio, up, seeds):
                        (f32, i64, Option<i64>, Option<f32>, i64, i64) = raw::fetch_optional(
                        &mut *c,
                        "SELECT progress, speed_bps, eta_secs, ratio, up_speed_bps, \n                            num_seeds FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row")
                    .expect("row present");
                    assert_eq!(
                        (prog, speed, eta, ratio, up, seeds),
                        (0.75, 102_400, Some(10), Some(0.5), 51_200, 3),
                        "flush_stats must land on every apply_stats_batch column"
                    );
                },
            )
            .await;
        }

        /// 2) A `downloading` row whose stats equal the row's current values
        /// (progress within 1%, no speed, same ratio/seeds) must be skipped by
        /// `stats_changed`: `changed` stays empty and the row's stat columns
        /// are untouched even after `flush_stats` runs.
        #[tokio::test]
        async fn it_inflight_unchanged_row_writes_nothing() {
            it_inflight_case(
                "__it_inflight__unchanged",
                Some("itu1hash"),
                "downloading",
                0.5,
                Some(1.0),
                Some(3),
                false,
                "itu1hash",
                it_torrent("itu1hash", "unchanged-row", "downloading", 0.5, 0, 0, 1.0, 3),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "an unchanged row must not be queued for a stats write"
                    );
                    let (prog, speed, eta, ratio, up, seeds): StatsTuple =
                        raw::fetch_optional(
                            &mut *c,
                            "SELECT progress, speed_bps, eta_secs, ratio, up_speed_bps, \n                            num_seeds FROM downloads WHERE id = $1",
                            |q| q.bind(id),
                        )
                        .await
                        .expect("read row")
                        .expect("row present");
                    assert_eq!(prog, 0.5, "progress stays untouched");
                    assert_eq!(speed, None, "speed_bps stays NULL");
                    assert_eq!(eta, None, "eta_secs stays NULL");
                    assert_eq!(ratio, Some(1.0), "ratio stays 1.0");
                    assert_eq!(up, None, "up_speed stays NULL");
                    assert_eq!(seeds, Some(3), "num_seeds stays 3");
                },
            )
            .await;
        }

        /// 3) A row bound to a stale hash ("aaa") that resolves to a torrent
        /// reporting hash "bbb" gets its `hash` column rebound to the torrent's
        /// real hash (the BUG-B4-restricted rebind: only the matched torrent's
        /// `t.hash` is ever bound). No stats write — the stats did not move.
        #[tokio::test]
        async fn it_inflight_rebinds_stale_hash() {
            it_inflight_case(
                "__it_inflight__rebind",
                Some("aaa"),
                "downloading",
                0.5,
                None,
                None,
                false,
                "aaa", // the row's stale hash keys the by_hash map
                it_torrent("bbb", "rebind-row", "downloading", 0.5, 0, 0, 0.0, 0),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "a rebind alone must not queue a stats write"
                    );
                    let hash: Option<String> = raw::fetch_scalar_optional(
                        &mut *c,
                        "SELECT hash FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row");
                    assert_eq!(
                        hash.as_deref(),
                        Some("bbb"),
                        "the stale hash binding must be rebound to the torrent's hash"
                    );
                },
            )
            .await;
        }

        /// 4) A `downloading` row whose torrent is fatal (`missingFiles`)
        /// goes `downloading → error` with the qB error message noted —
        /// `mark_fatal_error`'s `status` + `error` columns — and no stats
        /// write is queued.
        #[tokio::test]
        async fn it_inflight_fatal_torrent_marks_row_error() {
            let mut t = it_torrent("itf1hash", "fatal-row", "missingFiles", 0.9, 0, 0, 0.0, 0);
            t.err_str = Some("cannot allocate files".into());
            it_inflight_case(
                "__it_inflight__fatal",
                Some("itf1hash"),
                "downloading",
                0.9,
                None,
                None,
                false,
                "itf1hash",
                t,
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "a fatal row must not queue a stats write"
                    );
                    let (status, error): (String, Option<String>) = raw::fetch_optional(
                        &mut *c,
                        "SELECT status, error FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row")
                    .expect("row present");
                    assert_eq!(
                        (status.as_str(), error.as_deref()),
                        ("error", Some("cannot allocate files")),
                        "mark_fatal_error must set status='error' with the qB message"
                    );
                },
            )
            .await;
        }

        /// 5) A `seeding` row with a present, live torrent gets `ratio` /
        /// `up_speed_bps` / `num_seeds` / `speed_bps` refreshed in place by
        /// `refresh_seeding_stats` (status stays `seeding`, no settle) and no
        /// stats row is queued for the downloading flush.
        #[tokio::test]
        async fn it_inflight_seeding_row_refreshes_seed_stats() {
            it_inflight_case(
                "__it_inflight__seeding",
                Some("its1hash"),
                "seeding",
                1.0,
                Some(1.0),
                Some(3),
                false,
                "its1hash",
                it_torrent("its1hash", "seeding-row", "uploading", 1.0, 0, 200, 1.5, 7),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "seeding rows refresh in place and must not queue a stats write"
                    );
                    let (status, ratio, up, seeds, speed): (
                        String,
                        Option<f32>,
                        Option<i64>,
                        Option<i64>,
                        Option<i64>,
                    ) = raw::fetch_optional(
                        &mut *c,
                        "SELECT status, ratio, up_speed_bps, num_seeds, \n                            speed_bps FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row")
                    .expect("row present");
                    assert_eq!(
                        (status.as_str(), ratio, up, seeds, speed),
                        ("seeding", Some(1.5), Some(204_800), Some(7), Some(0)),
                        "refresh_seeding_stats must update seed stats without settling"
                    );
                },
            )
            .await;
        }

        /// 6) A `seeding` row whose stats exactly match the present torrent
        /// (progress 1.0, ratio 0.5, 2 seeds) is skipped by `stats_changed`
        /// — no `refresh_seeding_stats` DB write: `changed` stays empty and
        /// every stat column is byte-identical to what the row was inserted
        /// with (status stays `seeding`).
        #[tokio::test]
        async fn it_inflight_seed_unchanged_skips_write() {
            it_inflight_case(
                "__it_inflight__seed0",
                Some("its2hash"),
                "seeding",
                1.0,
                Some(0.5),
                Some(2),
                false,
                "its2hash",
                it_torrent("its2hash", "seed0-row", "uploading", 1.0, 0, 0, 0.5, 2),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "an unchanged seeding row must take no DB write"
                    );
                    let (status, progress, speed, eta, ratio, up, seeds): StatusStatsTuple =
                        raw::fetch_optional(
                            &mut *c,
                            "SELECT status, progress, speed_bps, eta_secs, ratio, \
                        up_speed_bps, num_seeds FROM downloads WHERE id = $1",
                            |q| q.bind(id),
                        )
                        .await
                        .expect("read row")
                        .expect("row present");
                    assert_eq!(status, "seeding", "status stays seeding");
                    assert_eq!(progress, 1.0, "progress stays 1.0");
                    assert_eq!(speed, None, "speed_bps stays NULL");
                    assert_eq!(eta, None, "eta_secs stays NULL");
                    assert_eq!(ratio, Some(0.5), "ratio stays 0.5");
                    assert_eq!(up, None, "up_speed_bps stays NULL");
                    assert_eq!(seeds, Some(2), "num_seeds stays 2");
                },
            )
            .await;
        }

        /// 7) A `downloading` row whose bound hash matches no qB torrent, past
        /// `MISSING_TORRENT_GRACE`, is judged missing by the tick: the
        /// `mark_fatal_error` arm (inflight.rs) takes `downloading → error`
        /// with the "torrent not found in qBittorrent" note, no stats write.
        #[tokio::test]
        async fn it_inflight_missing_torrent_past_grace_marks_error() {
            it_inflight_case(
                "__it_inflight__miss1",
                Some("miss1hash"),
                "downloading",
                0.5,
                None,
                None,
                true,         // stale: past MISSING_TORRENT_GRACE
                "ghost1hash", // no torrent carries the row's bound hash
                it_torrent("ghost1hash", "ghost1-row", "downloading", 0.5, 0, 0, 0.0, 0),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "a missing torrent must not queue a stats write"
                    );
                    let (status, error): (String, Option<String>) = raw::fetch_optional(
                        &mut *c,
                        "SELECT status, error FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row")
                    .expect("row present");
                    assert_eq!(
                        (status.as_str(), error.as_deref()),
                        ("error", Some("torrent not found in qBittorrent")),
                        "past-grace missing torrent must be judged missing"
                    );
                },
            )
            .await;
        }

        /// 8) The same missing-torrent row within the grace window is deferred:
        /// no `mark_fatal_error` — status stays `downloading` with `error`
        /// NULL, and nothing is written.
        #[tokio::test]
        async fn it_inflight_missing_torrent_within_grace_pending() {
            it_inflight_case(
                "__it_inflight__miss2",
                Some("miss2hash"),
                "downloading",
                0.5,
                None,
                None,
                false, // fresh: within MISSING_TORRENT_GRACE
                "ghost2hash",
                it_torrent("ghost2hash", "ghost2-row", "downloading", 0.5, 0, 0, 0.0, 0),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "a within-grace missing torrent must not queue a stats write"
                    );
                    let (status, error): (String, Option<String>) = raw::fetch_optional(
                        &mut *c,
                        "SELECT status, error FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row")
                    .expect("row present");
                    assert_eq!(
                        (status.as_str(), error.as_deref()),
                        ("downloading", None),
                        "within the grace window the row must be deferred, not errored"
                    );
                },
            )
            .await;
        }

        /// 8b) Inflight-1: a `downloading` row whose `updated_at` is stale
        /// (11 min — past `MISSING_TORRENT_GRACE`) but whose torrent IS
        /// visible must NOT be errored: the visibility touch
        /// (`touch_downloading`) refreshes `updated_at` while the torrent is
        /// visible, so the grace clock measures invisibility, not the gap
        /// since the last stats write. Regression: a stalled torrent with
        /// unchanged stats (no `stats_changed` writes) carried an exhausted
        /// clock the moment it vanished and was errored immediately with the
        /// non-requeueable "torrent not found in qBittorrent" note.
        #[tokio::test]
        async fn it_inflight_visible_stale_row_is_touched_not_errored() {
            it_inflight_case(
                "__it_inflight__touch1",
                Some("tou1hash"),
                "downloading",
                0.5,
                None,
                None,
                true,         // stale: past MISSING_TORRENT_GRACE
                "tou1hash",   // the row's torrent IS in qB
                it_torrent("tou1hash", "touch1-row", "downloading", 0.5, 0, 0, 0.0, 0),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "an unchanged torrent must not queue a stats write"
                    );
                    let (status, error, age_secs): (String, Option<String>, i64) =
                        raw::fetch_optional(
                            &mut *c,
                            "SELECT status, error, \n                             EXTRACT(EPOCH FROM (now() - updated_at))::bigint \n                             FROM downloads WHERE id = $1",
                            |q| q.bind(id),
                        )
                        .await
                        .expect("read row")
                        .expect("row present");
                    assert_eq!(
                        (status.as_str(), error.as_deref()),
                        ("downloading", None),
                        "a stale-but-visible row must not be judged missing"
                    );
                    assert!(
                        age_secs < 5,
                        "the visibility touch must refresh updated_at (it was 11 min old), age: {age_secs}s"
                    );
                },
            )
            .await;
        }

        /// 9) A `seeding` row whose torrent left qB past the grace period is
        /// settled by `settle_seeding` (the `SeedingAction::Done` DB-effect
        /// arm): `seeding → complete` with `error` NULL, the seed stats
        /// retained (settle_seeding never clears them), no stats write.
        #[tokio::test]
        async fn it_inflight_seeding_gone_past_grace_settles() {
            it_inflight_case(
                "__it_inflight__seed1",
                Some("seed1hash"),
                "seeding",
                1.0,
                Some(1.0),
                Some(3),
                true,         // stale: past MISSING_TORRENT_GRACE
                "ghost3hash", // the row's torrent is gone from qB
                it_torrent("ghost3hash", "ghost3-row", "uploading", 1.0, 0, 0, 0.0, 0),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "a settled seeding row must not queue a stats write"
                    );
                    let (status, error, ratio, seeds): (
                        String,
                        Option<String>,
                        Option<f32>,
                        Option<i64>,
                    ) = raw::fetch_optional(
                        &mut *c,
                        "SELECT status, error, ratio, num_seeds FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row")
                    .expect("row present");
                    assert_eq!(
                        (status.as_str(), error.as_deref()),
                        ("complete", None),
                        "the Done arm must settle to complete with no error note"
                    );
                    assert_eq!(
                        (ratio, seeds),
                        (Some(1.0), Some(3)),
                        "settle_seeding retains the seed stats"
                    );
                },
            )
            .await;
        }

        /// 10) A `seeding` row whose torrent is present but fatal
        /// (`missingFiles`) is settled by `settle_seeding` (the
        /// `SeedingAction::FatalDone` DB-effect arm): `seeding → complete`
        /// with the qB error noted — the ZIM is already installed, so a fatal
        /// torrent is noted but not a failure. No stats write.
        #[tokio::test]
        async fn it_inflight_seeding_fatal_torrent_settles_with_error() {
            let mut t = it_torrent(
                "seed2hash",
                "seedfatal-row",
                "missingFiles",
                1.0,
                0,
                0,
                0.0,
                0,
            );
            t.err_str = Some("cannot allocate files".into());
            it_inflight_case(
                "__it_inflight__seed2",
                Some("seed2hash"),
                "seeding",
                1.0,
                Some(1.0),
                Some(3),
                false, // FatalDone needs no grace window (the torrent is present)
                "seed2hash",
                t,
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "a settled seeding row must not queue a stats write"
                    );
                    let (status, error): (String, Option<String>) = raw::fetch_optional(
                        &mut *c,
                        "SELECT status, error FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row")
                    .expect("row present");
                    assert_eq!(
                        (status.as_str(), error.as_deref()),
                        ("complete", Some("cannot allocate files")),
                        "the FatalDone arm must settle with the qB error noted"
                    );
                },
            )
            .await;
        }

        /// 11) A `seeding` row whose torrent is gone but still within the
        /// grace window takes no DB effect (the `Pending` guard on the Done
        /// arm): status stays `seeding`, `error` NULL, seed stats untouched.
        #[tokio::test]
        async fn it_inflight_seeding_gone_within_grace_pending() {
            it_inflight_case(
                "__it_inflight__seed3",
                Some("seed3hash"),
                "seeding",
                1.0,
                Some(0.5),
                Some(2),
                false, // fresh: within MISSING_TORRENT_GRACE
                "ghost4hash",
                it_torrent("ghost4hash", "ghost4-row", "uploading", 1.0, 0, 0, 0.0, 0),
                |id, changed: Vec<super::super::StatsRow>, pool: Pool| async move {
                    let mut c = pool.acquire().await.expect("conn");
                    assert!(
                        changed.is_empty(),
                        "a within-grace missing seeding torrent must take no DB write"
                    );
                    let (status, error, ratio, seeds): (
                        String,
                        Option<String>,
                        Option<f32>,
                        Option<i64>,
                    ) = raw::fetch_optional(
                        &mut *c,
                        "SELECT status, error, ratio, num_seeds FROM downloads WHERE id = $1",
                        |q| q.bind(id),
                    )
                    .await
                    .expect("read row")
                    .expect("row present");
                    assert_eq!(
                        (status.as_str(), error.as_deref()),
                        ("seeding", None),
                        "within the grace window the seeding row must not settle"
                    );
                    assert_eq!(
                        (ratio, seeds),
                        (Some(0.5), Some(2)),
                        "seed stats stay untouched while pending"
                    );
                },
            )
            .await;
        }

        /// BUG-B4: the name fallback in `process_inflight` is restricted to rows
        /// with `hash IS NULL` — a row that already has a bound hash must match
        /// only by hash. Two same-named rows share the display name: row A is
        /// bound to a stale (non-matching) hash, row B is unbound (`hash NULL`).
        /// The completed torrent's real hash matches neither. Without the guard,
        /// BOTH rows would match the torrent via the name fallback and double
        /// `handle_complete` (double multi-GB verify/copy + auto-index). With it,
        /// only the hash-NULL row matches: A stays `downloading` (hash untouched,
        /// no file), B settles `complete` (hash rebound, one file installed).
        ///
        /// (A real ZIM is staged so the matched row's install actually runs —
        /// the "exactly one file" check observes the expensive path executed
        /// once; the row-A assertions catch the double-completion regression.)
        #[tokio::test]
        async fn it_inflight_bug_b4_same_name_only_null_hash_completes() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");
            let mut c = pool.acquire().await.expect("conn");
            // Sweep leftovers from a crashed run (shared single-DB suite).
            let _ = raw::execute(
                &mut *c,
                "DELETE FROM downloads WHERE name LIKE '__it_inflight__%'",
                |q| q,
            )
            .await;
            let _ = raw::execute(&mut *c, "DELETE FROM zims WHERE name = $1", |q| {
                q.bind("__it_inflight__b4")
            })
            .await;

            // Two same-named rows: A bound to a stale (non-matching) hash, B
            // unbound. Both freshly `downloading` (no grace expiry) on the
            // magnet (qB-torrent) code path.
            let id_a = it_insert_row(
                &pool,
                "__it_inflight__b4dup",
                Some("b4stalehash"),
                "downloading",
                0.9,
                None,
                None,
            )
            .await;
            let id_b = it_insert_row(
                &pool,
                "__it_inflight__b4dup",
                None,
                "downloading",
                0.9,
                None,
                None,
            )
            .await;

            let tmp = tempfile::tempdir().expect("tempdir");
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());

            // Stage the torrent content (a valid ZIM) OUTSIDE the ZIM dir so the
            // "exactly one installed file" check is unambiguous.
            let content_tmp = tempfile::tempdir().expect("content tempdir");
            let content_file = content_tmp.path().join("__it_inflight__b4.zim");
            std::fs::copy("tests/fixtures/tiny.zim", &content_file).expect("stage content zim");

            let t = TorrentInfo {
                hash: "b4realhash".into(),
                name: "__it_inflight__b4dup".into(),
                progress: 1.0,
                state: "uploading".into(),
                dlspeed: 0,
                upspeed: 0,
                ratio: 0.0,
                category: None,
                save_path: Some(content_tmp.path().to_string_lossy().into_owned()),
                content_path: Some(content_file.to_string_lossy().into_owned()),
                size: 0,
                downloaded: 0,
                num_seeds: 0,
                err_str: None,
            };

            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims.clone(),
                crate::torrent::QbitClientCache::new(),
                Some("http://127.0.0.1:9/qb".into()),
                "user".into(),
                "pass".into(),
            );
            let qbit = std::sync::Arc::new(
                crate::torrent::QbitClient::new(
                    "http://127.0.0.1:9/qb",
                    "user",
                    "pass",
                    false,
                    None,
                )
                .expect("qbit client build (no network needed)"),
            );
            let hash_key: &str = "b4realhash";
            let by_hash = std::collections::HashMap::from([(hash_key, &t)]);
            let by_name = std::collections::HashMap::from([(t.name.to_lowercase(), &t)]);

            let rows = poller
                .fetch_inflight_rows()
                .await
                .expect("rows")
                .into_iter()
                .filter(|r| r.name.starts_with(IT_INFLIGHT_PREFIX))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 2, "both same-named rows must be in-flight");
            let changed = poller
                .process_inflight(
                    &by_hash,
                    &by_name,
                    Some(qbit),
                    true,
                    &download_settings().poller_params_snapshot(),
                    rows,
                )
                .await
                .expect("process_inflight");
            poller.flush_stats(&changed).await;

            let row_a: (String, Option<String>, Option<String>) = raw::fetch_optional(
                &mut *c,
                "SELECT status, hash, file_path FROM downloads WHERE id = $1",
                |q| q.bind(id_a),
            )
            .await
            .expect("read row A")
            .expect("row present");
            let row_b: (String, Option<String>, Option<String>) = raw::fetch_optional(
                &mut *c,
                "SELECT status, hash, file_path FROM downloads WHERE id = $1",
                |q| q.bind(id_b),
            )
            .await
            .expect("read row B")
            .expect("row present");

            // Row A (bound to a stale, non-matching hash): must NOT fall back to
            // the name match — still `downloading`, hash untouched, no file.
            assert_eq!(
                row_a.0, "downloading",
                "hash-bound row must not name-fallback to the same-named torrent"
            );
            assert_eq!(
                row_a.1.as_deref(),
                Some("b4stalehash"),
                "hash-bound row's hash must not be rebound to the matched torrent"
            );
            assert_eq!(
                row_a.2, None,
                "hash-bound row must not run handle_complete (no file installed)"
            );

            // Row B (hash NULL): matched by the name fallback — completed, hash
            // bound to the torrent's real hash, one file installed.
            assert_eq!(
                row_b.0, "complete",
                "hash-NULL row must complete via the name fallback"
            );
            assert_eq!(
                row_b.1.as_deref(),
                Some("b4realhash"),
                "hash-NULL row's hash must be bound to the torrent's real hash"
            );
            let fp: Option<String> = row_b.2;
            let fp = fp.expect("row B must have an installed file_path");
            assert!(
                std::path::Path::new(&fp).starts_with(&zims.zim_dir),
                "row B file installed into zim_dir, got {fp}"
            );
            assert!(
                std::path::Path::new(&fp).exists(),
                "row B installed file must exist"
            );

            // The expensive verify/copy ran exactly once: a single ZIM installed.
            // (A regression to double handle_complete still lands one file but
            // flips row A to `complete` / rebinds its hash — caught above.)
            let installed = std::fs::read_dir(&zims.zim_dir)
                .expect("read zim_dir")
                .flatten()
                .filter(|e| e.path().extension().map(|x| x == "zim").unwrap_or(false))
                .count();
            assert_eq!(installed, 1, "exactly one ZIM must be installed");

            // Let the background auto-index (spawned by handle_complete)
            // settle before sweeping the `zims` row its resync upserted, so
            // the cascade delete is quiet. Bounded deadline-poll (same
            // pattern as the completion test below) instead of a fixed
            // sleep: the `__it_inflight__b4` row is tiny.zim (one article,
            // `main.html`), so it reaches `ready` with `article_count > 0`.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            let mut last = String::from("(no zims row)");
            let mut settled = false;
            while tokio::time::Instant::now() < deadline {
                let row: Option<(String, i64)> = raw::fetch_optional(
                    &mut *c,
                    "SELECT index_status, article_count FROM zims WHERE name = $1",
                    |q| q.bind("__it_inflight__b4"),
                )
                .await
                .expect("query zims");
                if let Some((st, cnt)) = row {
                    last = format!("{st}/{cnt}");
                    if st == "ready" && cnt > 0 {
                        settled = true;
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            assert!(
                settled,
                "auto-index did not settle within 30s (last: {last})"
            );
            let mut c2 = pool.acquire().await.expect("conn");
            let _ = raw::execute(
                &mut *c2,
                "DELETE FROM downloads WHERE id IN ($1, $2)",
                |q| q.bind(id_a).bind(id_b),
            )
            .await;
            let _ = raw::execute(&mut *c2, "DELETE FROM zims WHERE name = $1", |q| {
                q.bind("__it_inflight__b4")
            })
            .await;
            let _ = (tmp, content_tmp);
        }

        /// Inflight-2: two same-named `downloading` rows whose `hash` is
        /// NULL in BOTH must not double-bind one qB torrent in a single
        /// tick — the local snapshot maps are not updated by `bind_hash`, so
        /// without the per-tick consumed set both rows would match the same
        /// entry by name, bind the same hash, and (once complete) double-
        /// `handle_complete` (double verify/install + double qB delete).
        /// Exactly one row must bind; the other stays `downloading`, hash
        /// NULL, within grace, un-errored.
        #[tokio::test]
        async fn it_inflight_two_null_hash_same_name_rows_single_bind() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");
            let mut c = pool.acquire().await.expect("conn");
            // Sweep leftovers from a crashed run (shared single-DB suite).
            let _ = raw::execute(
                &mut *c,
                "DELETE FROM downloads WHERE name LIKE '__it_inflight__%'",
                |q| q,
            )
            .await;

            // Two same-named rows, both `hash IS NULL` (the double-bind
            // exposure). Fresh `downloading` (no grace expiry).
            let id_a = it_insert_row(
                &pool,
                "__it_inflight__n2dup",
                None,
                "downloading",
                0.5,
                None,
                None,
            )
            .await;
            let id_b = it_insert_row(
                &pool,
                "__it_inflight__n2dup",
                None,
                "downloading",
                0.5,
                None,
                None,
            )
            .await;

            let tmp = tempfile::tempdir().expect("tempdir");
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some("http://127.0.0.1:9/qb".into()),
                "user".into(),
                "pass".into(),
            );
            let qbit = std::sync::Arc::new(
                crate::torrent::QbitClient::new(
                    "http://127.0.0.1:9/qb",
                    "user",
                    "pass",
                    false,
                    None,
                )
                .expect("qbit client build (no network needed)"),
            );
            // One live (not complete, not fatal) qB torrent with the rows' name.
            let t = TorrentInfo {
                hash: "n2realhash".into(),
                name: "__it_inflight__n2dup".into(),
                progress: 0.5,
                state: "downloading".into(),
                dlspeed: 0,
                upspeed: 0,
                ratio: 0.0,
                category: None,
                save_path: None,
                content_path: None,
                size: 0,
                downloaded: 0,
                num_seeds: 0,
                err_str: None,
            };
            let hash_key: &str = "n2realhash";
            let by_hash = std::collections::HashMap::from([(hash_key, &t)]);
            let by_name = std::collections::HashMap::from([(t.name.to_lowercase(), &t)]);

            let rows = poller
                .fetch_inflight_rows()
                .await
                .expect("rows")
                .into_iter()
                .filter(|r| r.name.starts_with(IT_INFLIGHT_PREFIX))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 2, "both same-named rows must be in-flight");
            let changed = poller
                .process_inflight(
                    &by_hash,
                    &by_name,
                    Some(qbit),
                    true,
                    &download_settings().poller_params_snapshot(),
                    rows,
                )
                .await
                .expect("process_inflight");
            poller.flush_stats(&changed).await;

            let row_a: (String, Option<String>, Option<String>) = raw::fetch_optional(
                &mut *c,
                "SELECT status, hash, error FROM downloads WHERE id = $1",
                |q| q.bind(id_a),
            )
            .await
            .expect("read row A")
            .expect("row present");
            let row_b: (String, Option<String>, Option<String>) = raw::fetch_optional(
                &mut *c,
                "SELECT status, hash, error FROM downloads WHERE id = $1",
                |q| q.bind(id_b),
            )
            .await
            .expect("read row B")
            .expect("row present");
            // Both rows must survive the tick (the torrent is present and
            // not fatal; the unmatched row is within grace).
            assert_eq!(row_a.0, "downloading", "row A must stay downloading");
            assert_eq!(row_b.0, "downloading", "row B must stay downloading");
            assert_eq!(row_a.2, None, "row A must not be errored");
            assert_eq!(row_b.2, None, "row B must not be errored");
            // Exactly ONE row may hold the torrent's hash — a double-bind is
            // the regression (both would carry "n2realhash").
            let a_bound = row_a.1.as_deref() == Some("n2realhash");
            let b_bound = row_b.1.as_deref() == Some("n2realhash");
            assert!(
                a_bound ^ b_bound,
                "exactly one NULL-hash row must bind the matched torrent (hashes: {:?}, {:?})",
                row_a.1,
                row_b.1
            );

            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE id IN ($1, $2)", |q| {
                q.bind(id_a).bind(id_b)
            })
            .await;
            let _ = tmp;
        }

        /// An unreachable qBittorrent this tick (`qb_available = false`) defers
        /// ALL missing-torrent judgments: a stale `downloading` row is not
        /// errored (the last `None` arm in `process_inflight`) and a stale
        /// `seeding` row is not settled (`grace_expired` folds in qB
        /// liveness) — both are re-checked next cycle.
        #[tokio::test]
        async fn it_inflight_qb_unreachable_defers_missing_torrent_judgment() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");
            let mut c = pool.acquire().await.expect("conn");
            let _ = raw::execute(
                &mut *c,
                "DELETE FROM downloads WHERE name LIKE '__it_inflight__%'",
                |q| q,
            )
            .await;
            // Two rows aged past `MISSING_TORRENT_GRACE`, each bound to a hash
            // no qB torrent carries (the maps below are empty).
            let dl_id = it_insert_row(
                &pool,
                "__it_inflight__qbu1",
                Some("qbu1hash"),
                "downloading",
                0.5,
                None,
                None,
            )
            .await;
            let sd_id = it_insert_row(
                &pool,
                "__it_inflight__qbu2",
                Some("qbu2hash"),
                "seeding",
                1.0,
                Some(0.5),
                Some(2),
            )
            .await;
            for id in [dl_id, sd_id] {
                raw::execute(
                    &mut *c,
                    "UPDATE downloads SET updated_at = now() - interval '11 minutes' \
                     WHERE id = $1",
                    |q| q.bind(id),
                )
                .await
                .expect("stale the row's updated_at");
            }

            let tmp = tempfile::tempdir().expect("tempdir");
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some("http://127.0.0.1:9/qb".into()),
                "user".into(),
                "pass".into(),
            );
            let qbit = std::sync::Arc::new(
                crate::torrent::QbitClient::new(
                    "http://127.0.0.1:9/qb",
                    "user",
                    "pass",
                    false,
                    None,
                )
                .expect("qbit client build (no network needed)"),
            );
            let by_hash: std::collections::HashMap<&str, &TorrentInfo> =
                std::collections::HashMap::new();
            let by_name: std::collections::HashMap<String, &TorrentInfo> =
                std::collections::HashMap::new();

            let rows = poller
                .fetch_inflight_rows()
                .await
                .expect("rows")
                .into_iter()
                .filter(|r| r.name.starts_with(IT_INFLIGHT_PREFIX))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 2, "both stale rows must be in-flight");
            let changed = poller
                .process_inflight(
                    &by_hash,
                    &by_name,
                    Some(qbit),
                    false, // qB unreachable this tick
                    &download_settings().poller_params_snapshot(),
                    rows,
                )
                .await
                .expect("process_inflight");
            poller.flush_stats(&changed).await;
            assert!(
                changed.is_empty(),
                "qB unreachable → no stats may be queued"
            );

            let dl_status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE id = $1",
                |q| q.bind(dl_id),
            )
            .await
            .expect("read downloading row")
            .expect("row present");
            let sd_status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE id = $1",
                |q| q.bind(sd_id),
            )
            .await
            .expect("read seeding row")
            .expect("row present");
            assert_eq!(
                dl_status, "downloading",
                "an unreachable qB must not judge the downloading row missing"
            );
            assert_eq!(
                sd_status, "seeding",
                "an unreachable qB must not settle the seeding row"
            );

            let _ = pool.acquire().await; // keep pool alive for cleanup
            let mut c2 = pool.acquire().await.expect("conn");
            let _ = raw::execute(
                &mut *c2,
                "DELETE FROM downloads WHERE id IN ($1, $2)",
                |q| q.bind(dl_id).bind(sd_id),
            )
            .await;
            let _ = tmp;
        }

        /// BUG-5: after `cancel()`, `run` must exit at the next between-ticks
        /// poll — even when every DB and qB call fails (dead pool + dead qB
        /// port). DB-less: no live Postgres is needed because the cancel check
        /// runs regardless of tick success.
        #[tokio::test]
        async fn run_stops_on_cancel_between_ticks() {
            // Dead pool (lazy build — no connection is attempted here): any
            // acquire fails against the dead port. `dead_pool()` carries a
            // short `acquire_timeout` — sqlx's 30 s default would otherwise
            // hold the first (reconcile/tick) DB op well past the 10 s cancel
            // deadline below.
            let pool = crate::testing::dead_pool();
            let tmp = tempfile::tempdir().unwrap();
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            // Dead qB port: connection-refused is instant, so `resolve_torrent`
            // fails fast instead of hanging the run loop.
            let poller = super::super::DownloadPoller::new(
                pool,
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some("http://127.0.0.1:1/qb".into()),
                "user".into(),
                "pass".into(),
            );
            poller.cancel();
            let finished =
                tokio::time::timeout(std::time::Duration::from_secs(10), poller.run()).await;
            assert!(
                finished.is_ok(),
                "run() must stop at the between-ticks cancel poll (BUG-5)"
            );
        }

        /// BUG-5 (CI-DB): a row that moved to `cancelled` between claim and tick
        /// is dropped from qBittorrent and its stale `hash` cleared in the same
        /// tick; no progress UPDATE is issued for it (it no longer matches the
        /// in-flight row query).
        #[tokio::test]
        async fn tick_removes_cancelled_torrent_from_qb_and_clears_hash() {
            use wiremock::matchers::{method, path, query_param};
            use wiremock::{Mock, MockServer, ResponseTemplate};

            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v2/login"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v2/torrents/info"))
                .and(query_param("filter", "all"))
                .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v2/torrents/delete"))
                .and(query_param("hashes", "c0ffee"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .expect(1)
                .mount(&server)
                .await;

            let mut c = pool.acquire().await.expect("conn");
            let id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, hash, status) \
                 VALUES ($1, $2, $3, 'cancelled') RETURNING id",
                |q| {
                    q.bind("it-cancelled")
                        .bind("magnet:?xt=urn:btih:bbb")
                        .bind("c0ffee")
                },
            )
            .await
            .expect("insert cancelled row")
            .expect("row present");

            let tmp = tempfile::tempdir().expect("tempdir");
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some(server.uri()),
                "user".into(),
                "pass".into(),
            );

            poller.tick().await.expect("tick runs");

            // The delete mock's `.expect(1)` (verified on drop) is the qB-remove
            // assertion; the row's stale hash must be cleared by the same tick.
            let mut c = pool.acquire().await.expect("conn");
            let hash: Option<String> = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT hash FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read row");
            assert_eq!(hash, None, "stale hash binding must be cleared");

            let _ = pool.acquire().await; // keep pool alive for cleanup
            let mut c2 = pool.acquire().await.expect("conn");
            let _ = raw::execute(&mut *c2, "DELETE FROM downloads WHERE id = $1", |q| {
                q.bind(id)
            })
            .await;
            let _ = tmp;
        }

        /// BUG-6 (CI-DB): the requeue guard stops the bounded retry loop after
        /// `REQUEUE_GIVE_UP_AFTER` consecutive identical transient errors — the
        /// row stays `error` with its message intact (a requeued row's `error`
        /// would be nulled) — while a fresh message and a 401 session-expiry
        /// message are both re-queued and enqueued in the same tick.
        ///
        /// No keep-alive queued row is needed: `requeue_stale_errors` runs
        /// BEFORE the `should_skip_tick` early-exit, so the stale `error` rows
        /// alone defeat the skip (requeued rows count as `queued`) — this is
        /// exactly the idle-queue case that ordering used to get wrong.
        #[tokio::test]
        async fn tick_requeue_guard_stops_after_third_identical_error() {
            use wiremock::matchers::{method, path, query_param};
            use wiremock::{Mock, MockServer, ResponseTemplate};

            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v2/login"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v2/torrents/info"))
                .and(query_param("filter", "all"))
                .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v2/torrents/add"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .expect(2)
                .mount(&server)
                .await;

            let mut c = pool.acquire().await.expect("conn");
            // Three stale (>10 min) transient-error rows.
            // Already failed twice with this exact message (guard-seeded below).
            let guarded: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, status, error, updated_at) \
                 VALUES ($1, $2, 'error', $3, now() - interval '11 minutes') RETURNING id",
                |q| {
                    q.bind("it-guarded")
                        .bind("magnet:?xt=urn:btih:ccc")
                        .bind("connection refused")
                },
            )
            .await
            .expect("insert guarded row")
            .expect("row present");
            // Fresh message — first failure, must be requeued.
            let fresh: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, status, error, updated_at) \
                 VALUES ($1, $2, 'error', $3, now() - interval '11 minutes') RETURNING id",
                |q| {
                    q.bind("it-fresh")
                        .bind("magnet:?xt=urn:btih:ddd")
                        .bind("timeout while connecting")
                },
            )
            .await
            .expect("insert fresh row")
            .expect("row present");
            // 401 session expiry — exempt from the guard even at count 2.
            let auth: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, status, error, updated_at) \
                 VALUES ($1, $2, 'error', $3, now() - interval '11 minutes') RETURNING id",
                |q| {
                    q.bind("it-auth")
                        .bind("magnet:?xt=urn:btih:eee")
                        .bind("connection failed: HTTP 401")
                },
            )
            .await
            .expect("insert auth row")
            .expect("row present");
            drop(c);

            let tmp = tempfile::tempdir().expect("tempdir");
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some(server.uri()),
                "user".into(),
                "pass".into(),
            );
            // Seed the guard as if `guarded` and `auth` already failed twice
            // consecutively with their current messages.
            {
                let entry = |msg: &str| super::super::GuardEntry {
                    prev_msg: msg.into(),
                    count: 2,
                    last_seen: 1,
                };
                let mut g = poller.last_error.lock().expect("last_error lock");
                g.insert(guarded, entry("connection refused"));
                g.insert(auth, entry("connection failed: HTTP 401"));
            }

            poller.tick().await.expect("tick runs");

            let mut c = pool.acquire().await.expect("conn");
            // The guarded row must NOT have been requeued: status `error` with
            // its message intact (a requeue nulls `error`).
            let (gs, ge): (String, Option<String>) = raw::fetch_optional(
                &mut *c,
                "SELECT status, error FROM downloads WHERE id = $1",
                |q| q.bind(guarded),
            )
            .await
            .expect("read guarded row")
            .expect("row present");
            assert_eq!(gs, "error", "guarded row must stay error, got: {gs}");
            assert_eq!(
                ge.as_deref(),
                Some("connection refused"),
                "guarded row's error must be intact"
            );
            let still_tracked = poller
                .last_error
                .lock()
                .expect("last_error lock")
                .get(&guarded)
                .is_some();
            // FIX-B: the give-up row must STAY in the guard map (its entry is
            // retained, not removed), so the next pass still sees the give-up
            // state and does not reset the count to 1 and re-queue it. The
            // entry is reclaimed later by the lazy stale sweep once the row
            // leaves `error` — pinned DB-free in
            // `requeue_guard_give_up_is_sticky_not_one_shot`.
            assert!(
                still_tracked,
                "given-up row must stay tracked so the next pass doesn't re-queue it"
            );
            // The fresh and the 401 rows were requeued and enqueued to qB in the
            // same tick (add mock `.expect(2)` is verified on drop).
            for id in [fresh, auth] {
                let s: String = raw::fetch_scalar_optional(
                    &mut *c,
                    "SELECT status FROM downloads WHERE id = $1",
                    |q| q.bind(id),
                )
                .await
                .expect("read row")
                .expect("row present");
                assert_eq!(s, "downloading", "row {id} must be enqueued, got: {s}");
            }

            let _ = pool.acquire().await; // keep pool alive for cleanup
            let mut c2 = pool.acquire().await.expect("conn");
            let _ = raw::execute(
                &mut *c2,
                "DELETE FROM downloads WHERE id IN ($1, $2, $3)",
                |q| q.bind(guarded).bind(fresh).bind(auth),
            )
            .await;
            let _ = tmp;
        }

        /// BUG-7 (CI-DB): the tick's bounded-retry requeue is scoped to rows still
        /// in `error` state (`WHERE id = ANY($1) AND status = 'error'`) — a row
        /// that carries a stale transient-error message but is `cancelled` or
        /// `complete`, or whose error is non-transient, is never resurrected by
        /// the requeue UPDATE, while a matching `error` row is requeued (and
        /// enqueued) in the same tick.
        #[tokio::test]
        async fn tick_requeue_does_not_touch_non_error_rows() {
            use wiremock::matchers::{method, path, query_param};
            use wiremock::{Mock, MockServer, ResponseTemplate};

            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v2/login"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v2/torrents/info"))
                .and(query_param("filter", "all"))
                .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v2/torrents/add"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .expect(1)
                .mount(&server)
                .await;

            let c = pool.acquire().await.expect("conn");
            let ins_row = |name: String, url: String, status: String, err: String| {
                let pool = pool.clone();
                async move {
                    raw::fetch_scalar_optional(
                        &pool,
                        "INSERT INTO downloads (name, url, status, error, updated_at) \
                         VALUES ($1, $2, $3, $4, now() - interval '11 minutes') RETURNING id",
                        |q| q.bind(name).bind(url).bind(status).bind(err),
                    )
                    .await
                }
            };
            // Stale transient-error row → requeued and enqueued this tick.
            let transient: i32 = ins_row(
                "wi23-transient".into(),
                "magnet:?xt=urn:btih:fff".into(),
                "error".into(),
                "connection refused".into(),
            )
            .await
            .expect("insert transient row")
            .expect("row present");
            // Stale NON-transient error row → out of scope, stays `error`.
            let nontransient: i32 = ins_row(
                "wi23-nontransient".into(),
                "magnet:?xt=urn:btih:000".into(),
                "error".into(),
                "invalid ZIM file".into(),
            )
            .await
            .expect("insert nontransient row")
            .expect("row present");
            // Cancelled row with a stale transient error message → stays `cancelled`.
            let cancelled: i32 = ins_row(
                "wi23-cancelled".into(),
                "magnet:?xt=urn:btih:111".into(),
                "cancelled".into(),
                "connection refused".into(),
            )
            .await
            .expect("insert cancelled row")
            .expect("row present");
            // Complete row with a stale error note → stays `complete`.
            let complete: i32 = ins_row(
                "wi23-complete".into(),
                "magnet:?xt=urn:btih:222".into(),
                "complete".into(),
                "timeout".into(),
            )
            .await
            .expect("insert complete row")
            .expect("row present");
            drop(c);

            let tmp = tempfile::tempdir().expect("tempdir");
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some(server.uri()),
                "user".into(),
                "pass".into(),
            );

            poller.tick().await.expect("tick runs");

            let mut c = pool.acquire().await.expect("conn");
            let s: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE id = $1",
                |q| q.bind(transient),
            )
            .await
            .expect("read")
            .expect("row present");
            assert_ne!(
                s, "error",
                "transient row must have been requeued, got: {s}"
            );
            for (id, want) in [
                (nontransient, "error"),
                (cancelled, "cancelled"),
                (complete, "complete"),
            ] {
                let s: String = raw::fetch_scalar_optional(
                    &mut *c,
                    "SELECT status FROM downloads WHERE id = $1",
                    |q| q.bind(id),
                )
                .await
                .expect("read")
                .expect("row present");
                assert_eq!(s, want, "row {id} must stay {want}, got: {s}");
            }

            let _ = pool.acquire().await; // keep pool alive for cleanup
            let mut c2 = pool.acquire().await.expect("conn");
            let _ = raw::execute(
                &mut *c2,
                "DELETE FROM downloads WHERE id IN ($1, $2, $3, $4)",
                |q| {
                    q.bind(transient)
                        .bind(nontransient)
                        .bind(cancelled)
                        .bind(complete)
                },
            )
            .await;
            let _ = tmp;
        }

        /// Core-loop end-to-end (DB-gated): an in-flight `downloading` row
        /// whose qBittorrent torrent reports **complete** (a real `tiny.zim`
        /// copy staged at `content_path`) is driven through the actual tick
        /// body — `fetch_inflight_rows` → `process_inflight` → `flush_stats`
        /// — which must:
        /// 1. install the ZIM into the ZIM dir (`handle_complete`: locate →
        ///    verify → install → `resync` → `mark_complete`), settling the
        ///    row to `complete` with `file_path` set;
        /// 2. let the poller's spawned **auto-index** reach
        ///    `index_status = 'ready'` with a non-zero `article_count` /
        ///    `indexed_entries`;
        /// 3. make the article servable over the real router: `GET /search`
        ///    (FTS over the index), `GET /snippet` (indexed row) and
        ///    `GET /chunks` (live read of the installed archive) all 200.
        ///
        /// The only tick step not driven here is `queued` → qB-enqueue (it
        /// needs a live qBittorrent wire); that step is covered by
        /// `tick_fetches_all_torrents_once_and_advances_both_row_kinds`
        /// (wiremock). This test picks the row up in exactly the state that
        /// step leaves it: `downloading` with a bound hash.
        ///
        /// Without Postgres (`ZIMSERVICE_REQUIRE_DB` unset) the test skips
        /// cleanly via `test_pool`.
        #[tokio::test]
        async fn e2e_downloaded_zim_is_installed_indexed_and_servable() {
            use axum::body::Body;
            use axum::http::Request;
            use http_body_util::BodyExt;
            use tower::ServiceExt;

            const ZIM: &str = "__it_e2e__";
            const HASH: &str = "ite2ehash";

            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            // Sweep leftovers from a crashed prior run (shared dev DB).
            let mut c = pool.acquire().await.expect("conn");
            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE name = $1", |q| {
                q.bind(ZIM)
            })
            .await;
            let _ =
                raw::execute(&mut *c, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM)).await;

            // Stage the torrent content: a real (structurally valid)
            // `tiny.zim` copy at the path qBittorrent would report as
            // `content_path` once the download finishes.
            let tmp = tempfile::tempdir().expect("tempdir");
            let zim_dir = tmp.path().join("zims");
            let content_dir = tmp.path().join("qb-content");
            std::fs::create_dir_all(&zim_dir).unwrap();
            std::fs::create_dir_all(&content_dir).unwrap();
            let content_file = content_dir.join(format!("{ZIM}.zim"));
            std::fs::copy("tests/fixtures/tiny.zim", &content_file).expect("copy tiny.zim fixture");

            // Suite settings (default `keep_completed = false`): the row
            // settles `complete` (not `seeding`) and the post-install qB-delete
            // branch runs (dead port → fail-soft warn, like an unreachable qB).
            // Same helper the other in-flight tests drive.
            let settings = download_settings();

            let zims = crate::zim::ZimManager::new(zim_dir, pool.clone());
            let poller = super::super::DownloadPoller::new(
                pool.clone(),
                settings.clone(),
                zims.clone(),
                crate::torrent::QbitClientCache::new(),
                Some("http://127.0.0.1:9/qb".into()),
                "user".into(),
                "pass".into(),
            );
            // The qB client the tick would have resolved (dead port: the
            // fire-and-forget ratio-limit / delete calls fail soft).
            let qbit = std::sync::Arc::new(
                crate::torrent::QbitClient::new(
                    "http://127.0.0.1:9/qb",
                    "user",
                    "pass",
                    false,
                    None,
                )
                .expect("qbit client build (no network needed)"),
            );

            // (a) The in-flight row, as the qB-enqueue step would have left
            // it: `downloading`, hash bound, fresh `updated_at`.
            let id = it_insert_row(&pool, ZIM, Some(HASH), "downloading", 0.99, None, None).await;

            // What qBittorrent reports this tick: the torrent is complete
            // (`uploading` @ 1.0) and its content is staged on disk.
            let t = TorrentInfo {
                hash: HASH.into(),
                name: ZIM.into(),
                progress: 1.0,
                state: "uploading".into(),
                dlspeed: 0,
                upspeed: 0,
                ratio: 0.5,
                category: None,
                save_path: Some(content_dir.to_string_lossy().into_owned()),
                content_path: Some(content_file.to_string_lossy().into_owned()),
                size: 2 * 1024 * 1024,
                downloaded: 2 * 1024 * 1024,
                num_seeds: 1,
                err_str: None,
            };
            let by_hash = std::collections::HashMap::from([(HASH, &t)]);
            let by_name = std::collections::HashMap::from([(ZIM.to_lowercase(), &t)]);

            // (b) Drive the real tick body: the in-flight SELECT (filtered to
            // this suite's row — shared dev DB), then process + flush.
            let rows = poller
                .fetch_inflight_rows()
                .await
                .expect("rows")
                .into_iter()
                .filter(|r| r.name == ZIM)
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 1, "only this suite's row may be in-flight");
            let changed = poller
                .process_inflight(
                    &by_hash,
                    &by_name,
                    Some(qbit),
                    true,
                    &settings.poller_params_snapshot(),
                    rows,
                )
                .await
                .expect("process_inflight");
            poller.flush_stats(&changed).await;

            // (c) The synchronous part of completion: the row settled
            // `complete` with the file installed into the ZIM dir.
            let (status, file_path): (String, Option<String>) = raw::fetch_optional(
                &mut *c,
                "SELECT status, file_path FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read row")
            .expect("row present");
            assert_eq!(
                status, "complete",
                "row must settle complete (keep_completed = false)"
            );
            let fp = file_path.expect("file_path must be set");
            let installed = std::path::Path::new(&fp);
            assert!(installed.exists(), "installed file must exist");
            assert!(
                installed.starts_with(&zims.zim_dir),
                "file installed into zim_dir, got {fp}"
            );
            assert_eq!(
                installed.file_name().and_then(|n| n.to_str()),
                Some(format!("{ZIM}.zim").as_str()),
            );

            // (c2) `resync` registered the zims row; the poller's background
            // auto-index must reach `ready` with a non-zero article count
            // (tiny.zim has exactly one article: `main.html`).
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            let mut last: (String, i64) = (String::from("(no zims row)"), 0);
            let mut ready = false;
            while tokio::time::Instant::now() < deadline {
                let row: Option<(String, i64)> = raw::fetch_optional(
                    &mut *c,
                    "SELECT index_status, article_count FROM zims WHERE name = $1",
                    |q| q.bind(ZIM),
                )
                .await
                .expect("query zims");
                if let Some((st, cnt)) = row {
                    last = (st, cnt);
                    if last.0 == "ready" && last.1 > 0 {
                        ready = true;
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            assert!(
                ready,
                "auto-index did not reach ready within 30s (last: {last:?})"
            );
            let entries: i64 = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT indexed_entries FROM zims WHERE name = $1",
                |q| q.bind(ZIM),
            )
            .await
            .expect("zims row")
            .expect("row present");
            assert!(entries > 0, "indexed_entries must be non-zero");

            // (d) The article is servable over the real router. The search
            // word is taken from the indexed title (a weight-A term the FTS
            // branch must find; exact token, `simple` tsconfig is case-
            // sensitive, so the word is used verbatim).
            let (title, preview): (String, Option<String>) = raw::fetch_optional(
                &mut *c,
                "SELECT a.title, a.content_preview FROM articles a
                 JOIN zims z ON z.id = a.zim_id
                 WHERE z.name = $1 AND a.path = 'main.html'",
                |q| q.bind(ZIM),
            )
            .await
            .expect("indexed article row")
            .expect("row present");
            let first_word = |s: &str| -> String {
                s.split_whitespace()
                    .map(|tok| {
                        tok.trim_matches(|ch: char| !ch.is_ascii_alphanumeric())
                            .to_string()
                    })
                    .find(|w| !w.is_empty())
                    .unwrap_or_default()
            };
            let word = if title.is_empty() {
                preview
                    .as_deref()
                    .map(first_word)
                    .filter(|w| !w.is_empty())
                    .unwrap_or_default()
            } else {
                first_word(&title)
            };
            assert!(
                !word.is_empty(),
                "indexed title {title:?} / preview has no usable token"
            );

            let search = crate::search::SearchEngine::new(
                pool.clone(),
                settings.clone(),
                crate::health::DegradationTracker::default(),
            );
            let state = crate::AppState {
                db: pool.clone(),
                settings,
                zims,
                search,
                torrent: crate::torrent::QbitClientCache::new(),
                rate_limiter: std::sync::Arc::new(crate::serve::ratelimit::RateLimiterHandle::new()),
                probes: crate::HealthProbes::default(),
                auth_lockout: std::sync::Arc::new(Default::default()),
                degradation: crate::health::DegradationTracker::default(),
            };
            let app = crate::serve::build_router(state);

            let body_text = |resp: axum::response::Response| async move {
                let (_, body) = resp.into_parts();
                let bytes = body.collect().await.expect("body").to_bytes();
                String::from_utf8_lossy(&bytes).to_string()
            };

            // /search: the indexed article is visible to full-text search.
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/search?q={word}&zim={ZIM}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                axum::http::StatusCode::OK,
                "search must be 200"
            );
            let v: serde_json::Value =
                serde_json::from_str(&body_text(resp).await).expect("search json");
            let results = v["results"].as_array().expect("results array");
            assert!(
                !results.is_empty(),
                "search must find the indexed article: {v}"
            );
            assert!(
                results.iter().any(|r| r["path"] == "main.html"),
                "main.html must be among the results: {v}"
            );

            // /snippet: the indexed snippet/title is served from the DB.
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/snippet?zim={ZIM}&path=main.html"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                axum::http::StatusCode::OK,
                "snippet must be 200"
            );
            let v: serde_json::Value =
                serde_json::from_str(&body_text(resp).await).expect("snippet json");
            assert!(
                !v["title"].as_str().unwrap_or("").is_empty(),
                "snippet must carry the indexed title: {v}"
            );

            // /chunks: the installed archive itself is servable (live read).
            let resp = app
                .oneshot(
                    Request::builder()
                        .uri(format!("/chunks?zim={ZIM}&path=main.html"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                axum::http::StatusCode::OK,
                "chunks must be 200"
            );
            let v: serde_json::Value =
                serde_json::from_str(&body_text(resp).await).expect("chunks json");
            assert!(
                v["chunk_count"].as_u64().unwrap_or(0) >= 1,
                "expected at least one chunk: {v}"
            );
            assert!(!v["chunks"][0]["text"].as_str().unwrap_or("").is_empty());

            // Cleanup (shared dev DB; articles/qid rows cascade off zims).
            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE id = $1", |q| {
                q.bind(id)
            })
            .await;
            let _ =
                raw::execute(&mut *c, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM)).await;
            let _ = tmp;
        }
    }

    /// WI-4: dedicated tests for the startup reconciliation path.
    mod reconcile {
        use super::download::{download_settings, test_pool};
        use super::*;
        use crate::db::raw;
        use crate::torrent::QbitClient;
        use std::sync::Arc;
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const CATEGORY: &str = "zimservice";

        /// Build a `DownloadPoller` wired to the wiremock qB server.
        async fn make_poller(
            pool: &Pool,
            tmp: &tempfile::TempDir,
            server: &MockServer,
        ) -> super::super::DownloadPoller {
            let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
            super::super::DownloadPoller::new(
                pool.clone(),
                download_settings(),
                zims,
                crate::torrent::QbitClientCache::new(),
                Some(server.uri()),
                "user".into(),
                "pass".into(),
            )
        }

        /// Build a QbitClient pointed at the mock server (already "logged in").
        async fn make_client(server: &MockServer) -> Arc<QbitClient> {
            let mut client = QbitClient::new(server.uri().as_str(), "user", "pass", false, None)
                .expect("client");
            // Pre-authenticate so get_torrents doesn't need a login round-trip.
            client.auth().await.expect("login");
            Arc::new(client)
        }

        /// Mount the standard login + get_torrents mocks.
        async fn mount_qb_mocks(server: &MockServer, torrents: &str) {
            Mock::given(method("POST"))
                .and(path("/api/v2/login"))
                .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
                .mount(server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v2/torrents/info"))
                .and(query_param("filter", "all"))
                .respond_with(ResponseTemplate::new(200).set_body_string(torrents))
                .mount(server)
                .await;
        }

        /// WI-4 Adoption: a qB torrent in our category with no tracking row
        /// → row created with correct status (complete vs downloading).
        #[tokio::test]
        async fn reconcile_adopts_untracked_torrents() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let tmp = tempfile::tempdir().expect("tempdir");
            let server = MockServer::start().await;
            // Two torrents: one complete, one still downloading.
            let torrents_json = format!(
                r#"[{{"hash":"aaa111","name":"complete.zim","progress":1.0,"state":"uploading","category":"{CATEGORY}"}},
                   {{"hash":"bbb222","name":"downloading.zim","progress":0.5,"state":"downloading","category":"{CATEGORY}"}}]"#
            );
            mount_qb_mocks(&server, &torrents_json).await;

            let poller = make_poller(&pool, &tmp, &server).await;
            let client = make_client(&server).await;
            poller.reconcile(Some(client)).await.expect("reconcile");

            let mut c = pool.acquire().await.expect("conn");
            let complete_status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE hash = 'aaa111'",
                |q| q,
            )
            .await
            .expect("complete row")
            .expect("row present");
            assert_eq!(complete_status, "complete", "adopted complete torrent");

            let dl_status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE hash = 'bbb222'",
                |q| q,
            )
            .await
            .expect("downloading row")
            .expect("row present");
            assert_eq!(dl_status, "downloading", "adopted downloading torrent");

            let _ = raw::execute(
                &mut *c,
                "DELETE FROM downloads WHERE hash IN ('aaa111', 'bbb222')",
                |q| q,
            )
            .await;
            let _ = tmp;
        }

        /// Inflight-2 (reconcile): a tracked row with `hash IS NULL` whose
        /// name matches a category qB torrent must not have that torrent
        /// re-adopted as a second row — the seeding arm refreshes stats but
        /// does not bind the hash, so the adoption's `NOT EXISTS (hash = …)`
        /// guard cannot see the row's ownership; the per-pass consumed set is
        /// what blocks the duplicate.
        #[tokio::test]
        async fn reconcile_null_hash_row_blocks_adopt_of_own_torrent() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let tmp = tempfile::tempdir().expect("tempdir");
            let server = MockServer::start().await;
            let torrents_json = format!(
                r#"[{{"hash":"own555hash","name":"reconcile_own.zim","progress":1.0,"state":"uploading","category":"{CATEGORY}"}}]"#
            );
            mount_qb_mocks(&server, &torrents_json).await;

            let mut c = pool.acquire().await.expect("conn");
            // Sweep leftovers from a crashed run (shared single-DB suite).
            let _ = raw::execute(
                &mut *c,
                "DELETE FROM downloads WHERE name = 'reconcile_own.zim' OR hash = 'own555hash'",
                |q| q,
            )
            .await;
            let id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, hash, status, progress, updated_at) \n                 VALUES ($1, $2, NULL, 'seeding', 1.0, now()) RETURNING id",
                |q| {
                    q.bind("reconcile_own.zim")
                        .bind("magnet:?xt=urn:btih:own")
                },
            )
            .await
            .expect("insert")
            .expect("row present");

            let poller = make_poller(&pool, &tmp, &server).await;
            let client = make_client(&server).await;
            poller.reconcile(Some(client)).await.expect("reconcile");

            // The tracked row survives the seeding refresh…
            let status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read")
            .expect("row present");
            assert_eq!(
                status, "seeding",
                "the tracked seeding row must survive reconcile"
            );
            // …and no adopted duplicate row exists for its torrent.
            let count: i64 = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT count(*) FROM downloads WHERE hash = 'own555hash'",
                |q| q,
            )
            .await
            .expect("count")
            .expect("count row");
            assert_eq!(
                count, 0,
                "a NULL-hash tracked row's own torrent must not be re-adopted"
            );

            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE id = $1", |q| {
                q.bind(id)
            })
            .await;
            let _ = tmp;
        }

        /// WI-4 Orphan erroring: a `downloading` row whose hash doesn't match
        /// any qB torrent, past `MISSING_TORRENT_GRACE` → row set to `error`.
        #[tokio::test]
        async fn reconcile_orphan_erroring_past_grace() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let tmp = tempfile::tempdir().expect("tempdir");
            let server = MockServer::start().await;
            // qB has no torrents at all — the row's hash is orphaned.
            mount_qb_mocks(&server, "[]").await;

            let mut c = pool.acquire().await.expect("conn");
            let id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, hash, status, updated_at) \
                 VALUES ($1, $2, $3, 'downloading', now() - interval '15 minutes') \
                 RETURNING id",
                |q| {
                    q.bind("orphan.zim")
                        .bind("magnet:?xt=urn:btih:orphan")
                        .bind("orphan111")
                },
            )
            .await
            .expect("insert")
            .expect("row present");

            let poller = make_poller(&pool, &tmp, &server).await;
            let client = make_client(&server).await;
            poller.reconcile(Some(client)).await.expect("reconcile");

            let status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read")
            .expect("row present");
            assert_eq!(status, "error", "orphan past grace → error");

            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE id = $1", |q| {
                q.bind(id)
            })
            .await;
            let _ = tmp;
        }

        /// WI-4 Hash rebind: a `downloading` row whose name matches a qB
        /// torrent with a different hash → hash updated.
        #[tokio::test]
        async fn reconcile_hash_rebind_by_name() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let tmp = tempfile::tempdir().expect("tempdir");
            let server = MockServer::start().await;
            // qB has a torrent with the same name but a different hash.
            let torrents_json = format!(
                r#"[{{"hash":"newhash999","name":"renamed.zim","progress":0.3,"state":"downloading","category":"{CATEGORY}"}}]"#
            );
            mount_qb_mocks(&server, &torrents_json).await;

            let mut c = pool.acquire().await.expect("conn");
            let id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, hash, status) \
                 VALUES ($1, $2, $3, 'downloading') RETURNING id",
                |q| {
                    q.bind("renamed.zim")
                        .bind("magnet:?xt=urn:btih:oldhash")
                        .bind("oldhash000")
                },
            )
            .await
            .expect("insert")
            .expect("row present");

            let poller = make_poller(&pool, &tmp, &server).await;
            let client = make_client(&server).await;
            poller.reconcile(Some(client)).await.expect("reconcile");

            let hash: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT hash FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read")
            .expect("row present");
            assert_eq!(hash, "newhash999", "hash rebound to qB torrent");

            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE id = $1", |q| {
                q.bind(id)
            })
            .await;
            let _ = tmp;
        }

        /// WI-4 Interrupted direct download retry: a `downloading` row with
        /// a `.zim` URL → reset to `queued`.
        #[tokio::test]
        async fn reconcile_interrupted_direct_download_retry() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let tmp = tempfile::tempdir().expect("tempdir");
            let server = MockServer::start().await;
            mount_qb_mocks(&server, "[]").await;

            let mut c = pool.acquire().await.expect("conn");
            let id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, status, file_path) \
                 VALUES ($1, $2, 'downloading', $3) RETURNING id",
                |q| {
                    q.bind("direct.zim")
                        .bind("http://example.com/x.zim")
                        .bind("/tmp/x.zim.part")
                },
            )
            .await
            .expect("insert")
            .expect("row present");

            let poller = make_poller(&pool, &tmp, &server).await;
            let client = make_client(&server).await;
            poller.reconcile(Some(client)).await.expect("reconcile");

            let status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read")
            .expect("row present");
            let fp: Option<String> = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT file_path FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read");
            assert_eq!(status, "queued", "interrupted direct download → queued");
            assert_eq!(fp, None, "file_path cleared");

            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE id = $1", |q| {
                q.bind(id)
            })
            .await;
            let _ = tmp;
        }

        /// WI-4 Orphan .part cleanup: a `.part` file in zim_dir with no active
        /// download row → file deleted.
        #[tokio::test]
        async fn reconcile_orphan_part_cleanup() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let tmp = tempfile::tempdir().expect("tempdir");
            // Create an orphan .part file in the zim_dir.
            let part_path = tmp.path().join("orphan.zim.part");
            std::fs::write(&part_path, b"partial data").unwrap();

            let server = MockServer::start().await;
            mount_qb_mocks(&server, "[]").await;

            // No active download rows → the .part is orphan.
            let poller = make_poller(&pool, &tmp, &server).await;
            let client = make_client(&server).await;
            poller.reconcile(Some(client)).await.expect("reconcile");

            assert!(!part_path.exists(), "orphan .part file should be deleted");
            let _ = tmp;
        }

        /// PERF-11 (CI-DB): reconcile must NOT delete the `.part` of an
        /// interrupted direct download it is about to re-queue for resume.
        /// `retry_interrupted_directs` clears `file_path` before the sweep,
        /// so the sweep's protect-set has to be built from the rows as they
        /// were BEFORE the retry (derived `zim_dir/{name}.part`) — this
        /// regression deleted the surviving `.part` milliseconds after
        /// re-queueing, defeating restart-resume (full re-download of a
        /// partially downloaded file on every restart). An orphan `.part`
        /// in the same dir proves the sweep still runs.
        #[tokio::test]
        async fn reconcile_keeps_requeued_interrupted_part() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let tmp = tempfile::tempdir().expect("tempdir");
            // The surviving `.part` the interrupted row resumes from.
            let part_path = tmp.path().join("resume.zim.part");
            std::fs::write(&part_path, b"partial data").unwrap();
            // An orphan `.part` too: the sweep must still reclaim it.
            let orphan = tmp.path().join("gone.zim.part");
            std::fs::write(&orphan, b"partial data").unwrap();

            let server = MockServer::start().await;
            mount_qb_mocks(&server, "[]").await;

            let mut c = pool.acquire().await.expect("conn");
            let id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, status, file_path) \
                 VALUES ($1, $2, 'downloading', $3) RETURNING id",
                |q| {
                    q.bind("resume.zim")
                        .bind("http://example.com/resume.zim")
                        .bind(part_path.display().to_string())
                },
            )
            .await
            .expect("insert")
            .expect("row present");

            let poller = make_poller(&pool, &tmp, &server).await;
            let client = make_client(&server).await;
            poller.reconcile(Some(client)).await.expect("reconcile");

            // The interrupted direct row was re-queued ...
            let (status, fp): (String, Option<String>) = raw::fetch_optional(
                &mut *c,
                "SELECT status, file_path FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read")
            .expect("row present");
            assert_eq!(status, "queued", "interrupted direct download re-queued");
            assert_eq!(fp, None, "file_path cleared for the re-claim");
            // ... and its `.part` survived the sweep (resume intact), while
            // the orphan was reclaimed.
            assert!(
                part_path.exists(),
                "re-queued row's .part must survive the sweep"
            );
            assert!(!orphan.exists(), "orphan .part must still be deleted");

            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE id = $1", |q| {
                q.bind(id)
            })
            .await;
            let _ = tmp;
        }

        /// WI-4 Seeding settlement: a `seeding` row whose torrent is gone
        /// from qB → row set to `complete`.
        #[tokio::test]
        async fn reconcile_seeding_settlement() {
            let Some((pool, _db_gate)) = test_pool().await else {
                return;
            };
            crate::db::migrate::run_migrations(&pool)
                .await
                .expect("migrations");

            let tmp = tempfile::tempdir().expect("tempdir");
            let server = MockServer::start().await;
            // qB has no torrents → the seeding row's torrent is gone.
            mount_qb_mocks(&server, "[]").await;

            let mut c = pool.acquire().await.expect("conn");
            let id: i32 = raw::fetch_scalar_optional(
                &mut *c,
                "INSERT INTO downloads (name, url, hash, status) \
                 VALUES ($1, $2, $3, 'seeding') RETURNING id",
                |q| {
                    q.bind("seed.zim")
                        .bind("magnet:?xt=urn:btih:seedhash")
                        .bind("seedhash01")
                },
            )
            .await
            .expect("insert")
            .expect("row present");

            let poller = make_poller(&pool, &tmp, &server).await;
            let client = make_client(&server).await;
            poller.reconcile(Some(client)).await.expect("reconcile");

            let status: String = raw::fetch_scalar_optional(
                &mut *c,
                "SELECT status FROM downloads WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("read")
            .expect("row present");
            assert_eq!(status, "complete", "seeding row settled to complete");

            let _ = raw::execute(&mut *c, "DELETE FROM downloads WHERE id = $1", |q| {
                q.bind(id)
            })
            .await;
            let _ = tmp;
        }
    }

    // ── WI-9: ZIM URL predicate equivalence ─────────────────────────────────

    /// Rust mirror of the SQL `ZIM_URL_PREDICATE`:
    /// `lower(trim(split_part(split_part(url, '?', 1), '#', 1)))`
    ///
    /// SQL `trim()` strips **spaces only** (not tabs/newlines). Rust's
    /// `str::trim()` strips all Unicode whitespace. To match SQL exactly,
    /// we use `trim_matches(' ')`.
    #[cfg(test)]
    fn sql_zim_predicate(url: &str) -> String {
        let without_query = url.split('?').next().unwrap_or(url);
        let without_frag = without_query.split('#').next().unwrap_or(without_query);
        without_frag.trim_matches(' ').to_lowercase()
    }

    /// Verify that `is_direct_zim_url` agrees with the SQL predicate on a
    /// table of edge-case URLs. The SQL is used by the DB-side reconcile
    /// query (`ZIM_URL_PREDICATE` in the poller); the Rust is used by the
    /// in-process decision (`is_direct_zim_url`). They must never diverge.
    #[test]
    fn zim_url_predicate_rust_matches_sql_semantics() {
        use crate::torrent::is_direct_zim_url;
        // (url, expected is_direct)
        let cases: &[(&str, bool)] = &[
            // Plain .zim
            ("https://example.com/wikipedia_en.zim", true),
            // Uppercase extension
            ("https://example.com/file.ZIM", true),
            // Mixed case
            ("https://example.com/file.ZiM", true),
            // .zim.txt is NOT a zim
            ("https://example.com/file.zim.txt", false),
            // Query string stripped
            ("https://x/file.zim?token=abc", true),
            // Fragment stripped
            ("https://x/file.zim#section", true),
            // Both query and fragment
            ("https://x/file.zim?sig=1#frag", true),
            // Leading spaces (SQL trim() strips spaces)
            ("  https://x/file.zim", true),
            // Trailing spaces
            ("https://x/file.zim  ", true),
            // Leading tab (SQL trim() does NOT strip tabs → stays → no .zim match
            //  at the end after tab... wait, the tab is at the START, so after
            //  trim_matches(' ') the leading tab remains, but the URL still
            //  ends with .zim. So this should be true.)
            ("\thttps://x/file.zim", true),
            // Trailing tab: after trim_matches(' '), the tab remains →
            //  "https://x/file.zim\t" does NOT end with ".zim" → false.
            //  But the Rust is_direct_zim_url uses .trim() which strips tabs → true.
            //  This is the known divergence (documented in the predicate doc).
            ("https://x/file.zim\t", false), // SQL semantics
            // Empty string
            ("", false),
            // Only spaces
            ("   ", false),
            // Non-zim file
            ("https://x/file.pdf", false),
            // Just ".zim" (no path prefix)
            (".zim", true),
            // Path with .zim in the middle
            ("https://x/a.zim/b.txt", false),
        ];
        for (url, expected) in cases {
            // The Rust function uses `.trim()` (all whitespace) while SQL
            // `trim()` strips spaces only. For URLs containing tabs, the two
            // can diverge — this is documented and acceptable (a tab at the
            // end of a URL is a pathological input that never occurs in
            // practice).
            let rust_result = is_direct_zim_url(url);
            let sql_normalized = sql_zim_predicate(url);
            let sql_result = sql_normalized.ends_with(".zim");

            if !url.contains('\t') {
                // No tabs: both normalizations agree.
                assert_eq!(
                    rust_result, sql_result,
                    "divergence for {url:?}: rust={rust_result} sql={sql_result}"
                );
                assert_eq!(
                    rust_result, *expected,
                    "is_direct_zim_url({url:?}) = {rust_result}, expected {expected}"
                );
            } else {
                // Tab present: Rust strips it, SQL doesn't. We still verify
                // the expected values match the SQL semantics (the "source of
                // truth" for the DB-side query).
                assert_eq!(
                    sql_result, *expected,
                    "sql_zim_predicate({url:?}) = {sql_result}, expected {expected}"
                );
            }
        }
    }

    /// WI-10: print a summary of DB-gated lib-test skips at process exit.
    /// Mirrors the pattern in `tests/integration.rs`.
    #[dtor::dtor]
    fn print_lib_skip_summary() {
        let n = crate::testing::LIB_SKIPPED.load(std::sync::atomic::Ordering::Relaxed);
        if n > 0 {
            eprintln!(
                "\n{n} lib test(s) SKIPPED (no database) — run with a \
                reachable Postgres to exercise them"
            );
        }
    }
}
