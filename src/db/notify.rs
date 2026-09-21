//! Cross-process cache invalidation: Postgres `LISTEN`/`NOTIFY` on two
//! application channels.
//!
//! **Why**: a `serve` process keeps in-memory caches (settings, ZIM catalog)
//! that peer processes can stale — a second instance (the
//! `ZIMSERVICE_ALLOW_MULTI_INSTANCE=1` opt-out) or a mutating CLI subcommand
//! (`index`, `embed`, `list --sync`, which holds the advisory lock only while
//! the server is *stopped* — not against a running one when the full opt-out
//! is set) writes settings rows or the `zims` table, and every other process
//! keeps serving its copy until its own resync / restart. Before this module
//! the staleness bound was "until the next local resync or process restart";
//! now a connected peer invalidates within one `NOTIFY` round-trip.
//!
//! **Channels** (constants are the single source of truth — producers and
//! the listener must agree on the exact spelling):
//! - [`crate::db::notify::SETTINGS_CHANNEL`] — settings rows changed
//!   (`SettingsCache::update`, the seed in `SettingsCache::reload`, and the
//!   lazy admin-password upgrade in `settings::auth_service`). Consumer: the
//!   `on_settings` closure wired at the composition root (`startup.rs`) —
//!   a full `SettingsCache::reload()` re-read of the whole map.
//! - [`crate::db::notify::CATALOG_CHANNEL`] — the ZIM catalog changed
//!   (install finalize in the download poller, the startup/directory
//!   `resync` persist). Consumer: the `on_catalog` closure wired at the
//!   composition root (`startup.rs`) — `ZimManager::resync()` (re-scan +
//!   reconcile + persist; idempotent).
//!
//! **Listener topology**: the listener runs on a **dedicated max-1 pool** —
//! its own long-lived connection, never a checkout from the application
//! pool. A pooled connection is recycled by `max_lifetime` (NAT/socket
//! hardening), which would silently kill a `LISTEN` session; and parking a
//! pool slot on the listener would shrink the app's effective pool size.
//! The dedicated pool therefore disables lifetime recycling entirely
//! (`max_lifetime(None)` / `idle_timeout(None)` — mirroring sqlx's own
//! `PgListener::connect`) and is dropped with the listener (closing the
//! session).
//!
//! **Reconnect + backoff**: the session is re-established on every loss
//! (Postgres restart, network partition, backend terminate) with exponential
//! backoff 1s → 30s cap, reset on the first successful (re)connect.
//!
//! **Missed-notification recovery**: `LISTEN`/`NOTIFY` is fire-and-forget —
//! a notification fired while the session is down is gone for good. The
//! (re)connect therefore always triggers a full resync of both caches, so
//! the staleness bound degrades gracefully to "one reconnect" instead of
//! "until the next local resync".
//!
//! **Self-notify**: our own `NOTIFY`s (same database) also arrive on our
//! listener. That is harmless by construction: both consumers are idempotent
//! (a settings `reload()` re-reads the map we just wrote; a `resync()` with
//! an unchanged disk snapshot early-outs without persisting — and without
//! firing another `NOTIFY`, so there is no amplification loop).
//!
//! **Wire semantics**: Postgres delivers a `NOTIFY` only after the firing
//! transaction commits. Transactional producers fire `NOTIFY` as the last
//! statement inside the transaction (delivered at commit). Producers whose
//! write is a single autocommit statement (owned by the lifecycle helper)
//! fire it as an immediate follow-up statement — which can only run after
//! the write is durable, so a notification can never fire before its write
//! commits. A `NOTIFY` failure is best-effort: it degrades to the
//! pre-LISTEN/NOTIFY staleness bounds (the next local resync / restart).
//!
//! **Fail-closed supervision**: a listener task dying unexpectedly would
//! silently degrade freshness across all peers with no signal — the same
//! failure class the advisory-lock liveness monitor treats as fatal
//! (`startup.rs`: log + `process::exit(1)`). Each listener task reports its
//! death (a drop-guard, so panics are covered too); a supervisor that
//! observes a death before the graceful-stop flag is set exits the process.
//! Dropping the last [`crate::db::notify::NotifyListener`] handle sets the
//! stop flag **before** aborting the tasks, so a clean shutdown never trips
//! the supervisor.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgListener, PgPoolOptions};

use crate::db::pool;
use crate::error::{Error, Result};

/// Settings-invalidation channel: settings rows changed.
pub const SETTINGS_CHANNEL: &str = "zimservice_settings";
/// Catalog-invalidation channel: the ZIM catalog (`zims` table) changed.
pub const CATALOG_CHANNEL: &str = "zimservice_catalog";
/// Fire a settings invalidation (use as the `sql` for a `db::raw` execute —
/// keep the statement in one constant so producers and the listener cannot
/// drift). Transactional producers execute this as the last statement of
/// their settings transaction; autocommit producers execute it immediately
/// after their write.
pub const NOTIFY_SETTINGS_SQL: &str = "NOTIFY zimservice_settings";
/// Fire a catalog invalidation (see [`NOTIFY_SETTINGS_SQL`] for the
/// transactional vs. autocommit placement rule).
pub const NOTIFY_CATALOG_SQL: &str = "NOTIFY zimservice_catalog";
/// `application_name` of the listener's dedicated session (identifies it in
/// `pg_stat_activity` for operability).
pub const LISTENER_APPLICATION_NAME: &str = "zimservice-notify";

/// Reconnect backoff: double after each failed attempt, capped.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Next backoff: doubled, capped at 30s (1s, 2s, 4s, …, 30s, 30s, …).
fn next_backoff(current: Duration) -> Duration {
    let doubled = current.saturating_mul(2);
    if doubled > MAX_BACKOFF {
        MAX_BACKOFF
    } else {
        doubled
    }
}

/// Listener connectivity, as surfaced by [`NotifyStatusSnapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ListenerState {
    /// The dedicated `LISTEN` session is live and receiving notifications.
    Connected,
    /// No live `LISTEN` session: the initial connect is in progress, or the
    /// session dropped and a reconnect is being attempted with backoff.
    /// Peers degrade to the pre-LISTEN/NOTIFY staleness bounds meanwhile.
    Reconnecting,
}

/// Operator-facing snapshot of the invalidation listener (the `/diagnostic`
/// field). Counters are process-lifetime.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct NotifyStatusSnapshot {
    /// Session connectivity.
    pub state: ListenerState,
    /// `zimservice_settings` notifications received (excludes the
    /// connect-triggered resyncs, which are counted separately).
    pub settings_notifications: u64,
    /// `zimservice_catalog` notifications received (same convention).
    pub catalog_notifications: u64,
    /// Last `zimservice_settings` notification (absent until the first one).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_settings_at: Option<DateTime<Utc>>,
    /// Last `zimservice_catalog` notification (absent until the first one).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_catalog_at: Option<DateTime<Utc>>,
    /// Full cache resyncs triggered by (re)connects — the missed-notification
    /// recovery (see the module docs). Includes the initial connect.
    pub resyncs: u64,
    /// Session losses observed (the reconnect trigger, distinct from
    /// connect *failures* while offline).
    pub disconnects: u64,
}

#[derive(Debug)]
struct StatusInner {
    state: ListenerState,
    settings_notifications: u64,
    catalog_notifications: u64,
    last_settings_at: Option<DateTime<Utc>>,
    last_catalog_at: Option<DateTime<Utc>>,
    resyncs: u64,
    disconnects: u64,
}

impl Default for StatusInner {
    fn default() -> Self {
        Self {
            // Starts `reconnecting`: the first (re)connect happens in the
            // spawned listener task; a connected session flips it.
            state: ListenerState::Reconnecting,
            settings_notifications: 0,
            catalog_notifications: 0,
            last_settings_at: None,
            last_catalog_at: None,
            resyncs: 0,
            disconnects: 0,
        }
    }
}

/// Process-lifetime listener status. Shared behind an `Arc` (never cloned:
/// the inner `Mutex` is not `Clone`); the mutex is only ever held for a
/// counter bump (no I/O, no awaits).
#[derive(Debug, Default)]
pub struct NotifyStatus {
    inner: Mutex<StatusInner>,
}
impl NotifyStatus {
    fn lock(&self) -> std::sync::MutexGuard<'_, StatusInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The session (re)established.
    pub(crate) fn mark_connected(&self) {
        self.lock().state = ListenerState::Connected;
    }

    /// The session is down (connect failure or loss while reconnecting).
    pub(crate) fn mark_reconnecting(&self) {
        self.lock().state = ListenerState::Reconnecting;
    }

    /// Count a settings-channel notification; returns the running total.
    pub(crate) fn note_settings_notification(&self) -> u64 {
        let mut g = self.lock();
        g.settings_notifications += 1;
        g.last_settings_at = Some(Utc::now());
        g.settings_notifications
    }

    /// Count a catalog-channel notification; returns the running total.
    pub(crate) fn note_catalog_notification(&self) -> u64 {
        let mut g = self.lock();
        g.catalog_notifications += 1;
        g.last_catalog_at = Some(Utc::now());
        g.catalog_notifications
    }

    /// Count a (re)connect-triggered resync (missed-notification recovery).
    pub(crate) fn note_resync(&self) {
        self.lock().resyncs += 1;
    }

    /// Count a session loss (the reconnect trigger).
    pub(crate) fn note_disconnect(&self) {
        let mut g = self.lock();
        g.disconnects += 1;
        g.state = ListenerState::Reconnecting;
    }

    /// Snapshot for `/diagnostic`.
    pub fn snapshot(&self) -> NotifyStatusSnapshot {
        let g = self.lock();
        NotifyStatusSnapshot {
            state: g.state,
            settings_notifications: g.settings_notifications,
            catalog_notifications: g.catalog_notifications,
            last_settings_at: g.last_settings_at,
            last_catalog_at: g.last_catalog_at,
            resyncs: g.resyncs,
            disconnects: g.disconnects,
        }
    }
}

/// Handle to the spawned invalidation listener.
///
/// Cheap to clone (an `Arc`); `AppState` holds the primary handle and
/// handlers clone it per request. Dropping the **last** handle is a graceful
/// stop: the stop flag is set *first* (the supervisor then treats the tasks'
/// death as clean instead of fail-closing), then every listener task is
/// aborted, which closes the dedicated `LISTEN` session.
#[derive(Clone)]
pub struct NotifyListener {
    inner: Arc<NotifyListenerInner>,
}

struct NotifyListenerInner {
    status: Arc<NotifyStatus>,
    /// Graceful-stop flag: set by `Drop` **before** the tasks are aborted,
    /// so a shutdown is never mistaken for an unexpected death.
    stop: tokio::sync::watch::Sender<bool>,
    /// All listener tasks (the supervisor + the three worker tasks). The
    /// supervisor is in the vec too: aborting it on drop prevents a
    /// post-shutdown death report from reaching it.
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Drop for NotifyListenerInner {
    fn drop(&mut self) {
        // Order matters (see the struct docs): flag first, aborts second.
        let _ = self.stop.send(true);
        let mut handles = self.handles.lock().unwrap_or_else(|p| p.into_inner());
        for handle in handles.drain(..) {
            handle.abort();
        }
    }
}

impl NotifyListener {
    /// Snapshot of the listener state for `/diagnostic`.
    pub fn status(&self) -> NotifyStatusSnapshot {
        self.inner.status.snapshot()
    }
}

/// Per-bump invalidation action. The persistence layer is domain-agnostic:
/// the composition root (`startup.rs`) wires these to the domain caches —
/// the settings channel drives a `SettingsCache::reload()`, the catalog
/// channel a `ZimManager::resync()`. One call per coalesced bump: the watch
/// channel collapses a burst of notifications into a single call, which is
/// exactly the desired behavior since both domain actions are idempotent
/// (each re-reads everything).
type OnBump = Box<dyn FnMut(u64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// The bump-driven subscriber loop shared by both subscriber tasks: run
/// `action` once per coalesced bump until the sender is dropped (graceful
/// stop, which also aborts the task).
async fn run_on_bumps(mut rx: tokio::sync::watch::Receiver<u64>, mut action: OnBump) {
    while rx.changed().await.is_ok() {
        let bump = *rx.borrow_and_update();
        action(bump).await;
    }
}

/// Spawn the cross-process invalidation listener (see the module docs).
///
/// `on_settings` / `on_catalog` are the composition-root invalidation
/// actions (see the module docs); they are moved into the spawned subscriber
/// tasks. The listener starts in
/// [`ListenerState::Reconnecting`]: the first connect (and its full resync)
/// happens inside the spawned task, so this never serializes startup on the
/// database and cannot fail it — a listener that never connects degrades to
/// the pre-LISTEN/NOTIFY staleness bounds (next local resync / restart) and
/// reports `reconnecting` in `/diagnostic`.
pub fn spawn_listener(
    database_url: &str,
    on_settings: OnBump,
    on_catalog: OnBump,
) -> NotifyListener {
    spawn_listener_as(
        database_url,
        on_settings,
        on_catalog,
        LISTENER_APPLICATION_NAME,
    )
}

/// [`spawn_listener`] with an explicit `application_name` for the dedicated
/// session — the test seam: per-test unique tags let a test find and
/// terminate the listener's backend in `pg_stat_activity` to exercise the
/// reconnect path.
pub(crate) fn spawn_listener_as(
    database_url: &str,
    on_settings: OnBump,
    on_catalog: OnBump,
    application_name: &str,
) -> NotifyListener {
    let status = Arc::new(NotifyStatus::default());
    // Bump-on-notify channels: the workers only signal, the subscribers own
    // the work. A dropped subscriber (its receiver gone) turns `send_replace`
    // into a no-op; a queued value coalesces (watch semantics) — a burst of
    // notifications collapses into one reload, which is exactly the desired
    // behavior (reload is idempotent and re-reads everything).
    let (settings_tx, settings_rx) = tokio::sync::watch::channel(0u64);
    let (catalog_tx, catalog_rx) = tokio::sync::watch::channel(0u64);
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    // Death reports: each worker reports its death (a drop-guard, so a
    // panicking worker is covered too) and the supervisor decides
    // clean-stop vs. fail-closed exit.
    let (report_tx, report_rx) = tokio::sync::mpsc::unbounded_channel::<&'static str>();

    let url = database_url.to_string();
    let app_name = application_name.to_string();
    let listen_status = Arc::clone(&status);
    // One report-sender per spawned task (the original rides in the listen
    // task; each other task gets an explicit clone).
    let settings_report = report_tx.clone();
    let catalog_report = report_tx.clone();
    let supervisor_report = report_tx.clone();
    let listen_task = tokio::spawn(async move {
        let _report = DeathReporter("listen_loop", report_tx);
        listen_loop(url, listen_status, settings_tx, catalog_tx, app_name).await;
    });

    let settings_task = tokio::spawn(async move {
        let _report = DeathReporter("settings_subscriber", settings_report);
        run_on_bumps(settings_rx, on_settings).await;
    });

    let catalog_task = tokio::spawn(async move {
        let _report = DeathReporter("catalog_subscriber", catalog_report);
        run_on_bumps(catalog_rx, on_catalog).await;
    });

    let supervisor_task = tokio::spawn(async move {
        // The report senders in the spawned workers outlive this clone;
        // drop ours so the channel closes once every worker is dead.
        drop(supervisor_report);
        supervise(report_rx, stop_rx).await;
    });

    NotifyListener {
        inner: Arc::new(NotifyListenerInner {
            status,
            stop: stop_tx,
            handles: Mutex::new(vec![
                listen_task,
                settings_task,
                catalog_task,
                supervisor_task,
            ]),
        }),
    }
}

/// Drop-guard death reporter: sends the task's name to the supervisor when
/// the task terminates — for **any** reason, including a panic mid-body
/// (unwinding drops the guard).
struct DeathReporter(
    &'static str,
    tokio::sync::mpsc::UnboundedSender<&'static str>,
);

impl Drop for DeathReporter {
    fn drop(&mut self) {
        // The receiver is gone only after a full clean shutdown (the
        // supervisor returned) — ignore that send failure.
        let _ = self.1.send(self.0);
    }
}

/// Fail-closed supervision (see the module docs): the first worker death
/// observed **before** the graceful-stop flag is set exits the process,
/// because a dead listener silently degrades cache freshness across all
/// peers with no signal. After the stop flag is set, remaining reports are
/// drained (the workers are being aborted by `NotifyListenerInner::drop`);
/// the supervisor also exits when every report sender is gone.
async fn supervise(
    mut report_rx: tokio::sync::mpsc::UnboundedReceiver<&'static str>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    while let Some(name) = report_rx.recv().await {
        if !*stop.borrow() {
            tracing::error!(
                task = name,
                "cross-process invalidation listener task died unexpectedly — without it, \
                 peer instances' settings/catalog writes stay invisible until the next \
                 local resync or restart (silent freshness degradation). Exiting to fail \
                 closed (advisory-lock-monitor precedent)."
            );
            std::process::exit(1);
        }
        // Stop flag set: graceful shutdown in progress — drain the rest.
    }
}

/// The LISTEN session loop: (re)connect with backoff, receive notifications,
/// bump the subscriber channels. Never returns normally — it exits only via
/// cancellation (graceful stop) or a death report (supervisor fail-close).
async fn listen_loop(
    url: String,
    status: Arc<NotifyStatus>,
    settings_tx: tokio::sync::watch::Sender<u64>,
    catalog_tx: tokio::sync::watch::Sender<u64>,
    application_name: String,
) {
    let mut backoff = INITIAL_BACKOFF;
    // Running notification counters — the watch channels carry only a
    // monotonic "something happened" bump (watch coalesces; subscribers
    // reload everything, so the exact count never matters to them).
    let mut settings_bump: u64 = 0;
    let mut catalog_bump: u64 = 0;

    loop {
        match connect_listener(&url, &application_name).await {
            Ok(mut listener) => {
                status.mark_connected();
                backoff = INITIAL_BACKOFF;
                // (Re)connect = missed-notification recovery (module docs):
                // every (re)connect triggers a full resync of both caches.
                status.note_resync();
                settings_bump += 1;
                catalog_bump += 1;
                let _ = settings_tx.send_replace(settings_bump);
                let _ = catalog_tx.send_replace(catalog_bump);

                loop {
                    match listener.try_recv().await {
                        Ok(Some(notification)) => {
                            match notification.channel() {
                                SETTINGS_CHANNEL => {
                                    let n = status.note_settings_notification();
                                    settings_bump += 1;
                                    let _ = settings_tx.send_replace(settings_bump);
                                    tracing::debug!(count = n, "{SETTINGS_CHANNEL} notification");
                                }
                                CATALOG_CHANNEL => {
                                    let n = status.note_catalog_notification();
                                    catalog_bump += 1;
                                    let _ = catalog_tx.send_replace(catalog_bump);
                                    tracing::debug!(count = n, "{CATALOG_CHANNEL} notification");
                                }
                                other => {
                                    // We only `LISTEN` the two zimservice
                                    // channels, but a same-named channel
                                    // from another tool would be ignored
                                    // rather than misrouted.
                                    tracing::debug!(
                                        "ignoring notification on non-zimservice channel {other}"
                                    );
                                }
                            }
                            backoff = INITIAL_BACKOFF;
                        }
                        Ok(None) => {
                            // The session dropped and `PgListener` already
                            // re-established it and re-`LISTEN`ed (eager
                            // reconnect) — the drop is the loss event, and
                            // the reconnect is the recovery point.
                            status.note_disconnect();
                            status.mark_connected();
                            status.note_resync();
                            settings_bump += 1;
                            catalog_bump += 1;
                            let _ = settings_tx.send_replace(settings_bump);
                            let _ = catalog_tx.send_replace(catalog_bump);
                            backoff = INITIAL_BACKOFF;
                        }
                        Err(e) => {
                            // Even the eager reconnect failed — full
                            // reconnect with backoff below.
                            status.mark_reconnecting();
                            tracing::warn!(
                                "LISTEN/NOTIFY session lost ({e}) — reconnecting with backoff; \
                                 caches resync on reconnect"
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                status.mark_reconnecting();
                tracing::warn!("LISTEN/NOTIFY session connect failed: {e} — retrying with backoff");
            }
        }
        // The inner loop only exits on a session failure, so this sleep
        // paces the (re)connect attempts: 1s, 2s, 4s, … capped at 30s.
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff);
    }
}

/// Establish the dedicated `LISTEN` session: a **dedicated max-1 pool** (see
/// the module docs for why it is not a checkout from the app pool), the same
/// TLS semantics as the app pool (`pool::connect_options` /
/// `pool::tls_mode_from_dsn`), and `LISTEN` on both zimservice channels.
///
/// The returned [`PgListener`] owns a clone of the dedicated pool; dropping
/// the local pool handle at the end of this function keeps the pool alive
/// for the listener's lifetime (it is closed when the listener is dropped).
async fn connect_listener(url: &str, application_name: &str) -> Result<PgListener> {
    let tls_mode = pool::tls_mode_from_dsn(url)?;
    let opts = pool::connect_options(url, tls_mode)?.application_name(application_name);

    // `max_lifetime(None)` + `idle_timeout(None)` (mirroring sqlx's own
    // `PgListener::connect`): the connection must outlive any recycling
    // policy — a recycled `LISTEN` session is a silently-dropped
    // subscription. The short acquire timeout bounds a reconnect attempt so
    // the backoff loop stays responsive when the database is unreachable.
    let dedicated = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .max_lifetime(None)
        .idle_timeout(None)
        .connect_with(opts)
        .await
        .map_err(Error::Database)?;

    let mut listener = PgListener::connect_with(&dedicated)
        .await
        .map_err(Error::Database)?;
    listener
        .listen_all([SETTINGS_CHANNEL, CATALOG_CHANNEL])
        .await
        .map_err(Error::Database)?;
    Ok(listener)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::time::Instant;

    use super::*;

    // Test-only coupling: the production code above is domain-agnostic (the
    // composition root wires the closures); these tests exercise the real
    // end-to-end invalidation against the real caches.
    use crate::settings::SettingsCache;
    use crate::zim::ZimManager;

    /// The same `DATABASE_URL` idiom as [`crate::testing::test_pool`].
    fn test_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into())
    }

    /// Per-test unique session tag (finds this process's listener backend in
    /// `pg_stat_activity` without colliding with a running dev `serve`).
    fn app_name(tag: &str) -> String {
        format!("zimservice-notify-{tag}-{}", std::process::id())
    }

    /// Poll a sync predicate until it holds or the timeout expires.
    async fn wait_for(predicate: impl Fn() -> bool, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll an async predicate until it holds or the timeout expires.
    async fn wait_for_async<F, Fut>(mut predicate: F, timeout: Duration) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate().await {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Build the listener's dependencies against the shared suite DB (an
    /// empty temp ZIM dir + a real `SettingsCache` load). Returns `None` —
    /// counted as a skip inside [`crate::testing::test_pool`] via
    /// `gate_skip` — when the compose-URL DB is unreachable.
    async fn spawn_ready(
        tag: &str,
    ) -> Option<(
        crate::db::Pool,
        tempfile::TempDir,
        SettingsCache,
        Arc<ZimManager>,
        NotifyListener,
    )> {
        let url = test_url();
        // `?` (not let-else): this fn returns `Option`, so a `None` gate
        // propagates as the caller's counted skip (counted inside
        // `test_pool()` via `gate_skip`).
        let (pool, _gate) = crate::testing::test_pool().await?;
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let dir = tempfile::tempdir().expect("tempdir");
        let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
            .await
            .expect("settings load");
        let zims = ZimManager::new(dir.path().to_path_buf(), pool.clone());
        // Test-side stand-in for the composition-root wiring in
        // `startup.rs`: closures over the real caches, reproducing the
        // production per-notification behavior and log lines exactly.
        let settings_sub = settings.clone();
        let on_settings: OnBump = Box::new(move |bump| {
            let cache = settings_sub.clone();
            Box::pin(async move {
                match cache.reload().await {
                    Ok(()) => {
                        tracing::debug!(bump, "cross-process settings invalidation: reloaded")
                    }
                    Err(e) => {
                        tracing::error!("cross-process settings invalidation: reload failed: {e}")
                    }
                }
            })
        });
        let zims_sub = zims.clone();
        let on_catalog: OnBump = Box::new(move |bump| {
            let zims = zims_sub.clone();
            Box::pin(async move {
                match zims.resync().await {
                    Ok(report) if !report.is_empty() => {
                        tracing::info!(
                            bump,
                            ?report,
                            "cross-process catalog invalidation: resynced"
                        )
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("cross-process catalog invalidation: resync failed: {e}")
                    }
                }
            })
        });
        let listener = spawn_listener_as(&url, on_settings, on_catalog, &app_name(tag));
        Some((pool, dir, settings, zims, listener))
    }

    async fn wait_connected(listener: &NotifyListener) {
        assert!(
            wait_for(
                || listener.status().state == ListenerState::Connected,
                Duration::from_secs(10),
            )
            .await,
            "listener must reach `connected`; status: {:?}",
            listener.status()
        );
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let mut d = INITIAL_BACKOFF;
        let mut seen = vec![d];
        for _ in 0..6 {
            d = next_backoff(d);
            seen.push(d);
        }
        assert_eq!(
            seen,
            [1u8, 2, 4, 8, 16, 30, 30]
                .iter()
                .map(|s| Duration::from_secs(*s as u64))
                .collect::<Vec<_>>()
        );
    }

    /// DB-gated: a `zimservice_settings` notification (a "peer" firing it,
    /// then this process's own `update()` write path) each reach the
    /// subscriber and trigger a full cache reload.
    #[tokio::test]
    async fn settings_invalidations_reload_the_cache() {
        let Some((pool, _dir, settings, _zims, listener)) = spawn_ready("settings").await else {
            return;
        };
        wait_connected(&listener).await;

        let gen_before = settings.generation();

        // A "peer process" write: fire the channel directly on a second
        // connection (exactly what another instance's `update()` would do
        // after its transaction commits).
        let before = listener.status().settings_notifications;
        crate::db::raw::execute(&pool, NOTIFY_SETTINGS_SQL, |q| q)
            .await
            .expect("NOTIFY");
        assert!(
            wait_for(
                || {
                    let s = listener.status();
                    s.settings_notifications > before && settings.generation() > gen_before
                },
                Duration::from_secs(10),
            )
            .await,
            "peer notification must reload the settings cache; status: {:?}, gen: {} → {}",
            listener.status(),
            gen_before,
            settings.generation()
        );

        // This process's own write path: `update()` fires the channel as the
        // last statement of its transaction — the listener must see it too
        // (self-notify is expected and must reload). `search.fts_weight`
        // carries the default (fully mutable) policy, so the write — and its
        // NOTIFY — is what's under test, with no env/config lock in play.
        let before = listener.status().settings_notifications;
        let key = crate::settings::KEY_SEARCH_FTS_WEIGHT;
        let mut values = HashMap::new();
        values.insert(
            key.to_string(),
            settings
                .get(key)
                .unwrap_or(serde_json::Value::String(String::new())),
        );
        let res = settings.update(&values, true).await.expect("update");
        assert!(res.is_empty(), "update errors: {res:?}");
        assert!(
            wait_for(
                || listener.status().settings_notifications > before,
                Duration::from_secs(10),
            )
            .await,
            "the update()'s transaction NOTIFY must reach our own listener; status: {:?}",
            listener.status()
        );
    }

    /// Row count of `zims` rows named `name` (test assertion helper).
    async fn row_count(pool: &crate::db::Pool, name: &str) -> i64 {
        crate::db::raw::fetch_scalar_optional(
            pool,
            "SELECT count(*) FROM zims WHERE name = $1",
            |q| q.bind(name),
        )
        .await
        .expect("zims count")
        .unwrap_or(-1)
    }

    /// DB-gated: a `zimservice_catalog` notification makes this process's
    /// resync persist an on-disk ZIM the "peer" installed, and the next
    /// notification (after the peer uninstalled) prune the row — the
    /// install/finalize and uninstall/delete propagation end to end.
    #[tokio::test]
    async fn catalog_invalidations_resync_from_disk() {
        let Some((pool, dir, _settings, _zims, listener)) = spawn_ready("catalog").await else {
            return;
        };
        wait_connected(&listener).await;

        let name = "notifycat";
        let zim_file = dir.path().join(format!("{name}.zim"));
        std::fs::copy("tests/fixtures/tiny.zim", &zim_file).expect("stage fixture ZIM");
        // Pre-clean (shared suite DB re-runs).
        crate::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(name))
            .await
            .expect("pre-clean");

        // The "peer install": the file is on disk but has no row. The
        // notification must make this process's resync persist it.
        crate::db::raw::execute(&pool, NOTIFY_CATALOG_SQL, |q| q)
            .await
            .expect("NOTIFY");
        assert!(
            wait_for_async(
                || {
                    let pool = &pool;
                    async move { row_count(pool, name).await == 1 }
                },
                Duration::from_secs(10),
            )
            .await,
            "catalog notification must persist the on-disk ZIM row; status: {:?}",
            listener.status()
        );

        // The "peer uninstall": the file disappears; the next notification
        // must prune the row.
        std::fs::remove_file(&zim_file).expect("remove ZIM");
        crate::db::raw::execute(&pool, NOTIFY_CATALOG_SQL, |q| q)
            .await
            .expect("NOTIFY");
        assert!(
            wait_for_async(
                || {
                    let pool = &pool;
                    async move { row_count(pool, name).await == 0 }
                },
                Duration::from_secs(10),
            )
            .await,
            "catalog notification after removal must prune the ZIM row; status: {:?}",
            listener.status()
        );
    }

    /// DB-gated: terminating the listener's backend (simulating a Postgres
    /// restart / network drop) reconnects the session with the resync, and
    /// the re-`LISTEN`ed session still receives notifications.
    #[tokio::test]
    async fn terminated_session_reconnects_resyncs_and_relistens() {
        let Some((pool, _dir, _settings, _zims, listener)) = spawn_ready("reconnect").await else {
            return;
        };
        wait_connected(&listener).await;
        let name = app_name("reconnect");

        // Terminate the dedicated backend — the per-test unique
        // `application_name` tags it in `pg_stat_activity`.
        let terminated: Option<bool> = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = $1 AND datname = current_database() LIMIT 1",
            |q| q.bind(&name),
        )
        .await
        .expect("terminate query");
        assert!(
            terminated.unwrap_or(false),
            "the listener backend must be findable and terminable (application_name={name})"
        );

        assert!(
            wait_for(
                || {
                    let s = listener.status();
                    s.state == ListenerState::Connected && s.disconnects >= 1 && s.resyncs >= 2
                },
                Duration::from_secs(15),
            )
            .await,
            "the session must reconnect (eager) and resync both caches; status: {:?}",
            listener.status()
        );

        // Re-`LISTEN` works: a fresh notification is delivered.
        crate::db::raw::execute(&pool, NOTIFY_SETTINGS_SQL, |q| q)
            .await
            .expect("NOTIFY");
        assert!(
            wait_for(
                || listener.status().settings_notifications >= 1,
                Duration::from_secs(10),
            )
            .await,
            "a notification after the reconnect must be delivered (re-LISTEN); status: {:?}",
            listener.status()
        );
    }

    /// DB-gated: dropping the last handle stops the listener gracefully —
    /// the dedicated session is closed (no `pg_stat_activity` residue) and,
    /// implicitly, the fail-closed supervisor is NOT tripped (a spurious
    /// `exit(1)` here would kill the test process).
    #[tokio::test]
    async fn last_handle_drop_closes_the_session_gracefully() {
        let Some((pool, _dir, _settings, _zims, listener)) = spawn_ready("drop").await else {
            return;
        };
        wait_connected(&listener).await;
        let name = app_name("drop");

        // Drop BOTH the original and the clone: `NotifyListener` is a cheap
        // `Arc` wrapper, so dropping only the clone would leave the original
        // (still in scope) as a live reference and never trip the `Drop`.
        let handle = listener.clone();
        drop(listener);
        drop(handle); // last handle → graceful stop

        assert!(
            wait_for_async(
                || {
                    let pool = &pool;
                    let name = &name;
                    async move {
                        let remaining: Option<i64> = crate::db::raw::fetch_scalar_optional(
                            pool,
                            "SELECT count(*) FROM pg_stat_activity \
                                 WHERE application_name = $1",
                            |q| q.bind(name),
                        )
                        .await
                        .expect("activity count");
                        remaining.unwrap_or(1) == 0
                    }
                },
                Duration::from_secs(10),
            )
            .await,
            "dropping the last handle must close the dedicated LISTEN session"
        );
    }

    /// Fix-1 meta-tripwire (S1): a DB gate must be a *counted skip*, never
    /// a panic. The historical bug was `test_pool().await` (which returns
    /// an Option) followed by a panic call on the result — on a DB-less
    /// machine that panicked where [`crate::testing::gate_skip`] had to
    /// count a skip. Walk every `.rs` file under `src/` (cwd is the crate
    /// root under `cargo test`, same convention as the `migrations/` walk
    /// in src/lib.rs) and fail on the panic chain over the returned
    /// Option. The legal let-else skip idiom, `?`, and `match` on the
    /// Option are exempt — see
    /// [`gate_matcher_flags_panic_chains_and_exempts_skip_idioms`].
    #[test]
    fn test_pool_gates_must_skip_not_panic() {
        let mut rs_files = Vec::new();
        collect_rs_files(std::path::Path::new("src"), &mut rs_files);
        assert!(
            !rs_files.is_empty(),
            "no .rs files under src/ — cwd is not the crate root?"
        );
        for file in &rs_files {
            let text = std::fs::read_to_string(file).unwrap_or_else(|e| panic!("read {file}: {e}"));
            if let Some(start) = find_panic_gate_chains(&text).into_iter().next() {
                let line = text[..start].matches('\n').count() + 1;
                panic!(
                    "DB gate panic: {file}:{line} — test_pool() must be a counted \
                     skip, not a panic (see gate_skip)"
                );
            }
        }
    }

    /// The tripwire's chain matcher (factored for unit testing): finds a
    /// `test_pool().await` whose statement chain then panics on the
    /// result, and returns the start offset of each hit. The first `.` or
    /// `;` after `.await` decides: a panic call there is a hit; a `;`, a
    /// `?`, a `{`, or any other member access ends the chain cleanly —
    /// which is what exempts the let-else skip idiom, `?`, and `match`
    /// shapes. (Hand-rolled rather than `regex`: the crate's syntax has no
    /// lookaround, and the decision is one character class.)
    fn find_panic_gate_chains(text: &str) -> Vec<usize> {
        const CALL: &str = "test_pool()";
        let mut out = Vec::new();
        let mut from = 0usize;
        while let Some(rel) = text[from..].find(CALL) {
            let call = from + rel;
            // Whitespace-only gap (line-break chains: `test_pool()` then a
            // newline then `.await`), then the `.await` suffix.
            let after = text[call + CALL.len()..].trim_start();
            if let Some(rest) = after.strip_prefix(".await") {
                // The first `.` or `;` in the statement chain decides.
                if let Some(dot) = rest.find(['.', ';']) {
                    let at = dot + 1;
                    if rest.as_bytes()[dot] == b'.'
                        && (rest[at..].starts_with("expect(") || rest[at..].starts_with("unwrap("))
                    {
                        out.push(call);
                    }
                }
            }
            from = call + CALL.len();
        }
        out
    }

    /// Recursively collect `*.rs` paths under `dir` (crate root is the
    /// cwd under `cargo test` — the docs_freshness walk in src/lib.rs
    /// relies on the same convention for `migrations/`).
    fn collect_rs_files(dir: &std::path::Path, out: &mut Vec<String>) {
        let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {dir:?}: {e}"));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|e| panic!("read dir entry: {e}"))
                .path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path.display().to_string());
            }
        }
    }

    #[test]
    fn gate_matcher_flags_panic_chains_and_exempts_skip_idioms() {
        // The historical bug shape. `\n` escapes on purpose: at runtime
        // the string holds real line breaks (the chain spans them), while
        // this file's own scanner pass sees literal backslash-n and never
        // self-trips.
        let bad = "crate::testing::test_pool()\n.await\n.expect(\"test_pool (DB gate)\")";
        assert_eq!(
            find_panic_gate_chains(bad).len(),
            1,
            "chained panic call on test_pool() must be flagged"
        );
        // The legal counted-skip idiom (plus its `?` / `match` siblings).
        let good = "let Some((pool, _gate)) = test_pool().await else { return };";
        assert!(
            find_panic_gate_chains(good).is_empty(),
            "let-else skip idiom must be exempt"
        );
        let good_q = "let (pool, _gate) = test_pool().await?;";
        assert!(
            find_panic_gate_chains(good_q).is_empty(),
            "`?` on the Option must be exempt"
        );
        let good_m = "match test_pool().await { Some(_) => (), None => return }";
        assert!(
            find_panic_gate_chains(good_m).is_empty(),
            "`match` on the Option must be exempt"
        );
    }
}
