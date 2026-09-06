//! Startup orchestration: state construction and the single-instance guards
//! (extracted from `main.rs` so the guard-acquisition
//! order around the mutating `build_state` is an isolated, testable unit —
//! ARCH m-1 / M-1). Lives in the lib (not the binary) so its guard tests
//! run under `--lib` and can use the lib's `crate::testing` DB-gating
//! infrastructure (M3).
//!
//! Ordering contract (M-1): `cmd_serve` acquires the single-instance guards
//! **before** `build_state`. `build_state` in `StartupMode::Serve` runs the
//! startup resync, which persists new ZIM rows and deletes rows for files
//! that are missing on disk — a DELETE/INSERT cycle on the shared database
//! that two concurrently-starting instances must not interleave. The guards
//! need only `Config` (the DSN + `zim_dir`), so they come first.

use std::sync::Arc;

use sqlx::postgres::PgConnection;

use crate::config::Config;
use crate::db;
use crate::search::SearchEngine;
use crate::serve;
use crate::settings::{
    SettingsCache, KEY_ACCESS_ADMIN_PASSWORD, KEY_TORRENT_ALLOW_PRIVATE_NETWORKS, KEY_TORRENT_URL,
};
use crate::torrent::{connect_qbit, qbit_fingerprint, resolve_qbit_inputs, QbitClientCache};
use crate::zim::ZimManager;
use crate::AppState;

/// What a subcommand asks [`build_state`] to do at startup (ARCH-9:
/// replaces the behavioral `resync: bool` that smuggled two concepts —
/// "reconcile disk↔DB" and "which subcommand am I" — through one flag).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupMode {
    /// `serve` — full startup: resync; the multi-instance hard refusal
    /// applies (cmd_serve is the sole enforcer, via `acquire_instance_guard`).
    Serve,
    /// Mutating CLI subcommands (`index`, `embed`) — reconcile the DB
    /// with disk, since they write by design (indexing upserts article rows,
    /// embedding writes vectors), so the startup resync is allowed.
    ///
    /// H1: these subcommands (and `list --sync`) acquire the per-database
    /// advisory lock via [`acquire_mutating_guard`] **before** calling
    /// `build_state` and refuse when a running server holds it, so they can
    /// never silently stale a live server's in-memory caches.
    Mutating,
    /// Read-only subcommands (`status`, `mcp`) — no **data** reconciliation.
    /// Status must not DELETE zims rows for files that are merely absent at
    /// this moment (resync does that); a stdio MCP session shouldn't persist
    /// ZIM rows as a side effect of starting. "Read-only" refers to ZIM data,
    /// not the schema: [`build_state`] still applies pending migrations for
    /// every mode (they are idempotent, additive, and serialized by a session
    /// advisory lock), because a subcommand must be able to read the current
    /// schema. So a `status`/`mcp` run against a DB that is behind will apply
    /// the pending DDL (never touching ZIM data rows).
    ReadOnly,
}
impl StartupMode {
    /// Whether `populate_zims` should run `resync()` (persist new ZIMs,
    /// prune missing).
    const fn resync(self) -> bool {
        matches!(self, Self::Serve | Self::Mutating)
    }
}

/// Populate the ZIM manager: scan disk, load persisted state from Postgres
/// (which takes precedence over stale cache entries), and — when the startup
/// mode reconciles (`Serve`/`Mutating`) — reconcile the cache + DB with the
/// actual files (persists new ones, prunes missing). Returns the ZIM names
/// found on disk.
pub async fn populate_zims(zims: &Arc<ZimManager>, resync: bool) -> anyhow::Result<Vec<String>> {
    let found = zims.scan().await?;
    zims.load_from_db().await?;
    if resync {
        zims.resync().await?;
    }
    Ok(found)
}

/// Shared bootstrap for subcommands that need a migrated pool + ZIM manager
/// (ARCH M1): create the Postgres pool, apply pending migrations, and build
/// the [`ZimManager`]. Both [`build_state`] and the binary's `cmd_list` use
/// this, so the two startup paths cannot diverge (`cmd_list` previously
/// re-implemented this first half by hand).
pub async fn bootstrap_pool_and_zims(
    config: &Config,
) -> anyhow::Result<(db::pool::Pool, Arc<ZimManager>)> {
    // Create Postgres pool
    let pool = db::pool::create_pool(config).await?;

    // Run migrations for every startup mode — including ReadOnly (`status`,
    // `mcp`) and `list`. Migrations are idempotent, additive, and serialized
    // by a session advisory lock, so a read-only run only ever applies
    // pending schema DDL (never ZIM data). See the `StartupMode::ReadOnly`
    // doc for the "read-only = data, not schema" contract.
    db::migrate::run_migrations(&pool).await?;

    let zims = ZimManager::new(config.zim_dir.clone(), pool.clone());
    Ok((pool, zims))
}

/// The exact non-blocking single-instance advisory lock query: every code
/// path that tries to take (or verify) the per-DB instance lock runs this
/// one statement, kept as a single constant so the string cannot drift
/// between production sites and tests.
pub const INSTANCE_LOCK_SQL: &str = "SELECT pg_try_advisory_lock(hashtext('zimservice:instance'))";

/// `pg_locks` WHERE fragment matching the 1-argument int8 advisory lock this
/// codebase acquires: `pg_try_advisory_lock(hashtext('zimservice:instance'))`.
///
/// Postgres stores a 64-bit key split in two columns: `classid` = the upper
/// 32 bits of the (sign-extended) key, `objid` = the lower 32 bits, and
/// `objsubid = 1` (the 2-argument form uses `objsubid = 0`). `hashtext` is a
/// signed `int4`; `::bigint` sign-extends it and the shift/mask recover the
/// two halves. Both `pg_locks` probes in this file share this fragment so
/// the two predicates cannot drift apart.
const ADVISORY_LOCK_MATCH: &str = "l.locktype = 'advisory' AND l.objsubid = 1 \
     AND l.classid = (hashtext('zimservice:instance')::bigint >> 32)::oid \
     AND l.objid = (hashtext('zimservice:instance')::bigint & 4294967295)::oid \
     AND a.pid <> pg_backend_id()";

/// Build the full application state (DB pool, settings, ZIM manager, etc.)
///
/// `mode` selects the startup behavior: [`StartupMode::Serve`] and
/// [`StartupMode::Mutating`] reconcile the ZIM cache + DB with disk;
/// [`StartupMode::ReadOnly`] (`status`, `mcp`) never mutates on startup.
///
/// `advisory_lock_held` is true when this process already owns the
/// single-instance advisory lock (the `cmd_serve` guard acquired it before
/// building state, M-1); in that case the lightweight `pg_locks` warning
/// below is skipped — its own dedicated lock connection would otherwise
/// register as "another instance" and warn on every start.
///
/// `connect_torrent` (ARCH minor #3): whether to pay the qBittorrent login
/// round-trip at startup. The stdio `mcp` session never touches the
/// qBittorrent client, so it passes `false` and starts without the network
/// round-trip. `status` passes `true` because it reports the connection
/// state in its output.
///
/// Note: this function does **not** acquire the single-instance advisory
/// lock. `cmd_serve` uses `acquire_instance_guard` (a dedicated non-pooled
/// connection held for the process lifetime); the mutating subcommands use
/// `acquire_mutating_guard` (held for the command's duration) and pass
/// `advisory_lock_held = true`; read-only subcommands do the lightweight
/// `pg_locks` check for the warning only.
pub async fn build_state(
    config: &Config,
    mode: StartupMode,
    advisory_lock_held: bool,
    connect_torrent: bool,
) -> anyhow::Result<AppState> {
    // Shared pool + migration + ZIM-manager bootstrap (also used by
    // `cmd_list` — ARCH M1, so the two startup paths cannot diverge).
    let (pool, zims) = bootstrap_pool_and_zims(config).await?;

    // Lightweight instance check (warn-only, non-acquiring): query `pg_locks`
    // to see if another session holds the advisory lock. This avoids the
    // S1 pitfall (taking the lock on a pooled connection that gets recycled).
    // Skipped when we already own the lock (the `serve` guard or the
    // mutating-command guard) — our own dedicated lock connection would
    // otherwise be seen as a second instance. Mutating subcommands are
    // normally refused before reaching here (H1); this arm still covers the
    // `ZIMSERVICE_ALLOW_MULTI_INSTANCE=1` opt-out, where they proceed and
    // the warning is the only signal that they are staling a live server.
    if !advisory_lock_held {
        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("pool: {e}"))?;
        // Raw escape hatch (db::raw): `pg_locks` catalog probe — the SeaORM
        // builder only sees application tables.
        let held = db::raw::fetch_scalar_optional(
            &mut *conn,
            &format!(
                "SELECT EXISTS(\n\
                 SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON l.pid = a.pid\n\
                 WHERE {ADVISORY_LOCK_MATCH}\n\
                 )"
            ),
            |q| q,
        )
        .await
        .ok()
        .flatten()
        .unwrap_or(false);
        if held {
            if mode.resync() {
                tracing::warn!(
                    "Another zimservice instance appears to be running on this database (advisory lock \
                     already held), and this subcommand mutates the shared database. The running \
                     instance's in-memory caches (settings, ZIM manager, qBittorrent client) will be \
                     STALE after this run until it is restarted — prefer restarting the server, or \
                     run this command while no server is up."
                );
            } else {
                tracing::warn!("Another zimservice instance appears to be running on this database (advisory lock already held).");
            }
        }
    }

    // Load settings (seeds defaults on first run). Env access is injected
    // once: the same getter feeds both the UI lock map and the reload
    // snapshot, so the two can never disagree about which vars were set.
    let env_get = |k: &str| std::env::var(k).ok();
    let env_locked = config.locked_env_settings(&env_get);
    let env_snapshot = config.env_settings_snapshot(&env_get);
    let settings = SettingsCache::load(pool.clone(), env_locked, env_snapshot).await?;
    settings.sync_from_config(config); // ARCH-1: general.* display keys <- Config

    // Password mode with no password would fail open on every request —
    // refuse to start instead. The placeholder is rejected in **both** modes:
    // a placeholder means unfinished setup (open mode with CHANGE_ME would
    // otherwise start unauthenticated without anyone noticing).
    admin_password_startup_check(
        &settings.access_mode(),
        &settings
            .get_typed::<String>(KEY_ACCESS_ADMIN_PASSWORD)
            .unwrap_or_default(),
    )
    .map_err(anyhow::Error::msg)?;

    // Discover ZIMs on disk, load persisted index state from Postgres, then
    // (when `resync`) reconcile the cache + DB with the actual files. (The
    // `zims` manager itself came from `bootstrap_pool_and_zims` above.)
    populate_zims(&zims, mode.resync()).await?;

    // Search engine
    let degradation = crate::DegradationTracker::default();
    let search = SearchEngine::new(pool.clone(), settings.clone(), degradation.clone());

    // qBittorrent client (optional) — stored in a runtime cache (ARCH M3) so
    // a `PUT /settings` to `torrent.url` rebuilds the client within one poll
    // tick. The initial connect keeps the startup log + fail-soft semantics
    // (auth failure → cache stays empty, same warnings). Skipped entirely
    // when `connect_torrent` is false (the `mcp` stdio session — ARCH minor
    // #3): it never touches the client, so the login round-trip would be
    // pure startup latency.
    let torrent = QbitClientCache::new();
    if connect_torrent && settings.torrent_enabled() {
        // Single resolution site shared with the poller's per-tick
        // `resolve_torrent` (ARCH-10): config override beats the runtime
        // `torrent.url` setting; empty/whitespace disables. Warnings stay here.
        let inputs = resolve_qbit_inputs(
            settings.get_typed::<String>(KEY_TORRENT_URL).as_deref(),
            &config.torrent_url,
            &config.torrent_user,
            &config.torrent_pass,
        );
        if inputs.is_none() {
            tracing::warn!("torrent integration disabled: no torrent.url");
        }
        if let Some((url, user, pass)) = inputs {
            if user.is_empty() || pass.is_empty() {
                tracing::warn!(
                    "qBittorrent URL configured without credentials; login will fail \
                     if the Web API requires auth — set QBITTORRENT_USER/QBITTORRENT_PASS"
                );
            }
            // SEC M-1: the private-network opt-in gates whether a private
            // (LAN) qBittorrent Web API is reachable.
            let allow_private = settings
                .get_typed::<bool>(KEY_TORRENT_ALLOW_PRIVATE_NETWORKS)
                .unwrap_or(false);
            if allow_private {
                tracing::info!(
                    "qBittorrent: private/LAN network access enabled via torrent.allow_private_networks"
                );
            }
            if let Some(client) = connect_qbit(&url, &user, &pass, allow_private).await {
                let redacted = crate::redact_url(&url);
                tracing::info!("connected to qBittorrent at {redacted}");
                let fp = qbit_fingerprint(&url, &user, &pass, allow_private);
                torrent.store(&fp, client);
            }
        }
    }

    Ok(AppState {
        db: pool,
        settings,
        zims,
        search,
        torrent,
        rate_limiter: Arc::new(serve::ratelimit::RateLimiterHandle::new()),
        probes: crate::HealthProbes::default(),
        auth_lockout: Arc::new(Default::default()),
        degradation,
    })
}

/// RAII single-instance guard: pins the Postgres advisory lock to a dedicated
/// non-pooled connection (S1: a pooled connection may be recycled by
/// deadpool, silently releasing the lock) and owns the `.zimservice.lock`
/// PID lock file (S2: unlinks it on drop so a graceful shutdown does not leave a
/// stale lock that blocks the next start). Drop order: advisory lock released
/// first, then PID lock unlinked. Either component may be absent: the
/// `ZIMSERVICE_ALLOW_MULTI_DB` opt-out keeps only the PID lock (per-zim_dir)
/// guard, and `ZIMSERVICE_ALLOW_MULTI_INSTANCE` keeps neither.
///
/// **Liveness contract** (H1): while `lock_task` is present, the guard
/// monitors the advisory-lock connection for the failure the S1 guard
/// cannot otherwise see — the lock connection dying *mid-run* (Postgres
/// restart, network partition), which silently releases the advisory lock
/// and would let a second `serve` against the same database start undetected.
/// The monitor task **owns** the dedicated connection and probes it on an
/// interval (`SELECT 1`); on a failed probe it logs and exits the process
/// (fail-closed), because a running server whose uniqueness guarantee has
/// silently evaporated must not keep serving. A graceful drop aborts the
/// monitor task first, which closes the connection (releasing the lock)
/// before any further probe — no spurious exit on shutdown. Monitoring is
/// skipped when no advisory lock is held (`ZIMSERVICE_ALLOW_MULTI_DB`'s
/// `None` arm: there is no connection, and hence no lock, to lose).
pub struct SingleInstanceGuard {
    /// Join handle of the H1 liveness-monitor task, which owns the dedicated
    /// connection holding the advisory lock. Aborting it (on drop) drops the
    /// connection, closing the socket so Postgres releases the lock.
    /// (sqlx's `PgConnection` has no separately abortable I/O driver the way
    /// tokio-postgres' spawned `Connection` did, so the guard holds the
    /// monitor task's handle instead of a client + driver handle pair.)
    lock_task: Option<tokio::task::JoinHandle<()>>,
    /// Path to the `.zimservice.lock` file; unlinked on drop.
    lock_path: Option<std::path::PathBuf>,
}

impl SingleInstanceGuard {
    /// Whether this guard holds the per-database advisory lock (false when
    /// the `ZIMSERVICE_ALLOW_MULTI_DB` opt-out let another instance keep it,
    /// or when both guards were disabled).
    pub fn advisory_lock_held(&self) -> bool {
        self.lock_task.is_some()
    }
}

impl Drop for SingleInstanceGuard {
    fn drop(&mut self) {
        // Release the advisory lock first by aborting the monitor task: that
        // drops the dedicated connection, so the socket closes and Postgres
        // drops the lock. Best-effort (worst case: the server-side socket
        // timeout releases it).
        if let Some(task) = self.lock_task.take() {
            task.abort();
        }
        // Best-effort: a failing unlink (e.g. read-only fs) is logged, not
        // fatal.
        if let Some(p) = &self.lock_path {
            if let Err(e) = std::fs::remove_file(p) {
                tracing::debug!("could not remove instance lock file {}: {e}", p.display());
            }
        }
    }
}

/// Open a *dedicated* non-pooled connection (honoring the DSN's TLS mode,
/// same as the pool) and try to take the per-DB single-instance advisory
/// lock non-blockingly. Returns `(conn, acquired)`; the connection is kept
/// open in all cases (an unacquired lock releases nothing) and the caller
/// decides what `acquired == false` means. Dropping the returned connection
/// closes the socket (sqlx does this on drop of `PgConnection`).
///
/// Shared by [`try_acquire_advisory_lock`] (S1: the connection lives for the
/// process lifetime in the `serve` guard, owned by its liveness monitor) and
/// [`acquire_mutating_guard`] (H1: held for the guard's lifetime) — the
/// dedicated connection means the lock cannot be silently released by pool
/// recycling.
async fn connect_and_try_instance_lock(config: &Config) -> anyhow::Result<(PgConnection, bool)> {
    let mut conn = db::pool::connect_dedicated(&config.database_url)
        .await
        .map_err(|e| anyhow::anyhow!("advisory-lock connect: {e}"))?;

    let acquired: Option<bool> =
        db::raw::fetch_scalar_optional(&mut conn, INSTANCE_LOCK_SQL, |q| q)
            .await
            .map_err(|e| anyhow::anyhow!("advisory lock: {e}"))?;

    Ok((conn, acquired.unwrap_or(false)))
}

/// S1: open a *dedicated* non-pooled connection for the advisory lock,
/// honoring the DSN's TLS mode (same as the pool), and try to take the lock.
/// Returns `Ok(Some((client, driver)))` when the lock was acquired,
/// `Ok(None)` when another instance already holds it (the connection is
/// closed in both cases — an unacquired lock releases nothing, and the
/// caller decides what `None` means: hard refusal, or a warned opt-out).
/// `Err` on connect/lock-query failure.
async fn try_acquire_advisory_lock(config: &Config) -> anyhow::Result<Option<PgConnection>> {
    let (conn, acquired) = connect_and_try_instance_lock(config).await?;

    if acquired {
        Ok(Some(conn))
    } else {
        // Drop the dedicated connection (releases nothing — we didn't get it).
        Ok(None)
    }
}

/// H1 (cross-process cache invalidation): RAII guard pinning the per-database
/// single-instance advisory lock for the duration of a **mutating** CLI run
/// (`index`, `embed`, `list --sync`).
///
/// A running `serve` holds this lock for its whole lifetime (see
/// [`acquire_instance_guard`]) and reconciles its in-memory caches (settings,
/// ZIM metadata, article counts, ETags) with Postgres only at startup. A
/// mutating CLI racing it would therefore leave the server serving stale data
/// indefinitely, with no signal — the previous protection was a warn-only
/// `pg_locks` notice. Mutating subcommands now **refuse** in that case: they
/// take the same `hashtext('zimservice:instance')` lock non-blockingly
/// (see [`acquire_mutating_guard`]) and bail when another session holds it.
///
/// While acquired, the lock also serializes concurrent mutating runs against
/// each other (their startup resyncs are DELETE/INSERT cycles on the shared
/// `zims` table) and refuses a concurrently-starting `serve` — the same M-1
/// interleave hazard the instance guard protects against at startup.
///
/// `pg_try_advisory_lock` is session-scoped: the guard's dedicated connection
/// is a *different session* from the server's, so a `false` result means
/// another session (the server, or a concurrent mutating CLI) holds the lock
/// — exactly the case to refuse. The connection is dedicated (non-pooled, S1)
/// and held for the guard's lifetime; `Drop` aborts its I/O driver, closing
/// the socket and releasing the lock.
#[derive(Debug)]
pub struct MutatingGuard {
    /// Dedicated connection holding the advisory lock. The field is never
    /// read — it exists solely to keep the connection (and therefore the
    /// lock) alive for the guard's lifetime. Dropping it closes the socket,
    /// which makes Postgres release the lock (sqlx closes `PgConnection` on
    /// drop — the old tokio-postgres I/O-driver abort is gone with the driver).
    #[allow(dead_code)]
    lock_conn: PgConnection,
}

/// H1: acquire a [`MutatingGuard`] for a mutating subcommand (`index`,
/// `embed`, `list --sync`), before any mutation of the shared database.
///
/// - `ZIMSERVICE_ALLOW_MULTI_INSTANCE=1` (the full single-instance opt-out
///   that `cmd_serve` honors): no lock, `Ok(None)` — the operator has
///   accepted multi-instance risk, so the mutating command proceeds (the
///   warn-only `pg_locks` notice in `build_state` remains the signal for the
///   `index`/`embed` paths).
/// - Lock free: acquired on a dedicated non-pooled connection; `Ok(Some)`,
///   held until the guard drops (command end).
/// - Lock held by another session (a running server, or a concurrent
///   mutating CLI): `Err` naming the holder's PID when discoverable — the
///   command must stop before touching the shared database.
///
/// The opt-out is keyed off `ZIMSERVICE_ALLOW_MULTI_INSTANCE` (not
/// `ZIMSERVICE_ALLOW_MULTI_DB`): the advisory lock is per-database, so a
/// different-database deployment on a shared zim_dir never holds *this*
/// database's lock and must not silence the refusal.
pub async fn acquire_mutating_guard(config: &Config) -> anyhow::Result<Option<MutatingGuard>> {
    if matches!(std::env::var("ZIMSERVICE_ALLOW_MULTI_INSTANCE"), Ok(v) if v == "1") {
        tracing::warn!(
            "ZIMSERVICE_ALLOW_MULTI_INSTANCE=1: this mutating command will run even \
             while a server holds this database's advisory lock — the server's \
             in-memory caches (ZIM metadata, article counts, ETags) will be stale \
             until it is restarted"
        );
        return Ok(None);
    }

    // Dedicated (non-pooled) connection, same as the `serve` guard (S1):
    // a pooled connection could be recycled and silently release the lock.
    let (mut client, acquired) = connect_and_try_instance_lock(config).await?;

    if !acquired {
        // Someone else holds it — surface the holder's PID (best-effort) for
        // an actionable error, then refuse before any mutation. The pg_locks
        // predicate mirrors the warn-only check in `build_state`.
        let pid: Option<i32> = db::raw::fetch_scalar_optional(
            &mut client,
            &format!(
                "SELECT a.pid FROM pg_locks l JOIN pg_stat_activity a ON l.pid = a.pid\n\
                 WHERE {ADVISORY_LOCK_MATCH} LIMIT 1"
            ),
            |q| q,
        )
        .await
        .ok()
        .flatten();
        let holder = match pid {
            Some(p) => format!("advisory lock held by pid {p}"),
            None => "advisory lock held by another process".to_string(),
        };
        drop(client);
        anyhow::bail!(
            "another zimservice instance ({holder}) is already using this database. A \
             running server reconciles its in-memory caches (ZIM metadata, article \
             counts, ETags) with the database only at startup, so this mutation would \
             be silently stale. Stop the server and re-run this command, use a \
             different DATABASE_URL, or set ZIMSERVICE_ALLOW_MULTI_INSTANCE=1 to \
             override (not recommended)."
        );
    }

    Ok(Some(MutatingGuard { lock_conn: client }))
}

/// How often the advisory-lock liveness monitor probes the lock connection
/// (a `SELECT 1` on the dedicated connection). Connection death is only
/// ever detected within this interval; 30s keeps the extra probe cost
/// negligible while bounding the window in which a silently-released
/// advisory lock would be unnoticed.
const ADVISORY_LOCK_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// H1: detect that the advisory-lock connection has died mid-run, and invoke
/// `on_connection_lost` exactly once when it has.
///
/// sqlx's `PgConnection` has no separately abortable I/O driver to join
/// (tokio-postgres' spawned `Connection` task did), so liveness is probed by
/// a `SELECT 1` on the dedicated connection itself, every
/// [`ADVISORY_LOCK_CHECK_INTERVAL`]. Any probe failure — Postgres restart,
/// network drop, or a server-side close — means the socket is gone and
/// Postgres has silently released the advisory lock, so the callback fires
/// (and the function returns). The function is only ever *cancelled* on a
/// graceful drop (the caller aborts the owning task), in which case the
/// callback never runs and no connection loss has been observed.
async fn detect_advisory_lock_loss(
    mut conn: PgConnection,
    on_connection_lost: impl FnOnce(String),
) {
    loop {
        match tokio::time::timeout(
            ADVISORY_LOCK_CHECK_INTERVAL,
            db::raw::execute(&mut conn, "SELECT 1", |q| q),
        )
        .await
        {
            // Timeout: connection still alive — probe again.
            Err(_) => {}
            Ok(Ok(_)) => {}
            // Probe failed — the socket is gone and the lock was released.
            Ok(Err(e)) => {
                let reason = format!("advisory-lock connection dropped: {e}");
                on_connection_lost(reason);
                return;
            }
        }
    }
}

/// H1: spawn the detached liveness monitor for an acquired advisory lock.
/// The monitor task **owns** the dedicated connection (probing it on an
/// interval); fail-closed action: when that connection dies mid-run the
/// process exits (1), because a second `serve` on the same database could
/// otherwise start against a silently-released lock. Graceful shutdown
/// aborts the task first (which closes the connection), so the monitor never
/// fires on shutdown. Returns the monitor handle so the guard can abort it
/// on drop.
fn spawn_advisory_lock_monitor(conn: PgConnection) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // The production callback logs and exits(1) on a real loss, so the
        // detector only returns after the fail-closed action already ran
        // inside the callback.
        detect_advisory_lock_loss(conn, |reason| {
            tracing::error!(
                reason = %reason,
                "single-instance advisory lock lost mid-run — another `serve` on \
                 this database could now start undetected. Exiting to fail closed."
            );
            std::process::exit(1);
        })
        .await;
    })
}

/// S2: cross-database PID lock with PID-liveness check. If the file exists but
/// the recorded PID is dead, steal it (remove + recreate). This makes a
/// graceful shutdown's residual lock file self-healing on the next start.
/// Returns the lock path on success (keep it for cleanup on guard drop).
fn acquire_zim_dir_pid_lock(config: &Config) -> anyhow::Result<std::path::PathBuf> {
    let lock_path = config.zim_dir.join(".zimservice.lock");
    if lock_path.exists() {
        // Read the PID from the existing lock file.
        let existing_pid: u32 = std::fs::read_to_string(&lock_path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        if existing_pid != 0 && pid_is_alive(existing_pid) {
            // Live process holds the lock — bail.
            anyhow::bail!(
                "another zimservice instance (PID {existing_pid}) is using this \
                 zim_dir ({}) — stopping. Stop the other instance, or delete \
                 {} if it is stale.",
                config.zim_dir.display(),
                lock_path.display()
            );
        }
        // Stale lock (dead PID or unreadable) — steal it.
        tracing::info!(
            "removing stale instance lock file {} (PID {} not alive)",
            lock_path.display(),
            existing_pid
        );
        std::fs::remove_file(&lock_path)
            .map_err(|e| anyhow::anyhow!("remove stale lock file: {e}"))?;
    }
    use std::io::Write;
    // If the create fails (race: another process created it between our
    // exists() check and the create), the `?` propagates the error and any
    // advisory-lock connection held by the caller is dropped at its exit —
    // releasing the lock automatically.
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&lock_path)
        .map_err(|e| anyhow::anyhow!("create lock file {}: {e}", lock_path.display()))?;
    let _ = writeln!(file, "{}", std::process::id());
    let _ = file.flush();
    // Drop the file handle — the caller keeps the path for cleanup on guard drop.
    drop(file);
    Ok(lock_path)
}

/// Acquire the single-instance guards (advisory lock + PID lock). Returns `None`
/// if the advisory lock could not be acquired (another instance holds it),
/// or `Some(Err)` on fatal failures (PID lock already held by a live process,
/// I/O errors).
pub async fn acquire_instance_guard(
    config: &Config,
) -> anyhow::Result<Option<SingleInstanceGuard>> {
    let advisory = try_acquire_advisory_lock(config).await?;
    let Some(conn) = advisory else {
        return Ok(None);
    };
    let lock_path = acquire_zim_dir_pid_lock(config)?;
    // A PID lock conflict above bailed, dropping `conn` at the
    // error boundary and releasing the advisory lock.

    tracing::warn!(
        zim_dir = %config.zim_dir.display(),
        "the single-process advisory lock is per-database: it cannot exclude a \
         zimservice instance running against a *different* database on this shared \
         zim_dir. The PID lock guard above covers the same-zim_dir case; run at \
         most one `serve` per zim_dir (not just per database). Set \
         ZIMSERVICE_ALLOW_MULTI_DB=1 only if you deliberately run separate \
         databases on a shared zim_dir."
    );

    // H1: watch the lock connection for a mid-run death (Postgres restart /
    // network drop silently releases the advisory lock). The monitor owns
    // the connection; the guard keeps its handle to abort it on Drop.
    let monitor_task = spawn_advisory_lock_monitor(conn);

    Ok(Some(SingleInstanceGuard {
        lock_task: Some(monitor_task),
        lock_path: Some(lock_path),
    }))
}

/// m-7: the partial multi-instance opt-out. The per-zim_dir PID lock guard is
/// still enforced (a live process using the same `zim_dir` blocks startup),
/// but the per-database advisory lock is best-effort: when another instance
/// holds it we warn and proceed, so *different*-database deployments can
/// share a `zim_dir` — the scenario the full `ZIMSERVICE_ALLOW_MULTI_INSTANCE`
/// opt-out previously covered only by disabling **all** guards.
pub async fn acquire_instance_guard_multi_db(
    config: &Config,
) -> anyhow::Result<SingleInstanceGuard> {
    match try_acquire_advisory_lock(config).await? {
        Some(conn) => {
            let lock_path = acquire_zim_dir_pid_lock(config)?;
            // The lock is free, so both guards are in force exactly as in the
            // default path — the opt-out only changes the behavior when the
            // lock is already held by another instance (the arm below).
            tracing::info!(
                "ZIMSERVICE_ALLOW_MULTI_DB=1 set, but the advisory lock was free — \
                 running with the full single-instance guards."
            );
            // H1: the lock is held, so it is monitored for a mid-run loss
            // exactly as in the default path (the opt-out's `None` arm holds
            // no lock and spawns no monitor).
            let monitor_task = spawn_advisory_lock_monitor(conn);
            Ok(SingleInstanceGuard {
                lock_task: Some(monitor_task),
                lock_path: Some(lock_path),
            })
        }
        None => {
            // Another instance holds this database's advisory lock — the
            // opt-out accepts that (e.g. a second database deployment on a
            // shared zim_dir). The PID lock guard is still enforced here.
            let lock_path = acquire_zim_dir_pid_lock(config)?;
            tracing::warn!(
                "ZIMSERVICE_ALLOW_MULTI_DB=1: another instance holds the advisory \
                 lock for this database — proceeding without the per-DB guard. The \
                 per-zim_dir PID lock guard was enforced, so only different-database \
                 deployments on a shared zim_dir are supported by this opt-out."
            );
            Ok(SingleInstanceGuard {
                lock_task: None,
                lock_path: Some(lock_path),
            })
        }
    }
}

/// Best-effort PID liveness check. On Unix, uses `kill(pid, 0)` — the
/// portable POSIX probe (no subprocess, no signal sent): `ESRCH` means the
/// PID is gone, any other result means it still exists. This works on Linux,
/// macOS, and FreeBSD alike (previously Linux-only via `/proc`, so the S2
/// self-healing PID lock was a no-op on macOS/Windows dev boxes). On non-Unix, it
/// conservatively returns `true` (assumes alive) so the operator deletes the
/// file manually.
pub fn pid_is_alive(pid: u32) -> bool {
    pid_probe(pid)
}

#[cfg(unix)]
#[allow(unsafe_code)] // one narrow, well-documented POSIX probe
fn pid_probe(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    match rc {
        0 => true, // process exists and we may signal it
        -1 => std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH),
        _ => true,
    }
}

#[cfg(not(unix))]
fn pid_probe(_pid: u32) -> bool {
    // No portable std PID check on this platform; assume alive so the
    // operator deletes the file manually.
    true
}

/// Ok when startup is allowed; Err message otherwise.
///
/// The placeholder is rejected in **both** access modes: a placeholder means
/// unfinished setup (open mode with `CHANGE_ME` would otherwise start
/// unauthenticated without anyone noticing). A hash can't be "empty", so the
/// empty check only bites on unconfigured password mode.
pub fn admin_password_startup_check(mode: &str, password: &str) -> Result<(), String> {
    if mode == crate::settings::ACCESS_MODE_PASSWORD && password.is_empty() {
        return Err(
            "access.mode is \"password\" but access.admin_password is empty — set the AUTH_PASSWORD environment variable (or set access.mode back to \"open\" in the settings table)"
                .into(),
        );
    }
    if password == "CHANGE_ME" {
        return Err(
            "access.admin_password is the placeholder \"CHANGE_ME\" — set a real password before starting"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn startup_mode_resync_matrix() {
        assert!(StartupMode::Serve.resync());
        assert!(StartupMode::Mutating.resync());
        assert!(!StartupMode::ReadOnly.resync());
    }

    #[test]
    fn admin_password_startup_check_matrix() {
        // password mode, empty → refuse
        assert!(admin_password_startup_check("password", "").is_err());
        // password mode, placeholder → refuse
        assert!(admin_password_startup_check("password", "CHANGE_ME").is_err());
        // password mode, real password → ok
        assert!(admin_password_startup_check("password", "s3cret").is_ok());
        // open mode, empty → ok (unauthenticated by design)
        assert!(admin_password_startup_check("open", "").is_ok());
        // open mode, placeholder → refuse (unfinished setup must not start silently)
        assert!(admin_password_startup_check("open", "CHANGE_ME").is_err());
    }

    // ── H1: PID lock + liveness unit tests (no Postgres) ─────────────────

    /// A [`Config`] whose `zim_dir` points at the given temp dir; all other
    /// fields stay at their (irrelevant-to-the-PID-lock) defaults.
    fn config_with_zim_dir(dir: &std::path::Path) -> Config {
        Config {
            zim_dir: dir.to_path_buf(),
            ..Config::default()
        }
    }

    fn lock_file(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join(".zimservice.lock")
    }

    /// A guaranteed-dead PID: spawn `true`, let it exit, and reap it. After
    /// `wait()` the child is gone, so `kill(pid, 0)` reports `ESRCH` and
    /// `pid_is_alive` returns `false` — the deterministic "dead holder" the
    /// stale-lock steal path needs (PID reuse in the microsecond window is
    /// negligible for a test).
    fn a_dead_pid() -> u32 {
        let mut child = std::process::Command::new("true")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn `true`");
        let pid = child.id();
        child.wait().expect("wait on `true`");
        assert_ne!(pid, 0, "child id must be nonzero");
        assert!(
            !pid_is_alive(pid),
            "sanity: the reaped child must read dead"
        );
        pid
    }

    #[test]
    fn pid_lock_no_lock_file_created_with_our_pid() {
        // (a) no lock file → created, recording our PID.
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_zim_dir(dir.path());
        let path = acquire_zim_dir_pid_lock(&config).unwrap();
        assert_eq!(path, lock_file(dir.path()));
        assert!(path.exists());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            std::process::id().to_string(),
            "lock file must record our PID"
        );
    }

    #[test]
    fn pid_lock_stale_dead_pid_is_stolen() {
        // (b) existing lock file with a DEAD pid → stale lock is stolen.
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_zim_dir(dir.path());
        let path = lock_file(dir.path());
        let dead = a_dead_pid();
        std::fs::write(&path, dead.to_string()).unwrap();
        // Steal: the stale file is removed and recreated with our PID.
        let new_path = acquire_zim_dir_pid_lock(&config).unwrap();
        assert_eq!(new_path, path);
        assert_eq!(
            std::fs::read_to_string(&new_path).unwrap().trim(),
            std::process::id().to_string(),
            "stolen lock file must be rewritten with our PID"
        );
    }

    #[test]
    fn pid_lock_live_own_pid_is_refused() {
        // (c) existing lock file holding OUR OWN (live) PID → refusal.
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_zim_dir(dir.path());
        let path = lock_file(dir.path());
        std::fs::write(&path, std::process::id().to_string()).unwrap();
        let err = acquire_zim_dir_pid_lock(&config).unwrap_err();
        assert!(
            err.to_string().contains("another zimservice instance"),
            "expected a live-holder refusal, got: {err}"
        );
        // The live holder's file must be left untouched (not stolen).
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            std::process::id().to_string(),
            "a live holder's lock file must not be stolen"
        );
    }

    #[test]
    fn pid_lock_garbage_content_is_stolen() {
        // (d) existing lock file with unparseable content → treated as stale
        // (pid parses to 0 → steal) and recreated.
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_zim_dir(dir.path());
        let path = lock_file(dir.path());
        std::fs::write(&path, "not-a-pid\n").unwrap();
        let new_path = acquire_zim_dir_pid_lock(&config).unwrap();
        assert_eq!(new_path, path);
        assert_eq!(
            std::fs::read_to_string(&new_path).unwrap().trim(),
            std::process::id().to_string(),
            "garbage lock file must be stolen and rewritten with our PID"
        );
    }

    // ── H1: advisory-lock liveness-monitor detection ─────────────────────
    //
    // The sqlx detector probes a *real* dedicated connection (`SELECT 1` on
    // an interval), so both tests are DB-gated like the smoke tests below:
    // they skip cleanly when no DB is reachable unless `ZIMSERVICE_REQUIRE_DB`
    // is set.

    #[tokio::test]
    async fn liveness_monitor_fires_on_closed_connection() {
        use std::sync::atomic::{AtomicBool, Ordering};
        // A dedicated connection whose backend is terminated up front — the
        // "socket went away" case the monitor must catch. Because the backend
        // is already gone, the first `SELECT 1` probe fails at once; no
        // interval wait.
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        let mut conn = match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            crate::db::pool::connect_dedicated(&url),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB set but cannot reach {url}: {e}");
                }
                eprintln!("skipping liveness_monitor_fires_on_closed_connection: cannot reach {url} ({e})");
                return;
            }
            Err(_) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB set but timed out reaching {url}");
                }
                eprintln!("skipping liveness_monitor_fires_on_closed_connection: timed out reaching {url}");
                return;
            }
        };
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        // sqlx's `Connection::close` consumes the handle, so sever the socket
        // the way a real crash does: a second dedicated connection terminates
        // this one's backend server-side. Any in-flight or next query on
        // `conn` now fails at once.
        let mut helper = crate::db::pool::connect_dedicated(&url)
            .await
            .expect("helper conn");
        let pid: i32 =
            crate::db::raw::fetch_scalar_optional(&mut conn, "SELECT pg_backend_pid()", |q| q)
                .await
                .expect("backend pid")
                .expect("row");
        let _ = crate::db::raw::execute(&mut helper, "SELECT pg_terminate_backend($1)", |q| {
            q.bind(pid)
        })
        .await;
        drop(helper);
        let fired = Arc::new(AtomicBool::new(false));
        let fired_cb = fired.clone();
        let detector =
            detect_advisory_lock_loss(conn, move |_| fired_cb.store(true, Ordering::SeqCst));
        tokio::time::timeout(std::time::Duration::from_secs(5), detector)
            .await
            .expect("detection of a closed connection must fire within 5s");
        assert!(
            fired.load(Ordering::SeqCst),
            "on_connection_lost must be invoked"
        );
    }

    #[tokio::test]
    async fn liveness_monitor_stays_quiet_while_alive() {
        use std::sync::atomic::{AtomicBool, Ordering};
        // A live dedicated connection: within a short window the detector
        // must NOT report a loss and must NOT fire the callback (no false
        // positive on a healthy connection). We abandon the (still-pending)
        // detector — its probe loop simply never completes in this window —
        // which also drops the connection.
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        let conn = match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            crate::db::pool::connect_dedicated(&url),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB set but cannot reach {url}: {e}");
                }
                eprintln!(
                    "skipping liveness_monitor_stays_quiet_while_alive: cannot reach {url} ({e})"
                );
                return;
            }
            Err(_) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB set but timed out reaching {url}");
                }
                eprintln!(
                    "skipping liveness_monitor_stays_quiet_while_alive: timed out reaching {url}"
                );
                return;
            }
        };
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        let fired = Arc::new(AtomicBool::new(false));
        let fired_cb = fired.clone();
        let detector =
            detect_advisory_lock_loss(conn, move |_| fired_cb.store(true, Ordering::SeqCst));
        let _ = tokio::time::timeout(std::time::Duration::from_millis(120), detector).await;
        assert!(
            !fired.load(Ordering::SeqCst),
            "a healthy connection must not trip the liveness monitor"
        );
    }

    // ── H1: DB-gated single-instance refusal (skips without Postgres) ─────

    /// Hold the single-instance advisory lock on a dedicated connection, then
    /// assert `acquire_instance_guard` **refuses** (`Ok(None)`) instead of a
    /// second `serve` silently starting against the same database. Mirrors the
    /// lib's `test_pool` gating: skips cleanly when no DB is reachable unless
    /// `ZIMSERVICE_REQUIRE_DB` is set. Runs in the lib's test context
    /// (startup.rs is a lib module), so it builds its own dedicated
    /// connection via `crate::db::pool::connect_dedicated` and serializes via
    /// `crate::testing::DbExclusiveGuard`.
    #[tokio::test]
    async fn smoke_single_instance_refusal() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        // Open the dedicated "first instance" connection (gated skip).
        let first = match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            crate::db::pool::connect_dedicated(&url),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB set but cannot reach {url}: {e}");
                }
                eprintln!("skipping smoke_single_instance_refusal: cannot reach {url} ({e})");
                return;
            }
            Err(_) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB set but timed out reaching {url}");
                }
                eprintln!("skipping smoke_single_instance_refusal: timed out reaching {url}");
                return;
            }
        };
        // Serialize with other DB tests (cross-process lockfile).
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        // The first instance takes the advisory lock (and keeps the
        // connection alive so the lock stays open).
        let mut holder = first;
        let held: bool =
            crate::db::raw::fetch_scalar_optional(&mut holder, INSTANCE_LOCK_SQL, |q| q)
                .await
                .expect("first connection must hold the lock")
                .unwrap();
        assert!(held, "setup: first connection must acquire the lock");

        // A second `serve` against the same DB must be refused. The advisory
        // lock check fails first, so this returns Ok(None) without touching the
        // PID lock (the temp zim_dir below is only present in case that changes).
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            database_url: url.clone(),
            zim_dir: tmp.path().to_path_buf(),
            ..Config::default()
        };
        let result = acquire_instance_guard(&config)
            .await
            .expect("acquire_instance_guard must not error when the lock is held");
        assert!(
            result.is_none(),
            "acquire_instance_guard must refuse (Ok(None)) while another instance holds the lock"
        );
        // `holder` stayed alive across the assertion above, so the first
        // instance genuinely held the lock the whole time. Dropping it here
        // closes the socket and releases the lock.
        drop(holder);
    }

    // ── H1: DB-gated mutating-guard refusal (skips without Postgres) ─────

    /// The mutating-command guard (H1) must refuse while a "server" holds the
    /// per-DB advisory lock, acquire when it is free, serialize a
    /// concurrently-starting `serve` while held, and release on drop.
    /// Inverse of [`smoke_single_instance_refusal`]: same lock, opposite
    /// direction. Gating mirrors that test (`DATABASE_URL` +
    /// `ZIMSERVICE_REQUIRE_DB`).
    #[tokio::test]
    async fn smoke_mutating_guard_refusal() {
        // The opt-out short-circuits the refusal this test asserts; a
        // developer's export must not silently break it.
        if matches!(
            std::env::var("ZIMSERVICE_ALLOW_MULTI_INSTANCE"),
            Ok(v) if v == "1"
        ) {
            eprintln!("skipping: ZIMSERVICE_ALLOW_MULTI_INSTANCE=1 is set");
            return;
        }
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        // Open the dedicated "server" connection (gated skip).
        let server = match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            crate::db::pool::connect_dedicated(&url),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB set but cannot reach {url}: {e}");
                }
                eprintln!("skipping smoke_mutating_guard_refusal: cannot reach {url} ({e})");
                return;
            }
            Err(_) => {
                if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
                    panic!("ZIMSERVICE_REQUIRE_DB set but timed out reaching {url}");
                }
                eprintln!("skipping smoke_mutating_guard_refusal: timed out reaching {url}");
                return;
            }
        };
        // Serialize with other DB tests (cross-process lockfile).
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        // The "server" takes the advisory lock and keeps the connection open
        // so the lock stays open for the test.
        let mut server_conn = server;
        let held: bool =
            crate::db::raw::fetch_scalar_optional(&mut server_conn, INSTANCE_LOCK_SQL, |q| q)
                .await
                .expect("server-sim must hold the lock")
                .unwrap();
        assert!(held, "setup: server-sim must acquire the lock");

        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            database_url: url.clone(),
            zim_dir: tmp.path().to_path_buf(),
            ..Config::default()
        };

        // 1) A mutating subcommand must refuse while the server holds the
        //    lock, with an actionable message naming the holder.
        let err = acquire_mutating_guard(&config)
            .await
            .expect_err("mutating guard must refuse while another session holds the lock");
        let msg = err.to_string();
        assert!(
            msg.contains("already using this database"),
            "unexpected refusal message: {msg}"
        );
        assert!(
            msg.contains("advisory lock held by pid"),
            "the holder's pid should be surfaced: {msg}"
        );

        // 2) Release the server-sim's lock → the guard must now acquire.
        let released: bool = crate::db::raw::fetch_scalar_optional(
            &mut server_conn,
            "SELECT pg_advisory_unlock(hashtext('zimservice:instance'))",
            |q| q,
        )
        .await
        .expect("unlock must run")
        .unwrap();
        assert!(released, "setup: server-sim must release the lock");
        let guard = acquire_mutating_guard(&config)
            .await
            .expect("a free lock must be acquirable")
            .expect("no opt-out env var is set, so the guard must be Some");

        // 3) While the guard holds the lock, a concurrently-starting `serve`
        //    must be refused (serialization in the other direction).
        let result = acquire_instance_guard(&config)
            .await
            .expect("acquire_instance_guard must not error while the guard holds the lock");
        assert!(
            result.is_none(),
            "a new serve must be refused while the mutating guard holds the lock"
        );

        // 4) Dropping the guard releases the lock again. Bounded retry: the
        //    dropped connection's socket close and Postgres's server-side lock
        //    release are asynchronous (the connect handshake below usually
        //    orders them first). A failed attempt holds nothing, so retries
        //    are safe; a successful one means *we* now hold it.
        drop(guard);
        let mut verify = crate::db::pool::connect_dedicated(&url)
            .await
            .expect("verify connect");
        let mut free = false;
        for _ in 0..20 {
            free = crate::db::raw::fetch_scalar_optional(&mut verify, INSTANCE_LOCK_SQL, |q| q)
                .await
                .expect("verify try-lock")
                .unwrap();
            if free {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            free,
            "dropping the MutatingGuard must release the advisory lock"
        );
        // Dropping the connections closes the sockets, releasing any held lock.
        drop(verify);
        drop(server_conn);
    }
}
