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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use sqlx::postgres::PgConnection;

use crate::access;
use crate::config::Config;
use crate::db;
use crate::search::SearchEngine;
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
     AND l.classid = ((hashtext('zimservice:instance')::bigint >> 32) & 4294967295)::oid \
     AND l.objid = (hashtext('zimservice:instance')::bigint & 4294967295)::oid \
     AND a.pid <> pg_backend_pid()";

/// The per-caller options that steer [`build_state`], bundled so the call
/// chain doesn't accrete more positional booleans (m1, 2026-09 review).
/// Use the named constructors — they encode the per-command contract:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupRequest {
    /// The startup behavior (serve / mutating / read-only).
    pub mode: StartupMode,
    /// This process already owns the single-instance advisory lock
    /// (`cmd_serve`'s lifetime guard, or a mutating command's guard) — the
    /// lightweight `pg_locks` warning probe is skipped (our own lock
    /// connection would otherwise register as "another instance").
    pub advisory_lock_held: bool,
    /// Whether to pay the qBittorrent login round-trip at startup
    /// (ARCH minor #3): the stdio `mcp` session never touches the
    /// qBittorrent client and skips it; everything else connects.
    pub connect_torrent: bool,
}

impl StartupRequest {
    /// `serve`: the `cmd_serve` guard was acquired before state was built
    /// (M-1); the qBittorrent client is part of the served state.
    pub fn serve(advisory_lock_held: bool) -> Self {
        Self {
            mode: StartupMode::Serve,
            advisory_lock_held,
            connect_torrent: true,
        }
    }

    /// A mutating subcommand (`index`, `embed`) that holds
    /// `acquire_mutating_guard` for the command's duration.
    pub fn mutating(advisory_lock_held: bool) -> Self {
        Self {
            mode: StartupMode::Mutating,
            advisory_lock_held,
            connect_torrent: true,
        }
    }

    /// `status`: read-only, but reports the qBittorrent connection state.
    pub fn status() -> Self {
        Self {
            mode: StartupMode::ReadOnly,
            advisory_lock_held: false,
            connect_torrent: true,
        }
    }

    /// `mcp`: read-only, and never touches the qBittorrent client
    /// (ARCH minor #3), so it skips the startup login round-trip.
    pub fn mcp() -> Self {
        Self {
            mode: StartupMode::ReadOnly,
            advisory_lock_held: false,
            connect_torrent: false,
        }
    }
}

/// Build the full application state (DB pool, settings, ZIM manager, etc.)
///
/// `req` bundles the startup behavior ([`StartupRequest::mode`]) with the
/// per-caller options (`advisory_lock_held`, `connect_torrent`) — see the
/// struct's constructors for the per-command contract.
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
pub async fn build_state(config: &Config, req: StartupRequest) -> anyhow::Result<AppState> {
    let StartupRequest {
        mode,
        advisory_lock_held,
        connect_torrent,
    } = req;

    // Shared pool + migration + ZIM-manager bootstrap (also used by
    // `cmd_list` — ARCH M1, so the two startup paths cannot diverge).
    let (pool, zims) = bootstrap_pool_and_zims(config).await?;

    // Read-replica pool (optional, `DATABASE_URL_READ` → `None` = current
    // single-pool behavior) + dedicated background pool (primary URL,
    // capped at `db::pool::BG_POOL_MAX_CONNECTIONS`) — built here so a
    // misconfigured read URL fails startup through the same validation
    // path as `DATABASE_URL`, before anything else uses the DB (PERF-10 /
    // read-replica finding: one shared pool for search + background work).
    let db_read = db::pool::create_read_pool(config).await?;
    let db_bg = db::pool::create_bg_pool(config).await?;

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
        // Raw SQL (db::raw): `pg_locks` catalog probe — the `db::raw`
        // helpers only cover application tables.
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
                    "Another zimservice instance appears to be running on this database \
                     (advisory lock already held), and this subcommand mutates the shared \
                     database. The running instance's in-memory caches (settings, ZIM \
                     manager, qBittorrent client) will be STALE after this run until it \
                     is restarted — prefer restarting the server, or run this command \
                     while no server is up."
                );
            } else {
                tracing::warn!(
                    "Another zimservice instance appears to be running on this \
                database (advisory lock already held)."
                );
            }
        }
    }

    // Load settings (seeds defaults on first run). Env access is injected
    // once: the same getter feeds both the UI lock map and the reload
    // snapshot, so the two can never disagree about which vars were set.
    let env_get = |k: &str| std::env::var(k).ok();
    let env_locked = config.locked_env_settings(&env_get);
    let mut env_snapshot = config.env_settings_snapshot(&env_get);
    // M-1: non-loopback binds default `access.require_auth_for_reads` to
    // true unless REQUIRE_AUTH_FOR_READS was set explicitly.
    config.apply_require_reads_default(&mut env_snapshot);
    // SEC L-1: an env-seeded legacy plaintext password must never ride the
    // snapshot (and the lazy upgrade write) into the settings table as
    // plaintext — hash it here, before the settings load applies it.
    hash_legacy_env_seeded_password(&mut env_snapshot);
    // Load settings (seeds defaults on first run). `config.security_key`
    // enables the at-rest encryption of the secret settings (SEC-L5);
    // `None` keeps the legacy plaintext behavior.
    let settings = SettingsCache::load_with_security_key(
        pool.clone(),
        env_locked,
        env_snapshot,
        config.security_key.as_deref(),
    )
    .await?;
    settings.sync_from_config(config); // ARCH-1: general.* display keys <- Config

    // Password mode with no password would fail open on every request —
    // refuse to start instead. The placeholder is rejected in **both** modes:
    // a placeholder means unfinished setup (open mode with CHANGE_ME would
    // otherwise start unauthenticated without anyone noticing).
    let admin_password = settings
        .get_typed::<String>(KEY_ACCESS_ADMIN_PASSWORD)
        .unwrap_or_default();
    admin_password_startup_check(&settings.access_mode(), &admin_password)
        .map_err(anyhow::Error::msg)?;

    // Legacy admin password (plaintext or sha2:) is still verifiable until
    // the next successful auth — at which point it is transparently upgraded
    // to argon2id. The operator may not know a plaintext secret still sits
    // in the DB, so warn at serve startup (SEC L-3). One-shot CLI subcommands
    // don't get it: they don't keep the credential warm for an operator to
    // act on. Env-seeded plaintext is hashed at startup (SEC L-1, above), so
    // this only fires for non-env legacy values (DB rows, or `sha2:`-prefixed
    // AUTH_PASSWORD values, which stay on the lazy upgrade path).
    if mode == StartupMode::Serve
        && legacy_password_startup_warn(&settings.access_mode(), &admin_password)
    {
        tracing::warn!(
            "access.admin_password is stored in a legacy format (plaintext or \
             sha2:): it still verifies, but a plaintext secret may sit in the \
             database. It upgrades to argon2id on the next successful auth — \
             rotate the password (set AUTH_PASSWORD to a new value) to upgrade \
             it now."
        );
    }

    // Discover ZIMs on disk, load persisted index state from Postgres, then
    // (when `resync`) reconcile the cache + DB with the actual files. (The
    // `zims` manager itself came from `bootstrap_pool_and_zims` above.)
    populate_zims(&zims, mode.resync()).await?;

    // Search engine — the arms / `suggest()` / the `ensure_trgm` slow path
    // check out from the read replica when `DATABASE_URL_READ` is set, else
    // from the primary pool (PERF-10: foreground search must not contend
    // with background work on the shared pool).
    let degradation = crate::DegradationTracker::default();
    let search_pool = db_read.clone().unwrap_or_else(|| pool.clone());
    let search = SearchEngine::new(search_pool, settings.clone(), degradation.clone());

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
                    "qBittorrent: private/LAN network access enabled via \
                    torrent.allow_private_networks"
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

    // Cross-process cache invalidation (LISTEN/NOTIFY, `db::notify`): the
    // serve process keeps in-memory caches (settings, ZIM catalog) that a
    // peer instance or a mutating CLI (under the multi-instance opt-out) can
    // stale. Spawn the listener only for `serve` — the long-running process
    // whose freshness matters; one-shot CLI subcommands (mutating / read-only
    // / MCP) read the DB fresh and must not keep a background listener alive.
    // The listener starts in `reconnecting` and never blocks or fails startup
    // (a lost session degrades to the next local resync / restart and reports
    // itself in `/diagnostic`). Spawned after `populate_zims` so the startup
    // resync has already converged the caches and the listener's
    // connect-resync is a no-op.
    let notify = if mode == StartupMode::Serve {
        // Composition-root invalidation wiring: `db::notify` is
        // domain-agnostic, so the per-bump domain actions are closures over
        // the caches built here (settings channel → full reload, catalog
        // channel → resync).
        let settings_action = settings.clone();
        let on_settings = Box::new(
            move |bump: u64| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                let cache = settings_action.clone();
                Box::pin(async move {
                    match cache.reload().await {
                        Ok(()) => {
                            tracing::debug!(bump, "cross-process settings invalidation: reloaded")
                        }
                        Err(e) => {
                            tracing::error!(
                                "cross-process settings invalidation: reload failed: {e}"
                            )
                        }
                    }
                })
            },
        );
        let catalog_action = zims.clone();
        let on_catalog = Box::new(
            move |bump: u64| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                let zims = catalog_action.clone();
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
                            tracing::error!(
                                "cross-process catalog invalidation: resync failed: {e}"
                            )
                        }
                    }
                })
            },
        );
        Some(crate::db::notify::spawn_listener(
            &config.database_url,
            on_settings,
            on_catalog,
        ))
    } else {
        None
    };

    Ok(AppState {
        db: pool,
        db_read,
        db_bg,
        settings,
        zims,
        search,
        torrent,
        rate_limiter: Arc::new(access::ratelimit::RateLimiterHandle::new()),
        probes: crate::HealthProbes::default(),
        auth_lockout: Arc::new(Default::default()),
        degradation,
        build_probe: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        index_building: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        notify,
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
/// Returns `Ok(Some(conn))` when the lock was acquired (`conn` is the
/// dedicated `PgConnection` holding the advisory lock),
/// `Ok(None)` when another instance already holds it (the connection is
/// closed in that case — an unacquired lock releases nothing, and the
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

/// M1: whether this process started with the full single-instance opt-out
/// (`ZIMSERVICE_ALLOW_MULTI_INSTANCE=1`) — the one place that reading of the
/// env var lives so the guard sites, the `/health` flag, and the settings-
/// divergence warnings all agree.
///
/// In that mode every instance keeps its own in-memory caches (settings,
/// rate limiter, ZIM metadata) with **no cross-instance invalidation path**:
/// a change persisted by one instance is invisible to the others until
/// restart. The callers use this to surface the divergence continuously
/// instead of only via the one startup `tracing::warn!`.
///
/// This is the **single, deliberate** reader of
/// `ZIMSERVICE_ALLOW_MULTI_INSTANCE`, kept outside `Config` on purpose: it
/// must stay re-readable at **any point after process start** (startup
/// guards, `/health`, the settings-divergence warnings), not just at
/// `Config::load()` time. Note that `SettingsCache` (`src/settings/cache.rs`)
/// calls this at construction and **snapshots the result once** — callers
/// that need the live value must call this fn directly, not the cached copy.
pub fn multi_instance_allowed() -> bool {
    matches!(std::env::var("ZIMSERVICE_ALLOW_MULTI_INSTANCE"), Ok(v) if v == "1")
}

/// m-7 partial opt-out: live reader of `ZIMSERVICE_ALLOW_MULTI_DB` (exact
/// "1" — the same parse [`crate::config::Config::load`] applies). Like
/// [`multi_instance_allowed`], a deliberate env reader kept outside `Config`
/// so `/health` can surface the process's degraded single-instance guarantee
/// without a state field.
pub fn multi_db_allowed() -> bool {
    matches!(std::env::var("ZIMSERVICE_ALLOW_MULTI_DB"), Ok(v) if v == "1")
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
    if multi_instance_allowed() {
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
#[cfg(unix)]
#[allow(unsafe_code)] // one narrow, well-documented POSIX probe
pub fn pid_is_alive(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    match rc {
        0 => true, // process exists and we may signal it
        -1 => std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH),
        _ => true,
    }
}

#[cfg(not(unix))]
pub fn pid_is_alive(_pid: u32) -> bool {
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
            "access.mode is \"password\" but access.admin_password is empty — set the \
            AUTH_PASSWORD environment variable (or set access.mode back to \"open\" in the \
            settings table)"
                .into(),
        );
    }
    if password == "CHANGE_ME" {
        return Err(
            "access.admin_password is the placeholder \"CHANGE_ME\" — set a real password before \
            starting"
                .into(),
        );
    }
    Ok(())
}

/// True when the serve-startup legacy-password warning should be emitted
/// (SEC L-3): password mode with a configured password whose stored value
/// is not a current argon2id hash — i.e. a legacy `sha2:` hash or legacy
/// plaintext. Empty passwords are excluded: in password mode they are
/// already a hard refusal via [`admin_password_startup_check`].
pub(crate) fn legacy_password_startup_warn(mode: &str, password: &str) -> bool {
    mode == crate::settings::ACCESS_MODE_PASSWORD
        && !password.is_empty()
        && crate::settings::is_legacy_password(password)
}

/// SEC L-1: hash an env-seeded legacy **plaintext** `access.admin_password`
/// in the startup env snapshot, before [`SettingsCache::load`] applies it —
/// so a plaintext secret from `AUTH_PASSWORD` is stored (and re-applied on
/// every `reload()` from the env-locked snapshot) as an argon2id hash from
/// first boot, instead of persisting plaintext until the middleware's lazy
/// upgrade on the first successful auth. Pure + DB-free, like the other
/// snapshot helpers. Passthrough for every non-plaintext value:
///
/// - absent/empty — untouched (`env_settings_snapshot` skips empty vars, so
///   this is the defensive arm only);
/// - current argon2id hash — untouched (`is_legacy_password` is false);
/// - `sha2:`-prefixed legacy hash — untouched (out of scope; it still
///   verifies and stays upgradable via the existing lazy path);
/// - the `CHANGE_ME` placeholder — **deliberately** not hashed, so
///   [`admin_password_startup_check`] still refuses to start on it (hashing
///   it would convert "unfinished setup" into a valid credential).
///
/// Verification is unaffected: `verify_admin_password` accepts the argon2id
/// PHC string produced by `hash_admin_password` for the original plaintext.
pub(crate) fn hash_legacy_env_seeded_password(snapshot: &mut HashMap<String, String>) {
    let Some(raw) = snapshot.get(KEY_ACCESS_ADMIN_PASSWORD) else {
        return;
    };
    if raw.is_empty()
        || raw == "CHANGE_ME"
        || raw.starts_with("sha2:")
        || !crate::settings::is_legacy_password(raw)
    {
        return;
    }
    let hashed = crate::settings::hash_admin_password(raw);
    snapshot.insert(KEY_ACCESS_ADMIN_PASSWORD.into(), hashed);
}

/// Log level for a [`StartupWarning`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarnLevel {
    /// Logged via `tracing::warn!`.
    Warn,
    /// Logged via `tracing::info!`.
    Info,
}

/// A warning message from the startup security-policy checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupWarning {
    /// The human-readable warning text to log.
    pub message: String,
    /// Whether this is logged at `warn` or `info` level.
    pub level: WarnLevel,
}

/// Run the startup security-policy checks for a `serve` invocation.
///
/// Returns `Ok(warnings)` with any non-fatal warnings the operator should
/// see, or `Err(message)` for fatal policy violations that must refuse
/// startup (open-mode non-loopback bind, `/0` CIDR in trusted proxies).
///
/// This is a pure function (no I/O, no DB) so it can be unit-tested without
/// a running server or database.
pub fn serve_policy_checks(
    host: &str,
    access_mode: &str,
    require_auth_for_reads: bool,
    trusted_proxy_cidrs: &str,
) -> Result<Vec<StartupWarning>, String> {
    use crate::access::cidr;
    use crate::settings;

    let is_loopback = matches!(host, "127.0.0.1" | "localhost" | "::1");

    // 1. Open mode on non-loopback: refuse.
    if access_mode == settings::ACCESS_MODE_OPEN && !is_loopback {
        return Err(
            "access.mode=open requires a loopback bind (host=127.0.0.1 or ::1); \
             set access.mode=password (with AUTH_PASSWORD) for network-facing deployments"
                .to_string(),
        );
    }

    let mut warnings: Vec<StartupWarning> = Vec::new();

    // 2 + 3. Password mode on non-loopback: TLS + read-gating warnings.
    if access_mode == settings::ACCESS_MODE_PASSWORD && !is_loopback {
        warnings.push(StartupWarning {
            message: format!(
                "access.mode=password with non-loopback bind ({host}): ensure TLS \
                 termination (reverse proxy) to protect AUTH_PASSWORD in transit. \
                 See the README 'Security' section."
            ),
            level: WarnLevel::Warn,
        });
        if require_auth_for_reads {
            warnings.push(StartupWarning {
                message: format!(
                    "access.require_auth_for_reads is effective-true on non-loopback bind \
                     ({host}): GET/HEAD/OPTIONS require the admin password (M-1 \
                     default). Set REQUIRE_AUTH_FOR_READS=false to restore open \
                     reads — see the README 'Security' section."
                ),
                level: WarnLevel::Warn,
            });
        } else {
            warnings.push(StartupWarning {
                message: format!(
                    "access.require_auth_for_reads is false on non-loopback bind ({host}): \
                     reads (GET/HEAD/OPTIONS) stay open — full article content is \
                     exposed to any network peer. Set REQUIRE_AUTH_FOR_READS=true \
                     to gate reads."
                ),
                // Explicit operator opt-out exposing full article content to
                // any network peer — same level as its effective-true sibling.
                level: WarnLevel::Warn,
            });
        }
    }

    // 4. /0 CIDR: refuse.
    if cidr::has_zero_prefix_cidr(trusted_proxy_cidrs) {
        return Err(
            "general.trusted_proxy_cidrs contains a /0 CIDR (prefix length 0, e.g. \
             0.0.0.0/0 or ::/0): a /0 entry matches every address, so it would trust \
             every X-Forwarded-For value and defeat the per-IP auth-failure lockout. \
             Restrict the list to your actual proxy IPs."
                .to_string(),
        );
    }

    // 5. Over-broad CIDR: warn.
    if cidr::has_over_broad_cidr(trusted_proxy_cidrs) {
        warnings.push(StartupWarning {
            message: "general.trusted_proxy_cidrs contains an over-broad CIDR \
             (≥ /8 IPv4 or ≥ /56 IPv6): this allows X-Forwarded-For lockout bypass. \
             Restrict to your actual proxy IPs."
                .to_string(),
            level: WarnLevel::Warn,
        });
    }

    // 6. Open mode behind a proxy: warn (Sec M2, 2026-09 review). The TLS
    // warning above only fires in password mode, so a public reverse proxy
    // fronting a loopback open-mode instance gets NO startup warning at all —
    // while silently defeating the loopback-only guarantee (open mode has no
    // password to gate reads). Make the operator acknowledge the exposure.
    if access_mode == settings::ACCESS_MODE_OPEN && !trusted_proxy_cidrs.trim().is_empty() {
        warnings.push(StartupWarning {
            message: "access.mode=open with general.trusted_proxy_cidrs set: if a public \
                 reverse proxy fronts this loopback instance, the loopback-only \
                 guarantee is defeated and the full library is exposed to network \
                 peers with no password to gate reads. For network-facing \
                 deployments use access.mode=password (with AUTH_PASSWORD) and \
                 terminate TLS at the proxy — see the README 'Security' section."
                .to_string(),
            level: WarnLevel::Warn,
        });
    }

    // 7. Password mode, non-loopback, behind a proxy: warn (2026-09 review
    // round 2). The `?access_token=` query-string credential channel is
    // last-resort: through a reverse proxy the token also passes proxy
    // access logs, Referer headers, and browser history — outside the app's
    // control. Prefer the Bearer Authorization header; treat `?access_token=`
    // as last-resort.
    if access_mode == settings::ACCESS_MODE_PASSWORD
        && !is_loopback
        && !trusted_proxy_cidrs.trim().is_empty()
    {
        warnings.push(StartupWarning {
            message: format!(
                "access.mode=password with non-loopback bind ({host}) and \
                 general.trusted_proxy_cidrs set: the last-resort ?access_token= \
                 query-string credential channel passes through the proxy (proxy \
                 access logs, Referer headers, browser history are outside the \
                 app's control). Prefer the Bearer Authorization header; keep \
                 ?access_token= for clients that cannot set headers."
            ),
            level: WarnLevel::Warn,
        });
    }

    Ok(warnings)
}

/// SEC-L5: the plaintext-at-rest warning — present when `SECURITY_KEY` is
/// unset (`key_set == false`) while one of the at-rest-encrypted settings
/// `crate::settings::encrypt::ENCRYPTED_AT_REST_KEYS`]) is non-empty
/// after `SettingsCache::load` (`secret_stored == true`). The legacy
/// plaintext behavior stays VALID (loopback / throwaway deployments may
/// accept it), so this WARNS rather than refusing. Both inputs are
/// precomputed at the call site (`src/main.rs` — the one place that owns
/// both the `Config` and the loaded settings cache); this helper keeps the
/// text + level pure and unit-testable without I/O.
pub fn security_key_plaintext_warning(
    key_set: bool,
    secret_stored: bool,
) -> Option<StartupWarning> {
    if key_set || !secret_stored {
        return None;
    }
    Some(StartupWarning {
        message: "SECURITY_KEY is not set — the settings table stores \
                  torrent.password / embedding.api_key / access.read_only_token \
                  in PLAINTEXT at rest; set SECURITY_KEY to enable \
                  AES-256-GCM encryption at rest (README 'Security' section)"
            .to_string(),
        level: WarnLevel::Warn,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn startup_mode_resync_matrix() {
        assert!(StartupMode::Serve.resync());
        assert!(StartupMode::Mutating.resync());
        assert!(!StartupMode::ReadOnly.resync());
    }

    #[test]
    fn security_key_plaintext_warning_matrix() {
        // No key + a stored secret → the Warn is present and named.
        let w = security_key_plaintext_warning(false, true)
            .expect("unset key + stored secret must warn");
        assert_eq!(w.level, WarnLevel::Warn);
        assert!(
            w.message.contains("SECURITY_KEY is not set"),
            "{}",
            w.message
        );
        // Key set → no warning, even with a stored secret.
        assert!(
            security_key_plaintext_warning(true, true).is_none(),
            "a set SECURITY_KEY must not warn"
        );
        // No stored secret → nothing exposed, no warning.
        assert!(
            security_key_plaintext_warning(false, false).is_none(),
            "an empty settings table must not warn"
        );
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

    // ── SEC L-1: env-seeded plaintext hashed at startup ──────────────────

    #[test]
    fn env_seeded_plaintext_password_hashed_at_startup() {
        // (a) plaintext in the snapshot is replaced by a salted argon2id
        // hash that verifies against the original plaintext.
        let mut snap = HashMap::from([(KEY_ACCESS_ADMIN_PASSWORD.into(), "plainpw".to_string())]);
        hash_legacy_env_seeded_password(&mut snap);
        let hashed = snap
            .get(KEY_ACCESS_ADMIN_PASSWORD)
            .expect("value must stay present");
        assert_ne!(hashed, "plainpw", "the plaintext must not survive");
        assert!(
            !crate::settings::is_legacy_password(hashed),
            "must be current-format"
        );
        assert!(crate::settings::verify_admin_password(hashed, "plainpw"));
        assert!(!crate::settings::verify_admin_password(hashed, "wrong"));
    }

    #[test]
    fn env_seeded_sha2_password_passes_through() {
        // (b) `sha2:`-prefixed values stay upgradable via the existing lazy
        // path — the startup transform must not touch them.
        let legacy = "sha2:100000$00000000000000000000000000000000$0000000000000000000000000000000\
        000000000000000000000000000000000";
        let mut snap = HashMap::from([(KEY_ACCESS_ADMIN_PASSWORD.into(), legacy.to_string())]);
        hash_legacy_env_seeded_password(&mut snap);
        assert_eq!(
            snap.get(KEY_ACCESS_ADMIN_PASSWORD),
            Some(&legacy.to_string()),
            "sha2: values must pass through unchanged"
        );
    }

    #[test]
    fn env_seeded_empty_or_absent_password_unchanged() {
        // (c) absent key stays absent; a current argon2id hash is untouched.
        let mut snap: HashMap<String, String> = HashMap::new();
        hash_legacy_env_seeded_password(&mut snap);
        assert!(
            !snap.contains_key(KEY_ACCESS_ADMIN_PASSWORD),
            "absent key must stay absent"
        );
        let hashed = crate::settings::hash_admin_password("s3cret");
        let mut snap2 = HashMap::from([(KEY_ACCESS_ADMIN_PASSWORD.into(), hashed.clone())]);
        hash_legacy_env_seeded_password(&mut snap2);
        assert_eq!(
            snap2.get(KEY_ACCESS_ADMIN_PASSWORD),
            Some(&hashed),
            "current-format hashes must pass through unchanged"
        );
    }

    #[test]
    fn env_seeded_placeholder_password_not_hashed() {
        // The CHANGE_ME placeholder must survive the transform un-hashed, so
        // `admin_password_startup_check` still refuses to start on it.
        let mut snap = HashMap::from([(KEY_ACCESS_ADMIN_PASSWORD.into(), "CHANGE_ME".to_string())]);
        hash_legacy_env_seeded_password(&mut snap);
        assert_eq!(
            snap.get(KEY_ACCESS_ADMIN_PASSWORD),
            Some(&"CHANGE_ME".to_string()),
            "the placeholder must reach the startup check un-hashed"
        );
        assert!(admin_password_startup_check("password", "CHANGE_ME").is_err());
    }

    #[test]
    fn legacy_password_startup_warn_matrix() {
        // password mode + legacy plaintext → warn
        assert!(legacy_password_startup_warn("password", "plainpw"));
        // password mode + legacy sha2: hash → warn
        let legacy = "sha2:100000$00000000000000000000000000000000$0000000000000000000000000000000\
        000000000000000000000000000000000";
        assert!(legacy_password_startup_warn("password", legacy));
        // password mode + current argon2id hash → no warn
        assert!(!legacy_password_startup_warn(
            "password",
            &crate::settings::hash_admin_password("s3cret")
        ));
        // open mode never warns (no credential involved)
        assert!(!legacy_password_startup_warn("open", "plainpw"));
        // password mode + empty → no warn (hard refusal above already covers it)
        assert!(!legacy_password_startup_warn("password", ""));
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
        // DB gate (src/testing.rs): counted skip, strict-mode hard-fail.
        let Some(mut conn) =
            crate::testing::test_conn("liveness_monitor_fires_on_closed_connection").await
        else {
            return;
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
        // DB gate (src/testing.rs): counted skip, strict-mode hard-fail.
        let Some(conn) =
            crate::testing::test_conn("liveness_monitor_stays_quiet_while_alive").await
        else {
            return;
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
    /// second `serve` silently starting against the same database. Gated via
    /// `crate::testing::test_conn` (counted skip; hard-fails under
    /// `ZIMSERVICE_REQUIRE_DB`) and serialized via
    /// `crate::testing::DbExclusiveGuard`.
    #[tokio::test]
    async fn smoke_single_instance_refusal() {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        // DB gate (src/testing.rs): counted skip, strict-mode hard-fail.
        let Some(first) = crate::testing::test_conn("smoke_single_instance_refusal").await else {
            return;
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
        // DB gate (src/testing.rs): counted skip, strict-mode hard-fail.
        let Some(server) = crate::testing::test_conn("smoke_mutating_guard_refusal").await else {
            return;
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

    /// M1: `multi_instance_allowed` mirrors the process env exactly.
    /// Mutating the process env in a parallel test would race every other
    /// test, so (like the DB-gated tests above) this asserts against the
    /// env as found: the unset/non-`1` arm unconditionally, the `1` arm
    /// only when a developer has exported the opt-out.
    #[test]
    fn multi_instance_allowed_mirrors_env() {
        match std::env::var("ZIMSERVICE_ALLOW_MULTI_INSTANCE") {
            Ok(v) if v == "1" => {
                assert!(multi_instance_allowed(), "the `1` opt-out must be honored")
            }
            _ => assert!(
                !multi_instance_allowed(),
                "unset (or non-`1`) must mean single-instance mode"
            ),
        }
    }
}

/// M4: core of the background-task supervisor — unit-testable in isolation.
/// (M4: moved here out of the binary's `cmd_serve` so the whole post-serve
/// sequence — supervisor core + shutdown protocol — lives in the lib and is
/// testable under `--lib`. Behavior unchanged.)
///
/// Polls the named `JoinHandle`s every `poll_interval` and reports the first
/// one that finishes — a panic (surfaced as `Err(JoinError)`) **or** a normal
/// early return (`Ok(value)`), since both mean the task will not keep the
/// pipeline alive for the server's lifetime. `is_finished()` is non-consuming,
/// so only the one handle that actually finished is `await`ed (consumed).
///
/// Returns the reported task plus the **remaining** handles untouched, so
/// the caller (the shutdown path) can still abort/await them. Returns
/// `None` (with all handles handed back) if the `stop` watch is set first,
/// which bounds the wait during clean shutdown. Never calls
/// `process::exit` — the caller decides how to react.
pub async fn wait_for_task_failure<T: Send + 'static>(
    tasks: Vec<(&'static str, tokio::task::JoinHandle<T>)>,
    poll_interval: std::time::Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> (
    Option<(&'static str, Result<T, tokio::task::JoinError>)>,
    Vec<(&'static str, tokio::task::JoinHandle<T>)>,
) {
    let mut tasks = tasks;
    loop {
        // First finished task wins. `is_finished()` is non-consuming.
        if let Some(i) = tasks.iter().position(|(_, handle)| handle.is_finished()) {
            let (name, handle) = tasks.remove(i);
            let result = handle.await; // consumes only the finished handle
            return (Some((name, result)), tasks);
        }
        // Sleep until the next poll or until the stop flag is set (clean
        // shutdown — hand the still-alive handles back without reporting).
        tokio::select! {
            _ = tokio::time::sleep(poll_interval) => {}
            _ = stop.changed() => {
                if *stop.borrow() {
                    return (None, tasks);
                }
            }
        }
    }
}

/// M4: the post-`axum::serve` shutdown protocol, extracted out of the binary
/// `cmd_serve` so the wiring is a library fn unit-testable with dummy
/// handles (which handle is stopped/aborted, which failure kind wins, and
/// that cleanup always runs — the old `?.await` could bail before the
/// task-abort block on a low-level network error, S3).
///
/// `axum::serve` resolves to `Result<(), std::io::Error>` in axum 0.8
/// (listener-level failures surface as I/O errors), hence the concrete
/// error type below.
///
/// Protocol (identical to the inlined `cmd_serve` sequence it replaced):
/// 1. stop the supervisor (so it hands back the still-alive handles),
/// 2. await the supervisor — a panicked supervisor is logged and treated as
///    "no survivors" (it only polls finished handles, so this is impossible
///    in practice; the shutdown must continue either way),
/// 3. abort every survivor, then await them under a bounded drain timeout,
///    logging a non-cancelled `JoinError` (a task that died in the narrow
///    window between the supervisor's last poll and the abort) and skipping
///    cancelled ones (we just aborted them — not a death),
/// 4. classify: `axum::serve` error wins (S3 ordering — it surfaced first in
///    the inlined code), else the supervisor's die flag (H2 fail-closed),
///    else a clean exit.
///
/// Never calls `process::exit` — the caller maps the outcome to its exit
/// code. All four background tasks return `()`, hence the concrete
/// `JoinHandle<()>` survivor type (the supervisor's `wait_for_task_failure`
/// is generic, but `cmd_serve`'s tasks are all unit-returning).
#[derive(Debug)]
pub enum ServeFailure {
    /// `axum::serve` returned a low-level error (not just Ctrl-C). Cleanup
    /// has already completed; the error is surfaced to the operator.
    Serve(std::io::Error),
    /// The supervisor observed a background task finish early (panic or
    /// early return) — the download→verify→index pipeline is broken,
    /// so the process must exit non-zero (H2).
    BackgroundTaskDied,
}

impl std::fmt::Display for ServeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // The `Display` here is the operator-facing exit message — keep
            // it in sync with the `cmd_serve` mapping (which formats it
            // verbatim into the `anyhow` error).
            Self::Serve(e) => write!(f, "server error: {e}"),
            Self::BackgroundTaskDied => {
                f.write_str("background task died (see log above); exiting non-zero")
            }
        }
    }
}

impl std::error::Error for ServeFailure {}

/// M4: run the post-serve shutdown protocol and report the exit kind. See
/// the module-level doc on [`ServeFailure`] for the exact sequence. `Ok(())`
/// is a clean exit (exit 0); `Err(ServeFailure)` is a non-zero exit.
pub async fn finalize_serve_shutdown(
    serve_result: Result<(), std::io::Error>,
    sup_stop_tx: tokio::sync::watch::Sender<bool>,
    supervisor: tokio::task::JoinHandle<Vec<(&'static str, tokio::task::JoinHandle<()>)>>,
    died_check: &tokio::sync::watch::Receiver<bool>,
    drain_timeout: std::time::Duration,
) -> Result<(), ServeFailure> {
    // Graceful shutdown: stop the supervisor (so it hands back the still-
    // alive handles), abort those tasks, and wait briefly for them to drain.
    // Reached on **both** Ok and Err of `axum::serve` (S3).
    let _ = sup_stop_tx.send(true);
    // The supervisor returns the survivors; a JoinError here means the
    // supervisor itself panicked (it only polls + awaits finished handles,
    // so this should be impossible — log and continue the shutdown).
    let survivors = match supervisor.await {
        Ok(s) => s,
        Err(join_err) => {
            tracing::error!("background-task supervisor died: {join_err}");
            Vec::new()
        }
    };

    tracing::info!("shutting down background tasks");
    for (_, handle) in &survivors {
        handle.abort();
    }

    // H2: the supervisor already `tracing::error!`-logged any task that died
    // and consumed that handle, so the survivors it hands back are only the
    // rest. A *cancelled* JoinError is expected (we just aborted it) and is
    // skipped so it does not masquerade as a death.
    let _ = tokio::time::timeout(drain_timeout, async move {
        for (name, handle) in survivors {
            if let Err(join_err) = handle.await {
                if !join_err.is_cancelled() {
                    tracing::error!(
                        "background task '{name}' died just before shutdown: {join_err}"
                    );
                }
            }
        }
    })
    .await;

    // Surface the serve error (if any) after cleanup completes (S3 ordering:
    // it won over the H2 die flag in the inlined sequence, so it wins here).
    if let Err(e) = serve_result {
        return Err(ServeFailure::Serve(e));
    }
    if *died_check.borrow() {
        return Err(ServeFailure::BackgroundTaskDied);
    }
    Ok(())
}

#[cfg(test)]
mod serve_shutdown_tests {
    use super::wait_for_task_failure;
    use super::{finalize_serve_shutdown, ServeFailure};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Drop flag: a task that drops this while running proves it was
    /// actually aborted/cancelled (not just "reported as a survivor").
    struct DropFlag(&'static AtomicBool);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A task that runs until aborted and flips `flag` on drop.
    fn long_task(flag: &'static AtomicBool) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let _flag = DropFlag(flag);
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
    }

    /// Supervisor dummy: reports nothing, stops when `stop_rx` flips, and
    /// hands `tasks` back as survivors (mirrors the real supervisor's
    /// clean-shutdown path).
    async fn stop_supervisor(
        mut stop_rx: tokio::sync::watch::Receiver<bool>,
        tasks: Vec<(&'static str, tokio::task::JoinHandle<()>)>,
    ) -> Vec<(&'static str, tokio::task::JoinHandle<()>)> {
        let _ = stop_rx.wait_for(|s: &bool| *s).await;
        tasks
    }

    /// Supervisor dummy: uses the real `wait_for_task_failure` core, so a
    /// task that finishes early is reported and the die flag is tripped —
    /// exactly the real supervisor's failure path.
    async fn dying_supervisor(
        stop_rx: tokio::sync::watch::Receiver<bool>,
        died_tx: tokio::sync::watch::Sender<bool>,
        tasks: Vec<(&'static str, tokio::task::JoinHandle<()>)>,
    ) -> Vec<(&'static str, tokio::task::JoinHandle<()>)> {
        let (report, survivors) =
            wait_for_task_failure(tasks, std::time::Duration::from_millis(10), stop_rx).await;
        if let Some((name, result)) = report {
            // Same log as the real supervisor (keeps the test honest about
            // what the operator would see in the log above the exit).
            match result {
                Err(join_err) => tracing::error!("background task '{name}' died: {join_err}"),
                Ok(()) => {
                    tracing::error!(
                        "background task '{name}' returned before shutdown 
                         (it must run for the lifetime of the server)"
                    )
                }
            }
            let _ = died_tx.send(true);
        }
        survivors
    }

    /// Clean shutdown: no serve error, no die flag → `Ok(())`, and every
    /// survivor the supervisor handed back was genuinely aborted.
    #[tokio::test]
    async fn clean_shutdown_aborts_all_survivors() {
        static A: AtomicBool = AtomicBool::new(false);
        static B: AtomicBool = AtomicBool::new(false);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let supervisor = tokio::spawn(stop_supervisor(
            stop_rx,
            vec![
                ("zim-watcher", long_task(&A)),
                ("trgm-probe", long_task(&B)),
            ],
        ));
        let (_died_tx, died_rx) = tokio::sync::watch::channel(false);

        let result = finalize_serve_shutdown(
            Ok(()),
            stop_tx,
            supervisor,
            &died_rx,
            std::time::Duration::from_secs(2),
        )
        .await;

        assert!(result.is_ok(), "clean shutdown must be Ok(()): {result:?}");
        assert!(
            A.load(Ordering::SeqCst),
            "survivor A must have been aborted"
        );
        assert!(
            B.load(Ordering::SeqCst),
            "survivor B must have been aborted"
        );
    }

    /// S3 path: `axum::serve` returned `Err` → cleanup still runs (both
    /// tasks aborted) and the failure kind is `Serve` with the error
    /// preserved.
    #[tokio::test]
    async fn serve_error_still_runs_cleanup_and_reports_serve() {
        static A: AtomicBool = AtomicBool::new(false);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let supervisor = tokio::spawn(stop_supervisor(
            stop_rx,
            vec![("zim-watcher", long_task(&A))],
        ));
        let (_died_tx, died_rx) = tokio::sync::watch::channel(false);
        let err = std::io::Error::other("listen socket closed");

        let result = finalize_serve_shutdown(
            Err(err),
            stop_tx,
            supervisor,
            &died_rx,
            std::time::Duration::from_secs(2),
        )
        .await;

        match result {
            Err(ServeFailure::Serve(e)) => {
                assert!(
                    e.to_string().contains("listen socket closed"),
                    "the axum error must be preserved: {e}"
                );
            }
            other => panic!("expected ServeFailure::Serve, got {other:?}"),
        }
        assert!(
            A.load(Ordering::SeqCst),
            "cleanup must abort tasks even on serve error"
        );
    }

    /// H2 path: a background task finished early, the supervisor tripped the
    /// die flag, no serve error → `BackgroundTaskDied` (fail-closed).
    #[tokio::test]
    async fn early_task_death_reports_background_task_died() {
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let (died_tx, died_rx) = tokio::sync::watch::channel(false);
        let early = tokio::spawn(async {}); // returns immediately
        let supervisor = tokio::spawn(dying_supervisor(stop_rx, died_tx, vec![("early", early)]));

        let result = finalize_serve_shutdown(
            Ok(()),
            stop_tx,
            supervisor,
            &died_rx,
            std::time::Duration::from_secs(2),
        )
        .await;

        match result {
            Err(ServeFailure::BackgroundTaskDied) => {}
            other => panic!("expected BackgroundTaskDied, got {other:?}"),
        }
    }

    /// S3 ordering: a serve error **and** a tripped die flag → `Serve` wins
    /// (the inlined `cmd_serve` surfaced the serve error first; keep it so).
    #[tokio::test]
    async fn serve_error_wins_over_die_flag() {
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let (died_tx, died_rx) = tokio::sync::watch::channel(false);
        let early = tokio::spawn(async {});
        let supervisor = tokio::spawn(dying_supervisor(stop_rx, died_tx, vec![("early", early)]));
        let err = std::io::Error::other("boom");

        let result = finalize_serve_shutdown(
            Err(err),
            stop_tx,
            supervisor,
            &died_rx,
            std::time::Duration::from_secs(2),
        )
        .await;

        assert!(
            matches!(result, Err(ServeFailure::Serve(_))),
            "the serve error must win over the die flag: {result:?}"
        );
    }

    /// A panicked supervisor is logged and treated as "no survivors" — the
    /// shutdown completes as a clean exit (nothing else failed).
    #[tokio::test]
    async fn panicked_supervisor_does_not_abort_shutdown() {
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        let supervisor = tokio::spawn(async { panic!("supervisor boom") });
        let (_died_tx, died_rx) = tokio::sync::watch::channel(false);

        let result = finalize_serve_shutdown(
            Ok(()),
            stop_tx,
            supervisor,
            &died_rx,
            std::time::Duration::from_secs(2),
        )
        .await;

        assert!(
            result.is_ok(),
            "a dead supervisor with no other failure is a clean exit: {result:?}"
        );
    }
}

// ── serve_policy_checks unit tests ───────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod serve_policy_checks_tests {
    use super::*;

    #[test]
    fn open_mode_loopback_ok() {
        let result = serve_policy_checks("127.0.0.1", crate::settings::ACCESS_MODE_OPEN, false, "");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn open_mode_non_loopback_refused() {
        let result = serve_policy_checks("0.0.0.0", crate::settings::ACCESS_MODE_OPEN, false, "");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("access.mode=open"));
    }

    #[test]
    fn password_mode_loopback_ok() {
        let result =
            serve_policy_checks("127.0.0.1", crate::settings::ACCESS_MODE_PASSWORD, true, "");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn password_mode_non_loopback_warns_tls() {
        let result =
            serve_policy_checks("0.0.0.0", crate::settings::ACCESS_MODE_PASSWORD, true, "")
                .unwrap();
        assert!(!result.is_empty());
        assert!(result[0].message.contains("TLS"));
    }

    #[test]
    fn zero_prefix_cidr_refused() {
        let result = serve_policy_checks(
            "127.0.0.1",
            crate::settings::ACCESS_MODE_OPEN,
            false,
            "0.0.0.0/0",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("/0 CIDR"));
    }

    #[test]
    fn over_broad_cidr_warns() {
        let result = serve_policy_checks(
            "127.0.0.1",
            crate::settings::ACCESS_MODE_OPEN,
            false,
            "10.0.0.0/8",
        )
        .unwrap();
        assert!(result.iter().any(|w| w.message.contains("over-broad")));
    }

    #[test]
    fn require_auth_for_reads_false_warns_open_reads() {
        let result =
            serve_policy_checks("0.0.0.0", crate::settings::ACCESS_MODE_PASSWORD, false, "")
                .unwrap();
        // Should have the TLS warning AND the open-reads warning.
        assert!(result
            .iter()
            .any(|w| w.message.contains("reads (GET/HEAD/OPTIONS) stay open")));
    }

    #[test]
    fn require_auth_for_reads_true_warns_gated_reads() {
        let result =
            serve_policy_checks("0.0.0.0", crate::settings::ACCESS_MODE_PASSWORD, true, "")
                .unwrap();
        assert!(result.iter().any(|w| w.message.contains("effective-true")));
    }

    #[test]
    fn localhost_is_loopback() {
        let result = serve_policy_checks("localhost", crate::settings::ACCESS_MODE_OPEN, false, "");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn warn_level_for_open_reads() {
        let result =
            serve_policy_checks("0.0.0.0", crate::settings::ACCESS_MODE_PASSWORD, false, "")
                .unwrap();
        // The "reads stay open" warning is an explicit operator opt-out
        // exposing full article content to any network peer — Warn level,
        // same as its effective-true sibling.
        let open_reads = result
            .iter()
            .find(|w| w.message.contains("reads (GET/HEAD/OPTIONS) stay open"))
            .expect("open-reads warning present");
        assert_eq!(open_reads.level, WarnLevel::Warn);
    }

    // Sec M2 (2026-09 review): open mode + proxy CIDRs must warn, because the
    // TLS warning only exists for password mode.
    #[test]
    fn open_mode_with_proxy_cidrs_warns() {
        let result = serve_policy_checks(
            "127.0.0.1",
            crate::settings::ACCESS_MODE_OPEN,
            false,
            "10.1.2.3/32",
        )
        .unwrap();
        let proxy_warn = result
            .iter()
            .find(|w| {
                w.message
                    .contains("reverse proxy fronts this loopback instance")
            })
            .expect("proxy-fronted open-mode warning present");
        assert_eq!(proxy_warn.level, WarnLevel::Warn);
    }

    #[test]
    fn open_mode_without_proxy_cidrs_stays_silent() {
        let result =
            serve_policy_checks("127.0.0.1", crate::settings::ACCESS_MODE_OPEN, false, "   ")
                .unwrap();
        assert!(
            result.is_empty(),
            "blank proxy list must not trigger the proxy warning: {result:?}"
        );
    }

    // 2026-09 review round 2: non-loopback password mode behind a proxy must
    // warn about the ?access_token= query-string leak channel.
    #[test]
    fn password_mode_non_loopback_with_proxy_warns_access_token() {
        let result = serve_policy_checks(
            "0.0.0.0",
            crate::settings::ACCESS_MODE_PASSWORD,
            true,
            "10.1.2.3/32",
        )
        .unwrap();
        let warn = result
            .iter()
            .find(|w| w.message.contains("?access_token="))
            .expect("access-token channel warning present");
        assert_eq!(warn.level, WarnLevel::Warn);
    }

    #[test]
    fn password_mode_loopback_with_proxy_stays_silent_on_access_token() {
        // Loopback: the token never crosses the network, no warning.
        let result = serve_policy_checks(
            "127.0.0.1",
            crate::settings::ACCESS_MODE_PASSWORD,
            true,
            "10.1.2.3/32",
        )
        .unwrap();
        assert!(
            !result.iter().any(|w| w.message.contains("?access_token=")),
            "loopback bind must not warn about the query-string channel: {result:?}"
        );
    }
}
