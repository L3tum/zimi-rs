//! Startup orchestration: state construction. The single-instance guards
//! live in the `guards` submodule (M4, 2026-10 review; originally
//! extracted from `main.rs` so the guard-acquisition order around the
//! mutating `build_state` is an isolated, testable unit — ARCH m-1 / M-1).
//! Lives in the lib (not the binary) so its guard tests run under `--lib`
//! and can use the lib's `crate::testing` DB-gating infrastructure (M3).
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

/// Single-instance guard machinery (S1 advisory lock, S2 PID lock,
/// H1 liveness monitor, m-7 multi-DB opt-out) — M4, 2026-10 review.
mod guards;
pub use guards::*;

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
///
/// `died_tx`: the process die-flag sender (the `cmd_serve` graceful-shutdown
/// channel) the LISTEN/NOTIFY listener's fail-closed supervisor signals
/// through (see `db::notify`) — `Some` only for `serve` (the long-running
/// process that must fail closed on a listener death); `None` for the
/// one-shot subcommands, which spawn no listener.
pub async fn build_state(
    config: &Config,
    req: StartupRequest,
    died_tx: Option<tokio::sync::watch::Sender<bool>>,
) -> anyhow::Result<AppState> {
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
                 WHERE {match_fragment}\n\
                 )",
                match_fragment = guards::ADVISORY_LOCK_MATCH
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
    //
    // Deliberately NOT gated on
    // `crate::process::multi_instance_allowed()` (2026 project-wide review,
    // D1): that env var describes *this* process's lock posture, not whether
    // peers may write to this database. A default-mode deployment is
    // not write-protected by its own env — an opted-out peer (a second serve,
    // or a mutating CLI run with the opt-out) can write settings/catalog rows
    // while this server runs, and this listener is that server's only
    // freshness path for such writes (this process's guards only keep
    // same-posture peers out). Gating would leave exactly the mixed-posture
    // deployment stale with no signal; the cost in a truly isolated
    // deployment is one dedicated connection plus a few idle tasks.
    //
    // The listener starts in `reconnecting` and never blocks or fails startup
    // (a lost session degrades to the next local resync / restart and reports
    // itself in `/diagnostic`). Spawned after `populate_zims` so the startup
    // resync has already converged the caches and the listener's
    // connect-resync is a no-op.
    let notify = if mode == StartupMode::Serve {
        // `cmd_serve` is the only caller that carries the die-flag sender
        // (`died_tx: Some(…)`); serve without it is a wiring bug — a
        // listener death would have no fail-closed path — so fail loudly.
        let died_tx = died_tx.ok_or_else(|| {
            anyhow::anyhow!("internal: serve startup requires the task-die flag sender")
        })?;
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
            died_tx,
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
        // `at_unix: 0` = “never probed”: the first `/diagnostic` pull finds
        // the snapshot stale and pays the exact probe itself (honest first
        // read), then publishes the result — until the auto-embed loop's
        // 60 s tick takes over publishing.
        vector_index_snapshot: Arc::new(std::sync::Mutex::new(crate::embed::VectorIndexSnapshot {
            embedded_rows: 0,
            index: crate::embed::VectorIndexState::Absent,
            at_unix: 0,
        })),
        notify,
    })
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
///
/// The message is rendered as a boxed banner (Sec M1 follow-up): this is
/// the only warning that secrets sit in plaintext at rest, so it must be
/// unmissable in the startup log, and it states the policy that
/// `SECURITY_KEY` is required for any deployment reachable beyond
/// loopback.
pub fn security_key_plaintext_warning(
    key_set: bool,
    secret_stored: bool,
) -> Option<StartupWarning> {
    if key_set || !secret_stored {
        return None;
    }
    Some(StartupWarning {
        message: "\
======================================================================\n\
SECURITY_KEY is not set — the settings table stores\n\
torrent.password / embedding.api_key / access.read_only_token\n\
in PLAINTEXT at rest. A database dump, a backup file, or\n\
any read access to the DB exposes those secrets in the clear.\n\
Set SECURITY_KEY to enable AES-256-GCM encryption at rest.\n\
It is REQUIRED for any deployment reachable beyond loopback\n\
(README 'At-rest encryption (SECURITY_KEY)').\n\
======================================================================"
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
        // The message is a multi-line boxed banner (unmissable): framed by
        // '=' borders, each content line its own line (Sec M1).
        assert!(w.message.starts_with("======"), "{}", w.message);
        assert!(w.message.ends_with("===="), "{}", w.message);
        assert!(
            w.message.contains("stores\ntorrent.password"),
            "banner lines must not collapse into one: {}",
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
    /// A critical background component failed — the task supervisor (a
    /// serve task panicked or returned early), the advisory-lock liveness
    /// monitor, or the LISTEN/NOTIFY listener supervisor (all three raise
    /// the shared die flag; see `cmd_serve`). The download→verify→index
    /// pipeline is broken, so the process must exit non-zero (H2) —
    /// through the shutdown protocol, never a bare `process::exit`.
    BackgroundTaskDied,
}

impl std::fmt::Display for ServeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // The `Display` here is the operator-facing exit message — keep
            // it in sync with the `cmd_serve` mapping (which formats it
            // verbatim into the `anyhow` error).
            Self::Serve(e) => write!(f, "server error: {e}"),
            Self::BackgroundTaskDied => f.write_str(
                "a critical background component failed (see log above); exiting non-zero",
            ),
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
                        "background task '{name}' returned before shutdown \
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
