//! zimservice — CLI entry point (see `zimservice::lib` for the library docs).

use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use zimservice::config::Config;
use zimservice::db;
use zimservice::serve;
use zimservice::torrent;
use zimservice::zim::index;

use zimservice::settings::{
    KEY_ACCESS_ADMIN_PASSWORD, KEY_ACCESS_READ_ONLY_TOKEN, KEY_EMBEDDING_API_KEY,
    KEY_GENERAL_TRUSTED_PROXY_CIDRS, KEY_TORRENT_PASSWORD,
};
use zimservice::startup;
use zimservice::startup::wait_for_task_failure;

/// Interval between background re-probes of a degraded pg_trgm (WI-5).
const TRGM_REPROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
/// How long graceful shutdown waits for background tasks to drain.
const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// H2: how often the supervisor polls the background tasks' `JoinHandle`s
/// for an early death (panic or normal early return) before shutdown.
const TASK_SUPERVISOR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
#[derive(Parser)]
#[command(
    name = "zimservice",
    version,
    about = "Cross-ZIM search and serve platform"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP server
    Serve,
    /// List ZIM archives
    List {
        /// Reconcile the DB with the files on disk (persist new ZIMs, prune
        /// missing ones). Off by default — `list` is read-only.
        #[arg(long)]
        sync: bool,
    },
    /// Show server status
    Status,
    /// Index a ZIM archive
    Index {
        /// ZIM name (or --all for all)
        #[arg(short, long)]
        zim: Option<String>,
        /// Index all ZIMs
        #[arg(long)]
        all: bool,
    },
    /// Run MCP server over stdio
    Mcp,
    /// Generate embeddings for a ZIM (or all embed-enabled ZIMs without --zim)
    Embed {
        /// ZIM name
        #[arg(short, long)]
        zim: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load config
    let config = Config::load()?;

    // Initialize tracing
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    match cli.command {
        Commands::Serve => cmd_serve(config).await,
        Commands::List { sync } => cmd_list(config, sync).await,
        Commands::Status => cmd_status(config).await,
        Commands::Index { zim, all } => cmd_index(config, zim, all).await,
        Commands::Mcp => cmd_mcp(config).await,
        Commands::Embed { zim } => cmd_embed(config, zim).await,
    }
}

async fn cmd_serve(config: Config) -> anyhow::Result<()> {
    // M-1: acquire the single-instance guards **before** `build_state`.
    // `build_state` in `StartupMode::Serve` runs the startup resync, which
    // persists new ZIM rows and deletes rows for files missing on disk — a
    // DELETE/INSERT cycle on the shared database that two concurrently-
    // starting instances must not interleave. The guards need only `Config`
    // (DSN + zim_dir), so they can come first.
    let allow_multi = startup::multi_instance_allowed();
    // m-7 partial opt-out: parsed from `ZIMSERVICE_ALLOW_MULTI_DB` (exact "1")
    // through `Config` at startup, like every other process concern.
    let allow_multi_db = !allow_multi && config.allow_multi_db;
    let guard = if allow_multi {
        // Opt-out: skip both guards (the operator accepts multi-instance risk).
        tracing::warn!("ZIMSERVICE_ALLOW_MULTI_INSTANCE=1: single-instance guards disabled");
        None
    } else if allow_multi_db {
        // m-7 partial opt-out: the per-zim_dir PID lock guard stays enforced;
        // only the per-database advisory lock is best-effort.
        Some(startup::acquire_instance_guard_multi_db(&config).await?)
    } else {
        match startup::acquire_instance_guard(&config).await? {
            Some(g) => Some(g),
            None => {
                anyhow::bail!(
                    "another zimservice instance already holds the single-process advisory \
                     lock for this database. A second `serve` would corrupt the shared \
                     in-process caches (settings, rate limiter, qBittorrent client, ZIM \
                     LRU). Stop the other instance, or set ZIMSERVICE_ALLOW_MULTI_DB=1 if \
                     you deliberately run a different-database deployment on a shared \
                     zim_dir, or ZIMSERVICE_ALLOW_MULTI_INSTANCE=1 to disable all guards \
                     (not recommended)."
                );
            }
        }
    };

    let state = startup::build_state(
        &config,
        startup::StartupRequest::serve(guard.as_ref().is_some_and(|g| g.advisory_lock_held())),
    )
    .await?;

    // Security policy checks (open-mode refusal, TLS warnings, CIDR policy).
    let cidrs_raw = state
        .settings
        .get_typed::<String>(KEY_GENERAL_TRUSTED_PROXY_CIDRS)
        .unwrap_or_default();
    let mut warnings = match startup::serve_policy_checks(
        &config.host,
        &state.settings.access_mode(),
        state.settings.require_auth_for_reads(),
        &cidrs_raw,
    ) {
        Ok(w) => w,
        Err(msg) => anyhow::bail!("{msg}"),
    };
    // SEC-L5: plaintext-at-rest warning. The check needs the loaded
    // settings (which target secret, if any, is non-empty) plus the
    // Config's `security_key`, so it runs here at the call site and pushes
    // into the same warnings list as `serve_policy_checks`.
    let secret_stored = [
        KEY_TORRENT_PASSWORD,
        KEY_EMBEDDING_API_KEY,
        KEY_ACCESS_READ_ONLY_TOKEN,
    ]
    .iter()
    .any(|key| {
        state
            .settings
            .get_typed::<String>(key)
            .is_some_and(|s| !s.is_empty())
    });
    if let Some(w) =
        startup::security_key_plaintext_warning(config.security_key.is_some(), secret_stored)
    {
        warnings.push(w);
    }
    for w in &warnings {
        match w.level {
            startup::WarnLevel::Warn => tracing::warn!("{}", w.message),
            startup::WarnLevel::Info => tracing::info!("{}", w.message),
        }
    }

    // Clean up invalid concurrent indexes from prior crashed index builds.
    // The DDL itself lives in the migration layer (ARCH M1).
    db::migrate::drop_invalid_indexes(&state.db).await?;

    let app = serve::build_router(state.clone());

    // ARCH-6: bind **before** spawning any background task, so an EADDRINUSE
    // exit cannot leave a full mutation tick (requeue, cancelled deletions,
    // worst-case handle_complete) persisted to the shared DB.
    let addr = format!("{}:{}", config.host, config.port);
    tracing::info!("zimservice listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    // Watch the ZIM directory for new/changed/deleted archives (auto-resyncs the DB).
    let zims_handle = state.zims.clone();
    let watcher_handle = tokio::spawn(async move {
        zimservice::zim::discovery::watch_zim_dir(&zims_handle).await;
    });

    // Download lifecycle: queue → qBittorrent/direct → verify → install → index.
    let poller_handle = tokio::spawn(
        torrent::poller::DownloadPoller::new(
            state.db.clone(),
            state.settings.clone(),
            state.zims.clone(),
            state.torrent.clone(),
            config.torrent_url.clone(),
            config.torrent_user.clone(),
            config.torrent_pass.clone(),
        )
        .run(),
    );

    // Background auto-embed: picks up newly indexed articles and generates
    // vectors while the server is running (no-op when embedding is disabled).
    let embed_state = state.clone();
    let embed_handle = tokio::spawn(zimservice::embed::auto_embed_loop(
        std::sync::Arc::new(embed_state),
        std::time::Duration::from_secs(60),
    ));

    // Background trgm re-probe (WI-5): when pg_trgm was unavailable at startup,
    // re-probe every 60 s so the trgm arms recover when the extension is
    // installed at runtime without a restart.
    let trgm_state = state.clone();
    let trgm_probe_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(TRGM_REPROBE_INTERVAL);
        interval.tick().await; // first tick is immediate — skip it
        loop {
            interval.tick().await;
            if trgm_state.search.trgm_is_degraded() {
                let _ = trgm_state.search.ensure_trgm().await;
            }
        }
    });

    // H2: supervise the four background tasks. Previously a panic
    // (JoinError) in any of them was silently discarded and only noticed at
    // shutdown via `let _ = handle.await;` — a dead poller left HTTP serving
    // while the download→verify→index pipeline was dead, with no log and no
    // /health signal. Fail-closed: the first finished handle (panic **or**
    // normal early return) gets a `tracing::error!` from the supervisor,
    // sets `task_died`, and trips the graceful-shutdown arm below; the end
    // of this fn then bails non-zero. Dead tasks are NOT restarted.
    //
    // Ownership: `wait_for_task_failure` consumes the four handles and, on
    // clean shutdown, hands back the still-alive ones via
    // `supervisor_handle.await` so the shutdown path can still abort them.
    let (task_died, mut died_rx) = tokio::sync::watch::channel(false);
    let died_check = task_died.subscribe();
    let (sup_stop_tx, sup_stop_rx) = tokio::sync::watch::channel(false);
    let supervisor_handle = tokio::spawn({
        let died_tx = task_died;
        async move {
            let (report, survivors) = wait_for_task_failure(
                vec![
                    ("zim-watcher", watcher_handle),
                    ("download-poller", poller_handle),
                    ("auto-embed", embed_handle),
                    ("trgm-probe", trgm_probe_handle),
                ],
                TASK_SUPERVISOR_INTERVAL,
                sup_stop_rx,
            )
            .await;
            if let Some((name, result)) = report {
                match result {
                    Err(join_err) => {
                        tracing::error!("background task '{name}' died: {join_err}");
                    }
                    Ok(()) => {
                        tracing::error!(
                            "background task '{name}' returned before shutdown \
                             (it must run for the lifetime of the server)"
                        );
                    }
                }
                // Trips the graceful-shutdown arm of `axum::serve` below.
                let _ = died_tx.send(true);
            }
            survivors
        }
    });

    let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();

    // S3: `axum::serve` may return `Err` on a low-level network error (not
    // just Ctrl-C). The old `?.await` would bail before the task-abort block,
    // leaving pollers/watchers mutating the shared DB in a dying process.
    // Capture the result and always run the shutdown path.
    let serve_result = axum::serve(listener, service)
        .with_graceful_shutdown(async move {
            // H2: shut down on Ctrl-C **or** the supervisor's die flag
            // (a background task died — already logged by the supervisor).
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = died_rx.wait_for(|died: &bool| *died) => {}
            }
        })
        .await;

    // M4: the post-serve shutdown protocol (stop supervisor → abort
    // survivors → bounded drain → exit-kind classification) is a lib fn —
    // reached on **both** Ok and Err of `axum::serve` (S3), unit-tested in
    // `startup::serve_shutdown_tests` with dummy handles.
    startup::finalize_serve_shutdown(
        serve_result,
        sup_stop_tx,
        supervisor_handle,
        &died_check,
        SHUTDOWN_TIMEOUT,
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    tracing::info!("zimservice shut down cleanly");
    // `guard` drops here: advisory lock released (connection closed) +
    // `.zimservice.lock` unlinked (S1 + S2).
    drop(guard);
    Ok(())
}

async fn cmd_list(config: Config, sync: bool) -> anyhow::Result<()> {
    // H1: `--sync` mutates the shared database (the resync's DELETE/INSERT
    // cycle), so it takes the per-DB advisory lock non-blockingly and refuses
    // when a running server holds it (the server's in-memory caches would be
    // left stale with no signal). Read-only `list` stays lock-free. The guard
    // must outlive the whole command, so the underscore-prefixed binding
    // (used for its lifetime, not read) is deliberate.
    let _mutating_guard = if sync {
        startup::acquire_mutating_guard(&config).await?
    } else {
        None
    };
    // Shared bootstrap (ARCH M1): the same pool + migration + ZimManager
    // path as `startup::build_state`, so the two startup paths cannot
    // diverge. Migrations run in every mode, so `list` stays usable on a
    // fresh/unmigrated database.
    let (_pool, zims) = startup::bootstrap_pool_and_zims(&config).await?;
    // Read-only by default; `--sync` reconciles the DB with disk (guarded by
    // `_mutating_guard` above — it refuses while a server runs, and holds the
    // lock so a concurrently-starting `serve` is serialized out).
    let found = startup::populate_zims(&zims, sync).await?;

    if found.is_empty() && zims.list().is_empty() {
        println!("No ZIM files found in {}", config.zim_dir.display());
        return Ok(());
    }

    println!(
        "{:<40} {:<6} {:>10} {:>10} {:<10}",
        "NAME", "LANG", "ENTRIES", "INDEXED", "STATUS"
    );
    println!("{}", "-".repeat(80));
    for meta in zims.list() {
        println!(
            "{:<40} {:<6} {:>10} {:>10} {:<10}",
            truncate(&meta.name, 40),
            meta.language,
            meta.entry_count,
            meta.indexed_entries,
            meta.index_status,
        );
    }

    Ok(())
}

async fn cmd_status(config: Config) -> anyhow::Result<()> {
    // `status` reports the qBittorrent connection state in its output, so it
    // keeps the startup connect round-trip (`connect_torrent = true`).
    let state = startup::build_state(&config, startup::StartupRequest::status()).await?;
    let zims = state.zims.list();
    let total_articles: i64 = zims.iter().map(|z| z.indexed_entries as i64).sum();

    println!("zimservice v{}", env!("CARGO_PKG_VERSION"));
    println!("  ZIM directory:  {}", config.zim_dir.display());
    println!(
        "  Database:       {}",
        zimservice::redact_url(&config.database_url)
    );
    println!("  ZIMs:           {}", zims.len());
    println!("  Articles:       {total_articles}");
    println!(
        "  qBittorrent:    {}",
        if state.torrent.current().is_some() {
            "connected"
        } else {
            "not configured"
        }
    );
    println!(
        "  Embeddings:     {}",
        if state.settings.embedding_enabled() {
            "enabled"
        } else {
            "disabled"
        }
    );

    Ok(())
}

async fn cmd_index(config: Config, zim: Option<String>, all: bool) -> anyhow::Result<()> {
    // H1: refuse while a running server holds the per-DB advisory lock (its
    // in-memory caches would be silently stale); otherwise hold the lock for
    // the whole run so concurrent mutating runs — and a concurrently-
    // starting `serve` — are serialized (resyncs must not interleave).
    // `mutating_guard` is read once below (`is_some`) and then only lives out
    // the rest of the command — it must NOT be dropped earlier (e.g. via
    // `let _ =`), since that would release the lock mid-run.
    let mutating_guard = startup::acquire_mutating_guard(&config).await?;
    let state = startup::build_state(
        &config,
        startup::StartupRequest::mutating(mutating_guard.is_some()),
    )
    .await?;

    let target = if all { None } else { zim.as_deref() };

    index::index_zims(&state.zims, &state.db, target).await?;
    Ok(())
}

async fn cmd_mcp(config: Config) -> anyhow::Result<()> {
    // `connect_torrent = false` (ARCH minor #3): the stdio MCP session never
    // touches the qBittorrent client, so skip the startup login round-trip.
    let state = startup::build_state(&config, startup::StartupRequest::mcp()).await?;
    let state = Arc::new(state);

    // DEC-2: In password mode, require MCP_AUTH_PASSWORD env var and verify
    // against the configured admin password (fail-closed). The env var is
    // read through `Config` at startup, like every other process concern.
    let access_mode = state.settings.access_mode();
    let configured_pw = state
        .settings
        .get_typed::<String>(KEY_ACCESS_ADMIN_PASSWORD)
        .unwrap_or_default();
    let provided = config.mcp_auth_password;
    zimservice::mcp::mcp_auth_ok(&access_mode, &configured_pw, provided.as_deref()).map_err(
        |e| {
            eprintln!("MCP auth failed: {e}");
            e
        },
    )?;

    tracing::info!("MCP server running on stdio");
    zimservice::mcp::run(state).await?;
    Ok(())
}

async fn cmd_embed(config: Config, zim: Option<String>) -> anyhow::Result<()> {
    // H1: same guard as `cmd_index` — refuse while a server runs, serialize
    // concurrent mutating runs. `mutating_guard` is read once below
    // (`is_some`) and then only lives out the rest of the command — it must
    // NOT be dropped earlier (e.g. via `let _ =`), since that would release
    // the lock mid-run.
    let mutating_guard = startup::acquire_mutating_guard(&config).await?;
    let state = startup::build_state(
        &config,
        startup::StartupRequest::mutating(mutating_guard.is_some()),
    )
    .await?;

    if !state.settings.embedding_enabled() {
        anyhow::bail!("embedding is disabled — set embedding.enabled = true in settings first");
    }

    let targets: Vec<String> = match &zim {
        Some(name) => {
            // Validate the ZIM exists
            if state.zims.get(name).is_none() {
                anyhow::bail!("ZIM '{name}' not found");
            }
            vec![name.clone()]
        }
        None => state
            .zims
            .list()
            .into_iter()
            .filter(|z| z.embed_enabled)
            .map(|z| z.name)
            .collect(),
    };

    if targets.is_empty() {
        println!("No embed-enabled ZIMs found (index one first with `zimservice index`)");
        return Ok(());
    }

    for name in &targets {
        println!("Embedding '{name}' …");
        zimservice::embed::run_pipeline(
            state.db.clone(),
            state.settings.clone(),
            name,
            &state.build_probe,
            &state.index_building,
        )
        .await?;
        println!("Done: {name}");
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let cut = s.floor_char_boundary(max.saturating_sub(1));
        format!("{}…", &s[..cut])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn truncate_ascii() {
        assert_eq!(truncate("abc", 5), "abc");
        assert_eq!(truncate("abcdef", 4), "abc…");
        assert_eq!(truncate("abcdef", 1), "…");
    }

    #[test]
    fn truncate_non_ascii_no_panic() {
        // Regression: `&s[..max-1]` panicked whenever the byte offset fell
        // inside a multi-byte character.
        assert_eq!(truncate("résumé très long", 8), "résum…");
        assert_eq!(truncate("日本語テキスト", 5), "日…");
        assert_eq!(truncate("日本語", 9), "日本語");
        // byte-based budget: 3 CJK chars = 9 bytes, so max 5 still truncates
        assert_eq!(truncate("日本語", 5), "日…");
    }

    // NOTE: the old `serve_should_refuse` unit tests have been removed — the
    // refusal logic is now in `cmd_serve` + `startup::acquire_instance_guard`.
    // The behavior is exercised in `src/startup.rs`'s `tests` module:
    // `smoke_single_instance_refusal` (DB-gated: a held advisory lock makes
    // `acquire_instance_guard` return `Ok(None)`) plus the PID lock unit tests
    // (`pid_lock_live_own_pid_is_refused`, `pid_lock_stale_dead_pid_is_stolen`,
    // `pid_lock_no_lock_file_created_with_our_pid`, `pid_lock_garbage_content_is_stolen`).

    // ── H2: background-task supervisor core ───────────────────────────────

    #[tokio::test]
    async fn supervisor_reports_panicked_task() {
        // (a) a handle that panicked is reported with the right name and a
        // panicking JoinError.
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let panicker = tokio::spawn(async {
            panic!("boom");
        });
        let (report, survivors) = wait_for_task_failure(
            vec![("panicker", panicker)],
            std::time::Duration::from_millis(10),
            stop_rx,
        )
        .await;
        let Some((name, result)) = report else {
            panic!("panicked task must be reported");
        };
        assert_eq!(name, "panicker");
        let join_err = result.expect_err("a panic surfaces as a JoinError");
        assert!(join_err.is_panic());
        assert!(survivors.is_empty());
    }

    #[tokio::test]
    async fn supervisor_reports_early_returning_task() {
        // (b) a handle that returns immediately (normal early return, no
        // panic) is reported with Ok(value) and the right name.
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let fast = tokio::spawn(async {});
        let (report, survivors) = wait_for_task_failure(
            vec![("fast", fast)],
            std::time::Duration::from_millis(10),
            stop_rx,
        )
        .await;
        let Some((name, result)) = report else {
            panic!("early-returned task must be reported");
        };
        assert_eq!(name, "fast");
        assert!(result.is_ok(), "normal early return is Ok(())");
        assert!(survivors.is_empty());
    }

    #[tokio::test]
    async fn supervisor_stays_quiet_while_tasks_run() {
        // (c) a still-running handle is not reported; the stop flag bounds
        // the wait and hands the handle back untouched.
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let sleeper = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        // Release the stop flag after a few poll cycles, from another task.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let _ = stop_tx.send(true);
        });
        let (report, survivors) = wait_for_task_failure(
            vec![("sleeper", sleeper)],
            std::time::Duration::from_millis(10),
            stop_rx,
        )
        .await;
        assert!(
            report.is_none(),
            "a still-running task must not be reported"
        );
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].0, "sleeper");
        for (_, handle) in &survivors {
            handle.abort(); // test cleanup — leave nothing pending
        }
    }

    // ── T9d: CLI parse tests ───────────────────────────────────────────────

    #[test]
    fn cli_parse_list() {
        let cli = Cli::try_parse_from(["zimservice", "list"]).unwrap();
        match &cli.command {
            Commands::List { sync } => assert!(!sync, "list defaults to read-only"),
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn cli_rejects_unknown_subcommand() {
        assert!(Cli::try_parse_from(["zimservice", "frobnicate"]).is_err());
    }
}
