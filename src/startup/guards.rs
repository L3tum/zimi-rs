//! Single-instance guard machinery (M4, 2026-10 review): the per-database
//! Postgres advisory lock (S1), the per-zim_dir PID lock file (S2), the
//! advisory-lock liveness monitor (H1), and the partial multi-DB opt-out
//! (m-7). Split out of `startup/mod.rs` so the composition root stays
//! focused on `build_state`; `mod.rs` re-exports this module's public items
//! so every `crate::startup::X` / `zimservice::startup::X` path used by
//! main.rs keeps compiling.
use sqlx::postgres::PgConnection;

use crate::config::Config;
use crate::db;
use crate::process;

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
/// Both `pg_locks` probes (the guard's lock check in this file and the
/// multi-instance probe in `startup/mod.rs`) share this fragment so the two
/// predicates cannot drift apart.
pub(crate) const ADVISORY_LOCK_MATCH: &str = "l.locktype = 'advisory' AND l.objsubid = 1 \
     AND l.classid = ((hashtext('zimservice:instance')::bigint >> 32) & 4294967295)::oid \
     AND l.objid = (hashtext('zimservice:instance')::bigint & 4294967295)::oid \
     AND a.pid <> pg_backend_pid()";

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
/// interval (`SELECT 1`); on a failed probe it logs and raises the process
/// die flag (the `cmd_serve` graceful-shutdown channel — fail-closed through
/// the normal shutdown protocol: non-zero exit with a clean lockfile
/// release), because a running server whose uniqueness guarantee has
/// silently evaporated must not keep serving. A graceful drop aborts the
/// monitor task first, which closes the connection (releasing the lock)
/// before any further probe — no spurious die flag on shutdown. Monitoring is
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
/// [`acquire_instance_guard`]) and refreshes its in-memory caches (settings,
/// ZIM metadata, article counts, ETags) at the startup resync and on
/// `LISTEN`/`NOTIFY` invalidation (`crate::db::notify`) — which covers
/// settings writes and catalog-membership changes, but *not* a mutating
/// CLI's index/embed status updates. A mutating CLI racing it would rewrite
/// the shared `zims` table (its resync is a DELETE/INSERT cycle) under a
/// live server and leave the server's in-memory copy of the un-notified
/// status columns stale, with no signal — the previous protection was a
/// warn-only `pg_locks` notice. Mutating subcommands now **refuse** in that
/// case: they take the same `hashtext('zimservice:instance')` lock
/// non-blockingly (see [`acquire_mutating_guard`]) and bail when another
/// session holds it.
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
    if process::multi_instance_allowed() {
        tracing::warn!(
            "ZIMSERVICE_ALLOW_MULTI_INSTANCE=1: this mutating command will run even \
             while a server holds this database's advisory lock — connected serves \
             pick up settings and catalog-membership writes over LISTEN/NOTIFY, \
             but this command's index/embed status updates are not \
             invalidation sources, so the server's in-memory copy of them stays \
             stale until its next resync or restart"
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
             running server keeps its ZIM catalog (metadata, article counts, \
             index/embed status) in memory, and this command's writes would race it \
             — several of them (index/embed status updates) are not cache-invalidation \
             sources, so the server would keep serving its stale copy. Stop the server \
             and re-run this command, use a different DATABASE_URL, or set \
             ZIMSERVICE_ALLOW_MULTI_INSTANCE=1 to override (not recommended)."
        );
    }

    Ok(Some(MutatingGuard { lock_conn: client }))
}

/// How often the advisory-lock liveness monitor probes the lock connection
/// (a `SELECT 1` on the dedicated connection). Connection death is detected
/// within at most two intervals (the pacing sleep plus the probe's own
/// deadline); 30s keeps the extra probe cost negligible while bounding the
/// window in which a silently-released advisory lock would be unnoticed.
/// This is the probe *cadence* — `detect_advisory_lock_loss` takes it as a
/// parameter (production passes this constant; tests pass a small value so
/// they can wait it out on a real clock).
const ADVISORY_LOCK_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// H1: detect that the advisory-lock connection has died mid-run, and invoke
/// `on_connection_lost` exactly once when it has.
///
/// sqlx's `PgConnection` has no separately abortable I/O driver to join
/// (tokio-postgres' spawned `Connection` task did), so liveness is probed by
/// a `SELECT 1` on the dedicated connection itself, once per `interval`
/// (production passes [`ADVISORY_LOCK_CHECK_INTERVAL`]; tests pass a small
/// value — the cadence is a parameter, not a constant a test cannot wait
/// out, per the real-clock test pattern).
///
/// **The sleep paces the loop — the timeout does not (CPU regression,
/// 2026-10).** The loop sleeps `interval`, then probes; the per-probe
/// [`tokio::time::timeout`] only bounds a *hung* socket. The original code
/// used the timeout as its only pacing — but a timeout bounds a hang, not an
/// interval: on success `SELECT 1` returns in ~1 ms and the loop re-probed
/// immediately, firing thousands of probes per second at the server and
/// holding a flat ~50 % of a core on both this process (per-round-trip
/// encode/decode) and Postgres (thousands of trivial queries) — while every
/// `pg_stat_activity` snapshot still showed the backend `idle`, each probe
/// being a sub-millisecond slice of its round trip. (The monitor's
/// connection is a dedicated non-pooled `PgConnection`, which is also why
/// the pool checkout metrics stayed quiet throughout.)
///
/// Any probe failure — Postgres restart, network drop, or a server-side
/// close — means the socket is gone and
/// Postgres has silently released the advisory lock, so the callback fires
/// (and the function returns). A probe that does not even complete within
/// one interval is the same failure class (a wedged socket cannot be proven
/// alive), so it also fails closed. The function is only ever *cancelled* on
/// a graceful drop (the caller aborts the owning task), in which case the
/// callback never runs and no connection loss has been observed.
async fn detect_advisory_lock_loss(
    mut conn: PgConnection,
    interval: std::time::Duration,
    on_connection_lost: impl FnOnce(String),
) {
    loop {
        // Pace the loop: sleep FIRST, then probe. A `timeout` around the
        // probe alone does not pace a loop whose probe succeeds in ~1 ms —
        // that was the 2026-10 CPU regression (see the module note above).
        tokio::time::sleep(interval).await;
        match tokio::time::timeout(interval, db::raw::execute(&mut conn, "SELECT 1", |q| q)).await {
            // Timeout: the probe never completed — the socket is wedged and
            // cannot be proven alive; fail closed like any probe failure.
            Err(_) => {
                let reason = format!(
                    "advisory-lock liveness probe made no progress within {interval:?} — \
                     treating the lock connection as lost"
                );
                on_connection_lost(reason);
                return;
            }
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
/// process die flag is raised (`died_tx` — the `cmd_serve` graceful-shutdown
/// channel), so the process exits non-zero through the normal shutdown
/// protocol (clean lockfile release) — a second `serve` on the same database
/// must not start against a silently-released lock. Graceful shutdown aborts
/// the task first (which closes the connection), so the monitor never fires
/// on shutdown. Returns the monitor handle so the guard can abort it on drop.
fn spawn_advisory_lock_monitor(
    conn: PgConnection,
    died_tx: tokio::sync::watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // The production callback logs and raises the die flag on a real
        // loss, so the detector only returns after the fail-closed action
        // already ran inside the callback.
        detect_advisory_lock_loss(conn, ADVISORY_LOCK_CHECK_INTERVAL, |reason| {
            tracing::error!(
                reason = %reason,
                "single-instance advisory lock lost mid-run — another `serve` on \
                 this database could now start undetected. Raising the process die \
                 flag to fail closed."
            );
            // `send` fails only if the die-flag channel was already dropped
            // (the process is tearing down); the shutdown owns the exit.
            let _ = died_tx.send(true);
        })
        .await;
    })
}

/// S2: cross-database PID lock with PID-liveness check. If the file exists but
/// the recorded PID is dead, steal it (remove + recreate). This makes a
/// graceful shutdown's residual lock file self-healing on the next start.
///
/// File format: line 1 = holder PID, line 2 = creation timestamp (unix
/// seconds — operator diagnostics: how old is this lock?). One-line files
/// from older builds still parse (no timestamp).
///
/// Sec L3 / Bugs #3 — the *live*-PID check is not liveness alone: after a
/// crash the kernel may reuse the holder's PID for an unrelated process,
/// and a bare liveness probe then refuses startup forever even though the
/// lock is stale. On Linux the holder's identity is verified via
/// /proc/<pid>/comm — a live PID whose process image is not a zimservice
/// executable is a reused PID, and the lock is stolen. Where /proc is
/// unavailable (non-Linux, or unreadable) the check is conservative: a
/// live PID still refuses, and the operator deletes the file manually as
/// before.
///
/// Returns the lock path on success (keep it for cleanup on guard drop).
fn acquire_zim_dir_pid_lock(config: &Config) -> anyhow::Result<std::path::PathBuf> {
    let lock_path = config.zim_dir.join(".zimservice.lock");
    if lock_path.exists() {
        // Read the holder PID (line 1) and, when present, the creation
        // timestamp (line 2) from the existing lock file.
        let existing = std::fs::read_to_string(&lock_path).unwrap_or_default();
        let mut lines = existing.lines();
        let existing_pid: u32 = lines
            .next()
            .and_then(|l| l.trim().parse().ok())
            .unwrap_or(0);
        let created_ts: Option<u64> = lines.next().and_then(|l| l.trim().parse().ok());
        if existing_pid != 0 && pid_is_alive(existing_pid) {
            if pid_lock_holder_is_zimservice(existing_pid) {
                // Live zimservice process holds the lock — bail.
                let age_note = match created_ts {
                    Some(ts) => format!(" (lock created at unix {ts})"),
                    None => String::new(),
                };
                anyhow::bail!(
                    "another zimservice instance (PID {existing_pid}{age_note}) is \
                     using this zim_dir ({}) — stopping. Stop the other \
                     instance, or delete {} if it is stale.",
                    config.zim_dir.display(),
                    lock_path.display()
                );
            }
            // Live PID that is not a zimservice process: the kernel reused
            // the crashed holder's PID — the lock is stale, steal it.
            tracing::info!(
                "stealing instance lock file {}: recorded PID {existing_pid} \
                 is alive but is not a zimservice process (PID reuse after \
                 the holder crashed) — treating the lock as stale",
                lock_path.display()
            );
        }
        // Stale lock (dead PID, unreadable, or a reused PID) — steal it.
        tracing::info!(
            "removing stale instance lock file {} (PID {existing_pid} not \
             alive or not a zimservice process)",
            lock_path.display()
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
    // Format: PID + creation timestamp (see the fn doc).
    let created_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let _ = writeln!(file, "{}\n{created_ts}", std::process::id());
    let _ = file.flush();
    // Drop the file handle — the caller keeps the path for cleanup on guard drop.
    drop(file);
    Ok(lock_path)
}

/// Holder-identity check for a *live* PID recorded in the instance lock
/// file (see [`acquire_zim_dir_pid_lock`]): `true` when the process owning
/// that PID looks like a zimservice instance — its `/proc/<pid>/comm` is
/// `zimservice` or a `zimservice-*` build (the latter covers the test
/// binaries, which must still refuse each other in tests). `false` on
/// Linux when /proc names a different executable: the kernel reused the
/// crashed holder's PID, so the lock is stale.
///
/// Conservative by design: on non-Linux (no /proc) or an unreadable /proc
/// entry, returns `true` — a live PID then refuses startup exactly as
/// before, and the operator deletes the stale file manually.
fn pid_lock_holder_is_zimservice(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        match std::fs::read_to_string(format!("/proc/{pid}/comm")) {
            Ok(comm) => {
                let name = comm.trim();
                name == "zimservice" || name.starts_with("zimservice-")
            }
            Err(_) => true, // unreadable → assume a live holder (refuse)
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        true
    }
}

/// Acquire the single-instance guards (advisory lock + PID lock). Returns `None`
/// if the advisory lock could not be acquired (another instance holds it),
/// or `Some(Err)` on fatal failures (PID lock already held by a live process,
/// I/O errors). `died_tx` is the process die-flag sender (the `cmd_serve`
/// graceful-shutdown channel) the lock-liveness monitor raises on a mid-run
/// lock-connection death (fail-closed; see `spawn_advisory_lock_monitor`).
pub async fn acquire_instance_guard(
    config: &Config,
    died_tx: tokio::sync::watch::Sender<bool>,
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
    let monitor_task = spawn_advisory_lock_monitor(conn, died_tx);

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
/// `died_tx` is the process die-flag sender the lock-liveness monitor raises
/// on a mid-run lock-connection death (see [`acquire_instance_guard`]).
pub async fn acquire_instance_guard_multi_db(
    config: &Config,
    died_tx: tokio::sync::watch::Sender<bool>,
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
            let monitor_task = spawn_advisory_lock_monitor(conn, died_tx);
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
        // (a) no lock file → created, recording our PID on line 1 and the
        // creation timestamp on line 2.
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_zim_dir(dir.path());
        let path = acquire_zim_dir_pid_lock(&config).unwrap();
        assert_eq!(path, lock_file(dir.path()));
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        let mut lines = content.lines();
        assert_eq!(
            lines.next().unwrap_or(""),
            std::process::id().to_string(),
            "lock file must record our PID on line 1"
        );
        assert!(
            lines.next().and_then(|ts| ts.parse::<u64>().ok()).is_some(),
            "line 2 must be a numeric creation timestamp: {content:?}"
        );
        assert_eq!(lines.next(), None, "exactly two lines: {content:?}");
    }

    #[test]
    fn pid_lock_stale_dead_pid_is_stolen() {
        // (b) existing lock file with a DEAD pid → stale lock is stolen.
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_zim_dir(dir.path());
        let path = lock_file(dir.path());
        let dead = a_dead_pid();
        std::fs::write(&path, dead.to_string()).unwrap();
        // Steal: the stale file is removed and recreated with our PID on
        // line 1. (The fixture is a legacy one-line file — the older
        // build's format must still parse and be stealable.)
        let new_path = acquire_zim_dir_pid_lock(&config).unwrap();
        assert_eq!(new_path, path);
        assert_eq!(
            std::fs::read_to_string(&new_path)
                .unwrap()
                .lines()
                .next()
                .unwrap_or(""),
            std::process::id().to_string(),
            "stolen lock file must be rewritten with our PID"
        );
    }

    /// (e) the lock file records a LIVE pid that is **not** a zimservice
    /// process → the kernel reused the crashed holder's PID: the lock is
    /// stale and must be stolen, not refused (Bugs #3 / Sec L3). Linux
    /// only — the identity check reads /proc/<pid>/comm; on other
    /// platforms a live PID is conservatively refused (see
    /// [`pid_lock_holder_is_zimservice`]).
    #[cfg(target_os = "linux")]
    #[test]
    fn pid_lock_live_reused_pid_is_stolen() {
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_zim_dir(dir.path());
        let path = lock_file(dir.path());
        // A live, non-zimservice PID: spawn `sleep` and leave it running
        // (its /proc comm is "sleep").
        let mut child = std::process::Command::new("sleep")
            .arg("300")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn `sleep`");
        let reused = child.id();
        std::fs::write(&path, format!("{reused}\n1234567890")).unwrap();
        let new_path = acquire_zim_dir_pid_lock(&config).unwrap();
        assert_eq!(new_path, path);
        assert_eq!(
            std::fs::read_to_string(&new_path)
                .unwrap()
                .lines()
                .next()
                .unwrap_or(""),
            std::process::id().to_string(),
            "a live reused PID must not block startup — the lock is stolen"
        );
        let _ = child.kill();
        let _ = child.wait();
    }

    /// (f) the identity check itself: our own `zimservice-*` test binary is
    /// recognized as a holder; a live unrelated process is not; a dead
    /// PID's missing /proc entry is conservative-true.
    #[cfg(target_os = "linux")]
    #[test]
    fn pid_lock_holder_identity_matrix() {
        assert!(
            pid_lock_holder_is_zimservice(std::process::id()),
            "our zimservice-* test binary must be recognized as a holder"
        );
        let mut child = std::process::Command::new("sleep")
            .arg("300")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn `sleep`");
        assert!(
            !pid_lock_holder_is_zimservice(child.id()),
            "a live non-zimservice process must not pass as the holder"
        );
        let _ = child.kill();
        let _ = child.wait();
        assert!(
            pid_lock_holder_is_zimservice(a_dead_pid()),
            "an unreadable /proc entry (dead PID) must be conservative-true"
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
            std::fs::read_to_string(&new_path)
                .unwrap()
                .lines()
                .next()
                .unwrap_or(""),
            std::process::id().to_string(),
            "garbage lock file must be stolen and rewritten with our PID"
        );
    }

    // ── H1: advisory-lock liveness-monitor detection ─────────────────────
    //
    // The sqlx detector probes a *real* dedicated connection (`SELECT 1`
    // once per interval — the cadence is a parameter), so all three tests
    // are DB-gated like the smoke tests below: they skip cleanly when no DB
    // is reachable unless `ZIMSERVICE_REQUIRE_DB` is set.
    //
    /// Test cadence for the detector: small enough that a probe (and a
    /// probe failure) lands well inside the tests' timeouts, on a real
    /// clock. (50 ms × the windows below; production uses the 30 s
    /// [`ADVISORY_LOCK_CHECK_INTERVAL`].)
    const TEST_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

    #[tokio::test]
    async fn liveness_monitor_fires_on_closed_connection() {
        use std::sync::atomic::{AtomicBool, Ordering};
        // A dedicated connection whose backend is terminated up front — the
        // "socket went away" case the monitor must catch. With the test
        // cadence the first probe lands ~50 ms in and fails at once (the
        // backend is already gone).
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
        let detector = detect_advisory_lock_loss(conn, TEST_PROBE_INTERVAL, move |_| {
            fired_cb.store(true, Ordering::SeqCst)
        });
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
        // (a handful of successful probes at the test cadence) must NOT
        // report a loss and must NOT fire the callback (no false positive on
        // a healthy connection). We abandon the (still-pending) detector —
        // its probe loop simply never completes in this window — which also
        // drops the connection.
        // DB gate (src/testing.rs): counted skip, strict-mode hard-fail.
        let Some(conn) =
            crate::testing::test_conn("liveness_monitor_stays_quiet_while_alive").await
        else {
            return;
        };
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        let fired = Arc::new(AtomicBool::new(false));
        let fired_cb = fired.clone();
        let detector = detect_advisory_lock_loss(conn, TEST_PROBE_INTERVAL, move |_| {
            fired_cb.store(true, Ordering::SeqCst)
        });
        let _ = tokio::time::timeout(std::time::Duration::from_millis(120), detector).await;
        assert!(
            !fired.load(Ordering::SeqCst),
            "a healthy connection must not trip the liveness monitor"
        );
    }

    /// CPU regression (2026-10): the probe loop must pace its `SELECT 1`s at
    /// one per `interval` — not re-probe on every successful probe. The
    /// original loop's only pacing was `timeout(interval, probe)`; a timeout
    /// bounds a hang, not an interval, so a healthy ~1 ms probe re-fired
    /// immediately — thousands of probes per second, a flat ~50 % core on
    /// both this process and the Postgres server (the exact production
    /// symptom: 50 % CPU on service and Postgres while `pg_stat_activity`
    /// showed every backend `idle` — each trivial query occupied its
    /// backend for a sub-millisecond slice of a multi-ms round-trip, so
    /// the server CPU came from query *count*, never from long active
    /// states).
    ///
    /// Measured by the **delta** of the monitor connection's database
    /// `tup_returned` over a 2 s window (40 probe slots at the 50 ms test
    /// cadence) — each `SELECT 1` returns exactly one row. Two defenses
    /// make the delta trustworthy on a *churning* shared server:
    ///
    /// 1. **Private temp database** (pid + nanos in the name): nothing
    ///    else can know its name, so only the monitor's probes can write
    ///    to its counter.
    /// 2. **Pre-check**: database OIDs are recycled, and Postgres
    ///    attributes per-database stats by OID. A backend that is still
    ///    finishing teardown after its own database was dropped briefly
    ///    reports into whatever database now holds its OID — the temp DB
    ///    this test creates can inherit exactly that (this is what failed
    ///    the first CI run with a 1517-row "probe count"). Such a backend
    ///    is visible in `pg_stat_activity` under the temp DB's name, so
    ///    the test requires *exactly one* backend on the temp DB (the
    ///    monitor) before arming the window, recreating the temp DB once
    ///    if a straggler is seen and skipping loudly if one persists.
    /// 3. **Settle gate + control probe**: on a churning shared server a
    ///    fresh database's counter is not a trustworthy instrument at all:
    ///    it can absorb one or more one-off phantom stat batches (a
    ///    per-backend stats entry left behind by an abnormally terminated
    ///    predecessor is delivered into the new row on the stats worker's
    ///    next cycle — observed: 0 → ~800 → ~1700 within seconds, then
    ///    flat), and on the shared dev server it can *lose* live query
    ///    increments entirely (40 `SELECT 1`s from a clean psql session
    ///    never appearing in the counter, while a healthy database's row
    ///    ticks normally). The test therefore (a) takes the baseline only
    ///    once two counter readings 1.5 s apart agree, then (b) runs a
    ///    **control**: five `SELECT 1`s from the monitor connection, and
    ///    requires the counter to rise by exactly ~5 (a [5, 8] band) over
    ///    the next aggregation cycles — a phantom batch (hundreds of rows)
    ///    or a row that swallows live increments misses the band. Only a temp DB whose counter both settles
    ///    and tracks the control is used for the measurement; otherwise
    ///    the temp DB is recreated once and the test skips loudly if the
    ///    second attempt is equally unreliable. (CI's fresh postgres has
    ///    healthy per-database stats, so the measurement runs there —
    ///    the gate this test protects.)
    ///
    /// The monitor's connection is dropped at the window's end (its final
    /// stats flush then lands), and the counter is polled for up to 6 s to
    /// absorb the stats-aggregation lag. A paced loop yields ~40 probes;
    /// an unpaced one yields hundreds (1/RTT on a LAN) to thousands
    /// (loopback), so `10..=150` separates them with wide margins on both
    /// while also failing a dead loop (delta ≈ 0).
    ///
    /// Needs the server user to be able to `CREATE DATABASE` (true for the
    /// compose/CI/dev `zimservice` superuser); otherwise it skips.
    #[tokio::test]
    async fn liveness_monitor_probes_are_paced_by_interval() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into()
        });
        // DB gate (src/testing.rs): counted skip, strict-mode hard-fail.
        let Some(mut admin) =
            crate::testing::test_conn("liveness_monitor_probes_are_paced_by_interval").await
        else {
            return;
        };
        let _db_gate = crate::testing::DbExclusiveGuard::acquire();
        // Private temp database (see doc comment): its name is unknown to
        // anything but this test, so only the monitor's probes can write
        // to its `pg_stat_database` row.
        let pid = std::process::id();
        let base = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        // Measure once; if a straggler backend from a dropped database is
        // reporting into the temp DB's (recycled) OID, retry with a fresh
        // temp DB and skip loudly if the counter stays noisy.
        'outer: for attempt in 0..2u64 {
            let nanos = base + u128::from(attempt * 1_000_000);
            let db_name = format!("zimservice_guards_paced_{pid}_{nanos}");
            // Abandon this attempt: kill whatever is attached to the temp
            // DB (the monitor's connection included), drop the DB, and
            // either retry with a fresh name/OID or skip loudly.
            macro_rules! abandon_attempt {
                ($msg:expr) => {{
                    let _ = crate::db::raw::execute(
                        &mut admin,
                        &format!(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                              WHERE datname = '{db_name}'"
                        ),
                        |q| q,
                    )
                    .await;
                    let _ = crate::db::raw::execute(
                        &mut admin,
                        &format!("DROP DATABASE IF EXISTS \"{db_name}\""),
                        |q| q,
                    )
                    .await;
                    if attempt == 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue 'outer;
                    }
                    eprintln!("skipping: {}", $msg);
                    return;
                }};
            }
            if crate::db::raw::execute(&mut admin, &format!("CREATE DATABASE \"{db_name}\""), |q| q)
                .await
                .is_err()
            {
                // The compose/CI/dev superuser always can; a non-superuser
                // DATABASE_URL has nothing for this test to do.
                eprintln!("skipping: cannot CREATE DATABASE (user lacks CREATEDB?)");
                return;
            }
            let mut temp_url = url::Url::parse(&url).expect("valid test URL");
            temp_url.set_path(&format!("/{db_name}"));
            let mut mon_conn = match crate::db::pool::connect_dedicated(temp_url.as_str()).await {
                Ok(c) => c,
                Err(e) => {
                    let _ = crate::db::raw::execute(
                        &mut admin,
                        &format!("DROP DATABASE IF EXISTS \"{db_name}\""),
                        |q| q,
                    )
                    .await;
                    eprintln!("skipping: temp DB unreachable: {e}");
                    return;
                }
            };
            // Pre-check (doc comment): the temp DB's OID may have just been
            // recycled from a dropped database, and a backend still
            // finishing teardown of that database reports into the row by
            // OID. Any such straggler is visible in `pg_stat_activity`
            // under the temp DB's name — require exactly one backend (the
            // monitor) before arming the window.
            let mut clean = false;
            for _ in 0..50 {
                let n: i64 = crate::db::raw::fetch_scalar_optional(
                    &mut admin,
                    &format!("SELECT count(*) FROM pg_stat_activity WHERE datname = '{db_name}'"),
                    |q| q,
                )
                .await
                .expect("backend count")
                .expect("count row");
                if n == 1 {
                    clean = true;
                    break;
                }
                if n > 1 {
                    break; // straggler(s) on the recycled OID
                }
                // n == 0: the monitor's backend is not visible yet.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            if !clean {
                abandon_attempt!(
                    "a straggler backend kept reporting into the \
                                 temp DB's (recycled) OID"
                );
            }
            // Baseline after the counter settles (doc comment, defense
            // 3): two readings 1.5 s apart must agree before the window
            // arms, so one-off phantom batches are already in the
            // baseline and cancel in the delta.
            let mut baseline = None;
            let mut prev = None;
            for _ in 0..8 {
                let c: i64 = crate::db::raw::fetch_scalar_optional(
                    &mut admin,
                    &format!(
                        "SELECT tup_returned FROM pg_stat_database WHERE datname = '{db_name}'"
                    ),
                    |q| q,
                )
                .await
                .expect("counter read")
                .expect("temp DB stats row");
                if Some(c) == prev {
                    baseline = Some(c);
                    break;
                }
                prev = Some(c);
                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            }
            let Some(baseline) = baseline else {
                abandon_attempt!(
                    "the temp DB's counter never settled (phantom \
                                 stats delivery)"
                );
            };
            // Control probe (doc comment, defense 3): the settled counter
            // must also track live queries. Five known `SELECT 1`s from
            // the monitor connection have to show up in the counter
            // within a few aggregation cycles. The rise must land in the
            // [5, 8] band: a healthy server lands exactly +5, while a
            // one-off phantom batch (hundreds of rows) or a row that
            // swallows live increments (observed on the shared dev
            // server) misses the band and is abandoned.
            let control_base = baseline;
            for _ in 0..5 {
                crate::db::raw::execute(&mut mon_conn, "SELECT 1", |q| q)
                    .await
                    .expect("control probe SELECT 1");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let mut control_ok = false;
            let mut max_seen = control_base;
            let control_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
            while tokio::time::Instant::now() < control_deadline {
                let c: i64 = crate::db::raw::fetch_scalar_optional(
                    &mut admin,
                    &format!(
                        "SELECT tup_returned FROM pg_stat_database WHERE datname = '{db_name}'"
                    ),
                    |q| q,
                )
                .await
                .expect("counter read")
                .expect("temp DB stats row");
                if c > max_seen {
                    max_seen = c;
                }
                let delta = max_seen - control_base;
                if (5..=8).contains(&delta) {
                    control_ok = true;
                    break;
                }
                if delta > 8 {
                    break; // phantom batch — not the control's five rows
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            if !control_ok {
                abandon_attempt!(
                    "the temp DB's counter does not track live \
                                 queries (per-database stats unreliable here)"
                );
            }
            // Re-settle: the counter has absorbed the control rows (and
            // may still be batching them); wait for two agreeing readings
            // so the window's baseline is stable and includes them.
            let mut rebase = None;
            let mut prev = None;
            for _ in 0..8 {
                let c: i64 = crate::db::raw::fetch_scalar_optional(
                    &mut admin,
                    &format!(
                        "SELECT tup_returned FROM pg_stat_database WHERE datname = '{db_name}'"
                    ),
                    |q| q,
                )
                .await
                .expect("counter read")
                .expect("temp DB stats row");
                if Some(c) == prev {
                    rebase = Some(c);
                    break;
                }
                prev = Some(c);
                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            }
            let Some(baseline) = rebase else {
                abandon_attempt!(
                    "the temp DB's counter never settled after the \
                                 control probe"
                );
            };
            let fired = Arc::new(AtomicBool::new(false));
            let fired_cb = fired.clone();
            // Observation window: 40 probe slots at the 50 ms test cadence.
            let window = TEST_PROBE_INTERVAL * 40;
            let detector = detect_advisory_lock_loss(mon_conn, TEST_PROBE_INTERVAL, move |_| {
                fired_cb.store(true, Ordering::SeqCst)
            });
            // Run the detector for the window, then drop it: cancellation
            // closes the connection (its final stats flush then lands)
            // without firing the callback (a healthy connection never
            // reports a loss).
            let _ = tokio::time::timeout(window, detector).await;
            // Absorb the stats-aggregation lag: poll the counter for up to
            // 6 s and keep the highest reading (the counter is monotonic).
            let mut final_count = baseline;
            let poll_end = tokio::time::Instant::now() + std::time::Duration::from_secs(6);
            loop {
                if let Ok(Some(v)) = crate::db::raw::fetch_scalar_optional(
                    &mut admin,
                    &format!(
                        "SELECT tup_returned FROM pg_stat_database WHERE datname = '{db_name}'"
                    ),
                    |q| q,
                )
                .await
                {
                    final_count = final_count.max(v);
                }
                if tokio::time::Instant::now() >= poll_end {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            let probes = final_count - baseline;
            let in_range = (10i64..=150).contains(&probes);
            assert!(
                !fired.load(Ordering::SeqCst),
                "a healthy connection must not trip the liveness monitor"
            );
            assert!(
                in_range,
                "probes must be paced at one per interval: the temp DB's tup_returned \
                 rose by {probes} over a {} ms window at {} ms intervals; a paced loop \
                 yields ~40, an unpaced 1/RTT loop hundreds to thousands (the 2026-10 \
                 CPU regression), and ~0 means the loop never probed at all",
                window.as_millis(),
                TEST_PROBE_INTERVAL.as_millis()
            );
            // Best-effort cleanup: the monitor connection is already
            // dropped; drop the temp DB, retrying briefly in case its
            // backend has not finished closing. A leftover temp DB is
            // harmless — the name embeds pid + nanos.
            for _ in 0..10 {
                if crate::db::raw::execute(
                    &mut admin,
                    &format!("DROP DATABASE IF EXISTS \"{db_name}\""),
                    |q| q,
                )
                .await
                .is_ok()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            return; // measurement completed
        }
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
        // Test-side die flag: the refusal path never spawns the monitor.
        let (died_tx, _died_rx) = tokio::sync::watch::channel(false);
        let result = acquire_instance_guard(&config, died_tx)
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
        // Test-side die flag: the refusal path never spawns the monitor.
        let (died_tx, _died_rx) = tokio::sync::watch::channel(false);
        let result = acquire_instance_guard(&config, died_tx)
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
