//! Test-only support. Linked into every test binary; the in-process
//! semaphore is a per-process `static`, and the cross-process lockfile
//! serializes DB tests *across* binaries too.
//!
//! Scope: the semaphore serializes DB tests within a binary; the lockfile
//! (TESTS M1) extends that across the parallel lib + integration processes a
//! bare `cargo test` spawns. `cargo test` runs the lib and integration
//! binaries as parallel *processes*, so without the lockfile two processes
//! could interleave their migrations + fixture writes against the same dev
//! DB (e.g. two binaries seeding their fixture ZIMs at once). The lockfile is std-only (no `flock` crate) and
//! portable: it claims a file via atomic `create_new` (`O_CREAT|O_EXCL` on
//! Unix, `CREATE_NEW` on Windows) and, on conflict, waits or steals it only
//! if it is stale (mtime older than the timeout), which self-heals a lock
//! left behind by a killed process. The `0o600` mode bits are applied on
//! Unix only (Windows has no equivalent on this API).
//!
//! A live holder keeps the staleness check honest: while the guard is held,
//! a background heartbeat refreshes the lockfile's mtime every
//! `HEARTBEAT_INTERVAL`. This is required, not optional — the steal
//! threshold *doubles* as the waiter deadline, so without refreshing, a test
//! that holds the slot for longer than the timeout (e.g. a slow migration)
//! would look stale and have its slot stolen mid-run. With the heartbeat, an
//! mtime older than the timeout genuinely means a dead holder.
//!
//! `DbExclusiveGuard::acquire()` **blocks** (rather than panicking) when
//! another DB test — in this binary *or* another process — still holds the
//! slot, so the suite is safe to run with the default parallel
//! `--test-threads`.
//!
//! Same-pid rule: the heartbeat makes the lockfile always fresh while a live
//! holder holds it, so the staleness-based steal can no longer fire against
//! a live holder. That is fine for a foreign *process* (deadline + panic
//! after the timeout is the intended loud failure), but within one binary a
//! holder in this very process (a parallel DB test in this binary holding
//! the in-process slot) must not be treated as a foreign process: if the
//! lockfile's recorded PID equals this process's PID, waiters keep polling
//! without a deadline — the in-process flag is released by the holder's
//! `Drop`, so this cannot deadlock anything that previously resolved. The guard deliberately does not hold a `MutexGuard`
//! across `.await` (which would trip `clippy::await_holding_lock`): it only
//! briefly holds the mutex to flip a process-level "held" flag. The
//! cross-process `File` is an open descriptor, not a lock, so it is also safe
//! to hold across `.await`.
// LINT-3 (2026-09 sweep): test-support scaffolding — lazy dead-pool connect and fallback runtime build are intentional infallible panics; keeps the 19 expects unnoisy.
#![allow(clippy::expect_used)]
use std::sync::{Condvar, Mutex};

/// Test seam for the MCP tool pipeline: delegates to the crate-internal
/// `mcp::call_tool` so integration tests can drive the exact production
/// tool dispatch without it being part of the `mcp` module's public API.
pub async fn mcp_call_tool(
    state: &crate::AppState,
    name: &str,
    args: &serde_json::Value,
) -> Result<serde_json::Value, (i32, String)> {
    crate::mcp::call_tool(state, name, args).await
}

/// Test seam: [`crate::db::migrate::drop_invalid_indexes`] with an
/// injectable catalog probe. The integration suite (tests/integration/
/// invalid_index.rs) passes a deterministically failing probe to pin the
/// probe-failure degradation (warn + skip, `Ok(())`) without mutating
/// server-wide catalog grants.
#[doc(hidden)]
pub async fn drop_invalid_indexes_with_probe(
    pool: &crate::db::Pool,
    probe_sql: &str,
) -> crate::error::Result<()> {
    crate::db::migrate::drop_invalid_indexes_with_probe(pool, probe_sql).await
}

/// A lazy pool pointing at an unreachable URL — used by tests that need a
/// [`crate::db::Pool`](sqlx) value but never touch the database (or that
/// expect the write to fail at the pool). `connect_lazy` defers the first
/// (and here never-succeeding) connection, so no socket is opened at
/// construction time. The short acquire timeout makes any accidental pool
/// use fail fast (`sqlx::Error::PoolTimedOut`) instead of blocking on
/// sqlx's 30 s default. Safe to call from both sync `#[test]` bodies and
/// `#[tokio::test]`s (see the runtime fallback inside).
pub fn dead_pool() -> crate::db::Pool {
    let build = || {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_millis(250))
            .connect_lazy("postgres://u:p@127.0.0.1:1/nodb")
            .expect("dead_pool: lazy connect must not fail")
    };
    // sqlx 0.8 spawns background maintenance tasks at pool-creation time and
    // therefore requires a *current* Tokio runtime even for `connect_lazy`
    // (which opens no socket). Sync `#[test]` bodies have no runtime, so in
    // that case build the pool on a process-lifetime current-thread runtime
    // (held in a `OnceLock` so its spawned tasks stay valid for as long as
    // any pool outlives it).
    match tokio::runtime::Handle::try_current() {
        Ok(_) => build(),
        Err(_) => {
            // NOTE: must be an async block — `ready(build())` would run
            // `build()` eagerly, outside the runtime context.
            static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
            RT.get_or_init(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("dead_pool: fallback runtime must build")
            })
            .block_on(async { build() })
        }
    }
}

/// WI-10: count of DB-gated test skips in the lib test suite.
/// Read by the `#[dtor::dtor]` exit summary at process end.
#[cfg(test)]
pub static LIB_SKIPPED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The DB-gate skip/panic decision, shared by [`test_pool`], [`test_conn`],
/// and any DB-gated test with custom pool options (e.g. `embed::auto_loop`):
/// `ZIMSERVICE_REQUIRE_DB` set → a hard failure; unset → counted in
/// [`LIB_SKIPPED`] (the `#[dtor]` exit summary reads it) + a skip notice.
/// Every DB-connection skip in the lib suite goes through here so the exit
/// summary never under-reports on a DB-less machine (env-condition skips,
/// e.g. the IVFFlat-threshold dev-DB guard and multi-instance opt-outs, are
/// counted separately). Generic in the return type so each caller's `Option<T>` arm
/// can delegate to it directly.
#[cfg(test)]
pub fn gate_skip<T>(test: &str, url: &str, why: &str) -> Option<T> {
    if std::env::var("ZIMSERVICE_REQUIRE_DB").is_ok() {
        panic!("{test}: ZIMSERVICE_REQUIRE_DB is set but cannot reach {url}: {why}");
    }
    LIB_SKIPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    eprintln!("skipping {test}: cannot reach {url} ({why})");
    None
}

/// DB-gated helper (mirrors `tests/integration.rs::pool_or_skip`): build a
/// live pool from `DATABASE_URL` (default: the compose URL), or `None` when
/// unreachable — the [`gate_skip`] decision: a counted skip, or a hard
/// failure under `ZIMSERVICE_REQUIRE_DB` — so `cargo test --lib` stays green
/// on a DB-less machine. Migrations are applied by the caller. Lives here
/// (rather than in `torrent::poller`) so DB-gated tests outside the torrent
/// layer (e.g. `db::downloads`) can share it without depending on it (A-1).
#[cfg(test)]
pub async fn test_pool() -> Option<(crate::db::Pool, DbExclusiveGuard)> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into());
    let config = crate::config::Config {
        database_url: url.clone(),
        db_pool_size: 4,
        ..Default::default()
    };
    match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        crate::db::pool::create_pool(&config),
    )
    .await
    {
        Ok(Ok(pool)) => Some((pool, DbExclusiveGuard::acquire())),
        Ok(Err(e)) => gate_skip("test_pool", &url, &e.to_string()),
        Err(_) => gate_skip("test_pool", &url, "timed out connecting"),
    }
}

/// DB-gate for tests that need a **dedicated** connection (not a pool) —
/// the `test_pool` counterpart for the advisory-lock smoke tests, which hold
/// a single session-bound connection. Same gating: `DATABASE_URL` (or the
/// compose default) + 3 s connect timeout + [`gate_skip`] on failure, so a
/// skip is counted in [`LIB_SKIPPED`] and is a hard failure under
/// `ZIMSERVICE_REQUIRE_DB`. `test` is the caller's name (skip notice +
/// strict-mode panic). The caller still serializes with [`DbExclusiveGuard`]
/// for the test body (the connection itself is only used while the guard is
/// held). Callers that need pool options `test_pool`/`create_pool` don't
/// provide (e.g. the auto_loop tests' 24 h acquire timeout under a paused
/// clock) keep their own connect but must route their skip through
/// [`gate_skip`].
#[cfg(test)]
pub async fn test_conn(test: &str) -> Option<sqlx::postgres::PgConnection> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zimservice:zimservice@127.0.0.1:5432/zimservice".into());
    match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        crate::db::pool::connect_dedicated(&url),
    )
    .await
    {
        Ok(Ok(conn)) => Some(conn),
        Ok(Err(e)) => gate_skip(test, &url, &e.to_string()),
        Err(_) => gate_skip(test, &url, "timed out connecting"),
    }
}

/// Core test-state factory: the single place an `AppState` is built for
/// tests. `pool`/`settings` are the live test dependencies; everything else
/// uses production defaults. A new `AppState` field is added here once.
pub fn state_from_parts(
    pool: crate::db::Pool,
    settings: crate::settings::SettingsCache,
) -> crate::AppState {
    let zims =
        crate::zim::ZimManager::new(std::path::PathBuf::from("/nonexistent-zims"), pool.clone());
    let search = crate::search::SearchEngine::new(
        pool.clone(),
        settings.clone(),
        crate::health::DegradationTracker::default(),
    );
    crate::AppState {
        db: pool,
        settings,
        zims,
        search,
        torrent: crate::torrent::QbitClientCache::new(),
        rate_limiter: std::sync::Arc::new(crate::access::ratelimit::RateLimiterHandle::new()),
        probes: crate::HealthProbes::default(),
        auth_lockout: std::sync::Arc::new(Default::default()),
        degradation: crate::health::DegradationTracker::default(),
        build_probe: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        index_building: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }
}

/// [`test_state`] with a custom seed settings map (e.g. default settings plus
/// a few test-specific overrides) instead of the plain defaults.
pub fn test_state_with_settings(
    values: std::collections::HashMap<String, serde_json::Value>,
) -> crate::AppState {
    let pool = dead_pool();
    let settings = crate::settings::SettingsCache::new_with_map(
        pool.clone(),
        values,
        std::collections::HashMap::new(),
    );
    state_from_parts(pool, settings)
}

/// A fully in-memory [`crate::AppState`] for unit tests that need a complete
/// state but never touch the database: the pool is a [`dead_pool`] (unreachable),
/// settings are [`crate::settings::default_settings`], the ZIM dir does not
/// exist, and the qBittorrent / rate-limit / probe / lockout / degradation
/// fields all use their defaults (the state is built by
/// [`state_from_parts`]).
pub fn test_state() -> crate::AppState {
    test_state_with_settings(crate::settings::default_settings())
}

static DB_LOCK: Mutex<bool> = Mutex::new(false);
static DB_CV: Condvar = Condvar::new();

/// How long a cross-process lock is considered live before it may be stolen
/// (a killed process leaves its lockfile behind; after this it is stale).
/// This also doubles as the *waiter* deadline, so mtime-only staleness is
/// sound only because a live holder keeps refreshing — see
/// [`HEARTBEAT_INTERVAL`].
const CROSS_PROCESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// Poll interval while waiting for another process to release the lock.
const CROSS_PROCESS_POLL: std::time::Duration = std::time::Duration::from_millis(50);
/// How often a live holder refreshes the lockfile's mtime. Must stay well
/// below [`CROSS_PROCESS_TIMEOUT`]: a slot held longer than the timeout must
/// still read as live, or waiters would steal it mid-test.
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
/// How long the heartbeat loop waits on its stop condvar between checks, so
/// dropping the guard never blocks a full [`HEARTBEAT_INTERVAL`].
const HEARTBEAT_STOP_POLL: std::time::Duration = std::time::Duration::from_millis(250);

/// 32-bit FNV-1a over `bytes` — a tiny, dependency-free fingerprint used to
/// scope the cross-process lockfile per `DATABASE_URL` so two *different*
/// databases don't contend (the common case is one dev DB).
fn fnv1a(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in bytes {
        h ^= u32::from(*b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Path to the cross-process DB-test lockfile, scoped per `DATABASE_URL` so
/// runs against different databases don't contend.
fn cross_process_lock_path() -> std::path::PathBuf {
    let url = std::env::var("DATABASE_URL").unwrap_or_default();
    std::env::temp_dir().join(format!(
        "zimservice-test-db-{:08x}.lock",
        fnv1a(url.as_bytes())
    ))
}

/// Claim the cross-process DB-test lockfile, waiting for a live holder to
/// release it and stealing a stale one (mtime older than
/// [`CROSS_PROCESS_TIMEOUT`]). Returns the open `File`, which must be kept
/// alive (and removed) for the slot's lifetime. Std-only and portable: the
/// atomic `create_new` claim works on all platforms (the `0o600` mode bits
/// are applied on Unix only, where they keep the lockfile owner-private).
///
/// `timeout` is the *waiter* deadline: after `timeout` without acquiring,
/// a `TimedOut` error is returned — except when the holder is **this
/// process** (the lockfile contents record the holder PID at claim). A
/// same-pid holder can only be a same-binary DB test holding the in-process
/// slot: the heartbeat keeps its lockfile fresh, so the steal below can
/// never fire against it, and treating it as a foreign process would
/// deadline-out (`DbExclusiveGuard::acquire` panics) exactly the case this
/// module promises to block on. A same-pid holder is therefore waited out
/// indefinitely (poll interval unchanged); its in-process flag is released
/// by `Drop`, so this cannot deadlock a case that previously resolved. A
/// different pid — or an unparseable/empty lockfile (treated as different
/// pid) — keeps the deadline + steal behavior.
fn acquire_cross_process(timeout: std::time::Duration) -> std::io::Result<std::fs::File> {
    use std::io::Write;
    let path = cross_process_lock_path();
    let deadline = std::time::Instant::now() + timeout;
    loop {
        // Builder methods take `&mut self`, so configure the options before
        // moving on; the `0o600` mode bits are Unix-only.
        let mut opts = std::fs::OpenOptions::new();
        opts.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        match opts.open(&path) {
            Ok(mut f) => {
                // Record our PID in the file (debug aid; staleness is by mtime).
                let _ = f
                    .write_all(format!("{}\n", std::process::id()).as_bytes())
                    .and_then(|_| f.flush());
                return Ok(f);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Someone else holds it. If it is stale (holder killed), steal.
                if let Ok(modified) = std::fs::metadata(&path).and_then(|m| m.modified()) {
                    if modified
                        .elapsed()
                        .ok()
                        .is_some_and(|age| age > CROSS_PROCESS_TIMEOUT)
                    {
                        let _ = std::fs::remove_file(&path);
                        continue; // retry the claim
                    }
                }
                // Same-pid rule: if the holder is this process, it is a
                // same-binary DB test holding the in-process slot (the
                // heartbeat keeps its lockfile fresh, so the steal above
                // cannot fire against it). Wait it out with no deadline
                // instead of timing out; the in-process flag is released
                // by the holder's Drop. A different pid — or an
                // unparseable/empty file — keeps the deadline below.
                let holder_pid = std::fs::read_to_string(&path)
                    .ok()
                    .and_then(|s| s.trim().parse::<u32>().ok());
                let same_process_holder = holder_pid == Some(std::process::id());
                if !same_process_holder && std::time::Instant::now() >= deadline {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "timed out waiting for the cross-process DB-test lock at {} \n (another `cargo test` process holds it). Run the binaries separately (e.g. `make test-integration`) or stop the other run.",
                            path.display()
                        ),
                    ));
                }
                std::thread::sleep(CROSS_PROCESS_POLL);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Refresh `path`'s mtime so a long-held lockfile never reads as stale to a
/// waiter's steal check. Std-only and portable (`set_modified` works on
/// Unix and Windows). Failures are swallowed: a transient FS error must not
/// kill liveness — the next tick retries.
fn touch_mtime(path: &std::path::Path) {
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|f| f.set_modified(std::time::SystemTime::now()));
}

/// Spawn the heartbeat that keeps a held lockfile looking live: refresh
/// `path`'s mtime every `interval` while `stop`'s flag is false. The loop
/// waits on the condvar with a short timeout (not a full `interval`) so
/// stop→join at guard drop is prompt even for long intervals.
fn spawn_heartbeat(
    path: std::path::PathBuf,
    interval: std::time::Duration,
    stop: std::sync::Arc<(std::sync::Mutex<bool>, Condvar)>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let (flag, cv) = &*stop;
        let mut due = std::time::Instant::now() + interval;
        loop {
            let stopped = flag.lock().unwrap_or_else(|p| p.into_inner());
            if *stopped {
                return;
            }
            // Wake on the stop signal or the next touch deadline, whichever
            // comes first — but no later than `HEARTBEAT_STOP_POLL`.
            let now = std::time::Instant::now();
            let wait = if now >= due {
                HEARTBEAT_STOP_POLL
            } else {
                (due - now).min(HEARTBEAT_STOP_POLL)
            };
            let (stopped, _timeout) = cv
                .wait_timeout(stopped, wait)
                .unwrap_or_else(|p| p.into_inner());
            if *stopped {
                return;
            }
            if std::time::Instant::now() >= due {
                drop(stopped);
                touch_mtime(&path);
                due = std::time::Instant::now() + interval;
            }
        }
    })
}

/// Unix identity (dev, ino) of the inode behind an open `File` — lets
/// [`CrossProcessLock`] verify a lockfile is still the one it claimed
/// before removing it (a same-process sibling can have removed and
/// re-claimed the path in the meantime).
#[cfg(unix)]
fn file_identity(f: &std::fs::File) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    f.metadata().ok().map(|m| (m.dev(), m.ino()))
}

/// Unix identity (dev, ino) of the file at `path` (or `None` if it is
/// gone/unstat-able).
#[cfg(unix)]
fn path_identity(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
}

/// The live cross-process lock: the open descriptor (removed on drop) plus
/// the heartbeat that refreshes its mtime, so a holder holding the slot
/// longer than [`CROSS_PROCESS_TIMEOUT`] is not stolen mid-test. A `File`
/// is an open descriptor, not a lock, so it is fine to hold across `.await`.
struct CrossProcessLock {
    /// Kept open for the slot's lifetime; taken (and closed) on drop. A
    /// `File` is an open descriptor, not a lock, so it is fine to hold
    /// across `.await`.
    file: Option<std::fs::File>,
    /// Stop flag + condvar for the heartbeat (set + notify on drop, then
    /// join — prompt because of the short stop poll above).
    stop: std::sync::Arc<(std::sync::Mutex<bool>, Condvar)>,
    /// Taken (and joined) on drop; `Option` because `Drop` takes `&mut self`
    /// and `JoinHandle` is not `Copy`.
    heartbeat: Option<std::thread::JoinHandle<()>>,
    /// Identity (dev, ino) of the claimed lockfile (Unix only). Lets `Drop`
    /// remove *only* the file this instance actually claimed: a same-
    /// process sibling (e.g. a DB-gated test elsewhere in the binary that
    /// acquires the guard without the lockfile tests' `TEST_LOCK`) can
    /// remove and re-claim the path while we hold the in-process flag;
    /// deleting the sibling's file would break its own expectations.
    /// In-process DB access stays serialized by the in-process flag — the
    /// lockfile only needs to serialize across *processes*.
    #[cfg(unix)]
    claimed: Option<(u64, u64)>,
}

impl CrossProcessLock {
    fn new(file: std::fs::File) -> Self {
        #[cfg(unix)]
        let claimed = file_identity(&file);
        let stop = std::sync::Arc::new((std::sync::Mutex::new(false), Condvar::new()));
        let heartbeat = spawn_heartbeat(
            cross_process_lock_path(),
            HEARTBEAT_INTERVAL,
            std::sync::Arc::clone(&stop),
        );
        Self {
            file: Some(file),
            stop,
            heartbeat: Some(heartbeat),
            #[cfg(unix)]
            claimed,
        }
    }
}

impl Drop for CrossProcessLock {
    fn drop(&mut self) {
        let mut flag = self.stop.0.lock().unwrap_or_else(|p| p.into_inner());
        *flag = true;
        drop(flag);
        self.stop.1.notify_one();
        // Join before removing the file; the thread wakes within
        // `HEARTBEAT_STOP_POLL`, so this is prompt.
        if let Some(hb) = self.heartbeat.take() {
            let _ = hb.join();
        }
        // Close the descriptor, then release the lock by removing the
        // file — but only if it is still the file we claimed (see
        // `claimed`): a same-process sibling may have removed and
        // re-claimed the path in the meantime, and that file is now *its*
        // lock. (Windows has no portable (dev, ino) identity, so it keeps
        // the unconditional removal — the same-process race is only
        // observable there as a test-only oddity.)
        if let Some(f) = self.file.take() {
            drop(f);
        }
        #[cfg(unix)]
        {
            let path = cross_process_lock_path();
            // `None` identity (fstat failed at claim — virtually impossible
            // for an open fd) falls back to the unconditional removal.
            let ours = match self.claimed {
                Some(id) => path_identity(&path) == Some(id),
                None => true,
            };
            if ours {
                let _ = std::fs::remove_file(&path);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = std::fs::remove_file(cross_process_lock_path());
        }
    }
}

/// RAII guard that serializes DB-gated tests in this test binary **and**
/// across parallel test processes (via the lockfile). Cheap when the suite
/// runs single-threaded against a single binary; otherwise the second
/// contester blocks until the first drops its guard.
pub struct DbExclusiveGuard {
    /// The live cross-process lock (descriptor + heartbeat), removed on drop.
    cross_process: Option<CrossProcessLock>,
}

impl DbExclusiveGuard {
    /// Acquire the exclusive DB slot, blocking if another test (in this
    /// binary or another process) holds it.
    pub fn acquire() -> Self {
        // Cross-process first, so two binaries don't both grab the in-process
        // slot and then race on the DB. A failure on any platform means real
        // contention/timeout — fail loud (the footgun this lock exists to
        // catch).
        let cross_process = Some(CrossProcessLock::new(
            acquire_cross_process(CROSS_PROCESS_TIMEOUT)
                .unwrap_or_else(|e| panic!("DbExclusiveGuard: {e}")),
        ));
        let mut held = DB_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Wait (re-checking under the lock) until no other test holds the slot.
        while *held {
            held = DB_CV.wait(held).unwrap_or_else(|p| p.into_inner());
        }
        *held = true;
        // Release the mutex immediately; the "held" flag (not the lock) is
        // what the guard now owns, so no `MutexGuard` survives across `.await`.
        drop(held);
        Self { cross_process }
    }
}

impl Drop for DbExclusiveGuard {
    fn drop(&mut self) {
        // Release the cross-process lock first (so a waiting process can
        // proceed), then the in-process flag. `CrossProcessLock::drop`
        // stops + joins the heartbeat, then removes the file.
        if let Some(lock) = self.cross_process.take() {
            drop(lock);
        }
        let mut held = DB_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        *held = false;
        drop(held);
        DB_CV.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialize the guard/lockfile tests — they share the process-level
    /// slot and the same lockfile path.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Set `path`'s mtime to `when` (the test-side counterpart of
    /// [`super::touch_mtime`], which always stamps *now*).
    fn touch_mtime_at(path: &std::path::Path, when: std::time::SystemTime) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|f| f.set_modified(when))
            .expect("set mtime");
    }

    #[test]
    fn guard_sequential_ok() {
        let _lock = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Sequential acquire→drop→acquire must succeed.
        {
            let _g = DbExclusiveGuard::acquire();
        }
        {
            let _g = DbExclusiveGuard::acquire();
        }
    }

    #[test]
    fn guard_concurrent_serializes() {
        let _lock = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Hold the slot on this thread.
        let g = DbExclusiveGuard::acquire();
        // A concurrent acquire on another thread must block until `g` drops.
        let (tx, rx) = std::sync::mpsc::channel();
        let h = std::thread::spawn(move || {
            let _second = DbExclusiveGuard::acquire();
            tx.send(()).expect("signal after acquiring");
        });
        // It must still be blocked (the slot is held by `g`).
        assert!(
            rx.try_recv().is_err(),
            "concurrent acquire must block while another guard is held"
        );
        // Release the slot — the other thread's acquire proceeds.
        drop(g);
        h.join().expect("worker thread");
        assert!(rx.try_recv().is_ok(), "worker acquired after release");
    }

    #[test]
    fn fnv1a_lockfile_is_per_url() {
        // A fixed URL must map to a stable fingerprint (lockfile stability),
        // and a different URL must map to a different one (no cross-DB
        // contention).
        let same = b"postgres://zimservice:zimservice@127.0.0.1:5432/zimservice";
        assert_eq!(fnv1a(same), fnv1a(same));
        assert_ne!(
            fnv1a(same),
            fnv1a(b"postgres://zimservice:zimservice@127.0.0.1:5432/otherdb"),
            "different DBs must get different lockfiles"
        );
    }

    #[test]
    fn heartbeat_refreshes_mtime_and_stops_promptly() {
        let _lock = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let path = cross_process_lock_path();
        let _ = std::fs::remove_file(&path);
        let file = acquire_cross_process(CROSS_PROCESS_TIMEOUT).expect("claim lockfile");
        // Backdate the mtime so the assertion is robust to filesystem
        // timestamp granularity.
        let backdated = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        touch_mtime_at(&path, backdated);
        // A 50 ms heartbeat should fire several times in ~300 ms.
        let stop = std::sync::Arc::new((std::sync::Mutex::new(false), Condvar::new()));
        let hb = spawn_heartbeat(
            path.clone(),
            std::time::Duration::from_millis(50),
            std::sync::Arc::clone(&stop),
        );
        std::thread::sleep(std::time::Duration::from_millis(300));
        let mtime = std::fs::metadata(&path)
            .expect("stat lockfile")
            .modified()
            .expect("mtime");
        assert!(
            mtime > backdated,
            "heartbeat must refresh the lockfile mtime"
        );
        // Stop + join must be prompt — not a full (here 50 ms) interval, and
        // certainly well under a second.
        let stop_started = std::time::Instant::now();
        *stop.0.lock().unwrap_or_else(|p| p.into_inner()) = true;
        stop.1.notify_one();
        hb.join().expect("heartbeat thread");
        assert!(
            stop_started.elapsed() < std::time::Duration::from_secs(1),
            "stop + join must be prompt, not block on the interval"
        );
        drop(file);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn stale_lockfile_is_stealable() {
        let _lock = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let path = cross_process_lock_path();
        let _ = std::fs::remove_file(&path);
        // Plant a lockfile as if a dead holder left it, backdated past the
        // steal threshold.
        use std::io::Write;
        let stale = std::time::SystemTime::now() - std::time::Duration::from_secs(300);
        let mut f = std::fs::File::create(&path).expect("plant lockfile");
        f.write_all(b"999999\n").expect("write debug PID");
        touch_mtime_at(&path, stale);
        drop(f);
        // Acquire must steal it (remove + re-claim) instead of waiting out
        // the timeout.
        let started = std::time::Instant::now();
        let claimed =
            acquire_cross_process(CROSS_PROCESS_TIMEOUT).expect("stale lockfile must be stealable");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "steal must happen immediately, not at the waiter deadline"
        );
        drop(claimed);
        let _ = std::fs::remove_file(&path);
    }

    /// Same-pid rule: a holder in this process (which can only be a
    /// same-binary test holding the in-process slot) must be waited out,
    /// not deadlined — both threads here share the process PID, which is
    /// exactly that scenario.
    #[test]
    fn same_process_holder_is_waited_out_not_deadlined() {
        let _lock = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let path = cross_process_lock_path();
        let _ = std::fs::remove_file(&path);
        // A helper thread claims the lockfile first (signalling "held") and
        // then holds it ~500 ms — past the main thread's short 200 ms
        // deadline, but well inside the helper's own long timeout — before
        // releasing.
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (tx, rx) = std::sync::mpsc::channel();
        let holder_path = path.clone();
        let holder = std::thread::spawn(move || {
            let file = acquire_cross_process(std::time::Duration::from_secs(30))
                .expect("helper must claim the lockfile");
            held_tx.send(()).expect("signal held");
            std::thread::sleep(std::time::Duration::from_millis(500));
            drop(file);
            // Removing the file is what releases the lock.
            let _ = std::fs::remove_file(&holder_path);
            tx.send(()).expect("signal after release");
        });
        // Wait until the helper provably holds the lockfile, then the main
        // thread waits with a 200 ms deadline. Because the holder PID
        // equals this process's PID, it must NOT time out — it must keep
        // polling and succeed once the helper releases.
        held_rx.recv().expect("helper is holding the lockfile");
        let started = std::time::Instant::now();
        let file = acquire_cross_process(std::time::Duration::from_millis(200))
            .expect("same-pid holder must be waited out, not deadlined");
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(200),
            "must have been blocked past its own deadline while the holder held the file",
        );
        drop(file);
        let _ = std::fs::remove_file(&path);
        rx.recv().expect("helper released the lock");
        holder.join().expect("holder thread");
    }
}
