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
//! `DbExclusiveGuard::acquire()` **blocks** (rather than panicking) when
//! another DB test — in this binary *or* another process — still holds the
//! slot, so the suite is safe to run with the default parallel
//! `--test-threads`. The guard deliberately does not hold a `MutexGuard`
//! across `.await` (which would trip `clippy::await_holding_lock`): it only
//! briefly holds the mutex to flip a process-level "held" flag. The
//! cross-process `File` is an open descriptor, not a lock, so it is also safe
//! to hold across `.await`.
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

/// A fully in-memory [`crate::AppState`] for unit tests that need a complete
/// state but never touch the database: the pool is a [`dead_pool`] (unreachable),
/// settings are [`crate::settings::default_settings`], the ZIM dir does not
/// exist, and the qBittorrent / rate-limit / probe / lockout / degradation
/// fields all use their defaults.
pub fn test_state() -> crate::AppState {
    let pool = dead_pool();
    let settings = crate::settings::SettingsCache::new_with_map(
        pool.clone(),
        crate::settings::default_settings(),
        std::collections::HashMap::new(),
    );
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
        rate_limiter: std::sync::Arc::new(crate::serve::ratelimit::RateLimiterHandle::new()),
        probes: crate::HealthProbes::default(),
        auth_lockout: std::sync::Arc::new(Default::default()),
        degradation: crate::health::DegradationTracker::default(),
    }
}

static DB_LOCK: Mutex<bool> = Mutex::new(false);
static DB_CV: Condvar = Condvar::new();

/// How long a cross-process lock is considered live before it may be stolen
/// (a killed process leaves its lockfile behind; after this it is stale).
const CROSS_PROCESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// Poll interval while waiting for another process to release the lock.
const CROSS_PROCESS_POLL: std::time::Duration = std::time::Duration::from_millis(50);

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
fn acquire_cross_process() -> std::io::Result<std::fs::File> {
    use std::io::Write;
    let path = cross_process_lock_path();
    let deadline = std::time::Instant::now() + CROSS_PROCESS_TIMEOUT;
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
                if std::time::Instant::now() >= deadline {
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

/// RAII guard that serializes DB-gated tests in this test binary **and**
/// across parallel test processes (via the lockfile). Cheap when the suite
/// runs single-threaded against a single binary; otherwise the second
/// contester blocks until the first drops its guard.
pub struct DbExclusiveGuard {
    /// Open descriptor holding the cross-process lockfile (removed on drop).
    /// A `File` is an open descriptor, not a lock, so holding it across
    /// `.await` is fine.
    cross_process: Option<std::fs::File>,
}

impl DbExclusiveGuard {
    /// Acquire the exclusive DB slot, blocking if another test (in this
    /// binary or another process) holds it.
    pub fn acquire() -> Self {
        // Cross-process first, so two binaries don't both grab the in-process
        // slot and then race on the DB. A failure on any platform means real
        // contention/timeout — fail loud (the footgun this lock exists to
        // catch).
        let cross_process =
            Some(acquire_cross_process().unwrap_or_else(|e| panic!("DbExclusiveGuard: {e}")));
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
        // Release the cross-process lockfile first (so a waiting process can
        // proceed), then the in-process flag. Removing the file is what
        // releases the lock; the open descriptor is dropped alongside.
        if let Some(f) = self.cross_process.take() {
            drop(f);
            let _ = std::fs::remove_file(cross_process_lock_path());
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

    /// Serialize the two guard tests — they share the process-level slot.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
}
