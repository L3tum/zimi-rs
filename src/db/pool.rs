//! Postgres connection pool configuration and TLS handling.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, PgPoolOptions, PgSslMode};
use sqlx::{ConnectOptions, Connection};

use crate::config::Config;
use crate::error::{Error, Result};

/// The shared pool type used across the crate.
///
/// `sqlx::PgPool` implements `sqlx::Executor` directly, so most call sites
/// just pass `&pool` to [`crate::db::raw`] — no checkout needed. Multi-statement
/// / session-bound work (advisory locks, migration transactions) checks out a
/// connection with `pool.acquire()`; see [`connect_dedicated`] for the
/// instance-lock path.
pub type Pool = PgPool;

/// Determines the TLS mode to use for the database connection based on the DSN.
///
/// All `require`/`verify-ca` (and the `+tls` schemes) map to the `Tls` variant,
/// which enforces chain validation against the native root store
/// (sqlx's `SslMode::VerifyCa`). `verify-full` also maps to `Tls` but is
/// promoted to `SslMode::VerifyFull` in `connect_options`, which additionally
/// validates the server certificate's hostname.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// No TLS (plain connection)
    None,
    /// TLS required (native-root chain validation, no hostname check)
    Tls,
}

/// Parse the DATABASE_URL to determine the appropriate TLS mode.
///
/// Rules:
/// - `postgres://` or `postgresql://` with no `sslmode` param → `TlsMode::None`
/// - `postgres+tls://` or `postgresql+tls://` → `TlsMode::Tls`
/// - `sslmode=disable` or `off` → `TlsMode::None`
/// - `sslmode=require` / `verify-ca` / `verify-full` → `TlsMode::Tls`
/// - Any other `sslmode` value → `Err`
///
/// The `+tls` schemes are rewritten to their plain form for the driver in
/// [`driver_dsn`] — sqlx only accepts `postgres://` / `postgresql://`.
pub fn tls_mode_from_dsn(dsn: &str) -> Result<TlsMode> {
    // Check for explicit TLS scheme
    if dsn.starts_with("postgres+tls://") || dsn.starts_with("postgresql+tls://") {
        return Ok(TlsMode::Tls);
    }

    // Extract sslmode from query parameters. `split_once('?')` keeps the
    // *entire* remainder after the first `?` (so an accidental second `?` in
    // the query does not drop a later `sslmode` — B2); the driver applies the
    // same first-`?` rule.
    let query = dsn.split_once('?').map(|(_, q)| q).unwrap_or("");
    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            if key == "sslmode" {
                return match value.to_lowercase().as_str() {
                    "disable" | "off" => Ok(TlsMode::None),
                    "require" | "verify-ca" | "verify-full" => Ok(TlsMode::Tls),
                    other => Err(Error::Config(format!(
                        "Unsupported sslmode: '{other}'. Supported values: disable, require, \
                        verify-ca, verify-full"
                    ))),
                };
            }
        }
    }

    // No sslmode specified — plain connection
    Ok(TlsMode::None)
}

/// sqlx's `PgConnectOptions::from_str` rejects libpq-style `sslmode=off`
/// (it accepts `disable|allow|prefer|require|verify-ca|verify-full`).
/// `off` is a legacy alias for `disable` in libpq — rewrite it so DSNs that
/// worked before the driver swap keep working.
fn normalize_sslmode(dsn: &str) -> String {
    let Some((head, query)) = dsn.split_once('?') else {
        return dsn.to_string();
    };
    let rebuilt = query
        .split('&')
        .map(|pair| {
            if let Some((k, v)) = pair.split_once('=') {
                if k == "sslmode" && v.to_lowercase() == "off" {
                    return "sslmode=disable".to_string();
                }
            }
            pair.to_string()
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{head}?{rebuilt}")
}

/// Whether the DSN explicitly requests full certificate + hostname
/// verification (`sslmode=verify-full`, case-insensitive). Uses the same
/// first-`?` query rule as [`tls_mode_from_dsn`] so an accidental second `?`
/// does not drop a later `sslmode` (see the B2 tests).
fn dsn_has_verify_full(dsn: &str) -> bool {
    let query = dsn.split_once('?').map(|(_, q)| q).unwrap_or("");
    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            if key == "sslmode" {
                return value.eq_ignore_ascii_case("verify-full");
            }
        }
    }
    false
}

/// The `PgSslMode` to pin for a validated [`TlsMode`], given the original DSN.
///
/// `verify-full` is the one mode promoted to hostname verification; every
/// other TLS case stops at chain validation. Kept as a pure function so the
/// security-relevant mapping is unit-testable without opening a socket.
fn ssl_mode_for(tls_mode: TlsMode, dsn: &str) -> PgSslMode {
    match tls_mode {
        TlsMode::None => PgSslMode::Disable,
        TlsMode::Tls => {
            if dsn_has_verify_full(dsn) {
                PgSslMode::VerifyFull
            } else {
                PgSslMode::VerifyCa
            }
        }
    }
}

/// Build a `PgConnectOptions` for the DSN with the validated TLS mode applied.
///
/// `from_str` already parses the URL's `sslmode` (after [`normalize_sslmode`]);
/// we then pin the mode explicitly (see [`ssl_mode_for`]) because sqlx's URL
/// default is `prefer` and this codebase's no-`sslmode` behavior is a *plain*
/// connection, not a TLS negotiation.
///
/// With sqlx's `tls-rustls-ring-native-roots` feature, `SslMode::VerifyCa`
/// loads the OS trust store via rustls-native-certs and validates the server
/// chain against it. `SslMode::VerifyFull` (used when the DSN specifies
/// `sslmode=verify-full`) additionally validates the server certificate's
/// hostname.
fn connect_options(dsn: &str, tls_mode: TlsMode) -> Result<PgConnectOptions> {
    let opts = PgConnectOptions::from_str(&normalize_sslmode(&driver_dsn(dsn)))
        .map_err(|e| Error::Internal(anyhow::anyhow!("db connect options: {e}")))?;
    Ok(opts.ssl_mode(ssl_mode_for(tls_mode, dsn)))
}

/// Create a Postgres connection pool from config, with automatic TLS
/// negotiation based on the DATABASE_URL scheme and sslmode parameter.
/// `effective_pool_size` clamps the size to at least 1 so a misconfigured
/// `db_pool_size = 0` can never create an unbounded pool.
///
/// A hard ceiling (ARCH): without it, `DB_POOL_SIZE=999999` would open that
/// many connections and exhaust Postgres's `max_connections` (default 100).
/// Capping at 100 keeps one instance within Postgres's default budget while
/// leaving headroom for real load; raise both together if you raise
/// `max_connections` deliberately.
const POOL_SIZE_CEILING: u32 = 100;

/// Clamp a configured pool size into the safe range `[1, POOL_SIZE_CEILING]`.
///
/// Floor 1: a misconfigured `db_pool_size = 0` must never create an
/// unbounded pool. Ceiling 100: without it, `DB_POOL_SIZE=999999` would open
/// that many connections and exhaust Postgres's `max_connections` (default
/// 100) — raise both together if you raise `max_connections` deliberately.
pub fn effective_pool_size(raw: u32) -> u32 {
    raw.clamp(1, POOL_SIZE_CEILING)
}

/// tokio-postgres' `Config::from_str` only accepts `postgres://` /
/// `postgresql://` schemes, so hand the driver the plain scheme; TLS is
/// controlled separately by `TlsMode` (derived from the original DSN).
pub fn driver_dsn(raw: &str) -> String {
    if let Some(r) = raw.strip_prefix("postgres+tls://") {
        format!("postgres://{r}")
    } else if let Some(r) = raw.strip_prefix("postgresql+tls://") {
        format!("postgresql://{r}")
    } else {
        raw.to_string()
    }
}

/// Connection max lifetime (30 min): NAT and firewall devices silently
/// drop idle TCP, so a connection held across such a drop fails with a bare
/// network error on first reuse.
const MAX_LIFETIME: Duration = Duration::from_secs(30 * 60);

/// Hard cap on the per-acquire liveness ping (see `create_pool`'s
/// `before_acquire` hook). A zombie peer (gone without a FIN) must not be
/// able to hold an acquire past this.
const ACQUIRE_PING_TIMEOUT: Duration = Duration::from_secs(5);

/// Build the sqlx Postgres pool from [`Config`]: derives the TLS mode from
/// the DSN scheme/`sslmode` (see [`connect_options`] for the TLS semantics:
/// `verify-full` enforces chain *and* hostname validation; `require` /
/// `verify-ca` / `+tls` enforce chain validation against the native root
/// store), clamps the size via [`effective_pool_size`], and sets the
/// lifetime/recycling options (10 s acquire timeout for fast 503s under
/// exhaustion, 30 min max lifetime so NAT-dropped idle sockets are recycled,
/// and a **bounded** per-acquire liveness ping — see the `before_acquire`
/// hook below — so a zombie peer can't wedge the pool).
pub async fn create_pool(config: &Config) -> Result<Pool> {
    let tls_mode = tls_mode_from_dsn(&config.database_url)?;

    let opts = connect_options(&config.database_url, tls_mode)?;

    let pool = PgPoolOptions::new()
        .max_connections(effective_pool_size(config.db_pool_size))
        .min_connections(1)
        // Fast 503 under pool exhaustion instead of deadpool's 30 s default.
        .acquire_timeout(Duration::from_secs(10))
        // Recycle long-lived connections (ops hardening, Perf #8): NAT and
        // firewall devices silently drop idle TCP, so a connection held
        // across such a drop fails with a bare network error on first reuse.
        .max_lifetime(MAX_LIFETIME)
        // sqlx's default liveness check is an *unbounded* `ping()` before
        // each acquire. That is the right check with the wrong bound: if a
        // peer vanished without a FIN (NAT/firewall state flush, host
        // reboot), the ping's read blocks forever and the acquire — and
        // everything queued behind the pool — wedges indefinitely. The
        // bounded hook below provides the same per-acquire check with a
        // hard cap: a slow/zombie ping is an error, the pool hard-closes
        // the connection and dials a fresh one (2026-09 review, ops
        // hardening after a real remote-DB wedge).
        .test_before_acquire(false)
        .before_acquire(|conn, _meta| {
            Box::pin(async move {
                match tokio::time::timeout(ACQUIRE_PING_TIMEOUT, conn.ping()).await {
                    Ok(res) => Ok(res.is_ok()),
                    Err(_) => Err(sqlx::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("pool liveness ping exceeded {ACQUIRE_PING_TIMEOUT:?}"),
                    ))),
                }
            })
        })
        .connect_with(opts)
        .await
        .map_err(Error::Database)?;

    tracing::info!("Database pool created (tls_mode={tls_mode:?})");

    Ok(pool)
}

/// Open a dedicated, non-pooled connection that honors the same TLS mode as
/// the pool.
///
/// Used for the single-instance advisory lock (S1): a pooled connection may be
/// recycled by the pool, silently releasing the lock, so a dedicated
/// connection is held for the guard's lifetime. Dropping the returned
/// connection closes the socket and releases any advisory lock held on it —
/// the caller controls that via the `InstanceLock` guard.
pub async fn connect_dedicated(database_url: &str) -> Result<PgConnection> {
    let tls_mode = tls_mode_from_dsn(database_url)?;
    let opts = connect_options(database_url, tls_mode)?;
    let conn = opts.connect().await.map_err(Error::Database)?;
    Ok(conn)
}

// ─── Explicit checkout-wait metric (Architecture M1) ────────────────────────

/// Explicit `pool.acquire()` wait statistics since process start, in
/// microseconds. Describes either the process-wide aggregate or one
/// per-site bucket (see [`checkout_wait_stats_by_site`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckoutWaitStats {
    /// Checkouts that completed (failed/timed-out acquires are not counted).
    pub count: u64,
    /// Sum of all checkout waits, µs (saturating add).
    pub total_us: u64,
    /// Longest single checkout wait since start, µs (0 = never waited).
    pub max_us: u64,
}

impl CheckoutWaitStats {
    /// Mean checkout wait, µs (0 when no checkout has been recorded).
    pub fn avg_us(&self) -> u64 {
        self.total_us / self.count.max(1)
    }
}

/// Explicit pool-checkout-wait metric (Architecture M1).
///
/// This is the number behind the "the shared 20-connection pool is the main
/// scalability limiter" revisit decision (the PERF-10 trigger in
/// `search::SearchEngine::search`): a non-trivial `max`/`avg` checkout wait
/// under sustained search QPS is the signal to move the search arms onto
/// separate connections. One instance is the process-global aggregate
/// (`CHECKOUT_WAIT`); `record_checkout_wait` additionally keeps one
/// instance per call-site label in `CHECKOUT_WAIT_BY_SITE`, so `/diagnostic`
/// can attribute the wait to the checkout that caused it. Only **explicit**
/// `pool.acquire()` sites are instrumented — via [`acquire_timed`] — because
/// the implicit per-query acquires inside sqlx's `Executor` impl for `&Pool`
/// are not visible from call sites. Failed acquires (10 s acquire timeout →
/// 503) are not recorded: their wait is the known timeout and they surface
/// through the `/diagnostic` `pool` saturation snapshot instead.
#[derive(Debug)]
pub struct CheckoutWaitCounter {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl CheckoutWaitCounter {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }

    /// Record one completed checkout's wait (saturating; the wait is bounded
    /// by the pool's 10 s acquire timeout, so real waits cannot saturate).
    pub fn record(&self, wait: Duration) {
        let wait_us = wait.as_micros().min(u128::from(u64::MAX)) as u64;
        self.count.fetch_add(1, Ordering::SeqCst);
        self.total_us.fetch_add(wait_us, Ordering::SeqCst);
        // `fetch_max` (stable since 1.45) replaces the old
        // compare_exchange_weak retry loop for the max.
        self.max_us.fetch_max(wait_us, Ordering::SeqCst);
    }

    /// Point-in-time snapshot (count, total, max).
    pub fn snapshot(&self) -> CheckoutWaitStats {
        CheckoutWaitStats {
            count: self.count.load(Ordering::SeqCst),
            total_us: self.total_us.load(Ordering::SeqCst),
            max_us: self.max_us.load(Ordering::SeqCst),
        }
    }
}

/// The process-global aggregate counter (every [`acquire_timed`] record
/// lands here and in the caller's per-site bucket).
static CHECKOUT_WAIT: CheckoutWaitCounter = CheckoutWaitCounter::new();

/// Per-site checkout-wait counters, keyed by the `&'static str` label each
/// [`acquire_timed`] call site passes. One bucket per call site, never
/// growing under load — the label set is fixed by construction (there is a
/// finite number of explicit checkout sites). `Mutex` (not atomics): the
/// guard is held only across the cheap [`CheckoutWaitCounter::record`],
/// never across an `.await`.
static CHECKOUT_WAIT_BY_SITE: LazyLock<Mutex<HashMap<&'static str, CheckoutWaitCounter>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Record one completed checkout's wait into BOTH the aggregate
/// [`CHECKOUT_WAIT`] counter and the per-site bucket for `site`, so the
/// aggregate always equals the sum of all per-site buckets.
fn record_checkout_wait(site: &'static str, wait: Duration) {
    CHECKOUT_WAIT.record(wait);
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom —
    // grandfathered expect_used.
    #[allow(clippy::expect_used)]
    {
        let mut g = CHECKOUT_WAIT_BY_SITE
            .lock()
            .expect("checkout-wait by-site lock poisoned");
        g.entry(site)
            .or_insert_with(CheckoutWaitCounter::new)
            .record(wait);
    }
}

/// Aggregate explicit-checkout-wait statistics since process start (all
/// call sites combined).
pub fn checkout_wait_stats() -> CheckoutWaitStats {
    CHECKOUT_WAIT.snapshot()
}

/// Per-site explicit-checkout-wait statistics since process start (surfaced
/// by `/diagnostic`), sorted by site label for stable output.
// LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom —
// grandfathered expect_used.
#[allow(clippy::expect_used)]
pub fn checkout_wait_stats_by_site() -> Vec<(String, CheckoutWaitStats)> {
    let g = CHECKOUT_WAIT_BY_SITE
        .lock()
        .expect("checkout-wait by-site lock poisoned");
    let mut v: Vec<(String, CheckoutWaitStats)> = g
        .iter()
        .map(|(site, c)| (site.to_string(), c.snapshot()))
        .collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

/// `pool.acquire()` with the wait recorded into the explicit-checkout-wait
/// metric (surfaced by `/diagnostic`, Architecture M1): once into the
/// process-global aggregate, once into the per-site bucket named by `site`
/// (a `&'static str` unique to this call site, e.g. `"health:db_probe"`), so
/// per-site attribution shows which checkout is holding the pool.
/// Use at explicit checkout sites; failed acquires are not recorded (see
/// [`CheckoutWaitCounter`]).
pub async fn acquire_timed(
    pool: &Pool,
    site: &'static str,
) -> std::result::Result<sqlx::pool::PoolConnection<sqlx::Postgres>, sqlx::Error> {
    let started = Instant::now();
    let acquired = pool.acquire().await;
    if acquired.is_ok() {
        record_checkout_wait(site, started.elapsed());
    }
    acquired
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn effective_pool_size_clamps_both_ends() {
        assert_eq!(effective_pool_size(0), 1);
        assert_eq!(effective_pool_size(1), 1);
        assert_eq!(effective_pool_size(20), 20);
        assert_eq!(effective_pool_size(100), 100);
        // A huge (or misconfigured) value is capped at the ceiling, not passed
        // through — it must never open more connections than Postgres allows.
        assert_eq!(
            effective_pool_size(POOL_SIZE_CEILING + 1),
            POOL_SIZE_CEILING
        );
        assert_eq!(effective_pool_size(u32::MAX), POOL_SIZE_CEILING);
    }

    // Pool construction is now async and connection-backed, so the old
    // deadpool `build()` regression test no longer applies; the 10 s acquire
    // timeout is covered by the DB-gated tests.

    // ── explicit checkout-wait metric (Architecture M1) ────────────────────

    #[test]
    fn checkout_wait_counter_tracks_count_total_max() {
        let counter = CheckoutWaitCounter::new();
        assert_eq!(counter.snapshot(), CheckoutWaitStats::default());
        counter.record(Duration::from_micros(10));
        counter.record(Duration::from_micros(40));
        counter.record(Duration::from_micros(30));
        let s = counter.snapshot();
        assert_eq!(s.count, 3);
        assert_eq!(s.total_us, 80);
        assert_eq!(s.max_us, 40);
        assert_eq!(s.avg_us(), 80 / 3);
    }

    #[test]
    fn checkout_wait_counter_zero_count_avg_is_zero() {
        // 0/0 must be 0, not a division panic (a fresh process has
        // `count == 0` until the first explicit checkout).
        let s = CheckoutWaitCounter::new().snapshot();
        assert_eq!(s.avg_us(), 0);
    }

    #[test]
    fn checkout_wait_counter_records_max_under_contention() {
        // Two threads racing the max (`fetch_max`): the final max is the
        // largest recorded wait, the count is the sum of both threads.
        let counter = std::sync::Arc::new(CheckoutWaitCounter::new());
        let counter1 = counter.clone();
        let counter2 = counter.clone();
        let t1 = std::thread::spawn(move || {
            for _ in 0..100 {
                counter1.record(Duration::from_micros(50));
            }
        });
        let t2 = std::thread::spawn(move || {
            for _ in 0..100 {
                counter2.record(Duration::from_micros(7));
            }
        });
        t1.join().unwrap();
        t2.join().unwrap();
        let s = counter.snapshot();
        assert_eq!(s.count, 200);
        assert_eq!(s.max_us, 50);
        assert_eq!(s.total_us, 100 * 50 + 100 * 7);
    }

    #[test]
    fn checkout_wait_by_site_isolates_labels_and_matches_aggregate() {
        // Unique labels: this test never collides with real call sites (or
        // other tests) recording into the process-global statics.
        const SITE_A: &str = "pool-test:site-a";
        const SITE_B: &str = "pool-test:site-b";
        let agg_before = checkout_wait_stats();

        record_checkout_wait(SITE_A, Duration::from_micros(10));
        record_checkout_wait(SITE_A, Duration::from_micros(30));
        record_checkout_wait(SITE_B, Duration::from_micros(40));

        let by_site = checkout_wait_stats_by_site();
        let a = by_site
            .iter()
            .find(|(s, _)| s == SITE_A)
            .expect("site A bucket recorded")
            .1;
        let b = by_site
            .iter()
            .find(|(s, _)| s == SITE_B)
            .expect("site B bucket recorded")
            .1;
        // Per-site isolation: each label sees only its own count/total/max.
        assert_eq!(
            a,
            CheckoutWaitStats {
                count: 2,
                total_us: 40,
                max_us: 30
            }
        );
        assert_eq!(
            b,
            CheckoutWaitStats {
                count: 1,
                total_us: 40,
                max_us: 40
            }
        );
        // Output is sorted by site label for stable `/diagnostic` rendering.
        let labels: Vec<&str> = by_site.iter().map(|(s, _)| s.as_str()).collect();
        let mut sorted = labels.clone();
        sorted.sort();
        assert_eq!(labels, sorted);

        // The aggregate records every checkout: the before/after delta
        // equals the sum of the per-site deltas. (No other lib test records
        // into the static counters, so the delta is deterministic even under
        // parallel test threads.)
        let agg_after = checkout_wait_stats();
        assert_eq!(agg_after.count - agg_before.count, a.count + b.count);
        assert_eq!(
            agg_after.total_us - agg_before.total_us,
            a.total_us + b.total_us
        );
        assert!(agg_after.max_us >= b.max_us);
    }

    #[test]
    fn normalize_sslmode_off_to_disable() {
        assert_eq!(
            normalize_sslmode("postgres://u:p@h/db?sslmode=off"),
            "postgres://u:p@h/db?sslmode=disable"
        );
        assert_eq!(
            normalize_sslmode("postgres://u:p@h/db?a=1&sslmode=OFF&b=2"),
            "postgres://u:p@h/db?a=1&sslmode=disable&b=2"
        );
        // Non-off modes and no-sslmode DSNs pass through untouched.
        assert_eq!(
            normalize_sslmode("postgres://u:p@h/db?sslmode=require"),
            "postgres://u:p@h/db?sslmode=require"
        );
        assert_eq!(
            normalize_sslmode("postgres://u:p@h/db"),
            "postgres://u:p@h/db"
        );
    }

    #[test]
    fn plain_postgres_no_sslmode() {
        assert_eq!(
            tls_mode_from_dsn("postgres://user:pass@localhost:5432/db").unwrap(),
            TlsMode::None
        );
    }

    #[test]
    fn postgresql_scheme_no_sslmode() {
        assert_eq!(
            tls_mode_from_dsn("postgresql://user:pass@localhost:5432/db").unwrap(),
            TlsMode::None
        );
    }

    #[test]
    fn postgres_plus_tls_scheme() {
        assert_eq!(
            tls_mode_from_dsn("postgres+tls://user:pass@localhost:5432/db").unwrap(),
            TlsMode::Tls
        );
    }

    #[test]
    fn postgresql_plus_tls_scheme() {
        assert_eq!(
            tls_mode_from_dsn("postgresql+tls://user:pass@localhost:5432/db").unwrap(),
            TlsMode::Tls
        );
    }

    #[test]
    fn driver_dsn_plus_tls_rewrites() {
        assert_eq!(
            driver_dsn("postgres+tls://user:pass@localhost:5432/db"),
            "postgres://user:pass@localhost:5432/db"
        );
    }

    #[test]
    fn driver_dsn_postgresql_plus_tls_rewrites() {
        assert_eq!(
            driver_dsn("postgresql+tls://user:pass@localhost:5432/db"),
            "postgresql://user:pass@localhost:5432/db"
        );
    }

    #[test]
    fn driver_dsn_plain_unchanged() {
        assert_eq!(
            driver_dsn("postgres://user:pass@localhost:5432/db"),
            "postgres://user:pass@localhost:5432/db"
        );
    }

    #[test]
    fn driver_dsn_sslmode_untouched() {
        assert_eq!(
            driver_dsn("postgres://user@localhost/db?sslmode=require"),
            "postgres://user@localhost/db?sslmode=require"
        );
    }

    #[test]
    fn sslmode_disable() {
        assert_eq!(
            tls_mode_from_dsn("postgres://user:pass@localhost:5432/db?sslmode=disable").unwrap(),
            TlsMode::None
        );
    }

    #[test]
    fn sslmode_off() {
        assert_eq!(
            tls_mode_from_dsn("postgres://user:pass@localhost:5432/db?sslmode=off").unwrap(),
            TlsMode::None
        );
    }

    #[test]
    fn sslmode_require() {
        assert_eq!(
            tls_mode_from_dsn("postgres://user:pass@localhost:5432/db?sslmode=require").unwrap(),
            TlsMode::Tls
        );
    }

    #[test]
    fn sslmode_verify_ca() {
        assert_eq!(
            tls_mode_from_dsn("postgres://user:pass@localhost:5432/db?sslmode=verify-ca").unwrap(),
            TlsMode::Tls
        );
    }

    #[test]
    fn sslmode_verify_full() {
        assert_eq!(
            tls_mode_from_dsn("postgres://user:pass@localhost:5432/db?sslmode=verify-full")
                .unwrap(),
            TlsMode::Tls
        );
    }

    #[test]
    fn sslmode_case_insensitive() {
        assert_eq!(
            tls_mode_from_dsn("postgres://user:pass@localhost:5432/db?sslmode=REQUIRE").unwrap(),
            TlsMode::Tls
        );
    }

    #[test]
    fn sslmode_unknown_rejected() {
        assert!(tls_mode_from_dsn("postgres://user:pass@localhost:5432/db?sslmode=weird").is_err());
    }

    #[test]
    fn sslmode_with_other_params() {
        assert_eq!(
            tls_mode_from_dsn(
                "postgres://user:pass@localhost:5432/db?connect_timeout=10&sslmode=require"
            )
            .unwrap(),
            TlsMode::Tls
        );
    }

    // B2: a second `?` in the query must not drop a later `sslmode` (the old
    // `split('?').nth(1)` kept only the first segment and would have
    // connected without TLS).
    #[test]
    fn sslmode_after_second_question_mark() {
        assert_eq!(
            tls_mode_from_dsn("postgres://u:p@h/db?a=1?b=2&sslmode=require").unwrap(),
            TlsMode::Tls
        );
    }

    // The security-relevant direction of B2: a dropped `sslmode=disable` is
    // harmless, but a dropped `sslmode=require` would downgrade TLS — and the
    // reverse (a `disable` after a second `?`) must still be honored.
    #[test]
    fn sslmode_disable_after_second_question_mark() {
        assert_eq!(
            tls_mode_from_dsn("postgres://u:p@h/db?a=1?b=2&sslmode=disable").unwrap(),
            TlsMode::None
        );
    }

    // A second `?` with no `sslmode` anywhere → plain connection.
    #[test]
    fn question_mark_without_sslmode() {
        assert_eq!(tls_mode_from_dsn("?a=1?b=2").unwrap(), TlsMode::None);
    }

    #[test]
    fn no_query_params() {
        assert_eq!(
            tls_mode_from_dsn("postgres://user:pass@localhost:5432/db").unwrap(),
            TlsMode::None
        );
    }

    #[test]
    fn empty_dsn() {
        assert_eq!(tls_mode_from_dsn("").unwrap(), TlsMode::None);
    }

    // --- verify-full promotion (hostname verification) ---
    //
    // `PgSslMode` does not derive `PartialEq`, so these assert with `matches!`.

    #[test]
    fn dsn_has_verify_full_only_for_verify_full() {
        assert!(dsn_has_verify_full(
            "postgres://u:p@h/db?sslmode=verify-full"
        ));
        assert!(dsn_has_verify_full(
            "postgres://u:p@h/db?sslmode=VERIFY-FULL"
        ));
        // Other modes must not be mistaken for verify-full.
        assert!(!dsn_has_verify_full("postgres://u:p@h/db?sslmode=require"));
        assert!(!dsn_has_verify_full(
            "postgres://u:p@h/db?sslmode=verify-ca"
        ));
        assert!(!dsn_has_verify_full("postgres://u:p@h/db?sslmode=disable"));
        assert!(!dsn_has_verify_full("postgres://u:p@h/db"));
    }

    #[test]
    fn dsn_has_verify_full_after_second_question_mark() {
        // Mirrors the B2 rule: a `?` inside the query must not drop a later
        // sslmode — otherwise a `verify-full` request would silently fall
        // back to chain-only verification.
        assert!(dsn_has_verify_full(
            "postgres://u:p@h/db?a=1?b=2&sslmode=verify-full"
        ));
    }

    #[test]
    fn ssl_mode_full_when_verify_full() {
        assert!(matches!(
            ssl_mode_for(TlsMode::Tls, "postgres://u:p@h/db?sslmode=verify-full"),
            PgSslMode::VerifyFull
        ));
    }

    #[test]
    fn ssl_mode_ca_for_require_and_verify_ca() {
        assert!(matches!(
            ssl_mode_for(TlsMode::Tls, "postgres://u:p@h/db?sslmode=require"),
            PgSslMode::VerifyCa
        ));
        assert!(matches!(
            ssl_mode_for(TlsMode::Tls, "postgres://u:p@h/db?sslmode=verify-ca"),
            PgSslMode::VerifyCa
        ));
    }

    #[test]
    fn ssl_mode_ca_for_plus_tls_scheme() {
        // The `+tls` scheme carries no sslmode, so it stays at chain level.
        assert!(matches!(
            ssl_mode_for(TlsMode::Tls, "postgres+tls://u:p@h/db"),
            PgSslMode::VerifyCa
        ));
    }

    #[test]
    fn ssl_mode_disable_when_no_tls() {
        assert!(matches!(
            ssl_mode_for(TlsMode::None, "postgres://u:p@h/db"),
            PgSslMode::Disable
        ));
        assert!(matches!(
            ssl_mode_for(TlsMode::None, "postgres://u:p@h/db?sslmode=disable"),
            PgSslMode::Disable
        ));
    }
}
