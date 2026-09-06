//! Postgres connection pool configuration and TLS handling.

use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, PgPoolOptions, PgSslMode};
use sqlx::ConnectOptions;

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
/// All `require`/`verify-ca`/`verify-full` (and the `+tls` schemes) map to the
/// single `Tls` variant, which enforces chain validation against the native
/// root store (sqlx's `SslMode::VerifyCa`) — the same effective level as the
/// old tokio-postgres-rustls integration (certificate chain verified, hostname
/// NOT verified). `verify-full` is accepted for compatibility but does not
/// enable hostname verification.
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
/// `create_pool` (see `driver_dsn`) — tokio-postgres only accepts
/// `postgres://` / `postgresql://`.
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
                        "Unsupported sslmode: '{other}'. Supported values: disable, require, verify-ca, verify-full"
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

/// Build a `PgConnectOptions` for the DSN with the validated TLS mode applied.
///
/// `from_str` already parses the URL's `sslmode` (after [`normalize_sslmode`]);
/// we then pin the mode explicitly because sqlx's URL default is `prefer` and
/// this codebase's no-`sslmode` behavior is a *plain* connection, not a TLS
/// negotiation.
///
/// Native root certificates: with sqlx's `tls-rustls-ring-native-roots`
/// feature, `SslMode::VerifyCa` loads the OS trust store via
/// rustls-native-certs (pre-deadpool parity) and validates the server chain
/// against it (no hostname check — same effective level as before the
/// driver swap).
fn connect_options(dsn: &str, tls_mode: TlsMode) -> Result<PgConnectOptions> {
    let mut opts = PgConnectOptions::from_str(&normalize_sslmode(&driver_dsn(dsn))).map_err(
        |e| Error::Internal(anyhow::anyhow!("db connect options: {e}")),
    )?;
    match tls_mode {
        TlsMode::None => opts = opts.ssl_mode(PgSslMode::Disable),
        TlsMode::Tls => opts = opts.ssl_mode(PgSslMode::VerifyCa),
    }
    Ok(opts)
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

// Shared builder chain for both TLS modes so the timeout/runtime settings
// can't drift between them.
// (folded into `connect_options` — the options are built once and shared
// between `create_pool` and `connect_dedicated`)

pub async fn create_pool(config: &Config) -> Result<Pool> {
    let tls_mode = tls_mode_from_dsn(&config.database_url)?;

    // WI-6: warn when the operator asked for verification we can't fully
    // enforce: hostname verification (verify-full) is never performed, and
    // `require` now additionally gets chain validation (verify-ca level).
    if matches!(tls_mode, TlsMode::Tls) {
        let query = config
            .database_url
            .split_once('?')
            .map(|(_, q)| q)
            .unwrap_or("");
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                if key == "sslmode"
                    && matches!(value.to_lowercase().as_str(), "verify-ca" | "verify-full")
                {
                    tracing::warn!(
                        "DATABASE_URL specifies sslmode={value} but only verify-ca is enforced — \
                         the certificate chain is validated against the native root store, \
                         but the hostname is NOT verified. This is a limitation of the \
                         sqlx-rustls integration."
                    );
                }
            }
        }
    }

    let opts = connect_options(&config.database_url, tls_mode)?;

    let pool = PgPoolOptions::new()
        .max_connections(effective_pool_size(config.db_pool_size))
        .min_connections(1)
        // Fast 503 under pool exhaustion instead of deadpool's 30 s default.
        .acquire_timeout(Duration::from_secs(10))
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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
        assert_eq!(effective_pool_size(POOL_SIZE_CEILING + 1), POOL_SIZE_CEILING);
        assert_eq!(effective_pool_size(u32::MAX), POOL_SIZE_CEILING);
    }

    // Pool construction is now async and connection-backed, so the old
    // deadpool `build()` regression test no longer applies; the 10 s acquire
    // timeout is covered by the DB-gated tests.

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
}
