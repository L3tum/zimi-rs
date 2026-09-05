//! Postgres connection pool configuration and TLS handling.

use std::time::Duration;

use deadpool_postgres::Config as DpConfig;
pub use deadpool_postgres::Pool;
use deadpool_postgres::Runtime;
use rustls::RootCertStore;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::NoTls;
use tokio_postgres::Socket;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::config::Config;
use crate::error::{Error, Result};

/// Determines the TLS mode to use for the database connection based on the DSN.
///
/// The verification level is not differentiated — all `require`/`verify-ca`/
/// `verify-full` (and the `+tls` schemes) map to the single `Tls` variant,
/// for which rustls' defaults apply; `verify-ca`/`verify-full` are accepted
/// for compatibility, not because they change the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// No TLS (plain connection)
    None,
    /// TLS required (rustls defaults apply)
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

/// Build a rustls `ClientConfig` loaded with native root certificates.
fn build_rustls_config() -> Result<rustls::ClientConfig> {
    let mut root_store = RootCertStore::empty();
    let cert_result = rustls_native_certs::load_native_certs();
    for cert in cert_result.certs {
        if let Err(e) = root_store.add(cert) {
            tracing::warn!("Failed to add native cert to root store: {e}");
        }
    }
    if !cert_result.errors.is_empty() {
        tracing::warn!(
            "Some native certificates failed to load: {:?}",
            cert_result.errors.len()
        );
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(config)
}

/// Create a Postgres connection pool from config, with automatic TLS negotiation
/// based on the DATABASE_URL scheme and sslmode parameter.
/// deadpool's `max_size(0)` means "unbounded" — clamp to at least 1 so a
/// misconfigured `db_pool_size = 0` can never create an unbounded pool.
///
/// A hard ceiling (ARCH): without it, `DB_POOL_SIZE=999999` would open that
/// many connections and exhaust Postgres's `max_connections` (default 100).
/// Capping at 100 keeps one instance within Postgres's default budget while
/// leaving headroom for real load; raise both together if you raise
/// `max_connections` deliberately.
const POOL_SIZE_CEILING: u32 = 100;

pub fn effective_pool_size(raw: u32) -> usize {
    raw.clamp(1, POOL_SIZE_CEILING) as usize
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

/// Shared builder chain for both TLS modes so the timeout/runtime settings
/// can't drift between them.
fn build_pool<T>(dp_cfg: DpConfig, conn: T, size: usize) -> Result<Pool>
where
    T: MakeTlsConnect<Socket> + Clone + Sync + Send + 'static,
    T::Stream: Sync + Send,
    T::TlsConnect: Sync + Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    dp_cfg
        .builder(conn)
        .map_err(|e| Error::Internal(anyhow::anyhow!("db pool config: {e}")))?
        .max_size(size)
        // Fast 503 under pool exhaustion instead of deadpool's 30 s default.
        .wait_timeout(Some(Duration::from_secs(10)))
        // deadpool >=0.12 requires a runtime handle when any timeout is
        // configured, or `build()` fails with `NoRuntimeSpecified`.
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(|e| Error::Internal(anyhow::anyhow!("db pool: {e}")))
}

pub async fn create_pool(config: &Config) -> Result<Pool> {
    let tls_mode = tls_mode_from_dsn(&config.database_url)?;

    // WI-6: warn when the operator asked for verification but we can only
    // enforce encryption (require). The tokio-postgres-rustls integration does
    // not differentiate between verify-ca and verify-full.
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
                        "DATABASE_URL specifies sslmode={value} but only encryption (require) is enforced — \
                         certificate and hostname are NOT verified. This is a limitation of the \
                         tokio-postgres-rustls integration."
                    );
                }
            }
        }
    }

    let mut dp_cfg = DpConfig::new();
    dp_cfg.url = Some(driver_dsn(&config.database_url));

    let pool = match tls_mode {
        TlsMode::None => build_pool(dp_cfg, NoTls, effective_pool_size(config.db_pool_size))?,
        TlsMode::Tls => build_pool(
            dp_cfg,
            MakeRustlsConnect::new(build_rustls_config()?),
            effective_pool_size(config.db_pool_size),
        )?,
    };

    // Verify connection
    let _conn = pool
        .get()
        .await
        .map_err(|e| Error::Internal(anyhow::anyhow!("db connect: {e}")))?;
    drop(_conn);

    tracing::info!("Database pool created (tls_mode={tls_mode:?})");

    Ok(pool)
}

/// Open a dedicated, non-pooled connection that honors the same TLS mode as
/// the pool, with the I/O driver already spawned (detached). Returns the
/// `Client` and the spawned driver's `JoinHandle`.
///
/// Used for the single-instance advisory lock (S1): a pooled connection may be
/// recycled by deadpool, silently releasing the lock, so a dedicated connection
/// is held for the guard's lifetime. The caller MUST keep the `JoinHandle`
/// alive — dropping the `Connection` future closes the socket and releases any
/// advisory lock held on it; abort the handle to close the connection and
/// release the lock.
pub async fn connect_dedicated(
    database_url: &str,
) -> Result<(
    tokio_postgres::Client,
    tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
)> {
    let tls_mode = tls_mode_from_dsn(database_url)?;
    let dsn = driver_dsn(database_url);
    let map_err =
        |e: tokio_postgres::Error| Error::Internal(anyhow::anyhow!("dedicated db connect: {e}"));
    let (client, task) = match tls_mode {
        TlsMode::None => {
            let (c, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(&map_err)?;
            (c, tokio::spawn(conn))
        }
        TlsMode::Tls => {
            let (c, conn) =
                tokio_postgres::connect(&dsn, MakeRustlsConnect::new(build_rustls_config()?))
                    .await
                    .map_err(&map_err)?;
            (c, tokio::spawn(conn))
        }
    };
    Ok((client, task))
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
        assert_eq!(
            effective_pool_size(POOL_SIZE_CEILING + 1),
            POOL_SIZE_CEILING as usize,
        );
        assert_eq!(effective_pool_size(u32::MAX), POOL_SIZE_CEILING as usize);
    }

    /// deadpool >= 0.12 requires a `Runtime` handle whenever any pool timeout is
    /// configured, or `build()` fails with `NoRuntimeSpecified` — which made the
    /// real binary (whose `create_pool` sets `wait_timeout`) unstartable. `build()`
    /// is lazy (no connection), so this is verifiable without a live database.
    #[test]
    fn pool_builds_with_wait_timeout_only_when_runtime_set() {
        let cfg = || {
            let mut c = DpConfig::new();
            // Unreachable URL is fine: `build()` never connects.
            c.url = Some("postgres://u:p@127.0.0.1:1/db".into());
            c
        };
        // Regression: `wait_timeout` WITHOUT a runtime must NOT build.
        let no_runtime = cfg()
            .builder(NoTls)
            .unwrap()
            .max_size(1)
            .wait_timeout(Some(Duration::from_secs(10)))
            .build();
        assert!(
            no_runtime.is_err(),
            "wait_timeout without a runtime must fail to build (deadpool >=0.12)"
        );
        // The fix: adding the runtime handle makes the same builder build OK.
        let with_runtime = cfg()
            .builder(NoTls)
            .unwrap()
            .max_size(1)
            .wait_timeout(Some(Duration::from_secs(10)))
            .runtime(Runtime::Tokio1)
            .build();
        assert!(
            with_runtime.is_ok(),
            "wait_timeout + runtime must build: {with_runtime:?}"
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
