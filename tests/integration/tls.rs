//! TLS wiring gate: proves sqlx's TLS feature is actually compiled into
//! the test binary.
//!
//! `Cargo.toml` builds `sqlx` with `tls-rustls-ring-native-roots`, but DSN
//! parsing is exhaustively unit-tested as pure functions, and CI's Postgres
//! is plaintext (pgvector:pg16, no TLS config) — so nothing in the suite
//! had exercised the TLS stack at runtime: a build that silently dropped
//! the TLS feature would pass everything and only surface in production.
//!
//! The gate exploits the two distinct failure classes sqlx 0.9.0 (pinned in
//! `Cargo.lock`) produces for a `sslmode=verify-full` connect to a
//! plaintext server:
//!
//! - **TLS feature compiled in**: `error_if_unavailable()` passes, sqlx
//!   sends the `SSLRequest`, the plaintext server answers `'N'`, and the
//!   connect fails with `Error::Tls("server does not support TLS")` —
//!   constructed in sqlx-postgres 0.9.0 `src/connection/tls.rs`
//!   (`maybe_upgrade`, the `Require | VerifyFull | VerifyCa` arm).
//!   Reaching that error is itself proof the TLS backend is linked:
//!   without the feature the connect dies before the `SSLRequest` is sent.
//! - **TLS feature missing**: `error_if_unavailable()` fails first with
//!   `Error::Tls("TLS upgrade required by connect options but SQLx was
//!   built without TLS support enabled")` — sqlx-core 0.9.0
//!   `src/net/tls/mod.rs`.
//!
//! So the test pins the server-rejection wording AND asserts the
//! feature-missing marker is absent. Both messages are verbatim from the
//! 0.9.0 sources in the local registry cache
//! (`~/.cargo/registry/src/*/sqlx-{postgres,core}-0.9.0`).
//!
//! Assumption: the suite's Postgres is plaintext (compose default and CI
//! service — the stock image ships without SSL) and answers `'N'` to
//! `SSLRequest`. Pointing `DATABASE_URL` at a TLS-enabled server would
//! instead fail certificate validation and fail this gate, correctly
//! flagging an environment the gate is not calibrated for.
//!
//! DB-gated like every test here: skips without a reachable
//! `DATABASE_URL`, hard-fails under `ZIMSERVICE_REQUIRE_DB=1` (CI's
//! `test` job). No raw SQL runs, so `raw-sql-lint` is unaffected.

use std::str::FromStr;

use sqlx::postgres::PgConnectOptions;
use sqlx::ConnectOptions;

use super::common::*;

/// Rewrite the suite DSN to force `sslmode=verify-full`: keep every other
/// query param, drop any pre-existing `sslmode` (the plaintext test DB has
/// none, but an operator-set `DATABASE_URL` might), and pin ours.
fn with_verify_full(url: &str) -> String {
    let (head, query) = url.split_once('?').unwrap_or((url, ""));
    let pairs: Vec<&str> = query
        .split('&')
        .filter(|p| !p.is_empty() && !p.starts_with("sslmode="))
        .chain(["sslmode=verify-full"])
        .collect();
    format!("{head}?{}", pairs.join("&"))
}

/// TLS wiring: a `sslmode=verify-full` connect to the (plaintext) suite
/// Postgres must fail with sqlx's server-rejected-TLS error — the failure
/// only reachable when the TLS feature is compiled in (module docs for the
/// two failure classes).
#[tokio::test]
async fn tls_feature_compiled_in_plaintext_server_rejects_ssl() {
    // Gate: the suite DB must be reachable. The verify-full attempt below
    // targets the same server via the same URL resolution `pool_or_skip`
    // used, so a pass means "reachable AND TLS-handshake attempted AND
    // rejected by a plaintext server".
    let (_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.into());

    let opts = PgConnectOptions::from_str(&with_verify_full(&url)).expect("verify-full DSN parses");
    let err = match tokio::time::timeout(Duration::from_secs(3), opts.connect()).await {
        Ok(Ok(_)) => panic!("verify-full connect to a plaintext server must fail"),
        Ok(Err(e)) => e,
        Err(_) => panic!("verify-full connect attempt timed out (3s)"),
    };
    let msg = err.to_string();

    // TLS-handshake class (not a `Configuration`/parse error and not a
    // network `Io` error): sqlx got far enough to negotiate TLS.
    assert!(
        matches!(&err, sqlx::Error::Tls(_)),
        "expected a TLS-class sqlx error, got: {msg}"
    );
    // Server-rejection wording: only reachable after
    // `error_if_unavailable()` passed — i.e. the TLS backend is compiled
    // in (without the feature, sqlx fails before sending the SSLRequest).
    assert!(
        msg.contains("server does not support TLS"),
        "expected sqlx's server-rejected-TLS error (proves the TLS feature \
         is compiled in); got: {msg}"
    );
    // The feature-missing class must NOT be present.
    assert!(
        !msg.contains("built without TLS support enabled"),
        "sqlx TLS feature is missing from the build: {msg}"
    );
}
