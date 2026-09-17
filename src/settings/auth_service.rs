//! `SettingsAuth`: the admin-auth surface extracted out of `SettingsCache`
//! (2026-09 review, Arch M2 — the god-object direction). Owns the
//! token-verification KDF call sites (TTL-cached, process-wide in-flight
//! cap) and the legacy-plaintext upgrade; it reads the stored password
//! through the shared settings cache (the in-memory map is authoritative —
//! auth never re-reads the DB per request).
//!
//! Constructed from a `SettingsCache` via [`SettingsCache::auth`] (a cheap
//! `Arc` clone of the shared inner state), so the KDF cap, token cache,
//! and generation counter are the same process-wide ones the cache uses.

use std::sync::{Arc, LazyLock};

use crate::db::raw;
use crate::settings::auth::verify_admin_password;
use crate::settings::cache::SettingsInner;
use crate::settings::defs::KEY_ACCESS_ADMIN_PASSWORD;

/// SEC (KDF DoS): a 19.5 MiB argon2id verify is expensive in both memory and
/// CPU, and the token negative cache bounds *distinct* tokens, not *concurrent*
/// KDFs. A multi-IP burst of distinct Bearer tokens could otherwise launch
/// hundreds of parallel KDFs (~5 GiB transient on a small host). Cap the
/// number of in-flight KDFs process-wide; requests that can't get a permit in
/// time are denied (safe: an unverified token is never admitted). Denied
/// attempts are still recorded in the negative cache so a flood re-hits the
/// cache rather than re-queuing.
///
/// **DI exception (deliberate):** `KDF_SEM` is a process-global
/// `LazyLock` static — a deliberate process-global (one of a small set of
/// such statics) in an otherwise fully dependency-injected codebase.
/// It is kept outside `SettingsAuth` because the limit must hold
/// *across the whole process* (every request handler, middleware layer, and
/// background task verifies tokens through the same cap), not
/// per-auth-handle; it is a constant (no config dependency) and
/// nothing else may read or write it. `KdfGuard` wraps it so the
/// overload/deny arm of `verify_bg` is testable (tests inject their own
/// semaphore + timeout — reaching the production arm would require 16 real
/// concurrent verifies held for 10 s).
const KDF_MAX_CONCURRENT: usize = 16;
const KDF_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
static KDF_SEM: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(KDF_MAX_CONCURRENT)));

/// The in-flight-KDF acquire guard: shared semaphore + acquire timeout.
/// Production instances use the process-wide [`KDF_SEM`]; tests inject
/// their own (see [`SettingsAuth::new_for_test`]).
#[derive(Clone)]
struct KdfGuard {
    sem: Arc<tokio::sync::Semaphore>,
    acquire_timeout: std::time::Duration,
}

impl KdfGuard {
    /// The process-wide production guard.
    fn shared() -> Self {
        Self {
            sem: Arc::clone(&KDF_SEM),
            acquire_timeout: KDF_ACQUIRE_TIMEOUT,
        }
    }
}

/// Admin-auth service over the shared settings inner state (the same state
/// a `SettingsCache` clone points at — one `Arc`), so every handle verifies
/// against the same token cache and password value. `Clone` is an `Arc` clone.
#[derive(Clone)]
pub struct SettingsAuth {
    inner: Arc<SettingsInner>,
    kdf: KdfGuard,
}

/// Outcome of the shared token-verify fast path (see
/// [`SettingsAuth::verify_fast_path`]): a fresh cached verdict, or the
/// stored password to run the KDF against.
enum TokenVerifyFast {
    /// Fresh cached result (positive hit, negative hit, or empty token).
    Verdict(bool),
    /// No fresh cache entry — run the KDF against this stored password.
    RunKdf(String),
}

impl SettingsAuth {
    /// Wrap the shared settings inner state (constructed by
    /// [`SettingsCache::auth`]; `pub(crate)` — callers go through the cache
    /// so the handle can never be built from a divergent copy of the state).
    pub(crate) fn new(inner: Arc<SettingsInner>) -> Self {
        Self {
            inner,
            kdf: KdfGuard::shared(),
        }
    }

    /// Test constructor: inject the KDF acquire guard (semaphore + timeout)
    /// so the overload/deny arm of [`Self::verify_bg`] is reachable without
    /// holding `KDF_MAX_CONCURRENT` real permits for `KDF_ACQUIRE_TIMEOUT`.
    #[cfg(test)]
    pub(crate) fn new_for_test(
        inner: Arc<SettingsInner>,
        sem: Arc<tokio::sync::Semaphore>,
        acquire_timeout: std::time::Duration,
    ) -> Self {
        Self {
            inner,
            kdf: KdfGuard {
                sem,
                acquire_timeout,
            },
        }
    }

    /// Fast path shared by [`Self::verify`] and [`Self::verify_bg`]: an empty
    /// token is rejected and a fresh positive/negative cache hit
    /// short-circuits (the check runs with the cache lock held only for the
    /// lookup — a slow hash must not serialize concurrent lookups). Only when
    /// neither hits is the stored password returned so the caller can run the
    /// KDF (inline or on the blocking pool). Keeping the rules in one place
    /// means a cache-invalidation rule added later can't drift between
    /// variants.
    fn verify_fast_path(&self, presented: &str) -> TokenVerifyFast {
        if presented.is_empty() {
            return TokenVerifyFast::Verdict(false);
        }
        if self.inner.token_is_fresh(presented) {
            return TokenVerifyFast::Verdict(true);
        }
        if self.inner.token_is_negative_fresh(presented) {
            return TokenVerifyFast::Verdict(false);
        }
        TokenVerifyFast::RunKdf(self.inner.admin_password_raw())
    }

    /// Record a KDF result in the token cache.
    fn record_verify(&self, presented: &str, ok: bool) {
        self.inner.token_record(presented, ok);
    }

    /// Verify `presented` against the stored `access.admin_password`, with a
    /// short-TTL cache (M3) so the 100k-iteration hash runs at most once per
    /// `TOKEN_CACHE_TTL` per distinct token (failed verifies are cached for
    /// `TOKEN_NEGATIVE_TTL`, so a repeated wrong token skips the KDF).
    ///
    /// The stored password is read from the **raw** cache: `redact()` only
    /// applies to `all_grouped_for` output, so the in-memory map always holds
    /// the real (possibly `sha2:`-hashed) value. Both legacy plaintext and
    /// hashed stored values are accepted via `verify_admin_password`.
    ///
    /// Invalidated by `update()` / `upgrade()` / `reload()`, so a password
    /// change is picked up immediately (not after the TTL).
    ///
    /// Synchronous: the verify runs inline on the caller's thread. Use
    /// [`Self::verify_bg`] from an async context (the request middleware) so
    /// the 100k-iteration hash doesn't block a runtime worker.
    pub fn verify(&self, presented: &str) -> bool {
        match self.verify_fast_path(presented) {
            TokenVerifyFast::Verdict(ok) => ok,
            TokenVerifyFast::RunKdf(stored) => {
                let ok = verify_admin_password(&stored, presented);
                self.record_verify(presented, ok);
                ok
            }
        }
    }

    /// Async wrapper around [`Self::verify`] that runs the
    /// 100k-iteration `verify_admin_password` on the blocking thread pool
    /// (`spawn_blocking`) so a request handler never blocks a tokio worker
    /// thread. Fast paths (empty token, fresh positive/negative cache hit)
    /// resolve without any KDF. Used by the auth middleware and
    /// `settings_authed`; `mcp` (a single verify at startup) and tests keep
    /// the synchronous [`Self::verify`].
    // LINT-3 (2026-09 sweep): propagate a worker-task panic (JoinError) —
    // grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub async fn verify_bg(&self, presented: &str) -> bool {
        match self.verify_fast_path(presented) {
            TokenVerifyFast::Verdict(ok) => ok,
            TokenVerifyFast::RunKdf(stored) => {
                // SEC (KDF DoS): bound the number of in-flight argon2id KDFs.
                // On overload, deny and record the negative result so the
                // flood re-hits the cache instead of re-queueing every request.
                match tokio::time::timeout(self.kdf.acquire_timeout, self.kdf.sem.acquire()).await {
                    Ok(_permit) => {
                        let presented_owned = presented.to_string();
                        let ok = tokio::task::spawn_blocking(move || {
                            verify_admin_password(&stored, &presented_owned)
                        })
                        .await
                        .expect("blocking pool panicked");
                        self.record_verify(presented, ok);
                        ok
                    }
                    Err(_) => {
                        self.record_verify(presented, false);
                        false
                    }
                }
            }
        }
    }

    /// Best-effort transparent upgrade: persist the freshly-hashed admin
    /// password to Postgres **and** the in-memory cache in one go. The cache
    /// update is authoritative for subsequent auth (the middleware reads the
    /// cache, not the DB), so even if the DB write fails the process no longer
    /// re-runs the 100k-iteration verify + UPDATE on every authenticated
    /// request. DB failure is logged and ignored — this is a convenience, not
    /// a correctness path.
    pub async fn upgrade(&self, hashed: String) {
        // Best-effort: a pool blip is folded into the query error (sqlx has
        // no separate pool-get step), so both failure classes take the same
        // warn path as before.
        let val: serde_json::Value = serde_json::json!(hashed);
        match raw::execute(
            self.inner.pool(),
            "UPDATE settings SET value = $1 WHERE key = $2",
            |q| q.bind(&val).bind(KEY_ACCESS_ADMIN_PASSWORD),
        )
        .await
        {
            Ok(_) => tracing::info!("upgraded access.admin_password to a salted hash"),
            Err(e) => {
                tracing::warn!("failed to persist upgraded admin password (cached anyway): {e}")
            }
        }
        // Cache the hash regardless of DB outcome so the upgrade is sticky.
        self.inner.admin_password_set(&hashed);
        // The stored password just changed — cached verifies are stale now.
        self.inner.token_invalidate_all();
        self.inner.bump_generation();
    }
}
