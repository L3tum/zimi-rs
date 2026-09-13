//! `SettingsCache`: the in-memory, Postgres-backed settings cache —
//! load/reload, env-locked write validation, redaction, the token-verify
//! fast path, and the typed accessors (via `typed_getter!`).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::{Error, Result};

use super::auth::verify_admin_password;
use super::auth::VerifiedTokenCache;
use super::defs::{
    apply_env_snapshot, def, default_settings, default_value, is_security_sensitive, known_keys,
    normalize_embedding_endpoint, redact, sync_config_values, type_mismatch,
    KEY_ACCESS_ADMIN_PASSWORD, KEY_ACCESS_MODE, KEY_ACCESS_REQUIRE_AUTH_FOR_READS,
    KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS, KEY_EMBEDDING_ENABLED, KEY_EMBEDDING_ENDPOINT,
    KEY_TORRENT_ALLOW_PRIVATE_NETWORKS, KEY_TORRENT_ENABLED, KEY_TORRENT_MAX_ACTIVE,
    KEY_TORRENT_OPDS_URL, KEY_TORRENT_URL,
};

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
/// It is kept outside `SettingsCache`'s fields because the limit must hold
/// *across the whole process* (every request handler, middleware layer, and
/// background task verifies tokens through the same cap), not
/// per-cache-instance; it is a constant (no config dependency) and
/// nothing else may read or write it.
const KDF_MAX_CONCURRENT: usize = 16;
const KDF_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
static KDF_SEM: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(KDF_MAX_CONCURRENT));

/// S7: generates a typed accessor for a plain "read" setting. Because
/// `reload()` seeds every key, `get_typed` is total — the middle `default_value`
/// fallback is unreachable, so the accessor is `get_typed` → a last-resort
/// `panic!`. That panic is the unreachable last-resort (a test pins the seed
/// set, `default_settings_covers_all_typed_getter_keys`) — it exists only so
/// the accessor is total.
macro_rules! typed_getter {
    ($name:ident, $key:expr, $ty:ty) => {
        /// Typed read accessor for this setting (total — the key is always seeded by `reload()`).
        pub fn $name(&self) -> $ty {
            // Every key is seeded by `reload()`, so `get_typed` is total; the
            // panic is the unreachable last-resort if a key ever lacks a seed.
            self.get_typed($key)
                .unwrap_or_else(|| panic!("broken settings seed for {}", $key))
        }
    };
}

/// In-memory settings cache backed by Postgres.
///
/// All reads go through the cache (fast). Writes update both cache and Postgres.
/// Env-var-locked settings are marked and cannot be changed via API.
#[derive(Clone)]
pub struct SettingsCache {
    pub(crate) inner: Arc<SettingsInner>,
}

pub(crate) struct SettingsInner {
    pub(crate) cache: RwLock<HashMap<String, serde_json::Value>>,
    env_locked: RwLock<HashMap<String, String>>, // key -> env var name
    /// Startup snapshot of the reload-time env overrides (settings-key → raw
    /// value, non-empty only). `reload()` re-applies this instead of reading
    /// the process env, so a mid-process env mutation can't change behavior.
    env_snapshot: HashMap<String, String>,
    pool: Pool,
    /// M3: short-TTL cache of verified admin tokens, shared across all
    /// `SettingsCache` clones (via the outer `Arc`).
    ///
    /// **Locking discipline** (2026-09-04, ARCH minor #4): every std lock in
    /// this struct is a `std::sync::RwLock` (`cache`, `env_locked`,
    /// `token_cache`) and is *never* held across an `.await`; the single
    /// cross-await lock is the `tokio::sync::Mutex` `write_guard` (B1). All
    /// `token_cache` sections are O(1) memory work — the KDF runs *after*
    /// the guard is dropped, so a slow hash never serializes lookups.
    token_cache: RwLock<VerifiedTokenCache>,
    /// Monotonic generation counter bumped after every cache mutation
    /// (reload/update/upgrade_password). Lets long-lived readers (the search
    /// snapshot, the poller — WI-44) detect "settings changed" without
    /// re-reading every key. `Relaxed` is enough: we observe *change* only; the
    /// RwLock is the authority per the no-concurrent-reload contract.
    generation: std::sync::atomic::AtomicU64,
    /// B1: serializes `reload()` vs `update()` write phase (enforced by
    /// `write_guard`, a `tokio::sync::Mutex`). Must be `tokio::sync::Mutex`
    /// (not `std::sync::Mutex`) because the guard is held across `.await` in
    /// `update()` (the transaction + cache-update block).
    pub(crate) write_guard: tokio::sync::Mutex<()>,
    /// Observability (ARCH Major #3): keys whose stored value failed its
    /// `json_type` check at the last `reload()` (key → human-readable reason).
    /// A value that doesn't deserialize to its expected type silently runs on
    /// its (fail-closed) default; without this record the operator gets no
    /// signal that a stored setting is being ignored. Read by
    /// [`SettingsCache::type_mismatches`] and surfaced by the authenticated `/diagnostic` route.
    type_mismatches: RwLock<std::collections::BTreeMap<String, String>>,
    /// M1: whether this process started with the multi-instance opt-out
    /// (`ZIMSERVICE_ALLOW_MULTI_INSTANCE=1`), captured at construction time —
    /// it is a process startup decision, same philosophy as `env_snapshot`
    /// (a mid-process env mutation must not change behavior). Drives the
    /// post-commit divergence warning (`warn_multi_instance_divergence`).
    multi_instance: bool,
}

impl SettingsInner {
    /// Bump the generation counter (call after a cache mutation).
    pub(crate) fn bump_generation(&self) {
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Outcome of the shared token-verify fast path (see
/// [`SettingsCache::token_verify_fast_path`]): a fresh cached verdict, or the
/// stored password to run the KDF against.
enum TokenVerifyFast {
    /// Fresh cached result (positive hit, negative hit, or empty token).
    Verdict(bool),
    /// No fresh cache entry — run the KDF against this stored password.
    RunKdf(String),
}

/// Effective private-network flag for the write-time SSRF check: a bool
/// pending in the **same** update batch wins over the cache, so an operator
/// can enable the opt-in and set the LAN endpoint in one save. Fails closed
/// on a mistyped pending value (treated as `false` — the `type_mismatch`
/// pass reports the type error separately); an absent pending value falls
/// back to the current cache value.
fn effective_private_flag(pending: Option<&serde_json::Value>, cached: bool) -> bool {
    match pending {
        Some(v) => v.as_bool().unwrap_or(false),
        None => cached,
    }
}

/// Concurrent-delete guard for [`SettingsCache::update_zim_settings`]: the
/// existence pre-check and the UPDATEs are separate statements, so a
/// concurrent ZIM delete can land between them. A 0-row result means the row
/// is gone (both UPDATEs target the same row in one transaction, so it
/// cannot be a partial update) — return the same `NotFound` the fast path
/// returns, so the handler 404s instead of reporting a fake success. An
/// empty slice (no fields in the body) is trivially ok.
fn check_zim_update_affected(affected: &[u64], zim_name: &str) -> Result<()> {
    if affected.contains(&0) {
        return Err(Error::NotFound(format!("ZIM '{zim_name}' not found")));
    }
    Ok(())
}

/// Collect the keys whose value fails its `json_type` check — the same check
/// `update()` runs at write time, applied to *stored* contents (a corrupted
/// row, a bad direct SQL edit, or a migration that wrote the wrong shape). A
/// failed key silently runs on its (fail-closed) default; this is what makes
/// that visible (ARCH Major #3). Pure + DB-free, so unit-testable. Ordered by
/// key so the warn log and the `/diagnostic` report are deterministic.
fn snapshot_type_mismatches(
    map: &HashMap<String, serde_json::Value>,
) -> std::collections::BTreeMap<String, String> {
    map.iter()
        .filter_map(|(key, value)| type_mismatch(key, value).map(|reason| (key.clone(), reason)))
        .collect()
}

/// Log one warn per type-mismatched key (the warn pass `reload()` runs).
/// Extracted so the format is asserted against the *code path* — `reload()`
/// and the test both call this — instead of the test verbatim-copying the
/// log string (which a `reload()` regression could silently diverge from).
fn warn_type_mismatches(mismatches: &std::collections::BTreeMap<String, String>) {
    for (key, reason) in mismatches {
        tracing::warn!("settings value ignored, running on the default: {reason} ({key})");
    }
}

impl SettingsCache {
    /// Load all settings from Postgres, seeding defaults for missing keys.
    ///
    /// `env_locked` marks keys that may not be changed via API; `env_snapshot`
    /// is the startup capture of the env overrides re-applied on every
    /// `reload()`.
    pub async fn load(
        pool: Pool,
        env_locked: HashMap<String, String>,
        env_snapshot: HashMap<String, String>,
    ) -> Result<Self> {
        let inner = Arc::new(SettingsInner {
            cache: RwLock::new(HashMap::new()),
            env_locked: RwLock::new(env_locked),
            env_snapshot,
            pool: pool.clone(),
            token_cache: RwLock::new(VerifiedTokenCache::default()),
            generation: std::sync::atomic::AtomicU64::new(0),
            write_guard: tokio::sync::Mutex::new(()),
            type_mismatches: RwLock::new(std::collections::BTreeMap::new()),
            // M1: the opt-out is a process startup decision — capture it now.
            multi_instance: crate::startup::multi_instance_allowed(),
        });

        let cache = Self {
            inner: inner.clone(),
        };
        cache.reload().await?;
        Ok(cache)
    }

    /// Build a cache from an in-memory map without seeding from Postgres.
    /// The pool is still used for per-key reads/writes. Intended for tests.
    #[doc(hidden)]
    pub fn new_with_map(
        pool: Pool,
        values: HashMap<String, serde_json::Value>,
        env_snapshot: HashMap<String, String>,
    ) -> Self {
        Self {
            inner: Arc::new(SettingsInner {
                cache: RwLock::new(values),
                env_locked: RwLock::new(HashMap::new()),
                env_snapshot,
                pool,
                token_cache: RwLock::new(VerifiedTokenCache::default()),
                generation: std::sync::atomic::AtomicU64::new(0),
                write_guard: tokio::sync::Mutex::new(()),
                type_mismatches: RwLock::new(std::collections::BTreeMap::new()),
                // M1: same capture as `load` — the field is the process
                // startup decision (see the struct doc).
                multi_instance: crate::startup::multi_instance_allowed(),
            }),
        }
    }

    /// Reload all settings from Postgres. Seeds defaults for any missing keys.
    ///
    /// **B1 (concurrency)**: `reload()` atomically swaps the *whole* settings
    /// map, but callers must not run it concurrently with [`Self::update`] —
    /// the two are not lock-step serialized, so a concurrent `update()` could
    /// be lost to the whole-map swap. In production `reload()` is only called
    /// pre-traffic at startup (the sole caller is [`Self::load`]), so that
    /// race is unreachable today; this note documents the requirement rather
    /// than adding a lock.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub async fn reload(&self) -> Result<()> {
        let _guard = self.inner.write_guard.lock().await;

        // Load existing settings (single table — no checkout needed; a pool
        // blip surfaces as `Error::Database(PoolTimedOut)` → 503, same as
        // every other DB read in the crate).
        let rows: Vec<(String, serde_json::Value)> =
            raw::fetch_all(&self.inner.pool, "SELECT key, value FROM settings", |q| q).await?;

        let mut map: HashMap<String, serde_json::Value> = rows.into_iter().collect();

        // Seed defaults for missing keys
        let defaults = default_settings();
        let mut seeded = Vec::new();
        for (key, value) in defaults {
            if !map.contains_key(&key) {
                map.insert(key.clone(), value.clone());
                seeded.push((key, value));
            }
        }

        // Insert seeded defaults into Postgres — ONE bulk
        // `INSERT ... ON CONFLICT DO NOTHING` in a single transaction, so a
        // crash (or a racing first start) can never leave a *partial* seed
        // and re-seeding is a no-op when the rows already exist. Missing
        // keys are inserted; existing values are never overwritten.
        if !seeded.is_empty() {
            let placeholders: Vec<String> = (0..seeded.len())
                .map(|i| format!("${}, ${}, ${}", i * 3 + 1, i * 3 + 2, i * 3 + 3))
                .collect();
            let sql = format!(
                "INSERT INTO settings (key, value, category) VALUES {} \
                 ON CONFLICT (key) DO NOTHING",
                placeholders.join(", ")
            );
            let mut tx = self.inner.pool.begin().await.map_err(Error::Database)?;
            raw::execute(&mut *tx, &sql, |mut q| {
                for (key, value) in &seeded {
                    let category = key.split('.').next().unwrap_or("general");
                    q = q.bind(key).bind(value).bind(category);
                }
                q
            })
            .await?;
            tx.commit().await.map_err(Error::Database)?;
        }

        // Environment overrides: re-apply the startup env snapshot (non-empty
        // wins). The snapshot is captured once at load time via
        // config::env_settings_snapshot, so a mid-process env mutation cannot
        // change reload behavior. Snapshot values are parsed per each key's
        // `json_type` (raw strings in the snapshot).
        apply_env_snapshot(&mut map, &self.inner.env_snapshot);

        // Observability (ARCH Major #3): a stored value that fails its
        // `json_type` check silently runs on its default — warn once per
        // reload and record the keys so the authenticated `/diagnostic` route
        // can surface them.
        let mismatches = snapshot_type_mismatches(&map);
        warn_type_mismatches(&mismatches);
        self.set_type_mismatches(mismatches);

        *self
            .inner
            .cache
            .write()
            .expect("settings cache lock poisoned") = map;
        // A reload can change the stored password (env/config) — drop any
        // cached token verifies so the new password takes effect immediately.
        self.invalidate_token_cache();
        self.inner.bump_generation();
        Ok(())
    }

    /// Refresh the `general.*` display keys from the process `Config` (the
    /// authoritative source) so the table is not a stale second copy. Called
    /// once from `build_state` right after [`Self::load`].
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn sync_from_config(&self, config: &crate::config::Config) {
        let mut cache = self
            .inner
            .cache
            .write()
            .expect("settings cache lock poisoned");
        sync_config_values(&mut cache, config);
    }

    /// Best-effort transparent upgrade: persist the freshly-hashed admin
    /// password to Postgres **and** the in-memory cache in one go. The cache
    /// update is authoritative for subsequent auth (the middleware reads the
    /// cache, not the DB), so even if the DB write fails the process no longer
    /// re-runs the 100k-iteration verify + UPDATE on every authenticated
    /// request. DB failure is logged and ignored — this is a convenience, not
    /// a correctness path.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub async fn upgrade_password(&self, hashed: String) {
        // Best-effort: a pool blip is folded into the query error (sqlx has
        // no separate pool-get step), so both failure classes take the same
        // warn path as before.
        let val: serde_json::Value = serde_json::json!(hashed);
        match raw::execute(
            &self.inner.pool,
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
        let mut cache_guard = self
            .inner
            .cache
            .write()
            .expect("settings cache lock poisoned");
        cache_guard.insert(KEY_ACCESS_ADMIN_PASSWORD.into(), serde_json::json!(hashed));
        drop(cache_guard);
        // The stored password just changed — cached verifies are stale now.
        self.invalidate_token_cache();
        self.inner.bump_generation();
    }

    /// Get a single setting value.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.inner
            .cache
            .read()
            .expect("settings cache lock poisoned")
            .get(key)
            .cloned()
    }

    /// Get a setting as a specific type.
    pub fn get_typed<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.get(key).and_then(|v| serde_json::from_value(v).ok())
    }

    /// Keys whose stored value does not match its expected JSON type, each as
    /// `"key: reason"`. These settings silently run on their defaults —
    /// surfaced by the authenticated `/diagnostic` route as `settings_mismatches` so a corrupted settings
    /// row is not invisible. The record is seeded at each `reload()` (startup)
    /// and pruned per-key as a successful [`Self::update`] re-validates a row,
    /// so it tracks the current cache, not a frozen startup view. Empty when
    /// every value deserializes.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn type_mismatches(&self) -> Vec<String> {
        let g = self
            .inner
            .type_mismatches
            .read()
            .expect("settings type-mismatch lock poisoned");
        g.iter()
            .map(|(key, reason)| format!("{key}: {reason}"))
            .collect()
    }

    /// Set the recorded type mismatches (used by `reload()` and tests).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub(crate) fn set_type_mismatches(
        &self,
        mismatches: std::collections::BTreeMap<String, String>,
    ) {
        *self
            .inner
            .type_mismatches
            .write()
            .expect("settings type-mismatch lock poisoned") = mismatches;
    }

    /// Drop the mismatch record for each key (a successful [`Self::update`]
    /// re-validates the row, so a stale startup flag must not survive the
    /// fix until a restart). No-op for keys with no record.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub(crate) fn clear_type_mismatches(&self, keys: &[&str]) {
        let mut mm = self
            .inner
            .type_mismatches
            .write()
            .expect("settings type-mismatch lock poisoned");
        for key in keys {
            mm.remove(*key);
        }
    }

    /// Fast path shared by [`Self::token_verify_cached`] and
    /// [`Self::token_verify_cached_bg`]: an empty token is rejected and a
    /// fresh positive/negative cache hit short-circuits (the check runs with
    /// the cache lock held only for the lookup — a slow hash must not
    /// serialize concurrent lookups). Only when neither hits is the stored
    /// password returned so the caller can run the KDF (inline or on the
    /// blocking pool). Keeping the rules in one place means a
    /// cache-invalidation rule added later can't drift between variants.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    fn token_verify_fast_path(&self, presented: &str) -> TokenVerifyFast {
        if presented.is_empty() {
            return TokenVerifyFast::Verdict(false);
        }
        let guard = self
            .inner
            .token_cache
            .read()
            .expect("token cache rwlock poisoned");
        if guard.is_fresh(presented) {
            return TokenVerifyFast::Verdict(true);
        }
        if guard.is_negative_fresh(presented) {
            return TokenVerifyFast::Verdict(false);
        }
        TokenVerifyFast::RunKdf(
            self.get_typed::<String>(KEY_ACCESS_ADMIN_PASSWORD)
                .unwrap_or_default(),
        )
    }

    /// Record a KDF result in the token cache and return it.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    fn record_token_verify(&self, presented: &str, ok: bool) {
        self.inner
            .token_cache
            .write()
            .expect("token cache rwlock poisoned")
            .record(presented, ok);
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
    /// Invalidated by `update()` / `upgrade_password()` / `reload()`, so a
    /// password change is picked up immediately (not after the TTL).
    ///
    /// Synchronous: the verify runs inline on the caller's thread. Use
    /// [`Self::token_verify_cached_bg`] from an async context (the request
    /// middleware) so the 100k-iteration hash doesn't block a runtime worker.
    pub fn token_verify_cached(&self, presented: &str) -> bool {
        match self.token_verify_fast_path(presented) {
            TokenVerifyFast::Verdict(ok) => ok,
            TokenVerifyFast::RunKdf(stored) => {
                let ok = verify_admin_password(&stored, presented);
                self.record_token_verify(presented, ok);
                ok
            }
        }
    }

    /// Async wrapper around [`Self::token_verify_cached`] that runs the
    /// 100k-iteration `verify_admin_password` on the blocking thread pool
    /// (`spawn_blocking`) so a request handler never blocks a tokio worker
    /// thread. Fast paths (empty token, fresh positive/negative cache hit)
    /// resolve without any KDF. Used by the auth middleware and
    /// `settings_authed`; `mcp` (a single verify at startup) and tests keep
    /// the synchronous [`Self::token_verify_cached`].
    // LINT-3 (2026-09 sweep): propagate a worker-task panic (JoinError) — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub async fn token_verify_cached_bg(&self, presented: &str) -> bool {
        match self.token_verify_fast_path(presented) {
            TokenVerifyFast::Verdict(ok) => ok,
            TokenVerifyFast::RunKdf(stored) => {
                // SEC (KDF DoS): bound the number of in-flight argon2id KDFs.
                // On overload, deny and record the negative result so the
                // flood re-hits the cache instead of re-queueing every request.
                match tokio::time::timeout(KDF_ACQUIRE_TIMEOUT, KDF_SEM.acquire()).await {
                    Ok(_permit) => {
                        let presented_owned = presented.to_string();
                        let ok = tokio::task::spawn_blocking(move || {
                            verify_admin_password(&stored, &presented_owned)
                        })
                        .await
                        .expect("blocking pool panicked");
                        self.record_token_verify(presented, ok);
                        ok
                    }
                    Err(_) => {
                        self.record_token_verify(presented, false);
                        false
                    }
                }
            }
        }
    }

    /// Drop all cached verified tokens. Called on any path that can change
    /// the effective password (see [`Self::token_verify_cached`]).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    fn invalidate_token_cache(&self) {
        self.inner
            .token_cache
            .write()
            .expect("token cache rwlock poisoned")
            .clear();
    }

    /// Get all settings grouped by category, redacting topology values for
    /// unauthenticated callers (S6).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn all_grouped_for(&self, authenticated: bool) -> serde_json::Value {
        let cache = self
            .inner
            .cache
            .read()
            .expect("settings cache lock poisoned");
        let mut grouped: HashMap<String, serde_json::Map<String, serde_json::Value>> =
            HashMap::new();
        let env_locked = self
            .inner
            .env_locked
            .read()
            .expect("env-locked settings lock poisoned");

        for (key, value) in cache.iter() {
            let category = key.split('.').next().unwrap_or("general");
            let short_key = key.split('.').nth(1).unwrap_or(key);
            let setting = def(key);
            let mut entry = serde_json::json!({ "value": redact(key, value) });
            // Topology keys (policy flag) exposed to unauthenticated callers
            // are replaced with "[redacted]" so an open GET /settings doesn't
            // leak internal URLs.
            if !authenticated
                && setting.is_some_and(|d| d.policy.topology)
                && value.as_str().is_some_and(|s| !s.is_empty())
            {
                entry["value"] = serde_json::json!("[redacted]");
            }
            if let Some(env_var) = env_locked.get(key) {
                entry["locked"] = serde_json::json!(true);
                entry["locked_by"] = serde_json::json!(env_var);
            } else if setting.is_some_and(|d| d.policy.api_immutable) {
                entry["locked"] = serde_json::json!(true);
                entry["locked_by"] = serde_json::json!("set via environment at startup");
            } else if setting.is_some_and(|d| d.policy.config_only) {
                entry["locked"] = serde_json::json!(true);
                entry["locked_by"] = serde_json::json!("environment (not runtime)");
            }
            grouped
                .entry(category.to_string())
                .or_default()
                .insert(short_key.to_string(), entry);
        }

        serde_json::to_value(&grouped).unwrap_or(serde_json::json!({}))
    }

    /// Update one or more settings. Returns errors for invalid/locked keys.
    ///
    /// Rejections: unknown keys, API-immutable keys, env-locked keys, and
    /// values whose JSON type doesn't match the key's expected type.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub async fn update(
        &self,
        updates: &HashMap<String, serde_json::Value>,
        authenticated: bool,
    ) -> Result<Vec<String>> {
        let mut errors = Vec::new();
        let mut to_update = Vec::new();

        // Determine which updates are allowed. The read guard is scoped to this block
        // so it is dropped BEFORE any await (RwLock guards are !Send).
        let known_keys = known_keys();
        {
            let env_locked = self
                .inner
                .env_locked
                .read()
                .expect("env-locked settings lock poisoned");
            for (key, value) in updates {
                if def(key).is_some_and(|d| d.policy.api_immutable) {
                    errors.push(format!(
                        "{key}: not changeable via API — set ACCESS_MODE / AUTH_PASSWORD at startup"
                    ));
                    continue;
                }
                if def(key).is_some_and(|d| d.policy.config_only) {
                    errors.push(format!(
                        "{key}: set via environment at startup, not changeable at runtime"
                    ));
                    continue;
                }
                if !known_keys.contains(key.as_str()) {
                    errors.push(format!("{key}: unknown setting"));
                    continue;
                }
                if let Some(err) = type_mismatch(key, value) {
                    errors.push(err);
                    continue;
                }
                // Open-mode (unauthenticated) writes may not touch
                // security-sensitive keys — they gate auth itself, SSRF
                // policy, or secret-bearing integrations.
                if !authenticated && is_security_sensitive(key) {
                    errors.push(format!(
                        "{key}: security-sensitive — requires authentication"
                    ));
                    continue;
                }
                // SSRF guard: validate URL-typed settings at write time,
                // honoring the same per-integration private-network opt-in
                // the runtime gates read at connect/fetch time. Fail-closed:
                // an absent or mistyped flag keeps private networks off.
                if matches!(
                    key.as_str(),
                    KEY_EMBEDDING_ENDPOINT | KEY_TORRENT_URL | KEY_TORRENT_OPDS_URL
                ) {
                    if let Some(url_str) = value.as_str() {
                        if !url_str.is_empty() {
                            // Effective flag = the value pending in this
                            // batch (if the paired flag key is also being
                            // saved) else the current cache value — a PUT
                            // can enable the opt-in and set the LAN endpoint
                            // in one save.
                            let allow_private = match key.as_str() {
                                // `connect_qbit` (SEC M-1) gates the qB
                                // endpoint on `torrent.allow_private_networks`.
                                KEY_TORRENT_URL => effective_private_flag(
                                    updates.get(KEY_TORRENT_ALLOW_PRIVATE_NETWORKS),
                                    self.get_typed(KEY_TORRENT_ALLOW_PRIVATE_NETWORKS)
                                        .unwrap_or(false),
                                ),
                                // `validate_download_url` gates the catalog on
                                // `downloads.allow_private_networks`.
                                KEY_TORRENT_OPDS_URL => effective_private_flag(
                                    updates.get(KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS),
                                    self.get_typed(KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS)
                                        .unwrap_or(false),
                                ),
                                // The embed client pins allow_private=false
                                // at runtime (src/embed/mod.rs), so write time
                                // must agree.
                                _ => false,
                            };
                            if let Err(e) = crate::netguard::assert_host_not_blocked(
                                url_str,
                                allow_private,
                                /* allow_loopback */ true,
                            ) {
                                errors.push(format!("{key}: {e}"));
                                continue;
                            }
                        }
                    }
                }
                if env_locked.contains_key(key) {
                    errors.push(format!(
                        "{key}: locked by environment variable, cannot change"
                    ));
                    continue;
                }
                // B4: normalize `embedding.endpoint` before storing — strip a
                // case-insensitive trailing `/embeddings`, then trailing `/`.
                // `EmbedClient::embed` always appends `/embeddings`, so a
                // user-supplied suffix would double it.
                let value = if key.as_str() == KEY_EMBEDDING_ENDPOINT {
                    if let Some(url_str) = value.as_str() {
                        serde_json::Value::String(normalize_embedding_endpoint(url_str))
                    } else {
                        value.clone()
                    }
                } else {
                    value.clone()
                };
                to_update.push((key.clone(), value));
            }
        }

        if !to_update.is_empty() {
            let _guard = self.inner.write_guard.lock().await;

            // 1) Persist to Postgres atomically (one transaction). No in-memory
            //    lock is held across the awaits; the cache is updated only after
            //    the commit succeeds (no partial cache/DB divergence, D1d).
            let mut tx = self.inner.pool.begin().await.map_err(Error::Database)?;
            for (key, value) in &to_update {
                let category = key.split('.').next().unwrap_or("general");
                raw::execute(
                    &mut *tx,
                    "INSERT INTO settings (key, value, category, updated_at)
                     VALUES ($1, $2, $3, now())
                     ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = now()",
                    |q| q.bind(key).bind(value).bind(category),
                )
                .await?;
            }
            tx.commit().await.map_err(Error::Database)?;

            // 2) Update in-memory cache only after the commit (synchronous, no
            //    await inside the lock).
            let mut cache = self
                .inner
                .cache
                .write()
                .expect("settings cache lock poisoned");
            for (key, value) in &to_update {
                cache.insert(key.clone(), value.clone());
            }
            drop(cache);
            // A successful write made each touched key type-valid (it passed
            // `type_mismatch` above), so drop any stale mismatch record: the
            // record is otherwise a startup `reload()` snapshot, and a row the
            // operator just fixed must not keep flagging in `/diagnostic` until a
            // restart.
            let keys: Vec<&str> = to_update.iter().map(|(k, _)| k.as_str()).collect();
            self.clear_type_mismatches(&keys);
            // Defense in depth: `access.admin_password` is API-immutable, so a
            // settings write can't change the password — but if it ever could,
            // cached token verifies must not outlive it.
            self.invalidate_token_cache();
            self.inner.bump_generation();
            // M1: re-surface the cross-instance divergence for every
            // committed write (no-op in single-instance mode).
            self.warn_multi_instance_divergence();
        }

        Ok(errors)
    }

    /// M1: re-surface the cross-instance settings divergence when this
    /// process runs with the multi-instance opt-out. The startup opt-out
    /// warning is one-shot, but a committed change stays invisible to every
    /// other instance until it restarts (there is no cross-instance
    /// invalidation path), so [`Self::update`] calls this after **every**
    /// successful commit. No-op in single-instance mode. The `/health`
    /// `multi_instance` field exposes the mode to monitors in the meantime.
    fn warn_multi_instance_divergence(&self) {
        if self.inner.multi_instance {
            tracing::warn!(
                "multi-instance mode (ZIMSERVICE_ALLOW_MULTI_INSTANCE=1): settings \
                 written — other instances keep their cached copies until \
                 restarted (no cross-instance invalidation)"
            );
        }
    }

    /// Get per-ZIM settings from the zims table.
    pub async fn get_zim_settings(&self, zim_name: &str) -> Result<Option<serde_json::Value>> {
        let row = raw::fetch_optional::<(bool, Option<String>), _, _>(
            &self.inner.pool,
            "SELECT embed_enabled, category FROM zims WHERE name = $1",
            |q| q.bind(zim_name),
        )
        .await?;

        match row {
            Some((embed_enabled, category)) => Ok(Some(serde_json::json!({
                "embed_enabled": embed_enabled,
                "category": category,
            }))),
            None => Ok(None),
        }
    }

    /// Update per-ZIM settings.
    pub async fn update_zim_settings(
        &self,
        zim_name: &str,
        updates: &serde_json::Value,
    ) -> Result<()> {
        // BUG-16b: validate the body up front, before any UPDATE, so a type
        // mismatch or an unknown key 400s without touching the DB. Valid keys
        // are `embed_enabled` (bool) and `category` (string | null).
        if let Some(obj) = updates.as_object() {
            let mut problems: Vec<String> = Vec::new();
            for (k, v) in obj {
                match k.as_str() {
                    "embed_enabled" => {
                        if v.as_bool().is_none() {
                            problems.push("'embed_enabled' must be a boolean".to_string());
                        }
                    }
                    "category" => {
                        if !(v.is_string() || v.is_null()) {
                            problems.push("'category' must be a string or null".to_string());
                        }
                    }
                    other => problems.push(format!("unknown key '{other}'")),
                }
            }
            if !problems.is_empty() {
                return Err(Error::InvalidInput(problems.join("; ")));
            }
        }

        // Verify the ZIM exists before mutating (B6: return 404 for unknown names).
        let exists: i64 = raw::fetch_scalar_optional(
            &self.inner.pool,
            "SELECT COUNT(*) FROM zims WHERE name = $1",
            |q| q.bind(zim_name),
        )
        .await?
        .unwrap_or(0);
        if exists == 0 {
            return Err(Error::NotFound(format!("ZIM '{zim_name}' not found")));
        }

        // Atomic: both UPDATEs in one transaction so a partial failure
        // (e.g. embed_enabled succeeds, category fails) can't leave the
        // ZIM row in an inconsistent state.
        let mut tx = self.inner.pool.begin().await.map_err(Error::Database)?;

        // Track the rows-affected of every UPDATE: a concurrent ZIM delete
        // between the COUNT pre-check above and the UPDATEs would otherwise
        // leave 0 rows touched while the handler still reported success.
        let mut affected: Vec<u64> = Vec::new();

        if let Some(embed_enabled) = updates.get("embed_enabled").and_then(|v| v.as_bool()) {
            affected.push(
                raw::execute(
                    &mut *tx,
                    "UPDATE zims SET embed_enabled = $2, updated_at = now() WHERE name = $1",
                    |q| q.bind(zim_name).bind(embed_enabled),
                )
                .await?,
            );
        }

        if let Some(category) = updates.get("category") {
            if let Some(c) = category.as_str() {
                affected.push(
                    raw::execute(
                        &mut *tx,
                        "UPDATE zims SET category = $2, updated_at = now() WHERE name = $1",
                        |q| q.bind(zim_name).bind(c),
                    )
                    .await?,
                );
            } else if category.is_null() {
                // Explicit null resets the override to the default (NULL).
                affected.push(
                    raw::execute(
                        &mut *tx,
                        "UPDATE zims SET category = NULL, updated_at = now() WHERE name = $1",
                        |q| q.bind(zim_name),
                    )
                    .await?,
                );
            }
        }

        // Concurrent-delete guard: 0-row UPDATEs mean the ZIM vanished after
        // the existence pre-check — NotFound (→ 404), not a fake success.
        // The transaction rolls back when the error drops it.
        check_zim_update_affected(&affected, zim_name)?;

        tx.commit().await.map_err(Error::Database)?;

        Ok(())
    }
}

impl SettingsCache {
    // Typed access to common settings.
    //
    // Each accessor falls back to the `default_settings()` seed via
    // `default_value`; the last-resort `panic!` is unreachable while every key
    // has a seed (a test pins the seed set — see
    // `default_settings_covers_all_typed_getter_keys`).
    //
    // S7: plain "read → seed → last-resort" accessors are generated by
    // `typed_getter!`; the ones with extra logic (clamps, the `Option`
    // pass-through, the documented read-auth flag) stay hand-written.

    /// The current settings generation counter (see `SettingsInner::generation`).
    /// `Relaxed` — a change-detection signal only, not a synchronization point.
    pub fn generation(&self) -> u64 {
        self.inner
            .generation
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    typed_getter!(
        downloads_allow_private_networks,
        KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS,
        bool
    );
    typed_getter!(torrent_enabled, KEY_TORRENT_ENABLED, bool);
    typed_getter!(torrent_max_active, KEY_TORRENT_MAX_ACTIVE, i32);
    typed_getter!(embedding_enabled, KEY_EMBEDDING_ENABLED, bool);
    typed_getter!(access_mode, KEY_ACCESS_MODE, String);

    /// Whether **read** (GET/HEAD/OPTIONS) requests also require auth in
    /// password mode (M1). Seed default off — but the *startup* default is
    /// bind-based (M-1): non-loopback binds inject `true` into the env
    /// snapshot unless `REQUIRE_AUTH_FOR_READS` is set explicitly, so
    /// network-facing deployments gate reads by default while loopback keeps
    /// open reads. Turning this on gates reads too — **including `/w/` raw
    /// content** — which breaks the web UI's plain-anchor article links
    /// (documented in the README threat-model section). See
    /// `auth_required` for the exact verb matrix.
    pub fn require_auth_for_reads(&self) -> bool {
        self.get_typed(KEY_ACCESS_REQUIRE_AUTH_FOR_READS)
            .or_else(|| default_value(KEY_ACCESS_REQUIRE_AUTH_FOR_READS).as_bool())
            .unwrap_or(false)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use crate::settings::auth::{TOKEN_CACHE_MAX, TOKEN_CACHE_TTL, TOKEN_NEGATIVE_TTL};
    use crate::settings::defs::{
        KEY_EMBEDDING_API_KEY, KEY_EMBEDDING_DIMENSION, KEY_EMBEDDING_TIMEOUT_SECS,
        KEY_GENERAL_CORS_ORIGINS, KEY_GENERAL_HOST, KEY_GENERAL_LOG_LEVEL, KEY_GENERAL_PORT,
        KEY_GENERAL_ZIM_DIR, KEY_SEARCH_FTS_WEIGHT, KEY_SEARCH_MAX_LIMIT, KEY_TORRENT_PASSWORD,
        KEY_TORRENT_USERNAME, SETTING_DEFS,
    };
    use crate::testing::dead_pool;

    /// A cache whose `env_locked` map is preset, for exercising the API
    /// write-path rejection logic without a live DB.
    fn cache_with_locks(
        values: HashMap<String, serde_json::Value>,
        locked: HashMap<String, String>,
    ) -> SettingsCache {
        SettingsCache {
            inner: Arc::new(SettingsInner {
                cache: RwLock::new(values),
                env_locked: RwLock::new(locked),
                env_snapshot: HashMap::new(),
                pool: dead_pool(),
                token_cache: RwLock::new(VerifiedTokenCache::default()),
                generation: std::sync::atomic::AtomicU64::new(0),
                write_guard: tokio::sync::Mutex::new(()),
                type_mismatches: RwLock::new(std::collections::BTreeMap::new()),
                multi_instance: false,
            }),
        }
    }

    #[tokio::test]
    async fn update_unauthenticated_rejects_each_sensitive_key() {
        // Each security-sensitive key must be rejected with exactly one error
        // naming it, when the caller is unauthenticated (open mode).
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        for d in SETTING_DEFS.iter().filter(|d| d.policy.security_sensitive) {
            let key = d.key;
            let mut updates = HashMap::new();
            // The row's own default is the right type for the row's
            // `json_type`, so the security check is the rejection, not a
            // type mismatch.
            let v = d.default.clone();
            updates.insert(key.to_string(), v);
            let errors = cache.update(&updates, false).await.unwrap();
            assert_eq!(
                errors.len(),
                1,
                "expected 1 error for {key}, got {errors:?}"
            );
            assert!(
                errors[0].starts_with(&format!("{key}:")),
                "error should name the key: {}",
                errors[0]
            );
            assert!(
                errors[0].contains("security-sensitive"),
                "got: {}",
                errors[0]
            );
        }
    }

    #[tokio::test]
    async fn update_authenticated_allows_sensitive_keys() {
        // Authenticated caller: sensitive keys pass the security check and
        // reach the (dead) pool, so update surfaces a Pool error rather than
        // per-key security rejections.
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://127.0.0.1:9").clone(),
        );
        // A dead pool means the commit fails; assert we got a pool error (i.e.
        // the key was *allowed* past the security gate into to_update).
        let res = cache.update(&updates, true).await;
        assert!(
            matches!(
                res,
                Err(Error::Database(ref e))
                    if matches!(e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed)
            ),
            "authenticated sensitive write should reach the DB, got {res:?}"
        );
    }

    #[test]
    fn reload_env_snapshot_override() {
        // Snapshot value overrides a seeded cache value (non-empty wins), and
        // an empty snapshot value does NOT override. This exercises the same
        // application path `reload()` uses, without a live DB.
        let mut seeded = HashMap::new();
        seeded.insert(KEY_ACCESS_MODE.into(), serde_json::json!("open"));
        seeded.insert(
            KEY_ACCESS_ADMIN_PASSWORD.into(),
            serde_json::json!("stored"),
        );

        let mut snap = HashMap::new();
        snap.insert(KEY_ACCESS_MODE.into(), "password".to_string());
        snap.insert(KEY_ACCESS_ADMIN_PASSWORD.into(), String::new()); // empty → no override

        apply_env_snapshot(&mut seeded, &snap);
        assert_eq!(
            seeded.get(KEY_ACCESS_MODE),
            Some(&serde_json::json!("password"))
        );
        // Empty snapshot value left the stored password intact.
        assert_eq!(
            seeded.get(KEY_ACCESS_ADMIN_PASSWORD),
            Some(&serde_json::json!("stored"))
        );

        // embedding.dimension is parsed from the raw string.
        let mut dim = HashMap::new();
        dim.insert(KEY_EMBEDDING_DIMENSION.into(), serde_json::json!(768));
        let mut dsnap = HashMap::new();
        dsnap.insert(KEY_EMBEDDING_DIMENSION.into(), "1024".to_string());
        apply_env_snapshot(&mut dim, &dsnap);
        assert_eq!(
            dim.get(KEY_EMBEDDING_DIMENSION),
            Some(&serde_json::json!(1024))
        );

        // Bool-typed keys parse to a real boolean (the fail-open regression:
        // the raw string "true" used to be stored, which
        // `get_typed::<bool>` cannot deserialize → seed default `false`).
        let mut bmap = default_settings();
        let mut bsnap = HashMap::new();
        bsnap.insert(KEY_ACCESS_REQUIRE_AUTH_FOR_READS.into(), "true".to_string());
        apply_env_snapshot(&mut bmap, &bsnap);
        assert_eq!(
            bmap.get(KEY_ACCESS_REQUIRE_AUTH_FOR_READS),
            Some(&serde_json::json!(true)),
            "bool snapshot value must land as Value::Bool, not Value::String"
        );
        // Unparseable bool values are dropped (previous value stands).
        let mut bsnap2 = HashMap::new();
        bsnap2.insert(
            KEY_ACCESS_REQUIRE_AUTH_FOR_READS.into(),
            "maybe".to_string(),
        );
        apply_env_snapshot(&mut bmap, &bsnap2);
        assert_eq!(
            bmap.get(KEY_ACCESS_REQUIRE_AUTH_FOR_READS),
            Some(&serde_json::json!(true)),
            "unparseable bool must not clobber a good value"
        );
    }

    #[test]
    fn m1_snapshot_path_gates_reads_on_non_loopback() {
        // Fail-open regression (BUGS #1): the default non-loopback
        // deployment's read-gating rides the M-1 snapshot path —
        // `apply_require_reads_default` injects the *raw string* "true",
        // then `reload()`'s `apply_env_snapshot` must parse it to a real
        // boolean so `require_auth_for_reads()` (a `get_typed::<bool>`)
        // sees the gated default. Pre-fix, the map held
        // `Value::String("true")`, deserialization failed, and reads fell
        // back to the seed default `false` (open) on every network-facing
        // bind — and an explicit `REQUIRE_AUTH_FOR_READS=true` was a no-op
        // too.
        let cfg = crate::config::Config {
            host: "0.0.0.0".into(),
            ..Default::default()
        };
        let mut snap = HashMap::new();
        cfg.apply_require_reads_default(&mut snap);
        assert_eq!(
            snap.get(KEY_ACCESS_REQUIRE_AUTH_FOR_READS),
            Some(&"true".to_string()),
            "M-1 injects the raw string on non-loopback binds"
        );

        let mut map = default_settings();
        apply_env_snapshot(&mut map, &snap);
        let cache = SettingsCache::new_with_map(crate::testing::dead_pool(), map, HashMap::new());
        assert!(
            cache.require_auth_for_reads(),
            "non-loopback bind must gate reads by default (M-1)"
        );
    }

    #[test]
    fn sync_config_values_overwrites_general_keys() {
        // ARCH-1: the four Config-backed display keys are overwritten with
        // typed JSON; general.cors_origins (settings-owned) stays untouched.
        let cfg = crate::config::Config {
            zim_dir: std::path::PathBuf::from("/data"),
            host: "0.0.0.0".into(),
            port: 9999,
            log_level: "debug".into(),
            ..Default::default()
        };
        let mut map = default_settings();
        sync_config_values(&mut map, &cfg);
        assert_eq!(map[KEY_GENERAL_ZIM_DIR], serde_json::json!("/data"));
        assert_eq!(map[KEY_GENERAL_HOST], serde_json::json!("0.0.0.0"));
        assert_eq!(map[KEY_GENERAL_PORT], serde_json::json!(9999));
        assert!(
            map[KEY_GENERAL_PORT].is_i64(),
            "port must be a number, not \"9999\""
        );
        assert_eq!(map[KEY_GENERAL_LOG_LEVEL], serde_json::json!("debug"));
        assert_eq!(map[KEY_GENERAL_CORS_ORIGINS], serde_json::json!(""));
    }

    #[test]
    fn sync_from_config_refreshes_display_keys() {
        let cfg = crate::config::Config {
            zim_dir: std::path::PathBuf::from("/data"),
            host: "0.0.0.0".into(),
            port: 9999,
            log_level: "debug".into(),
            ..Default::default()
        };
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        cache.sync_from_config(&cfg);
        assert_eq!(
            cache.get(KEY_GENERAL_ZIM_DIR),
            Some(serde_json::json!("/data"))
        );
        assert_eq!(cache.get_typed::<i64>(KEY_GENERAL_PORT), Some(9999));
    }

    #[tokio::test]
    async fn settings_update_three_rejected_keys_yield_three_errors() {
        // Invariant (B4.13): update pushes exactly one error per rejected key
        // and continues, so updates.len() - errors.len() is the applied count.
        // Three distinct rejection classes, all named, applied count 0.
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let mut updates = HashMap::new();
        updates.insert(KEY_ACCESS_MODE.into(), serde_json::json!("open")); // API-immutable
        updates.insert(KEY_GENERAL_HOST.into(), serde_json::json!("1.2.3.4")); // config-only
        updates.insert("bogus.key".into(), serde_json::json!(1)); // unknown
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(errors.len(), 3, "expected 3 errors, got {errors:?}");
        assert!(errors.iter().any(|e| e.starts_with("access.mode:")));
        assert!(errors.iter().any(|e| e.starts_with("general.host:")));
        assert!(errors.iter().any(|e| e.starts_with("bogus.key:")));
        // Applied count = 3 - 3 = 0.
        let applied = updates.len().saturating_sub(errors.len());
        assert_eq!(applied, 0);
    }

    // ── redaction ────────────────────────────────────────────────────────

    #[test]
    fn redact_hides_set_secrets_only() {
        assert_eq!(
            redact(KEY_ACCESS_ADMIN_PASSWORD, &serde_json::json!("s3cret")),
            "***"
        );
        assert_eq!(redact(KEY_TORRENT_PASSWORD, &serde_json::json!("x")), "***");
        assert_eq!(
            redact(KEY_EMBEDDING_API_KEY, &serde_json::json!("sk-1")),
            "***"
        );
        // unset (empty) values pass through untouched
        assert_eq!(
            redact(KEY_ACCESS_ADMIN_PASSWORD, &serde_json::json!("")),
            ""
        );
        // non-secret keys always pass through
        assert_eq!(
            redact(KEY_TORRENT_URL, &serde_json::json!("http://x")),
            "http://x"
        );
        assert_eq!(
            redact(KEY_TORRENT_PASSWORD, &serde_json::json!(5)),
            5,
            "non-string values are not secrets"
        );
    }

    #[test]
    fn all_grouped_redacts_secrets() {
        let mut values = default_settings();
        values.insert(
            KEY_ACCESS_ADMIN_PASSWORD.into(),
            serde_json::json!("s3cret"),
        );
        values.insert(KEY_TORRENT_PASSWORD.into(), serde_json::json!("adminadmin"));
        values.insert(KEY_EMBEDDING_API_KEY.into(), serde_json::json!("sk-123"));
        let cache = SettingsCache::new_with_map(dead_pool(), values, HashMap::new());
        let g = cache.all_grouped_for(true);
        assert_eq!(g["access"]["admin_password"]["value"], "***");
        assert_eq!(g["torrent"]["password"]["value"], "***");
        assert_eq!(g["embedding"]["api_key"]["value"], "***");
        let dumped = g.to_string();
        assert!(!dumped.contains("s3cret"));
        assert!(!dumped.contains("adminadmin"));
        assert!(!dumped.contains("sk-123"));
    }

    #[test]
    fn all_grouped_leaves_unset_secrets_empty() {
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let g = cache.all_grouped_for(true);
        assert_eq!(g["access"]["admin_password"]["value"], "");
        assert_eq!(g["embedding"]["api_key"]["value"], "");
    }

    #[test]
    fn all_grouped_marks_api_immutable_keys_locked() {
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let g = cache.all_grouped_for(true);
        assert_eq!(g["access"]["mode"]["locked"], true);
        assert!(g["access"]["mode"]["locked_by"].is_string());
        assert_eq!(g["access"]["admin_password"]["locked"], true);
        assert!(g["search"]["max_limit"].get("locked").is_none());
        assert!(g["access"]["rate_limit_rps"].get("locked").is_none());
    }

    // ── topology redaction (S6) ─────────────────────────────────────────

    #[test]
    fn all_grouped_unauthenticated_redacts_topology() {
        let mut values = default_settings();
        values.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://10.0.0.5:8080"),
        );
        values.insert(KEY_TORRENT_USERNAME.into(), serde_json::json!("qbit-admin"));
        values.insert(
            KEY_EMBEDDING_ENDPOINT.into(),
            serde_json::json!("http://10.0.0.6:8000/embed"),
        );
        values.insert(
            KEY_GENERAL_ZIM_DIR.into(),
            serde_json::json!("/srv/private/zims"),
        );
        let cache = SettingsCache::new_with_map(dead_pool(), values, HashMap::new());

        // Unauthenticated: topology values are replaced, everything else kept.
        let g = cache.all_grouped_for(false);
        assert_eq!(g["torrent"]["url"]["value"], "[redacted]");
        assert_eq!(g["torrent"]["username"]["value"], "[redacted]");
        assert_eq!(g["embedding"]["endpoint"]["value"], "[redacted]");
        assert_eq!(g["general"]["zim_dir"]["value"], "[redacted]");
        let dumped = g.to_string();
        assert!(!dumped.contains("10.0.0.5"));
        assert!(!dumped.contains("10.0.0.6"));
        assert!(!dumped.contains("qbit-admin"));
        assert!(!dumped.contains("/srv/private/zims"));

        // Authenticated: full values.
        let g2 = cache.all_grouped_for(true);
        assert_eq!(g2["torrent"]["url"]["value"], "http://10.0.0.5:8080");
        assert_eq!(g2["torrent"]["username"]["value"], "qbit-admin");
        assert_eq!(
            g2["embedding"]["endpoint"]["value"],
            "http://10.0.0.6:8000/embed"
        );
        assert_eq!(g2["general"]["zim_dir"]["value"], "/srv/private/zims");
    }

    #[test]
    fn all_grouped_unauthenticated_empty_topology_not_redacted() {
        // Empty (unset) topology values carry no information; leave them
        // empty rather than a misleading "[redacted]". (embedding.endpoint
        // has a non-empty Ollama default, so it IS redacted.)
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let g = cache.all_grouped_for(false);
        assert_eq!(g["torrent"]["url"]["value"], "");
        assert_eq!(g["torrent"]["username"]["value"], "");
        assert_eq!(g["embedding"]["endpoint"]["value"], "[redacted]");

        // When the endpoint is explicitly cleared, it stays empty.
        let mut values = default_settings();
        values.insert(KEY_EMBEDDING_ENDPOINT.into(), serde_json::json!(""));
        let cache2 = SettingsCache::new_with_map(dead_pool(), values, HashMap::new());
        let g2 = cache2.all_grouped_for(false);
        assert_eq!(g2["embedding"]["endpoint"]["value"], "");
    }

    #[test]
    fn all_grouped_default_is_authenticated() {
        // `all_grouped_for(true)` (the authenticated view) must not redact topology.
        let mut values = default_settings();
        values.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://10.0.0.5:8080"),
        );
        let cache = SettingsCache::new_with_map(dead_pool(), values, HashMap::new());
        let g = cache.all_grouped_for(true);
        assert_eq!(g["torrent"]["url"]["value"], "http://10.0.0.5:8080");
    }

    // ── type validation ──────────────────────────────────────────────────

    #[test]
    fn type_mismatch_detection() {
        assert!(type_mismatch(KEY_SEARCH_MAX_LIMIT, &serde_json::json!(50)).is_none());
        assert!(type_mismatch(KEY_SEARCH_MAX_LIMIT, &serde_json::json!("a lot")).is_some());
        assert!(type_mismatch(KEY_TORRENT_ENABLED, &serde_json::json!("yes")).is_some());
        assert!(type_mismatch(KEY_TORRENT_ENABLED, &serde_json::json!(true)).is_none());
        assert!(type_mismatch(KEY_SEARCH_FTS_WEIGHT, &serde_json::json!(0.6)).is_none());
        assert!(type_mismatch(KEY_GENERAL_PORT, &serde_json::json!(8899)).is_none());
        assert!(type_mismatch(KEY_GENERAL_PORT, &serde_json::json!(8899.5)).is_some());
        assert!(def("nope.nope").is_none());
        assert!(type_mismatch("nope.nope", &serde_json::json!(1)).is_none());

        // WI-55 delta (d): the two keys the old hand-rolled `expected_type()`
        // silently omitted are now type-checked by the table.
        assert!(type_mismatch(KEY_EMBEDDING_TIMEOUT_SECS, &serde_json::json!("sixty")).is_some());
        assert!(type_mismatch(KEY_EMBEDDING_TIMEOUT_SECS, &serde_json::json!(60)).is_none());
        assert!(type_mismatch(KEY_ACCESS_ADMIN_PASSWORD, &serde_json::json!(42)).is_some());
        assert!(type_mismatch(KEY_ACCESS_ADMIN_PASSWORD, &serde_json::json!("x")).is_none());
    }

    #[test]
    fn type_mismatch_torrent_max_active_int() {
        // N1: `torrent.max_active` is Int — a stored float is rejected at
        // write time instead of silently falling back to the seed default
        // on read (the accessor reads i32).
        assert!(type_mismatch(KEY_TORRENT_MAX_ACTIVE, &serde_json::json!(4.5)).is_some());
        assert!(type_mismatch(KEY_TORRENT_MAX_ACTIVE, &serde_json::json!(8)).is_none());
    }

    #[tokio::test]
    async fn wrong_type_embedding_timeout_secs_rejected() {
        // Delta (d): `embedding.timeout_secs` is now write-time type-checked
        // (Int). The type check fires before the security check, so both the
        // unauthenticated and the authenticated path hit the same error.
        let mut updates = HashMap::new();
        updates.insert(
            KEY_EMBEDDING_TIMEOUT_SECS.into(),
            serde_json::json!("sixty"),
        );
        for authenticated in [false, true] {
            let cache =
                SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
            let errors = cache.update(&updates, authenticated).await.unwrap();
            assert_eq!(errors.len(), 1, "authenticated={authenticated}: {errors:?}");
            assert!(
                errors[0].starts_with("embedding.timeout_secs: expected an integer"),
                "authenticated={authenticated}: {}",
                errors[0]
            );
        }
    }

    #[tokio::test]
    async fn wrong_type_admin_password_rejected() {
        // Delta (d): `access.admin_password` now has a real expected type
        // (Str) in the table. The `api_immutable` check still fires first in
        // `update()` (check order pinned), so the update error is the
        // api-immutable one; the type path itself is pinned by the direct
        // `type_mismatch` assertions below (no pre-WI-55 test exercised this
        // key's type check — it was a table omission).
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let mut updates = HashMap::new();
        updates.insert(KEY_ACCESS_ADMIN_PASSWORD.into(), serde_json::json!(42));
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(errors.len(), 1, "got {errors:?}");
        assert!(
            errors[0].starts_with("access.admin_password:"),
            "{}",
            errors[0]
        );
        assert!(
            errors[0].contains("not changeable via API"),
            "api-immutable check must precede the type check: {}",
            errors[0]
        );
        assert!(type_mismatch(KEY_ACCESS_ADMIN_PASSWORD, &serde_json::json!(42)).is_some());
        assert!(type_mismatch(KEY_ACCESS_ADMIN_PASSWORD, &serde_json::json!("x")).is_none());
    }

    // ── update() rejections ────────────────────────────────────────────────

    #[tokio::test]
    async fn update_rejects_unknown_immutable_and_bad_types() {
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let mut updates = HashMap::new();
        updates.insert("no.such_key".into(), serde_json::json!("x"));
        updates.insert(KEY_ACCESS_MODE.into(), serde_json::json!("open"));
        updates.insert(KEY_ACCESS_ADMIN_PASSWORD.into(), serde_json::json!("x"));
        updates.insert(KEY_SEARCH_MAX_LIMIT.into(), serde_json::json!("a lot"));
        updates.insert(KEY_TORRENT_ENABLED.into(), serde_json::json!("yes"));
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            5,
            "each bad key gets its own error: {errors:?}"
        );
        assert!(errors.iter().any(|e| e.contains("unknown setting")));
        assert!(errors.iter().any(|e| e.contains("not changeable via API")));
        assert!(errors
            .iter()
            .any(|e| e.starts_with("search.max_limit: expected an integer")));
        assert!(errors
            .iter()
            .any(|e| e.starts_with("torrent.enabled: expected a boolean")));
        // everything was rejected → nothing changed
        assert_eq!(cache.get(KEY_SEARCH_MAX_LIMIT).unwrap(), 50);
        assert_eq!(cache.get(KEY_TORRENT_ENABLED).unwrap(), true);
    }

    #[tokio::test]
    async fn update_rejects_env_locked_keys() {
        let mut locked = HashMap::new();
        locked.insert(KEY_TORRENT_URL.to_string(), "QBITTORRENT_URL".to_string());
        let cache = cache_with_locks(default_settings(), locked);
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://elsewhere"),
        );
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("locked by environment variable"));
        assert_eq!(cache.get(KEY_TORRENT_URL).unwrap(), "");
    }

    #[tokio::test]
    async fn update_rejects_config_only_keys() {
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let mut updates = HashMap::new();
        // Config-file / env-only keys (no runtime effect) must be rejected, so
        // nothing reaches the DB (to_update stays empty).
        updates.insert(KEY_GENERAL_PORT.into(), serde_json::json!(9999));
        updates.insert(KEY_GENERAL_LOG_LEVEL.into(), serde_json::json!("debug"));
        updates.insert(KEY_GENERAL_ZIM_DIR.into(), serde_json::json!("/elsewhere"));
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            3,
            "all config-only keys are rejected: {errors:?}"
        );
        assert!(errors.iter().any(|e| e.starts_with("general.port:")));
        assert!(errors.iter().any(|e| e.starts_with("general.log_level:")));
        assert!(errors.iter().any(|e| e.starts_with("general.zim_dir:")));
        // config-only keys unchanged.
        assert_eq!(cache.get(KEY_GENERAL_PORT).unwrap(), 8899);
        assert_eq!(cache.get(KEY_GENERAL_LOG_LEVEL).unwrap(), "info");
        assert_eq!(cache.get(KEY_GENERAL_ZIM_DIR).unwrap(), "/zims");
    }

    // ── B3: `torrent.opds_url` SSRF validation + B4: endpoint normalization ──

    #[tokio::test]
    async fn update_torrent_opds_url_rejects_metadata_ip() {
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_OPDS_URL.into(),
            serde_json::json!("http://169.254.169.254/opds"),
        );
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            1,
            "cloud-metadata IP must be rejected: {errors:?}"
        );
        assert!(errors[0].starts_with("torrent.opds_url:"), "{errors:?}");
    }

    #[tokio::test]
    async fn update_torrent_opds_url_public_proceeds_to_pool() {
        // A valid public URL passes validation → the write reaches the (dead)
        // pool, so `update` returns `Err(Error::Database(PoolTimedOut/PoolClosed))`,
        // not a validation error.
        let cache = SettingsCache::new_with_map(dead_pool(), default_settings(), HashMap::new());
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_OPDS_URL.into(),
            serde_json::json!("https://example.com/opds"),
        );
        assert!(matches!(
            cache.update(&updates, true).await,
            Err(Error::Database(e))
                if matches!(e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed)
        ));
    }

    // ── M1: write-time SSRF honors the live per-integration flags ────────

    /// `default_settings()` with the two private-network opt-ins set, so the
    /// write-path SSRF check sees the live flags. Dead pool: an accepted
    /// write surfaces as `Err(Error::Database)`, a rejected one as errors.
    fn cache_with_private_flags(qbit: bool, downloads: bool) -> SettingsCache {
        let mut values = default_settings();
        values.insert(
            KEY_TORRENT_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!(qbit),
        );
        values.insert(
            KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!(downloads),
        );
        SettingsCache::new_with_map(dead_pool(), values, HashMap::new())
    }

    #[tokio::test]
    async fn update_torrent_url_honors_qbit_private_flag() {
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://10.0.0.5:8080"),
        );
        // Flag off (the default): private-IP qB endpoint rejected.
        let cache = cache_with_private_flags(false, false);
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            1,
            "private qB endpoint rejected by default: {errors:?}"
        );
        assert!(errors[0].starts_with("torrent.url:"));
        // Flag on: passes validation → the write reaches the (dead) pool.
        let cache = cache_with_private_flags(true, false);
        assert!(matches!(
            cache.update(&updates, true).await,
            Err(Error::Database(e))
                if matches!(e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed)
        ));
        // Cross-check: the downloads flag must not gate the qB endpoint.
        let cache = cache_with_private_flags(false, true);
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            1,
            "downloads flag must not open the qB endpoint: {errors:?}"
        );
    }

    #[tokio::test]
    async fn update_torrent_url_batch_pending_flag_opens_private_endpoint() {
        // Regression: the flag must be read *pending-in-batch*, not from the
        // stale pre-batch cache — an operator saving both in one PUT can set
        // up a LAN endpoint without a second save.
        //
        // The paired flag key is env-locked so its write is rejected: on the
        // dead pool that makes the URL key's fate observable — an accepted
        // URL is the only to_update entry and reaches the pool
        // (`Err(Database)`); a stale-cache regression would reject the URL
        // too, leaving nothing to write (`Ok(errors)`).
        let mut locked = HashMap::new();
        locked.insert(
            KEY_TORRENT_ALLOW_PRIVATE_NETWORKS.to_string(),
            "QBITTORRENT_ALLOW_PRIVATE_NETWORKS".to_string(),
        );
        let cache = cache_with_locks(default_settings(), locked);
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!(true),
        );
        updates.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://10.0.0.5:8080"),
        );
        assert!(
            matches!(
                cache.update(&updates, true).await,
                Err(Error::Database(e))
                    if matches!(e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed)
            ),
            "pending flag=true must open the private qB endpoint"
        );

        // Same for the catalog pair.
        let mut locked = HashMap::new();
        locked.insert(
            KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS.to_string(),
            "DOWNLOADS_ALLOW_PRIVATE_NETWORKS".to_string(),
        );
        let cache = cache_with_locks(default_settings(), locked);
        let mut updates = HashMap::new();
        updates.insert(
            KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!(true),
        );
        updates.insert(
            KEY_TORRENT_OPDS_URL.into(),
            serde_json::json!("http://10.0.0.5:8080/opds"),
        );
        assert!(
            matches!(
                cache.update(&updates, true).await,
                Err(Error::Database(e))
                    if matches!(e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed)
            ),
            "pending flag=true must open the private catalog endpoint"
        );
    }

    #[tokio::test]
    async fn update_torrent_url_batch_flag_off_rejects_despite_cached_true() {
        // The pending value wins in both directions: batch {flag: false,
        // url: private-IP} with the flag currently true in the cache must be
        // rejected against the effective flag (false). The flag key is
        // env-locked, so exactly two errors (flag locked + URL SSRF) and an
        // empty to_update prove the URL was rejected — a stale-cache
        // regression would accept the URL and surface `Err(Database)` from
        // the (dead) pool instead of `Ok(errors)`.
        let mut values = default_settings();
        values.insert(
            KEY_TORRENT_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!(true),
        );
        let mut locked = HashMap::new();
        locked.insert(
            KEY_TORRENT_ALLOW_PRIVATE_NETWORKS.to_string(),
            "QBITTORRENT_ALLOW_PRIVATE_NETWORKS".to_string(),
        );
        let cache = cache_with_locks(values, locked);
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!(false),
        );
        updates.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://10.0.0.5:8080"),
        );
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            2,
            "flag locked + url rejected, nothing written: {errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.starts_with("torrent.allow_private_networks:")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|e| e.starts_with("torrent.url:")),
            "{errors:?}"
        );
    }

    #[tokio::test]
    async fn update_torrent_url_batch_mistyped_flag_fails_closed() {
        // A non-bool pending flag is treated as false for the SSRF check
        // (fail-closed); the `type_mismatch` pass reports the type error for
        // the flag key separately — one error per key, both named.
        let cache = cache_with_private_flags(true, false);
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!("yes"),
        );
        updates.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://10.0.0.5:8080"),
        );
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(errors.len(), 2, "both keys get their own error: {errors:?}");
        assert!(
            errors
                .iter()
                .any(|e| e.starts_with("torrent.allow_private_networks:")),
            "flag key gets the type error: {errors:?}"
        );
        assert!(
            errors.iter().any(|e| e.starts_with("torrent.url:")),
            "url rejected against the fail-closed flag: {errors:?}"
        );
    }

    #[test]
    fn effective_private_flag_batch_value_wins() {
        // A bool pending in the same batch wins over the cache, in both
        // directions; a non-bool pending value fails closed regardless of
        // the cache; an absent pending value uses the cached value.
        assert!(effective_private_flag(
            Some(&serde_json::json!(true)),
            false
        ));
        assert!(!effective_private_flag(
            Some(&serde_json::json!(false)),
            true
        ));
        assert!(!effective_private_flag(
            Some(&serde_json::json!("yes")),
            true
        ));
        assert!(!effective_private_flag(Some(&serde_json::json!(1)), true));
        assert!(effective_private_flag(None, true));
        assert!(!effective_private_flag(None, false));
    }

    #[test]
    fn check_zim_update_affected_zero_rows_is_not_found() {
        // A 0-row UPDATE (ZIM deleted between the existence pre-check and
        // the UPDATE) yields the same NotFound the COUNT fast path returns.
        let err = check_zim_update_affected(&[0], "gone").unwrap_err();
        assert!(
            matches!(err, Error::NotFound(ref m) if m == "ZIM 'gone' not found"),
            "got: {err:?}"
        );
        // A present row is reported as 1 affected row even when the new
        // value equals the old one, so any non-zero count passes.
        assert!(check_zim_update_affected(&[1], "x").is_ok());
        assert!(check_zim_update_affected(&[1, 1], "x").is_ok());
        // An empty body runs no UPDATEs — nothing to check.
        assert!(check_zim_update_affected(&[], "x").is_ok());
    }

    #[tokio::test]
    async fn update_torrent_opds_url_honors_downloads_private_flag() {
        let mut updates = HashMap::new();
        updates.insert(
            KEY_TORRENT_OPDS_URL.into(),
            serde_json::json!("http://10.0.0.5:8080/opds"),
        );
        // Flag off (the default): private-IP catalog rejected.
        let cache = cache_with_private_flags(false, false);
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            1,
            "private catalog rejected by default: {errors:?}"
        );
        assert!(errors[0].starts_with("torrent.opds_url:"));
        // Flag on: passes validation → the write reaches the (dead) pool.
        let cache = cache_with_private_flags(false, true);
        assert!(matches!(
            cache.update(&updates, true).await,
            Err(Error::Database(e))
                if matches!(e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed)
        ));
        // Cross-check: the qB flag must not gate the catalog.
        let cache = cache_with_private_flags(true, false);
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            1,
            "qbit flag must not open the catalog: {errors:?}"
        );
    }

    #[tokio::test]
    async fn update_embedding_endpoint_private_rejected_even_with_flags() {
        // The embed client pins allow_private=false at runtime, so a private
        // endpoint is rejected even with both opt-ins on.
        let cache = cache_with_private_flags(true, true);
        let mut updates = HashMap::new();
        updates.insert(
            KEY_EMBEDDING_ENDPOINT.into(),
            serde_json::json!("http://10.0.0.5:8000"),
        );
        let errors = cache.update(&updates, true).await.unwrap();
        assert_eq!(
            errors.len(),
            1,
            "private endpoint rejected regardless of flags: {errors:?}"
        );
        assert!(errors[0].starts_with("embedding.endpoint:"));
    }

    #[test]
    fn update_embedding_endpoint_strips_embeddings_suffix() {
        assert_eq!(
            normalize_embedding_endpoint("http://h/v1/embeddings/"),
            "http://h/v1"
        );
        assert_eq!(
            normalize_embedding_endpoint("http://h/v1/embeddings"),
            "http://h/v1"
        );
        assert_eq!(
            normalize_embedding_endpoint("http://h/v1/EMBEDDINGS/"),
            "http://h/v1"
        );
        assert_eq!(normalize_embedding_endpoint("http://h/v1"), "http://h/v1");
        assert_eq!(normalize_embedding_endpoint("http://h/v1/"), "http://h/v1");
        assert_eq!(
            normalize_embedding_endpoint("http://h/embeddings"),
            "http://h"
        );
    }

    fn snapshot_settings(map: HashMap<String, serde_json::Value>) -> SettingsCache {
        SettingsCache::new_with_map(dead_pool(), map, HashMap::new())
    }

    #[test]
    fn bump_generation_increments_counter() {
        let settings = snapshot_settings(default_settings());
        let before = settings.generation();
        settings.inner.bump_generation();
        assert_eq!(settings.generation(), before + 1);
    }

    // ── M3: token verification cache ─────────────────────────────────

    /// A cache with a **legacy plaintext** password (fast verify — the hashed
    /// path is 100k iters and would slow the suite).
    fn cache_with_password(pw: &str) -> SettingsCache {
        let mut values = default_settings();
        values.insert(KEY_ACCESS_ADMIN_PASSWORD.into(), serde_json::json!(pw));
        SettingsCache::new_with_map(dead_pool(), values, HashMap::new())
    }

    #[test]
    fn token_verify_cached_hit_second_call() {
        let cache = cache_with_password("s3cret");
        assert!(cache.token_verify_cached("s3cret"));
        // Second call: cache hit → true, and exactly one entry recorded.
        assert!(cache.token_verify_cached("s3cret"));
        let guard = cache
            .inner
            .token_cache
            .read()
            .expect("token cache rwlock poisoned");
        assert_eq!(
            guard.len(),
            1,
            "a successful verify records exactly one entry"
        );
    }

    /// The async `_bg` variant and the sync variant must agree on every
    /// fast-path verdict (empty token, positive hit, negative hit) for the
    /// same inputs — they share `token_verify_fast_path`, so any drift in
    /// the rules fails this test.
    #[tokio::test]
    async fn token_verify_bg_matches_sync_on_fast_paths() {
        let cache = cache_with_password("s3cret");
        // Empty token: both reject without touching the cache.
        assert!(!cache.token_verify_cached(""));
        assert!(!cache.token_verify_cached_bg("").await);
        // Sync verify first (records a positive + a negative), then the bg
        // variant must serve the identical verdicts from the same cache.
        assert!(cache.token_verify_cached("s3cret"));
        assert!(!cache.token_verify_cached("wrong"));
        assert!(cache.token_verify_cached_bg("s3cret").await);
        assert!(!cache.token_verify_cached_bg("wrong").await);
        // Reverse direction: bg records, sync serves.
        assert!(cache.token_verify_cached_bg("s3cret").await);
        assert!(cache.token_verify_cached("s3cret"));
        let guard = cache
            .inner
            .token_cache
            .read()
            .expect("token cache rwlock poisoned");
        assert_eq!(
            guard.entries.len(),
            1,
            "one positive entry regardless of entry point"
        );
        assert!(guard.negatives.contains_key("wrong"));
    }

    #[test]
    fn token_verify_cached_wrong_token_negative_cached() {
        let cache = cache_with_password("s3cret");
        assert!(!cache.token_verify_cached("wrong"));
        let guard = cache
            .inner
            .token_cache
            .read()
            .expect("token cache rwlock poisoned");
        // A failed verify is now recorded in the short-TTL negative map (so a
        // repeat skips the KDF) — but it must never be a positive entry.
        assert!(
            !guard.entries.contains_key("wrong"),
            "a failed verify must not be a positive entry"
        );
        assert!(
            guard.negatives.contains_key("wrong"),
            "a failed verify is negative-cached"
        );
        drop(guard);
        // Repeat: still false, served from the negative cache (no second KDF).
        assert!(!cache.token_verify_cached("wrong"));
        assert!(!cache.token_verify_cached(""), "empty token never verifies");
    }

    #[test]
    fn token_verify_negative_expiry() {
        let cache = cache_with_password("s3cret");
        // Seed an *expired* negative entry: past the short TTL a repeat must
        // re-verify rather than trust the cached miss.
        {
            let mut guard = cache
                .inner
                .token_cache
                .write()
                .expect("token cache rwlock poisoned");
            guard.negatives.insert(
                "wrong".into(),
                Instant::now() - TOKEN_NEGATIVE_TTL - Duration::from_secs(1),
            );
        }
        let guard = cache
            .inner
            .token_cache
            .read()
            .expect("token cache rwlock poisoned");
        assert!(
            !guard.is_negative_fresh("wrong"),
            "expired negative must not be served"
        );
    }

    #[test]
    fn token_cache_negative_evicts_past_cap() {
        let cache = cache_with_password("p");
        let mut guard = cache
            .inner
            .token_cache
            .write()
            .expect("token cache rwlock poisoned");
        for i in 0..TOKEN_CACHE_MAX as u32 {
            guard.negatives.insert(format!("stale-{i}"), Instant::now());
        }
        assert_eq!(guard.negatives.len(), TOKEN_CACHE_MAX);
        guard.record("new-neg", false);
        assert_eq!(
            guard.negatives.len(),
            TOKEN_CACHE_MAX,
            "negative cap holds after record"
        );
        assert!(guard.negatives.contains_key("new-neg"));
    }

    #[test]
    fn token_cache_clear_drops_negative_map() {
        let cache = cache_with_password("s3cret");
        assert!(!cache.token_verify_cached("wrong")); // records a negative
        let guard = cache
            .inner
            .token_cache
            .read()
            .expect("token cache rwlock poisoned");
        assert!(guard.negatives.contains_key("wrong"));
        drop(guard);
        cache.invalidate_token_cache();
        let guard = cache
            .inner
            .token_cache
            .read()
            .expect("token cache rwlock poisoned");
        assert!(guard.negatives.is_empty(), "clear must drop negatives");
        assert!(guard.is_empty());
    }

    #[test]
    fn token_verify_cached_ttl_expiry() {
        let cache = cache_with_password("s3cret");
        // Seed an *expired* entry directly: a fresh verify must still run.
        {
            let mut guard = cache
                .inner
                .token_cache
                .write()
                .expect("token cache rwlock poisoned");
            guard.entries.insert(
                "s3cret".into(),
                Instant::now() - TOKEN_CACHE_TTL - Duration::from_secs(1),
            );
        }
        // Expired → not a cache hit → re-verify (succeeds) and refresh.
        assert!(cache.token_verify_cached("s3cret"));
        let guard = cache
            .inner
            .token_cache
            .read()
            .expect("token cache rwlock poisoned");
        assert!(
            guard.is_fresh("s3cret"),
            "entry refreshed after the re-verify"
        );
    }

    #[test]
    fn token_cache_evicts_past_cap() {
        let cache = cache_with_password("p");
        let mut guard = cache
            .inner
            .token_cache
            .write()
            .expect("token cache rwlock poisoned");
        // Fill to the cap, then record one more: total must stay at the cap.
        for i in 0..TOKEN_CACHE_MAX as u32 {
            guard.entries.insert(format!("stale-{i}"), Instant::now());
        }
        assert_eq!(guard.len(), TOKEN_CACHE_MAX);
        guard.record("new-token", true);
        assert_eq!(guard.len(), TOKEN_CACHE_MAX, "cap holds after record");
        assert!(guard.entries.contains_key("new-token"));
    }

    #[tokio::test]
    async fn upgrade_password_invalidates_token_cache() {
        let cache = cache_with_password("legacy");
        assert!(cache.token_verify_cached("legacy"));
        // NOTE: `VerifiedTokenCache` sits behind a non-reentrant
        // `std::sync::RwLock`. The guard MUST be dropped before any call
        // that re-locks it (the `upgrade_password` below invalidates the
        // cache, and `token_verify_cached` also locks). Holding it across
        // those would self-deadlock.
        {
            let guard = cache
                .inner
                .token_cache
                .read()
                .expect("token cache rwlock poisoned");
            assert_eq!(guard.len(), 1);
        }
        // upgrade_password (dead pool → DB write skipped, cache updated) must
        // drop the cached verify so the old token can't ride the TTL.
        let hashed = crate::settings::hash_admin_password("newpw");
        cache.upgrade_password(hashed).await;
        {
            let guard = cache
                .inner
                .token_cache
                .read()
                .expect("token cache rwlock poisoned");
            assert!(
                guard.is_empty(),
                "upgrade_password must clear the token cache"
            );
        }
        // The old plaintext no longer verifies against the hashed value.
        assert!(!cache.token_verify_cached("legacy"));
    }

    // ── M1: multi-instance divergence warning ──────────────────────────────

    /// `tracing_subscriber::fmt::writer::MakeWriter` over an in-memory
    /// buffer, so a scoped subscriber can capture event text without
    /// depending on harness log output (and without touching stdout).
    #[derive(Clone)]
    struct CapturingWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for CapturingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for CapturingWriter {
        type Writer = CapturingWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn capture_warnings<F: FnOnce()>(test: F) -> String {
        let buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sub = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(CapturingWriter(buf.clone()))
            .finish();
        // Thread-local: the capture cannot leak into other test threads.
        let _guard = tracing::subscriber::set_default(sub);
        test();
        let text = {
            let buf = buf.lock().unwrap_or_else(|p| p.into_inner());
            String::from_utf8_lossy(&buf).into_owned()
        };
        text
    }

    /// M1: the divergence warning trigger. A cache built in multi-instance
    /// mode must warn on a committed settings write; one built in
    /// single-instance mode (the default — `cache_with_locks` sets the flag
    /// explicitly, so the test is independent of the developer's env) must
    /// stay silent. The flag is captured at construction (process startup
    /// decision), so the assertions hold under any process env.
    #[test]
    fn multi_instance_divergence_warns_only_when_flag_set() {
        // Multi-instance: the warn fires, naming the mode and the restart
        // requirement.
        let text = capture_warnings(|| {
            let cache = SettingsCache {
                inner: Arc::new(SettingsInner {
                    cache: RwLock::new(HashMap::new()),
                    env_locked: RwLock::new(HashMap::new()),
                    env_snapshot: HashMap::new(),
                    pool: dead_pool(),
                    token_cache: RwLock::new(VerifiedTokenCache::default()),
                    generation: std::sync::atomic::AtomicU64::new(0),
                    write_guard: tokio::sync::Mutex::new(()),
                    type_mismatches: RwLock::new(std::collections::BTreeMap::new()),
                    multi_instance: true,
                }),
            };
            cache.warn_multi_instance_divergence();
        });
        assert!(
            text.contains("multi-instance mode"),
            "multi-instance mode must warn — got: {text:?}"
        );
        assert!(
            text.contains("restarted"),
            "the warn must state the restart requirement — got: {text:?}"
        );

        // Single-instance (flag explicitly off): no warn at all.
        let text = capture_warnings(|| {
            let cache = cache_with_locks(HashMap::new(), HashMap::new());
            cache.warn_multi_instance_divergence();
        });
        assert!(
            text.trim().is_empty(),
            "single-instance mode must stay silent — got: {text:?}"
        );
    }

    // ── type-mismatch observability (ARCH Major #3) ──────────────────────

    #[test]
    fn snapshot_type_mismatches_flags_corrupt_values_only() {
        // Seed defaults all pass their `json_type` check.
        let mut map = default_settings();
        assert!(
            snapshot_type_mismatches(&map).is_empty(),
            "seed defaults must all pass their json_type check"
        );
        // A bool-typed row storing a string (e.g. a direct SQL edit) →
        // flagged, with the key named and the expected type in the reason.
        map.insert(
            KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!("yes"),
        );
        let bad = snapshot_type_mismatches(&map);
        assert_eq!(
            bad.len(),
            1,
            "exactly the corrupted key must be flagged: {bad:?}"
        );
        let reason = &bad[KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS];
        assert!(
            reason.contains("boolean"),
            "reason must name the expected type: {reason}"
        );
    }

    #[test]
    fn type_mismatches_accessor_surfaces_and_clears() {
        let cache = cache_with_locks(default_settings(), HashMap::new());
        assert!(cache.type_mismatches().is_empty());
        let mut m = std::collections::BTreeMap::new();
        m.insert(
            KEY_ACCESS_REQUIRE_AUTH_FOR_READS.to_string(),
            "expected a boolean, got a string".to_string(),
        );
        cache.set_type_mismatches(m);
        assert_eq!(
            cache.type_mismatches(),
            vec![format!(
                "{KEY_ACCESS_REQUIRE_AUTH_FOR_READS}: expected a boolean, got a string"
            )]
        );
        cache.set_type_mismatches(std::collections::BTreeMap::new());
        assert!(
            cache.type_mismatches().is_empty(),
            "a clean reload must clear the record"
        );
    }

    #[test]
    fn clear_type_mismatches_drops_only_touched_keys() {
        // The `update()` stale-positive fix: a row the operator just
        // re-validated must stop flagging, while an untouched corrupt row
        // keeps its record.
        let cache = cache_with_locks(default_settings(), HashMap::new());
        let mut m = std::collections::BTreeMap::new();
        m.insert(
            KEY_TORRENT_MAX_ACTIVE.to_string(),
            "expected an integer, got a string".to_string(),
        );
        m.insert(
            KEY_ACCESS_REQUIRE_AUTH_FOR_READS.to_string(),
            "expected a boolean, got a string".to_string(),
        );
        cache.set_type_mismatches(m);
        assert_eq!(cache.type_mismatches().len(), 2);
        cache.clear_type_mismatches(&[KEY_TORRENT_MAX_ACTIVE]);
        assert_eq!(
            cache.type_mismatches(),
            vec![format!(
                "{KEY_ACCESS_REQUIRE_AUTH_FOR_READS}: expected a boolean, got a string"
            )],
            "only the re-validated key is dropped"
        );
        // Clearing a key with no record is a no-op.
        cache.clear_type_mismatches(&[KEY_TORRENT_MAX_ACTIVE]);
        assert_eq!(cache.type_mismatches().len(), 1);
    }

    #[test]
    fn mismatch_warn_names_key_and_expected_type() {
        // Exercises the *real* warn code path (`warn_type_mismatches`, the
        // same function `reload()` calls), so a `reload()` regression that
        // drops or alters the warn fails here instead of silently diverging
        // from a verbatim log string.
        let mut map = default_settings();
        map.insert(KEY_TORRENT_MAX_ACTIVE.into(), serde_json::json!("three"));
        let mismatches = snapshot_type_mismatches(&map);
        let text = capture_warnings(|| {
            warn_type_mismatches(&mismatches);
        });
        assert!(
            text.contains(KEY_TORRENT_MAX_ACTIVE),
            "warn must name the key: {text:?}"
        );
        assert!(text.contains("settings value ignored"), "got: {text:?}");
    }
}
