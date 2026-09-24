//! Security-sensitive settings state extracted out of `SettingsCache`
//! (Architecture M1, continuing the decomposition). The fields themselves
//! stay on `SettingsInner` (the process-wide shared state — `settings/
//! cache.rs`); this module owns the access rules around them:
//!
//! - **the two token-verify caches** — the admin/legacy-password cache
//!   (`SettingsInner::token_cache`) and the read-only token cache
//!   (`SettingsInner::ro_token_cache`; the `VerifiedTokenCache` type itself
//!   lives in `auth`) — with the centralized lock helpers, plus the sticky
//!   in-memory halves of the two secrets they verify against
//!   (`admin_password_raw`/`set`, `read_only_token_raw`/`set`, the
//!   in-memory half of `SettingsAuth::upgrade`/`upgrade_read_only`);
//! - **the type-mismatch observability record** (ARCH Major #3): the
//!   `SettingsCache::type_mismatches` accessors and the snapshot/warn
//!   passes `reload()` runs;
//! - **the read redaction rules** (S6): the per-entry shaping of
//!   `all_grouped_for` output (secret redaction, the unauthenticated
//!   topology `"[redacted]"` substitution, the `locked`/`locked_by`
//!   annotations).

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::auth::VerifiedTokenCache;
use super::cache::{SettingsCache, SettingsInner};
use super::defs::{
    def, redact, type_mismatch, KEY_ACCESS_ADMIN_PASSWORD, KEY_ACCESS_READ_ONLY_TOKEN,
};

impl SettingsInner {
    /// Centralized lock+expect for the token-cache accessors (read flavor):
    /// one helper per flavor so the grandfathered LINT-3 expect lives in a
    /// single place; `name` feeds the exact per-cache panic message.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    fn cache_read<'a>(
        &self,
        cache: &'a RwLock<VerifiedTokenCache>,
        name: &str,
    ) -> RwLockReadGuard<'a, VerifiedTokenCache> {
        let msg = format!("{name} cache rwlock poisoned");
        cache.read().expect(&msg)
    }

    /// Write-flavor twin of `cache_read` (same message construction).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    fn cache_write<'a>(
        &self,
        cache: &'a RwLock<VerifiedTokenCache>,
        name: &str,
    ) -> RwLockWriteGuard<'a, VerifiedTokenCache> {
        let msg = format!("{name} cache rwlock poisoned");
        cache.write().expect(&msg)
    }

    /// True if `token` verified successfully within the TTL (O(1) memory
    /// work; the KDF never runs under the lock — see `auth_service`).
    pub(crate) fn token_is_fresh(&self, token: &str) -> bool {
        self.cache_read(&self.token_cache, "token").is_fresh(token)
    }

    /// True if `token` failed verification within the negative TTL.
    pub(crate) fn token_is_negative_fresh(&self, token: &str) -> bool {
        self.cache_read(&self.token_cache, "token")
            .is_negative_fresh(token)
    }

    /// Record a KDF verdict for `token` (positive or negative).
    pub(crate) fn token_record(&self, token: &str, ok: bool) {
        self.cache_write(&self.token_cache, "token")
            .record(token, ok);
    }

    /// Drop all cached verified tokens (any path that can change the
    /// effective password: reload/update/upgrade).
    pub(crate) fn token_invalidate_all(&self) {
        self.cache_write(&self.token_cache, "token").clear();
    }

    /// Drop all cached verified read-only tokens (same sites as
    /// [`Self::token_invalidate_all`] — see `ro_token_cache`'s doc).
    pub(crate) fn ro_token_invalidate_all(&self) {
        self.cache_write(&self.ro_token_cache, "read-only token")
            .clear();
    }

    /// True if `token` verified against the read-only token within the TTL.
    pub(crate) fn ro_token_is_fresh(&self, token: &str) -> bool {
        self.cache_read(&self.ro_token_cache, "read-only token")
            .is_fresh(token)
    }

    /// True if `token` failed read-only verification within the negative TTL.
    pub(crate) fn ro_token_is_negative_fresh(&self, token: &str) -> bool {
        self.cache_read(&self.ro_token_cache, "read-only token")
            .is_negative_fresh(token)
    }

    /// Record a KDF verdict for `token` against the read-only token.
    pub(crate) fn ro_token_record(&self, token: &str, ok: bool) {
        self.cache_write(&self.ro_token_cache, "read-only token")
            .record(token, ok);
    }

    /// The raw (unredacted) stored admin password, or empty when unset. The
    /// in-memory map always holds the real (possibly `sha2:`-hashed) value —
    /// `redact()` only applies to `all_grouped_for` output.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub(crate) fn admin_password_raw(&self) -> String {
        self.cache
            .read()
            .expect("settings cache lock poisoned")
            .get(KEY_ACCESS_ADMIN_PASSWORD)
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default()
    }

    /// Overwrite the cached admin password (the legacy-upgrade path; the
    /// sticky in-memory half of `SettingsAuth::upgrade`).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub(crate) fn admin_password_set(&self, hashed: &str) {
        self.cache
            .write()
            .expect("settings cache lock poisoned")
            .insert(KEY_ACCESS_ADMIN_PASSWORD.into(), serde_json::json!(hashed));
    }

    /// The raw (unredacted) stored read-only token, or empty when unset.
    /// Mirrors [`Self::admin_password_raw`]: the in-memory map always holds
    /// the real (possibly hashed) value — `redact()` only applies to
    /// `all_grouped_for` output.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub(crate) fn read_only_token_raw(&self) -> String {
        self.cache
            .read()
            .expect("settings cache lock poisoned")
            .get(KEY_ACCESS_READ_ONLY_TOKEN)
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default()
    }

    /// Overwrite the cached read-only token (the legacy-upgrade path; the
    /// sticky in-memory half of `SettingsAuth::upgrade_read_only`).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub(crate) fn read_only_token_set(&self, hashed: &str) {
        self.cache
            .write()
            .expect("settings cache lock poisoned")
            .insert(KEY_ACCESS_READ_ONLY_TOKEN.into(), serde_json::json!(hashed));
    }
}

impl SettingsCache {
    /// Keys whose stored value does not match its expected JSON type, each as
    /// `"key: reason"`. These settings silently run on their defaults —
    /// surfaced by the authenticated `/diagnostic` route as
    /// `settings_mismatches` so a corrupted settings row is not invisible.
    /// The record is seeded at each `reload()` (startup)
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
}

/// Collect the keys whose value fails its `json_type` check — the same check
/// `update()` runs at write time, applied to *stored* contents (a corrupted
/// row, a bad direct SQL edit, or a migration that wrote the wrong shape). A
/// failed key silently runs on its (fail-closed) default; this is what makes
/// that visible (ARCH Major #3). Pure + DB-free, so unit-testable. Ordered by
/// key so the warn log and the `/diagnostic` report are deterministic.
pub(super) fn snapshot_type_mismatches(
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
pub(super) fn warn_type_mismatches(mismatches: &std::collections::BTreeMap<String, String>) {
    for (key, reason) in mismatches {
        tracing::warn!("settings value ignored, running on the default: {reason} ({key})");
    }
}

/// One row of `SettingsCache::all_grouped_for`: the stored value shaped for
/// the caller — secrets redacted per the `secret` policy ([`redact`]),
/// topology values replaced with `"[redacted]"` for unauthenticated callers
/// (S6), and the `locked`/`locked_by` annotation from the env lock or the
/// policy flags. `env_var` is the env-lock annotation for `key`
/// (`Some(name)` = locked by that env var at startup).
pub(super) fn redacted_entry(
    key: &str,
    value: &serde_json::Value,
    authenticated: bool,
    env_var: Option<&String>,
) -> serde_json::Value {
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
    if let Some(env_var) = env_var {
        entry["locked"] = serde_json::json!(true);
        entry["locked_by"] = serde_json::json!(env_var);
    } else if setting.is_some_and(|d| d.policy.api_immutable) {
        entry["locked"] = serde_json::json!(true);
        entry["locked_by"] = serde_json::json!("set via environment at startup");
    } else if setting.is_some_and(|d| d.policy.config_only) {
        entry["locked"] = serde_json::json!(true);
        entry["locked_by"] = serde_json::json!("environment (not runtime)");
    }
    entry
}
