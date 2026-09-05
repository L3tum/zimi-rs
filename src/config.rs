//! Process configuration: environment overrides + validation.
//!
//! `Config::load()` merges compiled-in defaults with env overrides and
//! validates ranges; it also owns the env→settings lock/snapshot helpers
//! used at startup to lock env-provided settings and to seed defaults.
use std::collections::HashMap;
use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::settings::{
    KEY_ACCESS_ADMIN_PASSWORD, KEY_ACCESS_MODE, KEY_EMBEDDING_API_KEY, KEY_EMBEDDING_DIMENSION,
    KEY_EMBEDDING_ENDPOINT, KEY_EMBEDDING_MODEL, KEY_GENERAL_HOST, KEY_GENERAL_PORT,
    KEY_GENERAL_TRUSTED_PROXY_CIDRS, KEY_GENERAL_ZIM_DIR, KEY_TORRENT_PASSWORD, KEY_TORRENT_URL,
    KEY_TORRENT_USERNAME,
};

/// Application configuration, loaded from env + compiled-in defaults.
///
/// Precedence: env var > compiled-in default.
///
/// This only holds startup/process concerns. Per-instance tunables (search
/// weights, embedding endpoint/model, torrent save path / category / max
/// active, etc.) live in the `settings` table (`SettingsCache`), not here —
/// they are runtime-editable and seed from env where applicable.
#[derive(Debug, Clone)]
pub struct Config {
    pub zim_dir: PathBuf,
    pub database_url: String,
    pub db_pool_size: u32,
    pub host: String,
    pub port: u16,
    pub log_level: String,

    // Torrent (qBittorrent Web API endpoint only — category / save path /
    // max-active are settings-table keys, not process config).
    pub torrent_url: Option<String>,
    pub torrent_user: String,
    pub torrent_pass: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            zim_dir: PathBuf::from("/zims"),
            database_url: "postgres://localhost:5432/zimservice".into(),
            db_pool_size: 20,
            host: "127.0.0.1".into(),
            port: 8899,
            log_level: "info".into(),
            torrent_url: None,
            torrent_user: String::new(),
            torrent_pass: String::new(),
        }
    }
}

/// All env-backed settings: `(settings_key, env_var, in_snapshot)`.
/// `in_snapshot` marks the subset `SettingsCache::reload()` re-applies on every
/// reload. Both `locked_env_settings` (every key whose env var is set) and
/// `env_settings_snapshot` (the in-snapshot keys with non-empty values) derive
/// from this single table (L5), so the two can't drift.
///
/// The 12 settings-table rows set `policy.env_locked = true` in
/// `settings::SETTING_DEFS` (static eligibility only); `database.url` is the
/// documented 1-entry exception — it is process config, not a settings-table
/// row, so it has no `SETTING_DEFS` row and stays here.
const ENV_SETTING_KEYS: [(&str, &str, bool); 14] = [
    (KEY_GENERAL_ZIM_DIR, "ZIM_DIR", false),
    (KEY_GENERAL_PORT, "PORT", false),
    (KEY_GENERAL_HOST, "HOST", false),
    (KEY_GENERAL_TRUSTED_PROXY_CIDRS, "TRUSTED_PROXY_CIDRS", true),
    ("database.url", "DATABASE_URL", false),
    (KEY_TORRENT_URL, "QBITTORRENT_URL", false),
    (KEY_TORRENT_USERNAME, "QBITTORRENT_USER", false),
    (KEY_TORRENT_PASSWORD, "QBITTORRENT_PASS", false),
    (KEY_EMBEDDING_ENDPOINT, "EMBEDDING_ENDPOINT", true),
    (KEY_EMBEDDING_API_KEY, "EMBEDDING_API_KEY", true),
    (KEY_EMBEDDING_MODEL, "EMBEDDING_MODEL", true),
    (KEY_EMBEDDING_DIMENSION, "EMBEDDING_DIM", true),
    (KEY_ACCESS_MODE, "ACCESS_MODE", true),
    (KEY_ACCESS_ADMIN_PASSWORD, "AUTH_PASSWORD", true),
];

impl Config {
    /// Load config from compiled-in defaults + environment variables.
    ///
    /// The service is configured exclusively by environment variables (see
    /// the README) and the runtime-editable `settings` table; there is no
    /// config file.
    pub fn load() -> Result<Self> {
        let mut config = Self::default();
        config.apply_env(&|key| std::env::var(key).ok())?;
        Ok(config)
    }

    /// Apply environment-variable overrides on top of a base config.
    ///
    /// `get` is an injected env-var lookup so callers (and unit tests) can
    /// supply values without mutating the process-global environment — which
    /// would race with parallel tests. Precedence is env > default.
    pub(crate) fn apply_env(&mut self, get: &dyn Fn(&str) -> Option<String>) -> Result<()> {
        if let Some(v) = get("ZIM_DIR") {
            self.zim_dir = PathBuf::from(v);
        }
        if let Some(v) = get("DATABASE_URL") {
            self.database_url = v;
        }
        if let Some(v) = get("DB_POOL_SIZE") {
            self.db_pool_size = v
                .parse()
                .map_err(|e| Error::Config(format!("DB_POOL_SIZE: {e}")))?;
        }
        if let Some(v) = get("PORT") {
            self.port = v.parse().map_err(|e| Error::Config(format!("PORT: {e}")))?;
        }
        if let Some(v) = get("HOST") {
            self.host = v;
        }
        if let Some(v) = get("LOG_LEVEL") {
            self.log_level = v;
        }

        // Torrent (qBittorrent endpoint). The endpoint's presence is what
        // enables torrent support; the `torrent.enabled` setting gates the
        // poller independently (settings table).
        if let Some(v) = get("QBITTORRENT_URL") {
            self.torrent_url = Some(v);
        }
        if let Some(v) = get("QBITTORRENT_USER") {
            self.torrent_user = v;
        }
        if let Some(v) = get("QBITTORRENT_PASS") {
            self.torrent_pass = v;
        }
        Ok(())
    }

    /// List of (key, env_var) pairs that lock settings in the UI.
    ///
    /// `get` is the env getter (injected for testability — L14); a set
    /// variable (even empty) locks the key, matching the old
    /// `std::env::var(...).is_ok()` semantics. Derived from
    /// [`ENV_SETTING_KEYS`] (L5).
    pub fn locked_env_settings(
        &self,
        get: &dyn Fn(&str) -> Option<String>,
    ) -> HashMap<String, String> {
        let mut locked = HashMap::new();
        for (key, var, _in_snapshot) in ENV_SETTING_KEYS {
            if get(var).is_some() {
                locked.insert(key.to_string(), var.to_string());
            }
        }
        locked
    }

    /// Snapshot of the env vars that `SettingsCache::reload()` re-applies on
    /// every reload, as **settings-key → raw value** (non-empty only).
    ///
    /// Captured once at startup so a later `reload()` sees the same overrides
    /// the process started with, instead of re-reading the process env (which
    /// can drift if the env is mutated). `EMBEDDING_DIM` stays a raw string —
    /// it is parsed at the use site, same as before.
    pub fn env_settings_snapshot(
        &self,
        get: &dyn Fn(&str) -> Option<String>,
    ) -> HashMap<String, String> {
        let mut snap = HashMap::new();
        for (key, var, in_snapshot) in ENV_SETTING_KEYS {
            if in_snapshot {
                if let Some(v) = get(var) {
                    if !v.is_empty() {
                        snap.insert(key.to_string(), v);
                    }
                }
            }
        }
        snap
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn env_setting_keys_agree_with_setting_defs() {
        use crate::settings::{def, SETTING_DEFS};

        // (a) Every env-backed settings-table key is a row whose policy marks
        //     it environment-fixed: `env_locked` OR `config_only`. The one
        //     env-backed-but-not-`env_locked` table key is
        //     `general.trusted_proxy_cidrs` (config_only). `database.url` is
        //     the documented process-config exception with no table row.
        for (key, _var, _in_snapshot) in ENV_SETTING_KEYS {
            if key == "database.url" {
                assert!(
                    def(key).is_none(),
                    "database.url is process config and must have no SETTING_DEFS row"
                );
                continue;
            }
            let row = def(key)
                .unwrap_or_else(|| panic!("missing SETTING_DEFS row for env-backed key {key}"));
            assert!(
                row.policy.env_locked || row.policy.config_only,
                "env-backed key {key} must be environment-fixed (env_locked or config_only)"
            );
        }

        // (b) Every `env_locked` settings-table row has a corresponding
        //     ENV_SETTING_KEYS entry — a missing one would mean the env var
        //     silently stops re-applying on reload.
        for row in SETTING_DEFS.iter() {
            if row.policy.env_locked {
                assert!(
                    ENV_SETTING_KEYS.iter().any(|(k, _, _)| *k == row.key),
                    "env_locked key {} has no ENV_SETTING_KEYS entry",
                    row.key
                );
            }
        }
    }

    #[test]
    fn locked_env_settings_injected() {
        // The env getter is injected (L14): only the vars the getter reports
        // as set lock their keys. A set-but-empty var still locks (is_some).
        let c = Config::default();
        let get = |k: &str| match k {
            "ZIM_DIR" => Some("/custom".to_string()),
            "PORT" => Some(String::new()),
            "EMBEDDING_DIM" => Some("768".to_string()),
            _ => None,
        };
        let locked = c.locked_env_settings(&get);
        assert!(locked.contains_key(KEY_GENERAL_ZIM_DIR));
        assert!(locked.contains_key(KEY_GENERAL_PORT));
        assert!(locked.contains_key(KEY_EMBEDDING_DIMENSION));
        assert!(!locked.contains_key(KEY_GENERAL_HOST));
        assert!(!locked.contains_key("database.url"));
        // The getter, not the process env, drives the result.
        assert_eq!(locked.len(), 3);

        // env_settings_snapshot returns only the six reload keys, non-empty only.
        let snap = c.env_settings_snapshot(&get);
        assert!(!snap.contains_key(KEY_ACCESS_MODE));
        assert!(!snap.contains_key(KEY_ACCESS_ADMIN_PASSWORD));
        // PORT is not a snapshot key; EMBEDDING_DIM is (raw string).
        assert_eq!(snap.get(KEY_EMBEDDING_DIMENSION), Some(&"768".to_string()));
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn default_torrent_creds_are_empty() {
        let c = Config::default();
        assert_eq!(c.torrent_user, "");
        assert_eq!(c.torrent_pass, "");
        assert!(c.torrent_url.is_none());
    }

    #[test]
    fn default_host_is_loopback() {
        let c = Config::default();
        assert_eq!(c.host, "127.0.0.1");
    }

    #[test]
    fn db_pool_size_env_applies_and_malformed_is_err() {
        let mut c = Config::default();
        let env = env_from(&[("DB_POOL_SIZE", "5")]);
        c.apply_env(&env).expect("apply_env should succeed");
        assert_eq!(c.db_pool_size, 5);
        let mut c2 = Config::default();
        let env2 = env_from(&[("DB_POOL_SIZE", "not-a-number")]);
        assert!(c2.apply_env(&env2).is_err());
    }

    #[test]
    fn default_pool_size_is_20() {
        let c = Config::default();
        assert_eq!(c.db_pool_size, 20);
    }

    /// Helper: build a closure over a map to inject env values without touching
    /// the process-global environment (avoids racing with parallel tests).
    fn env_from(map: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = map
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| m.get(k).cloned()
    }

    #[test]
    fn env_overrides_base_for_port_and_host() {
        let mut c = Config {
            port: 9999,
            host: "0.0.0.0".into(),
            ..Config::default()
        };
        let env = env_from(&[("PORT", "8080"), ("HOST", "127.0.0.1")]);
        c.apply_env(&env).expect("apply_env should succeed");
        assert_eq!(c.port, 8080, "env PORT must override the base value");
        assert_eq!(c.host, "127.0.0.1", "env HOST must override the base value");
    }

    #[test]
    fn env_absent_preserves_base_values() {
        let mut c = Config {
            port: 7777,
            host: "10.0.0.1".into(),
            ..Config::default()
        };
        let env = env_from(&[("LOG_LEVEL", "debug")]); // no PORT or HOST
        c.apply_env(&env).expect("apply_env should succeed");
        assert_eq!(c.port, 7777, "base port must be preserved when env absent");
        assert_eq!(
            c.host, "10.0.0.1",
            "base host must be preserved when env absent"
        );
    }

    #[test]
    fn malformed_port_env_is_err() {
        let mut c = Config::default();
        let env = env_from(&[("PORT", "not-a-number")]);
        let result = c.apply_env(&env);
        assert!(result.is_err(), "malformed PORT must produce an error");
    }

    #[test]
    fn port_out_of_range_is_err() {
        let mut c = Config::default();
        let env = env_from(&[("PORT", "99999")]); // > 65535
        let result = c.apply_env(&env);
        assert!(result.is_err(), "PORT > 65535 must produce an error");
    }
}
