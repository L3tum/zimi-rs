//! Process configuration: environment overrides + validation.
//!
//! `Config::load()` merges compiled-in defaults with env overrides and
//! validates ranges; it also owns the env→settings lock/snapshot helpers
//! used at startup to lock env-provided settings and to seed defaults.
use std::collections::HashMap;
use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::settings::{
    KEY_ACCESS_ADMIN_PASSWORD, KEY_ACCESS_MODE, KEY_ACCESS_REQUIRE_AUTH_FOR_READS,
    KEY_EMBEDDING_API_KEY, KEY_EMBEDDING_DIMENSION, KEY_EMBEDDING_ENDPOINT, KEY_EMBEDDING_MODEL,
    KEY_GENERAL_HOST, KEY_GENERAL_PORT, KEY_GENERAL_TRUSTED_PROXY_CIDRS, KEY_GENERAL_ZIM_DIR,
    KEY_TORRENT_PASSWORD, KEY_TORRENT_URL, KEY_TORRENT_USERNAME,
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
    /// Directory scanned for `.zim` archives (`ZIM_DIR`, default `/zims`).
    pub zim_dir: PathBuf,
    /// Postgres connection string (`DATABASE_URL`).
    pub database_url: String,
    /// Postgres connection pool size (`DB_POOL_SIZE`, default 20).
    pub db_pool_size: u32,
    /// HTTP bind address (`HOST`, default `127.0.0.1`).
    pub host: String,
    /// HTTP port (`PORT`, default 8899).
    pub port: u16,
    /// Tracing log level (`LOG_LEVEL`, default `info`).
    pub log_level: String,

    // Torrent (qBittorrent Web API endpoint only — category / save path /
    // max-active are settings-table keys, not process config).
    /// qBittorrent Web API endpoint (`QBITTORRENT_URL`); its presence enables
    /// torrent support — the `torrent.enabled` setting gates the poller.
    pub torrent_url: Option<String>,
    /// qBittorrent Web API username (`QBITTORRENT_USER`, default empty).
    pub torrent_user: String,
    /// qBittorrent Web API password (`QBITTORRENT_PASS`, default empty).
    pub torrent_pass: String,

    // Single-instance opt-outs (read by `cmd_serve` / `cmd_mcp`, not by the
    // runtime settings table): m-7 partial opt-out (`ZIMSERVICE_ALLOW_MULTI_DB`
    // = exact "1") and the stdio MCP auth password (DEC-2, `MCP_AUTH_PASSWORD`).
    /// Set only when `ZIMSERVICE_ALLOW_MULTI_DB` is exactly `"1"` — any other
    /// value keeps the full single-instance guard enforced.
    pub allow_multi_db: bool,
    /// stdio MCP session password (`MCP_AUTH_PASSWORD`); `None` when unset.
    pub mcp_auth_password: Option<String>,
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
            allow_multi_db: false,
            mcp_auth_password: None,
        }
    }
}

/// All env-backed settings: `(settings_key, env_var, in_snapshot)`.
/// `in_snapshot` marks the subset `SettingsCache::reload()` re-applies on every
/// reload. Both `locked_env_settings` (every key whose env var is set) and
/// `env_settings_snapshot` (the in-snapshot keys with non-empty values) derive
/// from this single table (L5), so the two can't drift.
///
/// The 13 settings-table rows set `policy.env_locked = true` in
/// `settings::SETTING_DEFS` (static eligibility only); `database.url` is the
/// documented 1-entry exception — it is process config, not a settings-table
/// row, so it has no `SETTING_DEFS` row and stays here.
const ENV_SETTING_KEYS: [(&str, &str, bool); 15] = [
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
    (
        KEY_ACCESS_REQUIRE_AUTH_FOR_READS,
        "REQUIRE_AUTH_FOR_READS",
        true,
    ),
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

        // m-7: the per-DB advisory lock is best-effort only when the env var
        // is exactly "1" (any other value — incl. "0"/"true"/"" — leaves the
        // full guard enforced). Mirrors the `matches!` the guard site used
        // before this was routed through `Config`.
        self.allow_multi_db = matches!(get("ZIMSERVICE_ALLOW_MULTI_DB"), Some(v) if v == "1");
        // DEC-2: stdio MCP session password, verified against the configured
        // admin password in `cmd_mcp` (fail-closed in password mode).
        if let Some(v) = get("MCP_AUTH_PASSWORD") {
            self.mcp_auth_password = Some(v);
        }
        Ok(())
    }

    /// List of (key, env_var) pairs that lock settings in the UI.
    ///
    /// `get` is the env getter (injected for testability — L14); a variable
    /// set to a non-empty value locks the key. A set-but-empty var is
    /// treated as unset — consistent with [`Config::env_settings_snapshot`], which
    /// also skips empty values: the snapshot never re-applies an empty var,
    /// so locking the UI key for it would be misleading. The rule applies
    /// uniformly to every key in `ENV_SETTING_KEYS`. Derived from that
    /// table (L5).
    pub fn locked_env_settings(
        &self,
        get: &dyn Fn(&str) -> Option<String>,
    ) -> HashMap<String, String> {
        let mut locked = HashMap::new();
        for (key, var, _in_snapshot) in ENV_SETTING_KEYS {
            if get(var).is_some_and(|v| !v.is_empty()) {
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
    /// can drift if the env is mutated). Values stay raw strings; the typed
    /// parse happens in `settings::apply_env_snapshot` per each key's
    /// `json_type`.
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

    /// M-1: apply the startup default for `access.require_auth_for_reads` to
    /// a reload snapshot: when the operator did **not** set
    /// `REQUIRE_AUTH_FOR_READS` (key absent) and the bind is non-loopback,
    /// inject `"true"` so reads are gated by default on network-facing
    /// deployments; a loopback bind injects nothing (the open-reads seed
    /// default stands). An explicit value (key present) always overrides the
    /// bind-based default in both directions. Pure and DB-free, like the
    /// other snapshot helpers.
    pub(crate) fn apply_require_reads_default(&self, snapshot: &mut HashMap<String, String>) {
        if !snapshot.contains_key(KEY_ACCESS_REQUIRE_AUTH_FOR_READS)
            && crate::settings::require_reads_startup_default(&self.host)
        {
            snapshot.insert(KEY_ACCESS_REQUIRE_AUTH_FOR_READS.into(), "true".to_string());
        }
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
        // as set to a non-empty value lock their keys. A set-but-empty var
        // does NOT lock (treated as unset — the snapshot skips it too).
        let c = Config::default();
        let get = |k: &str| match k {
            "ZIM_DIR" => Some("/custom".to_string()),
            "PORT" => Some(String::new()),
            "EMBEDDING_DIM" => Some("768".to_string()),
            _ => None,
        };
        let locked = c.locked_env_settings(&get);
        assert!(locked.contains_key(KEY_GENERAL_ZIM_DIR));
        assert!(
            !locked.contains_key(KEY_GENERAL_PORT),
            "set-but-empty PORT must not lock (treated as unset)"
        );
        assert!(locked.contains_key(KEY_EMBEDDING_DIMENSION));
        assert!(!locked.contains_key(KEY_GENERAL_HOST));
        assert!(!locked.contains_key("database.url"));
        // The getter, not the process env, drives the result.
        assert_eq!(locked.len(), 2);

        // env_settings_snapshot returns only the eight reload keys, non-empty only.
        let snap = c.env_settings_snapshot(&get);
        assert!(!snap.contains_key(KEY_ACCESS_MODE));
        assert!(!snap.contains_key(KEY_ACCESS_ADMIN_PASSWORD));
        // PORT is not a snapshot key; EMBEDDING_DIM is (raw string).
        assert_eq!(snap.get(KEY_EMBEDDING_DIMENSION), Some(&"768".to_string()));
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn allow_multi_db_env_parses_only_literal_one() {
        // Absent → false; the exact value "1" → true; any other value →
        // false (mirrors the `matches!` semantics `cmd_serve` used to have).
        let mut c = Config::default();
        c.apply_env(&env_from(&[]))
            .expect("apply_env should succeed");
        assert!(!c.allow_multi_db, "absent env must default to false");
        let mut c1 = Config::default();
        c1.apply_env(&env_from(&[("ZIMSERVICE_ALLOW_MULTI_DB", "1")]))
            .expect("apply_env should succeed");
        assert!(c1.allow_multi_db, "exact \"1\" must enable allow_multi_db");
        for v in ["0", "true", "yes", "", "2"] {
            let mut c2 = Config::default();
            c2.apply_env(&env_from(&[("ZIMSERVICE_ALLOW_MULTI_DB", v)]))
                .expect("apply_env should succeed");
            assert!(
                !c2.allow_multi_db,
                "value {v:?} must not enable allow_multi_db"
            );
        }
    }

    #[test]
    fn mcp_auth_password_env_present_and_absent() {
        let mut c = Config::default();
        c.apply_env(&env_from(&[("MCP_AUTH_PASSWORD", "s3cret")]))
            .expect("apply_env should succeed");
        assert_eq!(c.mcp_auth_password.as_deref(), Some("s3cret"));
        let mut c2 = Config::default();
        c2.apply_env(&env_from(&[]))
            .expect("apply_env should succeed");
        assert!(c2.mcp_auth_password.is_none(), "absent env must stay None");
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

    // ── M-1: bind-based startup default for access.require_auth_for_reads ──

    #[test]
    fn require_reads_default_loopback_keeps_reads_open() {
        // loopback bind ⇒ no injection; the seed default (false, open reads)
        // stands and local-dev ergonomics are unchanged.
        for host in ["127.0.0.1", "localhost", "::1"] {
            let c = Config {
                host: host.into(),
                ..Config::default()
            };
            let mut snap = HashMap::new();
            c.apply_require_reads_default(&mut snap);
            assert!(
                !snap.contains_key(KEY_ACCESS_REQUIRE_AUTH_FOR_READS),
                "{host}: loopback must not inject a read-gating default"
            );
        }
    }

    #[test]
    fn require_reads_default_non_loopback_gates_reads() {
        for host in ["0.0.0.0", "::", "10.0.0.5"] {
            let c = Config {
                host: host.into(),
                ..Config::default()
            };
            let mut snap = HashMap::new();
            c.apply_require_reads_default(&mut snap);
            assert_eq!(
                snap.get(KEY_ACCESS_REQUIRE_AUTH_FOR_READS),
                Some(&"true".to_string()),
                "{host}: non-loopback must default reads to gated"
            );
        }
    }

    #[test]
    fn require_reads_default_explicit_value_overrides_both_ways() {
        // Explicit false on a non-loopback bind: honored (reads stay open).
        let c = Config {
            host: "0.0.0.0".into(),
            ..Config::default()
        };
        let mut snap = HashMap::new();
        snap.insert(
            KEY_ACCESS_REQUIRE_AUTH_FOR_READS.into(),
            "false".to_string(),
        );
        c.apply_require_reads_default(&mut snap);
        assert_eq!(
            snap.get(KEY_ACCESS_REQUIRE_AUTH_FOR_READS),
            Some(&"false".to_string())
        );
        // Explicit true on a loopback bind: honored (reads gated).
        let c2 = Config::default(); // 127.0.0.1
        let mut snap2 = HashMap::new();
        snap2.insert(KEY_ACCESS_REQUIRE_AUTH_FOR_READS.into(), "true".to_string());
        c2.apply_require_reads_default(&mut snap2);
        assert_eq!(
            snap2.get(KEY_ACCESS_REQUIRE_AUTH_FOR_READS),
            Some(&"true".to_string())
        );
    }
}
