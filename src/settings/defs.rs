//! Setting definitions: the single source of truth for setting keys, seed
//! defaults, expected JSON types, and policy flags.
//!
//! [`SETTING_DEFS`] is the policy table (seeded by [`default_settings`]);
//! the pure map helpers (redaction, type-checking, env/config sync) are
//! shared by the cache, and [`default_value`] + `typed_getter!` back the
//! typed accessors.

use std::collections::HashMap;

/// Canonical embedding defaults shared across settings, embed, and search modules.
pub const EMBED_DEFAULT_MODEL: &str = "nomic-embed-text";
/// Default embedding vector dimension for the canonical model (nomic-embed-text).
pub const EMBED_DEFAULT_DIMENSION: u32 = 768;
/// Vector-index strategy thresholds (article count): below HNSW, at/above IVFFlat.
pub const EMBED_DEFAULT_HNSW_THRESHOLD: i64 = 1_000_000;
/// Article-count threshold at which the IVFFlat index is preferred over HNSW.
pub const EMBED_DEFAULT_IVFFLAT_THRESHOLD: i64 = 10_000_000;

/// Default `downloads.max_bytes` — 1 EiB, i.e. effectively unbounded.
///
/// Full ZIMs can be huge (the English Wikipedia is ~120 GB ≈ 111 GiB), so the
/// old 20 GiB default would have blocked legitimate archives and the old
/// 100 GiB ceiling would have blocked them too. The cap is now an
/// operator-set upper bound with **no** built-in ceiling (WP3.11); this
/// default simply means "don't stop a normal download". Only a floor of 1 is
/// enforced so a stored `0` can't disable the cap entirely.
pub const DEFAULT_MAX_BYTES: u64 = 1 << 60; // 1 EiB

/// Canonical `access.mode` values — compare against these, not string literals.
pub const ACCESS_MODE_OPEN: &str = "open";
pub const ACCESS_MODE_PASSWORD: &str = "password";

/// The JSON type a setting value must have (write-time type-checking).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum JsonType {
    Str,
    Int,
    Num,
    Bool,
}

/// Per-setting policy flags. Every policy a setting can carry is a flag on the
/// [`SETTING_DEFS`] row — the single source of truth for settings policy
/// (previously scattered across 8 hand-maintained lists).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SettingPolicy {
    /// Gating auth / SSRF policy / secret-bearing integrations — unauthenticated
    /// writes are rejected (rejected up front as 403 in `put_settings`).
    pub security_sensitive: bool,
    /// The key is env-backed (a row of `config::ENV_SETTING_KEYS`): if its env
    /// var is set, the runtime `env_locked` map locks it against API writes.
    /// Static eligibility only — the actual lock state is dynamic.
    pub env_locked: bool,
    /// Never returned with its real value in API responses (set values become
    /// `"***"`; unset values pass through as-is).
    pub secret: bool,
    /// Redacted to `"[redacted]"` for unauthenticated `all_grouped_for` callers
    /// (S6 topology leak guard).
    pub topology: bool,
    /// Cannot be changed via the API (seeded from the environment at startup,
    /// shown as locked in the UI).
    pub api_immutable: bool,
    /// Fixed by the environment at startup with no runtime effect
    /// if changed (server already bound, ZIM dir already opened, logger already
    /// configured). Stored for reference but rejected on API update (D1b).
    pub config_only: bool,
}

/// One row of [`SETTING_DEFS`]: key, seed default, expected JSON type, policy.
#[derive(Clone, Debug)]
pub struct SettingDef {
    pub key: &'static str,
    pub default: serde_json::Value,
    pub json_type: JsonType,
    pub policy: SettingPolicy,
}

// ── Setting key constants (single source of truth for key strings) ──
//
// One `pub const` per settings-table key. Every read/write of a setting key
// must use one of these — never a raw `"prefix.name"` string literal — so a
// key typo can't survive. The values are the exact key strings used in
// `SETTING_DEFS` below (resolved by key via `def` / `default_value`).
//
// Naming: `KEY_` + the key uppercased with `.` → `_`.

// general.* — process/topology config (host, port, storage, logging, CORS).
pub const KEY_GENERAL_ZIM_DIR: &str = "general.zim_dir";
pub const KEY_GENERAL_HOST: &str = "general.host";
pub const KEY_GENERAL_PORT: &str = "general.port";
pub const KEY_GENERAL_LOG_LEVEL: &str = "general.log_level";
pub const KEY_GENERAL_CORS_ORIGINS: &str = "general.cors_origins";
pub const KEY_GENERAL_TRUSTED_PROXY_CIDRS: &str = "general.trusted_proxy_cidrs";

// search.* — FTS / trigram / vector weighting, threshold, and result limits.
pub const KEY_SEARCH_FTS_WEIGHT: &str = "search.fts_weight";
pub const KEY_SEARCH_TRGM_WEIGHT: &str = "search.trgm_weight";
pub const KEY_SEARCH_VECTOR_WEIGHT: &str = "search.vector_weight";
pub const KEY_SEARCH_TRGM_THRESHOLD: &str = "search.trgm_threshold";
pub const KEY_SEARCH_DEFAULT_LIMIT: &str = "search.default_limit";
pub const KEY_SEARCH_MAX_LIMIT: &str = "search.max_limit";

// downloads.* — direct-download size cap and private-network policy.
pub const KEY_DOWNLOADS_MAX_BYTES: &str = "downloads.max_bytes";
pub const KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS: &str = "downloads.allow_private_networks";

// torrent.* — qBittorrent client + OPDS auto-update integration.
pub const KEY_TORRENT_ENABLED: &str = "torrent.enabled";
pub const KEY_TORRENT_URL: &str = "torrent.url";
pub const KEY_TORRENT_USERNAME: &str = "torrent.username";
pub const KEY_TORRENT_PASSWORD: &str = "torrent.password";
pub const KEY_TORRENT_ALLOW_PRIVATE_NETWORKS: &str = "torrent.allow_private_networks";
pub const KEY_TORRENT_SAVE_PATH: &str = "torrent.save_path";
pub const KEY_TORRENT_CATEGORY: &str = "torrent.category";
pub const KEY_TORRENT_MAX_ACTIVE: &str = "torrent.max_active";
pub const KEY_TORRENT_POLL_SECS: &str = "torrent.poll_secs";
pub const KEY_TORRENT_FILE_STRATEGY: &str = "torrent.file_strategy";
pub const KEY_TORRENT_SEED_RATIO: &str = "torrent.seed_ratio";
pub const KEY_TORRENT_KEEP_COMPLETED: &str = "torrent.keep_completed";
pub const KEY_TORRENT_OPDS_URL: &str = "torrent.opds_url";
pub const KEY_TORRENT_AUTO_UPDATE: &str = "torrent.auto_update";

// embedding.* — vector-embedding client + index strategy.
pub const KEY_EMBEDDING_ENABLED: &str = "embedding.enabled";
pub const KEY_EMBEDDING_ENDPOINT: &str = "embedding.endpoint";
pub const KEY_EMBEDDING_API_KEY: &str = "embedding.api_key";
pub const KEY_EMBEDDING_MODEL: &str = "embedding.model";
pub const KEY_EMBEDDING_DIMENSION: &str = "embedding.dimension";
pub const KEY_EMBEDDING_BATCH_SIZE: &str = "embedding.batch_size";
pub const KEY_EMBEDDING_MAX_CONCURRENCY: &str = "embedding.max_concurrency";
pub const KEY_EMBEDDING_TIMEOUT_SECS: &str = "embedding.timeout_secs";
pub const KEY_EMBEDDING_HNSW_THRESHOLD: &str = "embedding.hnsw_threshold";
pub const KEY_EMBEDDING_IVFFLAT_THRESHOLD: &str = "embedding.ivfflat_threshold";

// access.* — auth mode, rate limits, admin password, and read-gating.
pub const KEY_ACCESS_MODE: &str = "access.mode";
pub const KEY_ACCESS_RATE_LIMIT_RPS: &str = "access.rate_limit_rps";
pub const KEY_ACCESS_RATE_LIMIT_BURST: &str = "access.rate_limit_burst";
pub const KEY_ACCESS_ADMIN_PASSWORD: &str = "access.admin_password";
pub const KEY_ACCESS_REQUIRE_AUTH_FOR_READS: &str = "access.require_auth_for_reads";

/// The single source of truth for settings policy: every known setting, its
/// seed default (same constants as [`default_settings`]), its expected JSON
/// type, and its full policy flag set. Add a setting here — and nowhere else
/// (the bidirectional test `setting_defs_bidirectional_seed_coverage` pins the
/// seed agreement).
/// `LazyLock` (not `const`) because float defaults (`0.6`, `2.0`, …) can't be
/// built by the `json!` macro in const context; the table is still built once
/// and shared by reference — the single source of truth for settings policy.
pub static SETTING_DEFS: std::sync::LazyLock<[SettingDef; 43]> = std::sync::LazyLock::new(|| {
    [
        SettingDef {
            key: KEY_GENERAL_ZIM_DIR,
            default: serde_json::json!("/zims"),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                env_locked: true,
                config_only: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_GENERAL_HOST,
            default: serde_json::json!("127.0.0.1"),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                env_locked: true,
                config_only: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_GENERAL_PORT,
            default: serde_json::json!(8899),
            json_type: JsonType::Int,
            policy: SettingPolicy {
                env_locked: true,
                config_only: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_GENERAL_LOG_LEVEL,
            default: serde_json::json!("info"),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                config_only: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_GENERAL_CORS_ORIGINS,
            default: serde_json::json!(""),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                config_only: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_SEARCH_FTS_WEIGHT,
            default: serde_json::json!(0.6),
            json_type: JsonType::Num,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_SEARCH_TRGM_WEIGHT,
            default: serde_json::json!(0.4),
            json_type: JsonType::Num,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_SEARCH_VECTOR_WEIGHT,
            default: serde_json::json!(0.5),
            json_type: JsonType::Num,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_SEARCH_TRGM_THRESHOLD,
            default: serde_json::json!(0.3),
            json_type: JsonType::Num,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_SEARCH_DEFAULT_LIMIT,
            default: serde_json::json!(10),
            json_type: JsonType::Int,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_SEARCH_MAX_LIMIT,
            default: serde_json::json!(50),
            json_type: JsonType::Int,
            policy: SettingPolicy::default(),
        },
        // SEC M2: the download size cap is a resource-control knob — a
        // misconfiguration (or an unauthenticated write) to 0 can disable the
        // cap. Gate like the other SSRF/resource keys. The default is
        // effectively unbounded (1 EiB) and there is no upper ceiling (WP3.11
        // removed the old 100 GiB clamp that blocked ~120 GB ZIMs).
        SettingDef {
            key: KEY_DOWNLOADS_MAX_BYTES,
            default: serde_json::json!(DEFAULT_MAX_BYTES),
            json_type: JsonType::Int,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS,
            default: serde_json::json!(false),
            json_type: JsonType::Bool,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_TORRENT_ENABLED,
            default: serde_json::json!(true),
            json_type: JsonType::Bool,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_TORRENT_URL,
            default: serde_json::json!(""),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                security_sensitive: true,
                env_locked: true,
                topology: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_TORRENT_USERNAME,
            default: serde_json::json!(""),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                security_sensitive: true,
                env_locked: true,
                topology: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_TORRENT_PASSWORD,
            default: serde_json::json!(""),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                security_sensitive: true,
                env_locked: true,
                secret: true,
                ..Default::default()
            },
        },
        SettingDef {
            // SEC M-1: opt-in to point the qBittorrent Web API at a private
            // (RFC1918/ULA/CGNAT) address. Default false — the netguard blocks
            // such IPs so a misconfigured/reused `torrent.url` can't reach an
            // internal service. Loopback is always admitted (local qB); the
            // always-blocked ranges (metadata/link-local/IETF-doc/NAT64) stay
            // blocked even with this on. Runtime-reflective: flipping it
            // changes the connection fingerprint, so the cached client
            // reconnects within one poll tick (like `torrent.url`).
            key: KEY_TORRENT_ALLOW_PRIVATE_NETWORKS,
            default: serde_json::json!(false),
            json_type: JsonType::Bool,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_TORRENT_SAVE_PATH,
            default: serde_json::json!("/downloads"),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_TORRENT_CATEGORY,
            default: serde_json::json!("zimservice"),
            json_type: JsonType::Str,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_TORRENT_MAX_ACTIVE,
            default: serde_json::json!(4),
            json_type: JsonType::Int,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_TORRENT_POLL_SECS,
            default: serde_json::json!(5),
            json_type: JsonType::Int,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_TORRENT_FILE_STRATEGY,
            default: serde_json::json!("hardlink"),
            json_type: JsonType::Str,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_TORRENT_SEED_RATIO,
            default: serde_json::json!(2.0),
            json_type: JsonType::Num,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_TORRENT_KEEP_COMPLETED,
            default: serde_json::json!(true),
            json_type: JsonType::Bool,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_TORRENT_OPDS_URL,
            default: serde_json::json!("https://opds.kiwix.com/opds_catalog"),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_TORRENT_AUTO_UPDATE,
            default: serde_json::json!(false),
            json_type: JsonType::Bool,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_EMBEDDING_ENABLED,
            default: serde_json::json!(false),
            json_type: JsonType::Bool,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_EMBEDDING_ENDPOINT,
            default: serde_json::json!("http://localhost:11434/v1"),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                security_sensitive: true,
                env_locked: true,
                topology: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_EMBEDDING_API_KEY,
            default: serde_json::json!(""),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                security_sensitive: true,
                env_locked: true,
                secret: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_EMBEDDING_MODEL,
            default: serde_json::json!(EMBED_DEFAULT_MODEL),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                security_sensitive: true,
                env_locked: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_EMBEDDING_DIMENSION,
            default: serde_json::json!(EMBED_DEFAULT_DIMENSION),
            json_type: JsonType::Int,
            policy: SettingPolicy {
                security_sensitive: true,
                env_locked: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_EMBEDDING_BATCH_SIZE,
            default: serde_json::json!(64),
            json_type: JsonType::Int,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_EMBEDDING_MAX_CONCURRENCY,
            default: serde_json::json!(4),
            json_type: JsonType::Int,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_EMBEDDING_TIMEOUT_SECS,
            default: serde_json::json!(60),
            json_type: JsonType::Int,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_EMBEDDING_HNSW_THRESHOLD,
            default: serde_json::json!(EMBED_DEFAULT_HNSW_THRESHOLD),
            json_type: JsonType::Int,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_EMBEDDING_IVFFLAT_THRESHOLD,
            default: serde_json::json!(EMBED_DEFAULT_IVFFLAT_THRESHOLD),
            json_type: JsonType::Int,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_ACCESS_MODE,
            default: serde_json::json!(ACCESS_MODE_OPEN),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                env_locked: true,
                api_immutable: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_ACCESS_RATE_LIMIT_RPS,
            default: serde_json::json!(100),
            json_type: JsonType::Int,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_ACCESS_RATE_LIMIT_BURST,
            default: serde_json::json!(200),
            json_type: JsonType::Int,
            policy: SettingPolicy {
                security_sensitive: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_ACCESS_ADMIN_PASSWORD,
            default: serde_json::json!(""),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                env_locked: true,
                secret: true,
                api_immutable: true,
                ..Default::default()
            },
        },
        SettingDef {
            key: KEY_ACCESS_REQUIRE_AUTH_FOR_READS,
            default: serde_json::json!(false),
            json_type: JsonType::Bool,
            policy: SettingPolicy::default(),
        },
        SettingDef {
            key: KEY_GENERAL_TRUSTED_PROXY_CIDRS,
            default: serde_json::json!(""),
            json_type: JsonType::Str,
            policy: SettingPolicy {
                config_only: true,
                ..Default::default()
            },
        },
    ]
});

/// Look up a setting's policy row by key (`None` = unknown key).
pub fn def(key: &str) -> Option<&SettingDef> {
    SETTING_DEFS.iter().find(|d| d.key == key)
}

/// The set of all known setting keys (identical to the `default_settings()`
/// key set — the bidirectional test pins that).
pub fn known_keys() -> std::collections::HashSet<&'static str> {
    SETTING_DEFS.iter().map(|d| d.key).collect()
}

/// Whether mutating `key` requires authentication (rejected in open mode).
pub fn is_security_sensitive(key: &str) -> bool {
    def(key).is_some_and(|d| d.policy.security_sensitive)
}

/// Default settings seeded on first run.
///
/// ```
/// let s = zimservice::settings::default_settings();
/// assert!(s.get(zimservice::settings::KEY_ACCESS_MODE).is_some());
/// assert!(s.get(zimservice::settings::KEY_GENERAL_ZIM_DIR).is_some());
/// assert!(s.len() >= 41);
/// ```
pub fn default_settings() -> HashMap<String, serde_json::Value> {
    SETTING_DEFS
        .iter()
        .map(|d| (d.key.to_string(), d.default.clone()))
        .collect()
}

/// Whether a key holds a secret that must be redacted from responses.
pub(crate) fn is_secret_key(key: &str) -> bool {
    def(key).is_some_and(|d| d.policy.secret)
}

/// Redact a secret value for API responses: non-empty string → `"***"`.
pub(crate) fn redact(key: &str, value: &serde_json::Value) -> serde_json::Value {
    if is_secret_key(key) && value.as_str().is_some_and(|s| !s.is_empty()) {
        serde_json::json!("***")
    } else {
        value.clone()
    }
}

/// B4: normalize an `embedding.endpoint` value before storing — strip a
/// case-insensitive trailing `/embeddings`, then trailing `/` (the embed client
/// always re-appends `/embeddings`, so a user-supplied suffix would double it).
/// `http://h/v1/embeddings/` → `http://h/v1`; `http://h/v1` is unchanged.
pub(crate) fn normalize_embedding_endpoint(value: &str) -> String {
    const SUFFIX: &str = "/embeddings";
    let n = SUFFIX.len();
    let v = value.trim_end_matches('/');
    // Boundary-safe case-insensitive suffix test: `get(..)` returns `None` at a
    // non-char-boundary (no panic); a match on the ASCII suffix is always at a
    // boundary. `saturating_sub` guards the shorter-than-suffix case (compares
    // the whole string → no false positive).
    let tail = v.get(v.len().saturating_sub(n)..);
    let stripped = if tail.is_some_and(|t| t.eq_ignore_ascii_case(SUFFIX)) {
        &v[..v.len() - n]
    } else {
        v
    };
    stripped.trim_end_matches('/').to_string()
}

/// Check a setting value against its expected JSON type (the `json_type`
/// column of [`SETTING_DEFS`]; `None` = unknown key or no mismatch).
pub(crate) fn type_mismatch(key: &str, value: &serde_json::Value) -> Option<String> {
    let expected = def(key)?.json_type;
    let ok = match expected {
        JsonType::Str => value.is_string(),
        JsonType::Int => value.is_i64() || value.is_u64(),
        JsonType::Num => value.is_number(),
        JsonType::Bool => value.is_boolean(),
    };
    if ok {
        None
    } else {
        let want = match expected {
            JsonType::Str => "a string",
            JsonType::Int => "an integer",
            JsonType::Num => "a number",
            JsonType::Bool => "a boolean",
        };
        Some(format!(
            "{key}: expected {want}, got {}",
            value_type_name(value)
        ))
    }
}

fn value_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

/// Apply the startup env snapshot to a settings map (non-empty wins).
/// `embedding.dimension` is stored as a raw string in the snapshot and parsed
/// here; unparseable values are ignored (same as the pre-snapshot code). Pure
/// and DB-free so the override semantics are unit-testable.
pub(crate) fn apply_env_snapshot(
    map: &mut HashMap<String, serde_json::Value>,
    snapshot: &HashMap<String, String>,
) {
    for (key, raw) in snapshot {
        if raw.is_empty() {
            continue;
        }
        if key == KEY_EMBEDDING_DIMENSION {
            if let Ok(n) = raw.parse::<i64>() {
                map.insert(key.clone(), serde_json::json!(n));
            }
        } else {
            map.insert(key.clone(), serde_json::json!(raw.clone()));
        }
    }
}

/// Overwrite the `general.*` display keys from the process `Config`, the
/// authoritative source (the settings table is a stale second copy). Typed
/// JSON: `general.port` is written as a number so it passes [`type_mismatch`]
/// and the UI renders a number. Pure + DB-free, like [`apply_env_snapshot`].
pub(crate) fn sync_config_values(
    map: &mut HashMap<String, serde_json::Value>,
    config: &crate::config::Config,
) {
    map.insert(
        KEY_GENERAL_ZIM_DIR.into(),
        serde_json::json!(config.zim_dir.to_string_lossy()),
    );
    map.insert(
        KEY_GENERAL_HOST.into(),
        serde_json::json!(config.host.as_str()),
    );
    map.insert(
        KEY_GENERAL_PORT.into(),
        serde_json::json!(config.port as i64),
    );
    map.insert(
        KEY_GENERAL_LOG_LEVEL.into(),
        serde_json::json!(config.log_level.as_str()),
    );
}

/// The seed default for `key` from `default_settings()` — the single source
/// of truth for the typed accessors' fallbacks (previously each literal had
/// to be kept in sync with the seed by hand — D1c). Backed by a `LazyLock`
/// so the seed map is built once.
pub(crate) fn default_value(key: &str) -> serde_json::Value {
    static DEFAULTS: std::sync::LazyLock<HashMap<String, serde_json::Value>> =
        std::sync::LazyLock::new(default_settings);
    DEFAULTS
        .get(key)
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The 8 typed-accessor keys (5 `typed_getter!` instantiations + the 3
    /// hand-written clamped/Option accessors) with the `JsonType` family their
    /// Rust return type requires: usize/i32/u64 → Int (a Num row also admits
    /// integers), f64 → Num, bool → Bool, String → Str.
    const ACCESSOR_KEYS: &[(&str, JsonType)] = &[
        (KEY_SEARCH_TRGM_THRESHOLD, JsonType::Num),
        (KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS, JsonType::Bool),
        (KEY_TORRENT_ENABLED, JsonType::Bool),
        (KEY_TORRENT_MAX_ACTIVE, JsonType::Int),
        (KEY_TORRENT_POLL_SECS, JsonType::Int),
        (KEY_EMBEDDING_ENABLED, JsonType::Bool),
        (KEY_ACCESS_MODE, JsonType::Str),
        (KEY_ACCESS_REQUIRE_AUTH_FOR_READS, JsonType::Bool),
    ];

    #[test]
    fn default_settings_covers_all_typed_getter_keys() {
        // Safety net for the `typed_getter!` panic arm + table↔accessor
        // agreement: every typed-accessor key must exist in `SETTING_DEFS` and
        // its row's `json_type` must be able to serve the accessor's Rust
        // type (int-typed accessors accept Int or Num; the exact `JsonType`
        // expected per Rust type is pinned by the pairs list above). Add a
        // pair whenever a new typed accessor is introduced.
        for (key, want) in ACCESSOR_KEYS {
            let d = def(key)
                .unwrap_or_else(|| panic!("missing SETTING_DEFS row for accessor key {key:?}"));
            let got = d.json_type;
            assert!(
                got == *want || (matches!(want, JsonType::Int) && got == JsonType::Num),
                "accessor {key:?} wants {want:?}, SETTING_DEFS row says {got:?}"
            );
        }
    }

    #[test]
    fn setting_defs_bidirectional_seed_coverage() {
        // (a) Every `SETTING_DEFS` key appears in `default_settings()` with an
        // equal `Value` and a `default` whose JSON type matches `json_type`.
        // (b) Every `default_settings()` key appears in `SETTING_DEFS`.
        // Drift between the table and the DB seed fails CI here.
        let seeds = default_settings();
        let keys: std::collections::HashSet<&'static str> =
            SETTING_DEFS.iter().map(|d| d.key).collect();
        for d in SETTING_DEFS.iter() {
            let key = d.key;
            let v = seeds
                .get(key)
                .unwrap_or_else(|| panic!("missing seed for {key:?}"));
            assert_eq!(v, &d.default, "seed default drifted for {key:?}");
            let ok = match d.json_type {
                JsonType::Str => d.default.is_string(),
                JsonType::Int => d.default.is_i64() || d.default.is_u64(),
                JsonType::Num => d.default.is_number(),
                JsonType::Bool => d.default.is_boolean(),
            };
            assert!(
                ok,
                "default for {key:?} does not match json_type {:?}",
                d.json_type
            );
        }
        for key in seeds.keys() {
            assert!(keys.contains(key.as_str()), "un-tabled seed key {key:?}");
        }
    }
}
