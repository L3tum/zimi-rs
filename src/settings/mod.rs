//! Runtime settings store: Postgres-backed, env-locked, with typed accessors.
//!
//! `SettingsCache` seeds defaults on first load, locks env-provided keys so
//! the API can't override them, redacts secrets on unauthenticated reads, and
//! caches expensive verifies (admin password / access token) behind a TTL.
//!
//! Admin-password hashing (KDF) and the verified-token cache live in `auth`;
//! they are re-exported so `crate::settings::hash_admin_password` etc. keep
//! resolving (used by `mcp`).
//!
//! Submodules: `defs` (setting table + seeds), `snapshots` (one-pass read
//! snapshots), `cache` (`SettingsCache` itself), `encrypt` (SEC-L5 at-rest
//! encryption of the secret settings under `SECURITY_KEY`).

pub(crate) mod auth;
mod auth_service;
mod cache;
mod defs;
pub(crate) mod encrypt;
mod snapshots;

pub use auth::{hash_admin_password, is_legacy_password, verify_admin_password};
pub use auth_service::SettingsAuth;
pub use cache::SettingsCache;
pub use defs::{
    def, default_settings, is_security_sensitive, known_keys, require_reads_startup_default,
    JsonType, SettingDef, SettingPolicy, ACCESS_MODE_OPEN, ACCESS_MODE_PASSWORD, DEFAULT_MAX_BYTES,
    EMBED_DEFAULT_DIMENSION, EMBED_DEFAULT_HNSW_THRESHOLD, EMBED_DEFAULT_MODEL, SETTING_DEFS,
};
// Setting-key string constants (single source of truth for key strings).
pub use defs::{
    KEY_ACCESS_ADMIN_PASSWORD, KEY_ACCESS_MODE, KEY_ACCESS_RATE_LIMIT_BURST,
    KEY_ACCESS_RATE_LIMIT_RPS, KEY_ACCESS_READ_ONLY_TOKEN, KEY_ACCESS_REQUIRE_AUTH_FOR_READS,
    KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS, KEY_DOWNLOADS_MAX_BYTES, KEY_EMBEDDING_API_KEY,
    KEY_EMBEDDING_BATCH_SIZE, KEY_EMBEDDING_DIMENSION, KEY_EMBEDDING_ENABLED,
    KEY_EMBEDDING_ENDPOINT, KEY_EMBEDDING_HNSW_THRESHOLD, KEY_EMBEDDING_MAX_CONCURRENCY,
    KEY_EMBEDDING_MODEL, KEY_EMBEDDING_TIMEOUT_SECS, KEY_GENERAL_CORS_ORIGINS, KEY_GENERAL_HOST,
    KEY_GENERAL_LOG_LEVEL, KEY_GENERAL_PORT, KEY_GENERAL_TRUSTED_PROXY_CIDRS, KEY_GENERAL_ZIM_DIR,
    KEY_SEARCH_DEFAULT_LIMIT, KEY_SEARCH_FTS_WEIGHT, KEY_SEARCH_MAX_LIMIT,
    KEY_SEARCH_TRGM_THRESHOLD, KEY_SEARCH_TRGM_WEIGHT, KEY_SEARCH_VECTOR_WEIGHT,
    KEY_TORRENT_ALLOW_PRIVATE_NETWORKS, KEY_TORRENT_AUTO_UPDATE, KEY_TORRENT_CATEGORY,
    KEY_TORRENT_ENABLED, KEY_TORRENT_FILE_STRATEGY, KEY_TORRENT_KEEP_COMPLETED,
    KEY_TORRENT_MAX_ACTIVE, KEY_TORRENT_OPDS_URL, KEY_TORRENT_PASSWORD, KEY_TORRENT_POLL_SECS,
    KEY_TORRENT_SAVE_PATH, KEY_TORRENT_SEED_RATIO, KEY_TORRENT_URL, KEY_TORRENT_USERNAME,
};
pub use snapshots::{PollerParams, SearchParamsSnapshot};
