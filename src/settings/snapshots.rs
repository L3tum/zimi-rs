//! One-pass read snapshots of the settings the search path and the
//! torrent poller consume (no torn mixes across a concurrent mutation),
//! plus the simple clamped scalar accessors.
//!
//! **Snapshot discipline (2026-09-04, ARCH minor #5):** if a request/tick
//! reads *multiple related* settings, take them from one snapshot
//! ([`SearchParamsSnapshot`] / [`PollerParams`]) so all keys come from a
//! single point in time — never mix per-key reads of the same logical
//! group within one request. A single-key read via a typed accessor is
//! fine (one key cannot tear). One documented exemption: a reader that is
//! *generation-gated* (re-reads only when
//! [`SettingsCache::generation`](super::SettingsCache::generation) changed,
//! e.g. the rate limiter's `rps`/`burst` pair) may use per-key reads — a
//! torn mix there self-corrects on the next generation change.

use std::collections::HashMap;

use super::cache::SettingsCache;
use super::defs::{
    default_value, DEFAULT_MAX_BYTES, EMBED_DEFAULT_DIMENSION, EMBED_DEFAULT_MODEL,
    KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS, KEY_DOWNLOADS_MAX_BYTES, KEY_EMBEDDING_API_KEY,
    KEY_EMBEDDING_BATCH_SIZE, KEY_EMBEDDING_DIMENSION, KEY_EMBEDDING_ENABLED,
    KEY_EMBEDDING_ENDPOINT, KEY_EMBEDDING_MODEL, KEY_SEARCH_DEFAULT_LIMIT, KEY_SEARCH_FTS_WEIGHT,
    KEY_SEARCH_MAX_LIMIT, KEY_SEARCH_TRGM_THRESHOLD, KEY_SEARCH_TRGM_WEIGHT,
    KEY_SEARCH_VECTOR_WEIGHT, KEY_TORRENT_ALLOW_PRIVATE_NETWORKS, KEY_TORRENT_AUTO_UPDATE,
    KEY_TORRENT_CATEGORY, KEY_TORRENT_FILE_STRATEGY, KEY_TORRENT_KEEP_COMPLETED,
    KEY_TORRENT_OPDS_URL, KEY_TORRENT_POLL_SECS, KEY_TORRENT_SAVE_PATH, KEY_TORRENT_SEED_RATIO,
    KEY_TORRENT_URL,
};

/// Floor for the `search.trgm_threshold` clamp.
pub(crate) const TRGM_THRESHOLD_FLOOR: f64 = 0.3;

/// Clamp a trgm threshold value to [`TRGM_THRESHOLD_FLOOR`].
///
/// M2: floor at 0.3 — a lower value degrades the trgm similarity branch to a
/// near full-scan (the GiST index only prunes above ~0.3) and a misconfigured
/// `search.trgm_threshold` of 0 would match everything.
pub(crate) fn floor_trgm_threshold(v: f64) -> f64 {
    v.max(TRGM_THRESHOLD_FLOOR)
}

/// One-pass snapshot of every setting the search path needs, read under a
/// single `cache.read()` so a `search()` never sees a torn mix of values
/// across a concurrent mutation. Carries [`SettingsCache::generation`] so
/// callers can detect change without per-key re-reads.
#[derive(Clone, Debug, Default)]
pub struct SearchParamsSnapshot {
    /// Cache generation the values were read under.
    pub generation: u64,
    /// `search.default_limit` (seed default when unset).
    pub default_limit: usize,
    /// `search.max_limit`.
    pub max_limit: usize,
    /// Clamped `search.fts_weight`.
    pub fts_weight: f64,
    /// Clamped `search.trgm_weight`.
    pub trgm_weight: f64,
    /// Clamped `search.vector_weight`.
    pub vector_weight: f64,
    /// Clamped `search.trgm_threshold` (floored at 0.3).
    pub trgm_threshold: f64,
    /// Whether the vector branch is active (`embedding.enabled`).
    pub embedding_enabled: bool,
    /// `embedding.endpoint` (normalised on write).
    pub embed_endpoint: String,
    /// `embedding.api_key` (empty when unset).
    pub embed_api_key: String,
    /// `embedding.model`.
    pub embed_model: String,
    /// `embedding.dimension`.
    pub embed_dimension: u32,
    /// `embedding.batch_size`.
    pub embed_batch_size: usize,
}

/// Clamp a search weight from settings to a sane range.
///
/// A non-finite value (NaN/inf) or a missing/mistyped setting falls back to
/// `dflt`; a finite value is clamped to `[0.0, 10.0]` so an operator can't
/// push a weight to e.g. 1e9 (dominating the hybrid score) or a negative
/// (which pgvector/FTS treat as a ranking bug). Module-level (not an `impl`
/// method) so it is unit-testable in isolation.
fn clamp_weight(v: Option<f64>, dflt: f64) -> f64 {
    match v {
        Some(x) if x.is_finite() => x.clamp(0.0, 10.0),
        _ => dflt,
    }
}

/// Read a single key from a snapshot's map, deserialising into `T`.
/// Returns `None` when the key is absent or its value is mistyped.
///
/// This is the generic (slow) path: `from_value` takes the `Value` by
/// value, so each call clones it. The per-search snapshot keys use the
/// zero-clone [`FastRead`] variants instead (PERF F2); this stays the
/// reference semantics the fast path falls back to.
fn read<T: serde::de::DeserializeOwned>(
    cache: &HashMap<String, serde_json::Value>,
    key: &str,
) -> Option<T> {
    cache
        .get(key)
        .and_then(|v| serde_json::from_value::<T>(v.clone()).ok())
}

/// Zero-clone read of a cached `Value` for the snapshot fast path (PERF
/// F2): `Self` is materialised from the borrowed `Value` without cloning
/// it (the old generic [`read`] cloned one `Value` per key per call — the
/// search snapshot takes one per search, per key).
///
/// Only implemented for the snapshot's primitive/string key types; for
/// those, `from_cached` is exactly as strict as
/// `serde_json::from_value::<T>` (same `Some`/`None` outcome, equal value
/// when `Some`), so [`read_fast`]'s fallback to [`read`] is defensive and
/// never changes the result.
trait FastRead: serde::de::DeserializeOwned {
    /// Materialise `Self` from a borrowed cached `Value` (no clone).
    fn from_cached(v: &serde_json::Value) -> Option<Self>;
}

impl FastRead for f64 {
    fn from_cached(v: &serde_json::Value) -> Option<Self> {
        v.as_f64()
    }
}

impl FastRead for bool {
    fn from_cached(v: &serde_json::Value) -> Option<Self> {
        v.as_bool()
    }
}

impl FastRead for u64 {
    fn from_cached(v: &serde_json::Value) -> Option<Self> {
        v.as_u64()
    }
}

impl FastRead for u32 {
    fn from_cached(v: &serde_json::Value) -> Option<Self> {
        v.as_u64().and_then(|n| u32::try_from(n).ok())
    }
}

impl FastRead for usize {
    fn from_cached(v: &serde_json::Value) -> Option<Self> {
        v.as_u64().and_then(|n| usize::try_from(n).ok())
    }
}

impl FastRead for String {
    fn from_cached(v: &serde_json::Value) -> Option<Self> {
        // One string allocation — no `Value` clone, no serde round-trip.
        v.as_str().map(str::to_owned)
    }
}

/// Zero-clone counterpart of [`read`] (PERF F2): the primitive fast path
/// first, falling back to the generic [`read`] (the fallback is defensive —
/// see [`FastRead`] — and never changes the result). Same semantics: `None`
/// when the key is absent or mistyped.
fn read_fast<T: FastRead>(cache: &HashMap<String, serde_json::Value>, key: &str) -> Option<T> {
    match cache.get(key) {
        Some(v) => T::from_cached(v).or_else(|| read::<T>(cache, key)),
        None => None,
    }
}

/// The `SETTING_DEFS`-declared default for a key, deserialised into `T`.
/// The `expect` is an unreachable last-resort while a test pins every key's
/// seed (`default_settings_seeds_all_typed_accessor_keys`).
fn seed_default<T: serde::de::DeserializeOwned>(key: &str) -> T {
    serde_json::from_value::<T>(default_value(key))
        .unwrap_or_else(|_| panic!("broken settings seed for {}", key))
}

/// Zero-clone successor of the old generic `read_or_seed` (PERF F2): same
/// seed/default behaviour (the same `seed_default` fallback, the same read
/// semantics via [`read_fast`]'s defensive fallback to [`read`]), the
/// cached-value path is just clone-free. Used by the per-search / per-tick
/// snapshots for every key whose type implements [`FastRead`].
fn read_fast_or_seed<T: FastRead>(cache: &HashMap<String, serde_json::Value>, key: &str) -> T {
    read_fast::<T>(cache, key).unwrap_or_else(|| seed_default::<T>(key))
}

/// One read-lock pass over every setting the poller reads per tick/per call
/// (PERF-3/PERF-12). Captured once at the top of `tick()` / `reconcile()` /
/// `opds_check()` / `direct_download()` / the `run()` startup resolve so the
/// body reuses materialised values instead of re-taking the RwLock + cloning
/// a `Value` + deserialising for each accessor.
///
/// **N4:** the poller's loop cadence (`torrent.poll_secs`, via
/// [`SettingsCache::torrent_poll_secs`]) is read *outside* this snapshot — a
/// single-key value read once per `run()` loop iteration rather than part of
/// this per-tick multi-key set, so it is the documented permitted exception.
#[derive(Debug, Clone, PartialEq)]
pub struct PollerParams {
    /// `torrent.url` — effective qB endpoint; `None` when empty/missing
    /// (qBittorrent disabled). Feeds `resolve_torrent`.
    pub qbit_url: Option<String>,
    /// `torrent.category` — qB category filter (enqueue budget, add_torrent,
    /// startup adopt).
    pub category: String,
    /// `torrent.save_path` — default save path for `add_torrent`.
    pub save_path: String,
    /// `torrent.seed_ratio` — `None` = let qB decide (no default).
    pub seed_ratio: Option<f64>,
    /// `torrent.file_strategy` — `hardlink` | `copy` install strategy.
    pub file_strategy: String,
    /// `torrent.keep_completed` — keep seeding vs delete after install.
    pub keep_completed: bool,
    /// `torrent.auto_update` — gate for the OPDS catalog check.
    pub opds_auto_update: bool,
    /// `torrent.opds_url` — Kiwix catalog URL.
    pub opds_url: String,
    /// `downloads.allow_private_networks` — SSRF guard (validate_download_url,
    /// resolve_download_host) at enqueue / OPDS / direct-download entry.
    pub allow_private_networks: bool,
    /// `torrent.allow_private_networks` — SEC M-1: opt-in to point the
    /// qBittorrent Web API at a private (RFC1918/ULA/CGNAT) address. Gated at
    /// `connect_qbit` (host pin + redirect policy); loopback stays admitted and
    /// the always-blocked ranges (metadata/link-local/doc/NAT64) stay blocked.
    pub qbit_allow_private: bool,
    /// `downloads.max_bytes` — download size cap (floor 1; no upper ceiling,
    /// default effectively unbounded).
    pub max_bytes: u64,
}

impl SettingsCache {
    #[cfg(test)]
    fn search_trgm_threshold(&self) -> f64 {
        // See [`floor_trgm_threshold`] for why the floor exists.
        let v = self
            .get_typed(KEY_SEARCH_TRGM_THRESHOLD)
            .or_else(|| default_value(KEY_SEARCH_TRGM_THRESHOLD).as_f64())
            .unwrap_or(TRGM_THRESHOLD_FLOOR);
        floor_trgm_threshold(v)
    }

    /// Read every search-path setting in **one** `cache.read()` pass into a
    /// [`SearchParamsSnapshot`]. Each key is deserialized inline from the map
    /// under the single guard (no per-key `Value` clone escapes the function),
    /// applying the same clamps/defaults as the individual accessors — the
    /// `trgm_threshold` 0.3 floor and the `EMBED_DEFAULT_DIMENSION` fallback.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn search_params_snapshot(&self) -> SearchParamsSnapshot {
        let cache = self
            .inner
            .cache
            .read()
            .expect("settings cache lock poisoned");
        // WP5.2: single-source every fallback from the SETTING_DEFS-driven
        // `default_value` (via `read`/`read_or_seed`) — `SETTING_DEFS` is the
        // one source of truth for defaults, so a missing/mistyped key falls
        // back to the declared default. The `expect` last-resort is
        // unreachable while a test pins every key's seed
        // (`default_settings_seeds_all_typed_accessor_keys`).
        let threshold = read_fast_or_seed::<f64>(&cache, KEY_SEARCH_TRGM_THRESHOLD);
        SearchParamsSnapshot {
            generation: self.generation(),
            default_limit: read_fast_or_seed::<usize>(&cache, KEY_SEARCH_DEFAULT_LIMIT),
            max_limit: read_fast_or_seed::<usize>(&cache, KEY_SEARCH_MAX_LIMIT),
            fts_weight: clamp_weight(
                read_fast::<f64>(&cache, KEY_SEARCH_FTS_WEIGHT),
                seed_default::<f64>(KEY_SEARCH_FTS_WEIGHT),
            ),
            trgm_weight: clamp_weight(
                read_fast::<f64>(&cache, KEY_SEARCH_TRGM_WEIGHT),
                seed_default::<f64>(KEY_SEARCH_TRGM_WEIGHT),
            ),
            vector_weight: clamp_weight(
                read_fast::<f64>(&cache, KEY_SEARCH_VECTOR_WEIGHT),
                seed_default::<f64>(KEY_SEARCH_VECTOR_WEIGHT),
            ),
            // See [`floor_trgm_threshold`] for why the floor exists.
            trgm_threshold: floor_trgm_threshold(threshold),
            embedding_enabled: read_fast_or_seed::<bool>(&cache, KEY_EMBEDDING_ENABLED),
            embed_endpoint: read_fast_or_seed::<String>(&cache, KEY_EMBEDDING_ENDPOINT),
            embed_api_key: read_fast::<String>(&cache, KEY_EMBEDDING_API_KEY).unwrap_or_default(),
            embed_model: read_fast::<String>(&cache, KEY_EMBEDDING_MODEL)
                .unwrap_or_else(|| EMBED_DEFAULT_MODEL.to_string()),
            embed_dimension: read_fast::<u32>(&cache, KEY_EMBEDDING_DIMENSION)
                .unwrap_or(EMBED_DEFAULT_DIMENSION),
            embed_batch_size: read_fast_or_seed::<usize>(&cache, KEY_EMBEDDING_BATCH_SIZE),
        }
    }

    /// One read-lock pass over every setting the poller reads per tick/per call
    /// (PERF-3/PERF-12). Captured once at the top of `tick()` / `reconcile()` /
    /// `opds_check()` / `direct_download()` / the `run()` startup resolve so the
    /// body reuses materialised values instead of re-taking the RwLock + cloning
    /// a `Value` + deserialising for each accessor.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn poller_params_snapshot(&self) -> PollerParams {
        let cache = self
            .inner
            .cache
            .read()
            .expect("settings cache lock poisoned");
        let str_v = |k: &str| -> String {
            read_fast::<String>(&cache, k)
                .or_else(|| serde_json::from_value::<String>(default_value(k)).ok())
                .unwrap_or_default()
        };
        PollerParams {
            qbit_url: {
                let url = str_v(KEY_TORRENT_URL);
                if url.is_empty() {
                    None
                } else {
                    Some(url)
                }
            },
            category: str_v(KEY_TORRENT_CATEGORY),
            save_path: str_v(KEY_TORRENT_SAVE_PATH),
            seed_ratio: read_fast::<f64>(&cache, KEY_TORRENT_SEED_RATIO),
            file_strategy: str_v(KEY_TORRENT_FILE_STRATEGY),
            keep_completed: read_fast_or_seed::<bool>(&cache, KEY_TORRENT_KEEP_COMPLETED),
            opds_auto_update: read_fast_or_seed::<bool>(&cache, KEY_TORRENT_AUTO_UPDATE),
            opds_url: str_v(KEY_TORRENT_OPDS_URL),
            allow_private_networks: read_fast_or_seed::<bool>(
                &cache,
                KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS,
            ),
            qbit_allow_private: read_fast_or_seed::<bool>(
                &cache,
                KEY_TORRENT_ALLOW_PRIVATE_NETWORKS,
            ),
            max_bytes: read_fast::<u64>(&cache, KEY_DOWNLOADS_MAX_BYTES)
                .or_else(|| {
                    serde_json::from_value::<u64>(default_value(KEY_DOWNLOADS_MAX_BYTES)).ok()
                })
                .unwrap_or(DEFAULT_MAX_BYTES)
                .max(1),
        }
    }

    /// Loop-cadence poll interval (seconds), floored to 1.
    ///
    /// **N4 — documented snapshot exception.** This is a *single-key* read via
    /// `get_typed`, deliberately **not** folded into
    /// [`poller_params_snapshot`](Self::poller_params_snapshot). Per the
    /// snapshot discipline at the top of this module, a single-key read cannot
    /// tear and is explicitly permitted. It is the poller's loop cadence —
    /// read once per iteration in `torrent::poller`'s `run()` loop (not part
    /// of the per-tick multi-key [`PollerParams`]) — so the "one poller value
    /// outside the snapshot" is intentional, not a latent inconsistency.
    pub fn torrent_poll_secs(&self) -> u64 {
        self.get_typed(KEY_TORRENT_POLL_SECS)
            .or_else(|| default_value(KEY_TORRENT_POLL_SECS).as_u64())
            .unwrap_or_else(|| panic!("broken settings seed for torrent.poll_secs"))
            .max(1)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::settings::defs::default_settings;
    use crate::testing::dead_pool;
    // ── WI-35: search snapshot reads all keys in one pass ─────────────────

    fn snapshot_settings(map: HashMap<String, serde_json::Value>) -> SettingsCache {
        SettingsCache::new_with_map(dead_pool(), map, HashMap::new())
    }

    #[test]
    fn search_params_snapshot_reads_all_keys_and_clamps_floor() {
        let mut map = default_settings();
        map.insert(KEY_SEARCH_DEFAULT_LIMIT.into(), serde_json::json!(25));
        // 0.15 is below the 0.3 floor → clamped up.
        map.insert(KEY_SEARCH_TRGM_THRESHOLD.into(), serde_json::json!(0.15));
        let settings = snapshot_settings(map);
        let snap = settings.search_params_snapshot();
        assert_eq!(snap.default_limit, 25);
        assert_eq!(snap.max_limit, 50);
        assert!((snap.fts_weight - 0.6).abs() < 1e-9);
        assert!((snap.trgm_weight - 0.4).abs() < 1e-9);
        assert!((snap.vector_weight - 0.5).abs() < 1e-9);
        assert!(
            (snap.trgm_threshold - 0.3).abs() < 1e-9,
            "floor clamp 0.15 → 0.3"
        );
        assert!(!snap.embedding_enabled);
        assert_eq!(snap.embed_endpoint, "http://localhost:11434/v1");
        assert_eq!(snap.embed_api_key, "");
        assert_eq!(snap.embed_model, EMBED_DEFAULT_MODEL);
        assert_eq!(snap.embed_dimension, EMBED_DEFAULT_DIMENSION);
        assert_eq!(snap.embed_batch_size, 64);
    }

    #[test]
    fn search_params_snapshot_missing_key_falls_back_to_default() {
        let mut map = default_settings();
        map.remove(KEY_SEARCH_FTS_WEIGHT);
        map.remove(KEY_EMBEDDING_DIMENSION);
        let settings = snapshot_settings(map);
        let snap = settings.search_params_snapshot();
        assert!((snap.fts_weight - 0.6).abs() < 1e-9, "fts_weight → default");
        assert_eq!(
            snap.embed_dimension, EMBED_DEFAULT_DIMENSION,
            "missing dimension → EMBED_DEFAULT_DIMENSION"
        );
    }

    #[test]
    fn search_params_snapshot_clamps_weights_to_range() {
        // Above the 10.0 ceiling → 10.0.
        let mut map = default_settings();
        map.insert(KEY_SEARCH_FTS_WEIGHT.into(), serde_json::json!(12.5));
        let snap = snapshot_settings(map).search_params_snapshot();
        assert!((snap.fts_weight - 10.0).abs() < 1e-9, "12.5 → 10.0");

        // Below the 0.0 floor → 0.0.
        let mut map = default_settings();
        map.insert(KEY_SEARCH_TRGM_WEIGHT.into(), serde_json::json!(-1.0));
        let snap = snapshot_settings(map).search_params_snapshot();
        assert!((snap.trgm_weight - 0.0).abs() < 1e-9, "-1 → 0.0");

        // In-range value is preserved.
        let mut map = default_settings();
        map.insert(KEY_SEARCH_VECTOR_WEIGHT.into(), serde_json::json!(0.75));
        let snap = snapshot_settings(map).search_params_snapshot();
        assert!((snap.vector_weight - 0.75).abs() < 1e-9, "0.75 → 0.75");
    }

    #[test]
    fn search_params_snapshot_mistyped_weight_falls_back_to_default() {
        // A string where a number is expected → the default, not 0 or a panic.
        let mut map = default_settings();
        map.insert(KEY_SEARCH_FTS_WEIGHT.into(), serde_json::json!("bogus"));
        let snap = snapshot_settings(map).search_params_snapshot();
        assert!((snap.fts_weight - 0.6).abs() < 1e-9, "bogus → default 0.6");

        // NaN/inf are not representable in JSON, so exercise them directly on
        // the helper: non-finite values fall back to the default.
        assert_eq!(clamp_weight(Some(f64::NAN), 0.6), 0.6);
        assert_eq!(clamp_weight(Some(f64::INFINITY), 0.6), 0.6);
        assert_eq!(clamp_weight(None, 0.4), 0.4);
    }

    #[test]
    fn search_params_snapshot_rides_generation_counter() {
        let settings = snapshot_settings(default_settings());
        assert_eq!(settings.generation(), 0);
        let a = settings.search_params_snapshot();
        let b = settings.search_params_snapshot();
        assert_eq!(
            a.generation,
            settings.generation(),
            "snapshot carries generation"
        );
        assert_eq!(
            b.generation, a.generation,
            "no mutation → stable generation"
        );
    }

    // ── search_trgm_threshold floor (M2) ─────────────────────────────────

    fn cache_with_threshold(v: Option<serde_json::Value>) -> SettingsCache {
        let mut values = default_settings();
        match v {
            Some(val) => {
                values.insert(KEY_SEARCH_TRGM_THRESHOLD.into(), val);
            }
            None => {
                values.remove(KEY_SEARCH_TRGM_THRESHOLD);
            }
        }
        SettingsCache::new_with_map(dead_pool(), values, HashMap::new())
    }

    #[test]
    fn trgm_threshold_floor_clamps_low_values() {
        // 0.0 → clamped to floor 0.3
        assert!(
            (cache_with_threshold(Some(serde_json::json!(0.0))).search_trgm_threshold() - 0.3)
                .abs()
                < f64::EPSILON
        );
        // 0.15 → clamped to 0.3
        assert!(
            (cache_with_threshold(Some(serde_json::json!(0.15))).search_trgm_threshold() - 0.3)
                .abs()
                < f64::EPSILON
        );
        // 0.31 → unchanged (above floor)
        assert!(
            (cache_with_threshold(Some(serde_json::json!(0.31))).search_trgm_threshold() - 0.31)
                .abs()
                < f64::EPSILON
        );
        // 0.9 → unchanged
        assert!(
            (cache_with_threshold(Some(serde_json::json!(0.9))).search_trgm_threshold() - 0.9)
                .abs()
                < f64::EPSILON
        );
        // missing → default 0.3
        assert!((cache_with_threshold(None).search_trgm_threshold() - 0.3).abs() < f64::EPSILON);
    }

    // ── PERF-12: poller_params_snapshot ──────────────────────────────────────

    #[test]
    fn poller_params_snapshot_matches_accessors() {
        // Build a cache with non-default values for every poller-relevant key.
        let mut m = std::collections::HashMap::new();
        m.insert(KEY_TORRENT_URL.into(), serde_json::json!("http://qb:8080"));
        m.insert(KEY_TORRENT_CATEGORY.into(), serde_json::json!("custom_cat"));
        m.insert(KEY_TORRENT_SAVE_PATH.into(), serde_json::json!("/srv/zim"));
        m.insert(KEY_TORRENT_SEED_RATIO.into(), serde_json::json!(2.5));
        m.insert(KEY_TORRENT_FILE_STRATEGY.into(), serde_json::json!("copy"));
        m.insert(KEY_TORRENT_KEEP_COMPLETED.into(), serde_json::json!(false));
        m.insert(KEY_TORRENT_AUTO_UPDATE.into(), serde_json::json!(true));
        m.insert(
            KEY_TORRENT_OPDS_URL.into(),
            serde_json::json!("https://example/opds"),
        );
        m.insert(
            KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!(true),
        );
        m.insert(
            KEY_TORRENT_ALLOW_PRIVATE_NETWORKS.into(),
            serde_json::json!(true),
        );
        m.insert(
            KEY_DOWNLOADS_MAX_BYTES.into(),
            serde_json::json!(5_000_000_000u64),
        );
        let cache = SettingsCache::new_with_map(dead_pool(), m, std::collections::HashMap::new());
        let snap = cache.poller_params_snapshot();
        assert_eq!(snap.qbit_url.as_deref(), Some("http://qb:8080"));
        assert_eq!(snap.category, "custom_cat");
        assert_eq!(snap.save_path, "/srv/zim");
        assert_eq!(snap.seed_ratio, Some(2.5));
        assert_eq!(snap.file_strategy, "copy");
        assert!(!snap.keep_completed);
        assert!(snap.opds_auto_update);
        assert_eq!(snap.opds_url, "https://example/opds");
        assert!(snap.allow_private_networks);
        assert!(snap.qbit_allow_private);
        assert_eq!(snap.max_bytes, 5_000_000_000);
    }

    #[test]
    fn poller_params_snapshot_defaults() {
        // Empty cache → all defaults; empty torrent.url → qbit_url = None.
        let cache = SettingsCache::new_with_map(
            dead_pool(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        );
        let snap = cache.poller_params_snapshot();
        assert_eq!(snap.qbit_url, None);
        // max_bytes should be the default (1 EiB — effectively unbounded).
        assert_eq!(snap.max_bytes, DEFAULT_MAX_BYTES);
    }

    #[test]
    fn poller_params_snapshot_mutation_reflected() {
        let m = std::collections::HashMap::new();
        let cache = SettingsCache::new_with_map(dead_pool(), m, std::collections::HashMap::new());
        let s1 = cache.poller_params_snapshot();
        // Mutate the held map directly.
        cache
            .inner
            .cache
            .write()
            .unwrap()
            .insert(KEY_TORRENT_CATEGORY.into(), serde_json::json!("mutated"));
        let s2 = cache.poller_params_snapshot();
        assert_eq!(s2.category, "mutated");
        // All other fields unchanged.
        assert_eq!(s1.save_path, s2.save_path);
    }

    #[test]
    fn poller_params_max_bytes_no_upper_cap_floor_only() {
        // WP3.11: no upper ceiling — an arbitrarily large value is kept as-is
        // (full ZIMs can be ~120 GB, so the old 100 GiB clamp was removed).
        let mut m = std::collections::HashMap::new();
        m.insert(
            KEY_DOWNLOADS_MAX_BYTES.into(),
            serde_json::json!(200_000_000_000_000u64),
        );
        let cache = SettingsCache::new_with_map(dead_pool(), m, std::collections::HashMap::new());
        let snap = cache.poller_params_snapshot();
        assert_eq!(snap.max_bytes, 200_000_000_000_000);

        // A stored 0 is floored to 1 (so it can't disable the cap), not 0.
        let mut m = std::collections::HashMap::new();
        m.insert(KEY_DOWNLOADS_MAX_BYTES.into(), serde_json::json!(0u64));
        let cache = SettingsCache::new_with_map(dead_pool(), m, std::collections::HashMap::new());
        let snap = cache.poller_params_snapshot();
        assert_eq!(snap.max_bytes, 1);
    }
}
