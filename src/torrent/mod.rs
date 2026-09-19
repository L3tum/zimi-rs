//! qBittorrent integration: REST client, download helpers, OPDS, and poller.
//!
//! `QbitClient` is a thin authenticated REST client (with a runtime
//! fingerprint-keyed cache so a `PUT /settings` to `torrent.url` rebuilds it
//! within one poll tick); `is_direct_zim_url` decides between qBittorrent and
//! direct `.zim` HTTP download.
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result, TorrentKind};

pub mod files;
pub mod opds;
pub mod poller;

/// A qBittorrent session is expired/invalid on a 401 (unauthorized) or
/// 403 (forbidden). Used to distinguish "re-auth would help" from other
/// upstream failures (network errors, 5xx, 404s).
pub(crate) fn is_auth_failure(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
    )
}

/// Strip a query string (`?…`) and/or fragment (`#…`) from `url`, returning
/// the piece before the first `?` or `#` (the whole input if neither is
/// present). This is the single source of truth for the query/fragment strip
/// shared by `is_direct_zim_url`, `default_download_name`, and
/// `catalog_name_from_url`.
pub(crate) fn strip_query_fragment(url: &str) -> &str {
    url.split(['?', '#']).next().unwrap_or(url)
}

/// Whether `url` is a direct `.zim` file URL (as opposed to a magnet link or
/// torrent). A direct URL is downloaded with plain HTTP and does not require
/// qBittorrent. This is the single source of truth for the classification
/// (call sites previously each re-implemented
/// `trim().to_lowercase()` then strip query + fragment, then
/// `ends_with(".zim")`); the equivalent SQL predicate
/// used in queries is `lower(trim(split_part(split_part(url, '?', 1), '#', 1)))
/// LIKE '%.zim'` (strips query **and** fragment, matching this function).
pub fn is_direct_zim_url(url: &str) -> bool {
    let trimmed = url.trim().to_lowercase();
    strip_query_fragment(&trimmed).ends_with(".zim")
}

// A-1: the download lifecycle type and its SQL `IN`-list renderer live in
// the persistence layer — `crate::db::downloads_lifecycle` is their home, so
// the db → torrent edge stays one-way. Re-exported here so existing
// `crate::torrent::{DownloadStatus, in_list}` call sites keep working
// unchanged.
pub use crate::db::downloads_lifecycle::{in_list, DownloadStatus};

/// qBittorrent Web API client (raw `reqwest` against the HTTP API).
pub struct QbitClient {
    base_url: String,
    username: String,
    password: String,
    http: reqwest::Client,
}

/// Torrent info as reported by the qBittorrent Web API (`/api/v2/torrents/info`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentInfo {
    /// Torrent hash (its qBittorrent identifier).
    pub hash: String,
    /// Torrent name.
    pub name: String,
    /// Download progress in `0.0..=1.0`.
    #[serde(default)]
    pub progress: f64,
    /// qBittorrent state string (e.g. `downloading`, `error`).
    #[serde(default)]
    pub state: String,
    /// Current download speed in bytes/sec.
    #[serde(default)]
    pub dlspeed: i64,
    /// Current upload speed in bytes/sec.
    #[serde(default)]
    pub upspeed: i64,
    /// Upload/download share ratio.
    #[serde(default)]
    pub ratio: f64,
    /// qBittorrent category assigned to this torrent (when set).
    pub category: Option<String>,
    /// Save path configured for this torrent (when set).
    pub save_path: Option<String>,
    /// Path where the torrent's content is stored (when reported).
    #[serde(default)]
    pub content_path: Option<String>,
    /// Total torrent size in bytes.
    #[serde(default)]
    pub size: i64,
    /// Bytes downloaded so far.
    #[serde(default)]
    pub downloaded: i64,
    /// Number of connected seeds.
    #[serde(default)]
    pub num_seeds: i64,
    /// Human-readable error message when state == "error".
    #[serde(default)]
    pub err_str: Option<String>,
}

impl TorrentInfo {
    /// Download finished (regardless of whether seeding is ongoing).
    ///
    /// Only *post-download* states count: `pausedDL` (paused mid-download) is
    /// deliberately excluded so a torrent paused before it actually finished
    /// is never installed as complete.
    ///
    /// **Acceptance precondition (SEC trust boundary):** this predicate is
    /// the gate between "qBittorrent says the torrent is done" and "install
    /// the file into the library" (`handle_complete`). It accepts only
    /// post-download states, which qBittorrent reaches only AFTER it has
    /// verified every downloaded piece against the torrent's **info-hash**
    /// — including the re-check it runs on startup (`checking`, which
    /// deliberately does NOT count here). The system therefore trusts qB
    /// piece-verification and does not re-hash torrent content client-side;
    /// the info-hash is recorded on the `downloads` row (`bind_hash`) as the
    /// content-identity provenance, and the observed file SHA-256 is recorded
    /// at completion for drift detection.
    pub fn is_complete(&self) -> bool {
        self.progress >= 0.999
            && matches!(
                self.state.as_str(),
                "uploading" | "stalledUP" | "pausedUP" | "forcedUP"
            )
    }

    /// Actively transferring (or preparing to) data — counts toward the
    /// active-download limit.
    pub fn is_active_download(&self) -> bool {
        matches!(
            self.state.as_str(),
            "downloading"
                | "forcedDL"
                | "stalledDL"
                | "metaDL"
                | "forcedMetaDL"
                | "checking"
                | "allocating"
                | "moving"
        )
    }

    /// Fatal/stalled states worth surfacing on the download row.
    pub fn is_fatal(&self) -> bool {
        matches!(self.state.as_str(), "error" | "missingFiles")
    }
}

impl QbitClient {
    /// `pin` (host, addr) fixes the resolved address of the endpoint's initial
    /// host (DNS-rebinding guard); redirect hops are re-resolved and re-validated
    /// by the client's redirect policy. `None` pins nothing.
    pub fn new(
        base_url: &str,
        username: &str,
        password: &str,
        allow_private: bool,
        pin: Option<(String, std::net::SocketAddr)>,
    ) -> Result<Self> {
        let trimmed = base_url.trim_end_matches('/');
        // SSRF guard: block metadata IPs and private ranges (loopback is
        // admitted for local qBittorrent installs; SEC M-1 lets the operator
        // opt a LAN qB endpoint in via `allow_private`). Every redirect hop is
        // re-validated by the guarded client's policy with the same flag.
        // Operator-configured endpoint: the built-in per-hop re-validation is
        // accepted with its residual sub-second rebinding window on a
        // redirect to a *different* host — user-influenced URLs (direct
        // downloads, OPDS) instead follow redirects manually with per-hop
        // resolve + pin (`netguard::follow_pinned_get`).
        let http = crate::netguard::build_guarded_client(
            trimmed,
            allow_private,
            /* allow_loopback */ true,
            pin,
        )?
        .cookie_store(true)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| Error::Config(format!("failed to build qB HTTP client: {e}")))?;
        Ok(Self {
            base_url: trimmed.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            http,
        })
    }

    /// Authenticate with qBittorrent. Stores session cookie.
    pub async fn auth(&mut self) -> Result<()> {
        let url = format!("{}/api/v2/login", self.base_url);
        let resp = self
            .http
            .post(&url)
            .form(&[
                ("username", self.username.as_str()),
                ("password", self.password.as_str()),
            ])
            .send()
            .await
            .map_err(Error::Http)?;

        let status = resp.status();
        if !status.is_success() {
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg: format!("qBittorrent auth failed: HTTP {status}"),
            });
        }

        Ok(())
    }

    /// Add a torrent by URL.
    pub async fn add_torrent(&self, url: &str, category: &str, save_path: &str) -> Result<String> {
        let api_url = format!("{}/api/v2/torrents/add", self.base_url);
        let resp = self
            .http
            .post(&api_url)
            .form(&[
                ("urls", url),
                ("category", category),
                ("savepath", save_path),
            ])
            .send()
            .await
            .map_err(Error::Http)?;

        let status = resp.status();
        let text = resp.text().await.map_err(Error::Http)?;
        if !status.is_success() {
            // SEC-L2: the upstream body is logged for the operator but never
            // embedded in the error string (the poller persists that string).
            tracing::debug!("qB add_torrent upstream response (HTTP {status}): {text}");
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg: format!("add_torrent failed: HTTP {status}"),
            });
        }

        // qBittorrent returns the torrent name or an error message
        Ok(text)
    }

    /// Get torrent info. `filter` is the **raw qBittorrent filter value**
    /// (`"active"`, `"paused"`, `"all"`, or `""` = all) — not a `filter=…`
    /// query fragment.
    pub async fn get_torrents(&self, filter: &str) -> Result<Vec<TorrentInfo>> {
        let api_url = format!("{}/api/v2/torrents/info?filter={}", self.base_url, filter);
        let resp = self.http.get(&api_url).send().await.map_err(Error::Http)?;

        // Check the status **before** parsing: an expired/invalid session
        // (401/403) returns a non-JSON body, and `.json()` would turn it into
        // a misleading "HTTP error" instead of a session-expired signal.
        let status = resp.status();
        if !status.is_success() {
            if is_auth_failure(status) {
                return Err(Error::Torrent {
                    kind: TorrentKind::SessionExpired,
                    msg: format!("qBittorrent session expired (HTTP {status})"),
                });
            }
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg: format!("get_torrents failed: HTTP {status}"),
            });
        }

        let list = resp.json::<Vec<TorrentInfo>>().await.map_err(Error::Http)?;
        Ok(list)
    }

    /// Fetch the qBittorrent client version string (lightweight liveness probe,
    /// L6). `GET {base}/api/v2/app/version` returns the version as plain text.
    /// Status handling follows the `get_torrents` idiom: an expired/invalid
    /// session (401/403) is surfaced as a session-expired error so the cache
    /// re-authenticates; any other non-2xx is a plain failure.
    pub async fn version(&self) -> Result<String> {
        let api_url = format!("{}/api/v2/app/version", self.base_url);
        let resp = self.http.get(&api_url).send().await.map_err(Error::Http)?;

        // Check the status **before** parsing: an expired/invalid session
        // (401/403) returns a non-JSON body.
        let status = resp.status();
        if !status.is_success() {
            if is_auth_failure(status) {
                return Err(Error::Torrent {
                    kind: TorrentKind::SessionExpired,
                    msg: format!("qBittorrent session expired (HTTP {status})"),
                });
            }
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg: format!("version probe failed: HTTP {status}"),
            });
        }

        Ok(resp.text().await.map_err(Error::Http)?.trim().to_string())
    }

    /// Delete a torrent (and optionally files).
    pub async fn delete(&self, hash: &str, delete_files: bool) -> Result<()> {
        let api_url = format!(
            "{}/api/v2/torrents/delete?hashes={}&deleteFiles={}",
            self.base_url, hash, delete_files
        );
        let resp = self.http.post(&api_url).send().await.map_err(Error::Http)?;

        if !resp.status().is_success() {
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg: "delete failed".into(),
            });
        }
        Ok(())
    }

    /// Set per-torrent seed ratio limit.
    pub async fn set_ratio_limit(&self, hash: &str, ratio: f64) -> Result<()> {
        let api_url = format!("{}/api/v2/torrents/setRatioLimit", self.base_url);
        let resp = self
            .http
            .post(&api_url)
            .form(&[("hashes", hash), ("limit", &format!("{:.2}", ratio))])
            .send()
            .await
            .map_err(Error::Http)?;

        if !resp.status().is_success() {
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg: "set_ratio_limit failed".into(),
            });
        }
        Ok(())
    }
}

// ── Runtime qBittorrent client cache (ARCH M3) ───────────────────────────────

/// Pure connection decision (S5): given (url, user, pass), decide whether to
/// Whether the resolved qBittorrent URL is set (non-blank): the warn-then-
/// connect decision. Extracted from the old `QbDecision`/`qb_decision` in
/// main.rs so it is unit-testable without constructing a client. The
/// missing-creds warning itself is emitted by the callers (startup
/// `build_state`, which is the real consumer of that check).
/// Fingerprint of the effective qBittorrent connection inputs (ARCH M3).
/// The cache rebuilds only when this changes, so a runtime `torrent.url`
/// change invalidates the client within one poll tick. SEC-L4: the key is a
/// SHA-256 hex digest, so the in-memory map never holds plaintext creds —
/// the value is compared for equality only, never parsed or logged.
pub fn qbit_fingerprint(url: &str, username: &str, password: &str, allow_private: bool) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(url.as_bytes());
    h.update(b"|");
    h.update(username.as_bytes());
    h.update(b"|");
    h.update(password.as_bytes());
    // SEC M-1: fold the private-network opt-in in so flipping
    // `torrent.allow_private_networks` at runtime invalidates the cached
    // client (it must be rebuilt with a different netguard policy).
    h.update(b"|");
    h.update(allow_private.to_string().as_bytes());
    let digest = h.finalize();
    crate::settings::auth::hex_encode(digest.as_slice())
}

/// Merge the config override and runtime settings into the effective
/// (url, user, pass), or `None` when qBittorrent is effectively disabled (no
/// URL). `url` may come from the `torrent.url` setting (runtime-changeable);
/// `user`/`pass` come from config (env) and are stable for the process.
pub fn resolve_qbit_inputs(
    torrent_url: Option<&str>,
    url_override: &Option<String>,
    username: &str,
    password: &str,
) -> Option<(String, String, String)> {
    let url = url_override
        .clone()
        .or_else(|| torrent_url.map(str::to_string))?;
    (!url.trim().is_empty()).then(|| (url, username.to_string(), password.to_string()))
}

/// Build an authenticated qBittorrent client for (url, user, pass), or
/// `None` if construction or auth fails (fail-soft, logged). Both call sites
/// (startup `build_state` and the poller's per-tick resolve) share this so the
/// warn semantics stay identical.
pub async fn connect_qbit(
    url: &str,
    username: &str,
    password: &str,
    allow_private: bool,
) -> Option<std::sync::Arc<QbitClient>> {
    // DNS-rebinding guard: resolve the endpoint's initial host and pin it, so
    // a later DNS flip to a blocked IP can't slip past the initial check.
    // SEC M-1: `allow_private` admits a private (LAN) qBittorrent Web API when
    // the operator opts in; loopback is always admitted, metadata/link-local
    // and doc/NAT64 ranges always stay blocked.
    let pin = match crate::netguard::resolve_download_host(url, allow_private, true).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("qBittorrent disabled: {e}");
            return None;
        }
    };
    match QbitClient::new(url, username, password, allow_private, pin) {
        Ok(mut client) => match client.auth().await {
            Ok(()) => Some(std::sync::Arc::new(client)),
            Err(e) => {
                tracing::warn!("qBittorrent auth failed: {e} — torrent features disabled");
                None
            }
        },
        Err(e) => {
            tracing::warn!("qBittorrent URL rejected: {e} — torrent features disabled");
            None
        }
    }
}

/// The cache's guarded cell: the current fingerprint and its connected
/// client (a fresh `Arc` clone is handed out to each caller).
type QbitCell = Arc<Mutex<Option<(String, Arc<QbitClient>)>>>;

/// Runtime cache for the qBittorrent client (ARCH M3). Rebuilds when the
/// connection fingerprint changes so a `PUT /settings` to `torrent.url` takes
/// effect within one poll tick, without a restart. Cloned (via the shared
/// `Arc`) by the poller, handlers, and health probe — all hold a clone of the
/// same cache.
///
/// The inner `std::sync::Mutex` is held only for the in-memory read/swap —
/// connect + auth run **outside** the lock (mirrors the EmbedClient
/// DNS-pin-outside-lock discipline), so no lock is ever held across an await.
#[derive(Clone)]
pub struct QbitClientCache {
    inner: QbitCell,
}

impl QbitClientCache {
    /// Create an empty cache (no client connected yet).
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Sync read of the currently-connected client (no rebuild).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn current(&self) -> Option<std::sync::Arc<QbitClient>> {
        self.inner
            .lock()
            .expect("qbit cache lock poisoned")
            .as_ref()
            .map(|(_, c)| c.clone())
    }

    /// Drop the cached client so the next `ensure`/`store` reconnects. Called
    /// by the poller when a `get_torrents` fails with a session-expired
    /// 401/403: the in-memory cookie is stale, and clearing forces a fresh
    /// `connect_qbit` (re-login) on the next tick. The fingerprint check alone
    /// no longer covers this (same fingerprint, stale session), so this is the
    /// explicit clear for that case.
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn invalidate(&self) {
        *self.inner.lock().expect("qbit cache lock poisoned") = None;
    }

    /// Store an already-connected client under its fingerprint (used by
    /// `build_state` to seed the cache from the startup connect).
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub fn store(&self, fingerprint: &str, client: std::sync::Arc<QbitClient>) {
        *self.inner.lock().expect("qbit cache lock poisoned") =
            Some((fingerprint.to_string(), client));
    }

    /// Return the cached client if its fingerprint matches; otherwise rebuild
    /// via `build` (run outside the lock) and store the result. A `None` from
    /// `build` stores nothing, so the next call retries (a long outage →
    /// repeated `None` = the existing `qb_available=false` semantics). The
    /// fingerprint check handles `torrent.url` changes; a same-fingerprint
    /// session expiry (stale cookie) is cleared explicitly via
    /// [`Self::invalidate`].
    // LINT-3 (2026-09 sweep): intentional panic-on-poisoned-lock idiom — grandfathered expect_used.
    #[allow(clippy::expect_used)]
    pub async fn ensure<F, Fut>(
        &self,
        fingerprint: &str,
        build: F,
    ) -> Option<std::sync::Arc<QbitClient>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<std::sync::Arc<QbitClient>>>,
    {
        if let Some((fp, c)) = self.inner.lock().expect("qbit cache lock poisoned").clone() {
            if fp == fingerprint {
                return Some(c);
            }
        }
        let built = build().await;
        if let Some(client) = &built {
            *self.inner.lock().expect("qbit cache lock poisoned") =
                Some((fingerprint.to_string(), client.clone()));
        }
        built
    }
}

impl Default for QbitClientCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{
        is_auth_failure, is_direct_zim_url, qbit_fingerprint, resolve_qbit_inputs, QbitClient,
        QbitClientCache, TorrentInfo,
    };

    fn info(state: &str, progress: f64) -> TorrentInfo {
        TorrentInfo {
            hash: "h".into(),
            name: "n".into(),
            progress,
            state: state.into(),
            dlspeed: 0,
            upspeed: 0,
            ratio: 0.0,
            category: None,
            save_path: None,
            content_path: None,
            size: 0,
            downloaded: 0,
            num_seeds: 0,
            err_str: None,
        }
    }

    #[test]
    fn is_complete_only_post_download_states() {
        // Post-download (seeding) states at full progress → complete.
        for state in ["uploading", "stalledUP", "pausedUP", "forcedUP"] {
            assert!(info(state, 1.0).is_complete(), "{state}");
        }
        // Paused mid-download must NOT be treated as complete — the file may
        // not be finished/verified, so installing it would be wrong.
        assert!(!info("pausedDL", 1.0).is_complete());
        // And in-progress states never count, even at high progress.
        for state in ["downloading", "stalledDL", "metaDL", "forcedDL"] {
            assert!(!info(state, 1.0).is_complete(), "{state}");
        }
        // High progress alone is not enough — the state must be post-download.
        assert!(!info("downloading", 0.9999).is_complete());
    }

    /// SEC (acceptance precondition): the states that must NOT count are
    /// exactly those where qBittorrent has not FINISHED verifying the file
    /// against the info-hash — `checking` is the critical one (qB re-verifies
    /// pieces after a restart / on-demand; the on-disk file may be corrupt
    /// until it finishes), and `allocating` / `moving` are pre- or mid-file
    /// preparation. Asserting the predicate itself (not a copy of the state
    /// set) pins the trust boundary: we install only what qB has
    /// hash-verified.
    #[test]
    fn is_complete_rejects_verification_in_progress_states() {
        assert!(
            !info("checking", 1.0).is_complete(),
            "checking at full progress: qB is STILL verifying pieces — never install"
        );
        assert!(
            !info("allocating", 1.0).is_complete(),
            "allocating: file being prepared"
        );
        assert!(
            !info("moving", 1.0).is_complete(),
            "moving: file in transit"
        );
        assert!(
            !info("checking", 0.5).is_complete(),
            "checking at partial progress is doubly incomplete"
        );
    }

    #[test]
    fn is_direct_zim_url_basic() {
        assert!(is_direct_zim_url("https://example.com/wikipedia_en.zim"));
        assert!(is_direct_zim_url("http://example.com/file.ZIM"));
        assert!(is_direct_zim_url("  https://example.com/file.zim  "));
    }

    #[test]
    fn is_direct_zim_url_rejects_non_zim() {
        assert!(!is_direct_zim_url("https://example.com/file.txt"));
        assert!(!is_direct_zim_url("https://example.com/file.zim.txt"));
        assert!(!is_direct_zim_url("https://example.com/zim"));
        assert!(!is_direct_zim_url(""));
        assert!(!is_direct_zim_url("magnet:?xt=urn:btih:abc"));
    }

    #[test]
    fn is_direct_zim_url_strips_query_and_fragment() {
        assert!(is_direct_zim_url("https://x/file.zim#frag"));
        assert!(is_direct_zim_url(" https://x/FILE.ZIM?token=1#frag "));
        assert!(is_direct_zim_url("https://x/file.zim?sig=abc"));
        assert!(!is_direct_zim_url("https://x/file.txt#frag"));
        assert!(!is_direct_zim_url("https://x/file.zim.txt#frag"));
    }

    #[test]
    fn qbit_fingerprint_is_sha256_hex() {
        let fp = qbit_fingerprint("http://127.0.0.1:8080", "admin", "hunter2", false);
        assert_eq!(fp.len(), 64);
        assert!(
            fp.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "fingerprint should be lowercase hex: {fp}"
        );
        // Deterministic.
        assert_eq!(
            fp,
            qbit_fingerprint("http://127.0.0.1:8080", "admin", "hunter2", false)
        );
        // No input leaks into the digest.
        assert!(!fp.contains("hunter2"));
        assert!(!fp.contains("admin"));
        assert!(!fp.contains("127.0.0.1"));
    }

    #[test]
    fn qbit_fingerprint_changes_on_any_input() {
        let a = qbit_fingerprint("http://q:8080", "u", "p", false);
        assert_eq!(a, qbit_fingerprint("http://q:8080", "u", "p", false));
        // Any one input change → different fingerprint (cache rebuild).
        assert_ne!(a, qbit_fingerprint("http://q:8081", "u", "p", false));
        assert_ne!(a, qbit_fingerprint("http://q:8080", "u2", "p", false));
        assert_ne!(a, qbit_fingerprint("http://q:8080", "u", "p2", false));
        // SEC M-1: flipping the private-network opt-in must also invalidate
        // the cached client (different netguard policy → rebuild).
        assert_ne!(a, qbit_fingerprint("http://q:8080", "u", "p", true));
    }

    #[test]
    fn qbit_private_network_requires_opt_in() {
        // SEC M-1: a private (RFC1918) qBittorrent Web API is blocked unless
        // the operator opts in via `torrent.allow_private_networks`. This pins
        // the guard at the qB boundary specifically (the generic netguard rules
        // are covered separately in netguard::tests).
        let url = "http://192.168.1.100:8080";
        assert!(
            QbitClient::new(url, "u", "p", false, None).is_err(),
            "private qB URL must be blocked without the opt-in"
        );
        assert!(
            QbitClient::new(url, "u", "p", true, None).is_ok(),
            "private qB URL must be admitted with the opt-in"
        );
        // Loopback (a local qB install) stays admitted regardless of the flag.
        assert!(
            QbitClient::new("http://127.0.0.1:8080", "u", "p", false, None).is_ok(),
            "loopback qB must always be admitted"
        );
        // Always-blocked ranges stay blocked even with the opt-in.
        assert!(
            QbitClient::new("http://169.254.169.254/latest", "u", "p", true, None).is_err(),
            "cloud-metadata IP must stay blocked even with the opt-in"
        );
        assert!(
            QbitClient::new("http://10.0.0.5:8080", "u", "p", false, None).is_err(),
            "RFC1918 qB URL must be blocked without the opt-in"
        );
    }

    #[test]
    fn resolve_qbit_inputs_override_beats_setting() {
        assert_eq!(
            resolve_qbit_inputs(
                Some("http://from-setting"),
                &Some("http://override".into()),
                "u",
                "p",
            ),
            Some(("http://override".into(), "u".into(), "p".into()))
        );
    }

    #[test]
    fn resolve_qbit_inputs_setting_when_no_override() {
        assert_eq!(
            resolve_qbit_inputs(Some("http://from-setting"), &None, "u", "p"),
            Some(("http://from-setting".into(), "u".into(), "p".into()))
        );
    }

    #[test]
    fn resolve_qbit_inputs_empty_or_whitespace_disables() {
        // (a) No URL → None.
        assert_eq!(resolve_qbit_inputs(None, &None, "u", "p"), None);
        // (b) Empty URL → None.
        assert_eq!(resolve_qbit_inputs(Some(""), &None, "u", "p"), None);
        // (c) A whitespace override still *selects* the override, then
        // disables — pins the exact `or_else` semantics that `build_state`
        // previously re-implemented inline.
        assert_eq!(
            resolve_qbit_inputs(Some("http://from-setting"), &Some("   ".into()), "u", "p"),
            None
        );
    }

    #[test]
    fn resolve_qbit_inputs_ignores_creds_for_disable() {
        // Empty URL + junk creds → None: creds never re-enable a disabled URL.
        assert_eq!(
            resolve_qbit_inputs(None, &None, "junk-user", "junk-pass"),
            None
        );
    }

    #[test]
    fn is_auth_failure_matrix() {
        use reqwest::StatusCode;
        assert!(is_auth_failure(StatusCode::UNAUTHORIZED)); // 401
        assert!(is_auth_failure(StatusCode::FORBIDDEN)); // 403
        assert!(!is_auth_failure(StatusCode::OK)); // 200
        assert!(!is_auth_failure(StatusCode::NOT_FOUND)); // 404
        assert!(!is_auth_failure(StatusCode::INTERNAL_SERVER_ERROR)); // 500
    }

    fn test_client() -> std::sync::Arc<QbitClient> {
        // Loopback is allowed by the netguard, so this builds without a live
        // server (auth is never attempted here — only the cache is exercised).
        std::sync::Arc::new(
            QbitClient::new("http://127.0.0.1:1", "u", "p", false, None)
                .expect("loopback qbit client should build"),
        )
    }

    #[test]
    fn qbit_cache_invalidate_drops_client() {
        let cache = QbitClientCache::new();
        cache.store("fp", test_client());
        assert!(cache.current().is_some());
        cache.invalidate();
        assert!(
            cache.current().is_none(),
            "invalidate must drop the cached client"
        );
    }

    #[tokio::test]
    async fn qbit_cache_ensure_rebuilds_after_invalidate() {
        let cache = QbitClientCache::new();
        cache.store("fp", test_client());
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Same fingerprint → served from cache, build not invoked.
        let c = calls.clone();
        let _ = cache
            .ensure("fp", move || {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Some(test_client()) }
            })
            .await;
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "cache hit must not rebuild"
        );

        // Invalidate → the *same* fingerprint must now rebuild.
        cache.invalidate();
        let c2 = calls.clone();
        let _ = cache
            .ensure("fp", move || {
                c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Some(test_client()) }
            })
            .await;
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "after invalidate, same fingerprint must rebuild"
        );
    }
}
