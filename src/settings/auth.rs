//! Admin-password hashing (KDF — argon2id) and the short-TTL verified-token
//! cache (M3).
//!
//! Extracted from `settings/mod.rs` (and `serve/middleware.rs`, for
//! `constant_time_eq`) so that `settings` no longer depends on `serve::
//! middleware`. `constant_time_eq` is owned here; `verify_admin_password` is
//! its only production caller.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use argon2::{Algorithm, Argon2, PasswordHash, PasswordHasher, PasswordVerifier, Version};

/// TTL for a verified token in the short-lived auth cache (M3). A distinct
/// token that has verified within this window does not re-run the 100k-iter
/// `verify_admin_password` hash. Invalidated on `reload()` / `update()` /
/// `upgrade_password()` (the last two are defense-in-depth; the password is
/// API-immutable, so `reload()` is the realistic path).
pub const TOKEN_CACHE_TTL: Duration = Duration::from_secs(60);
/// Shorter TTL for negative (failed-verify) entries (M-verify-blocking). A
/// real password change clears the cache explicitly, so a stale negative can
/// at most delay a legitimate login this long. SEC-M2: raised from 5 s to 60 s
/// so an attacker rotating distinct tokens cannot keep re-triggering the full
/// argon2id KDF on every request (the per-IP lockout only fires after 5
/// failures; 60 s + 256 cap bounds the KDF work to at most 256 per minute per
/// distinct-token burst).
pub const TOKEN_NEGATIVE_TTL: Duration = Duration::from_secs(60);
/// Bound on distinct cached tokens (per map) — evicts the oldest when exceeded.
/// SEC-M2: raised from 16 to 256 so a short burst of distinct bad tokens is
/// fully cached within the TTL window, preventing KDF re-execution.
pub const TOKEN_CACHE_MAX: usize = 256;

/// Short-TTL cache of verified admin tokens (M3) and failed verifies
/// (M-verify-blocking). The 100k-iteration `verify_admin_password` hash is
/// expensive; without a cache, *every* authenticated request (middleware +
/// settings handlers) ran it. This caches a small set of (token →
/// last-verified-at) entries in each direction. It is a pure memory structure
/// guarded by `SettingsInner`'s `std::sync::RwLock` (all ops are O(1) memory
/// work; the verify itself runs **after** releasing the lock so a slow hash
/// never serializes concurrent cache lookups — read lock for lookups, write
/// lock for `record`/`clear`, never held across an `.await`).
///
/// The two TTLs are construction-injected ([`VerifiedTokenCache::with_ttl`])
/// so unit tests can exercise expiry with millisecond budgets; the
/// production TTLs are the [`Default`] ([`TOKEN_CACHE_TTL`] /
/// [`TOKEN_NEGATIVE_TTL`]).
pub struct VerifiedTokenCache {
    /// Successful verifies. `pub(crate)` so the `settings` token-cache tests
    /// can seed/expiry the entries directly (white-box).
    pub(crate) entries: HashMap<String, Instant>,
    /// Failed verifies (shorter TTL) — a repeated wrong token skips the KDF.
    pub(crate) negatives: HashMap<String, Instant>,
    /// Positive-entry TTL (fresh while `elapsed < ttl`).
    positive_ttl: Duration,
    /// Negative-entry TTL (fresh while `elapsed < ttl`).
    negative_ttl: Duration,
}

impl Default for VerifiedTokenCache {
    fn default() -> Self {
        Self::with_ttl(TOKEN_CACHE_TTL, TOKEN_NEGATIVE_TTL)
    }
}

impl VerifiedTokenCache {
    /// Constructor with the two TTLs injected (the test seam; [`Default`]
    /// uses the production [`TOKEN_CACHE_TTL`] / [`TOKEN_NEGATIVE_TTL`]).
    pub fn with_ttl(positive_ttl: Duration, negative_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            negatives: HashMap::new(),
            positive_ttl,
            negative_ttl,
        }
    }

    /// True if `token` verified within the cache's positive TTL. Refreshes
    /// nothing — callers re-verify on expiry.
    pub fn is_fresh(&self, token: &str) -> bool {
        self.entries
            .get(token)
            .is_some_and(|at| at.elapsed() < self.positive_ttl)
    }

    /// True if `token` failed within the cache's negative TTL.
    pub fn is_negative_fresh(&self, token: &str) -> bool {
        self.negatives
            .get(token)
            .is_some_and(|at| at.elapsed() < self.negative_ttl)
    }

    /// Record a verify, routing it to the positive or negative map and
    /// evicting the oldest entry in that map past the cap.
    pub fn record(&mut self, token: &str, ok: bool) {
        let map = if ok {
            &mut self.entries
        } else {
            &mut self.negatives
        };
        if !map.contains_key(token) && map.len() >= TOKEN_CACHE_MAX {
            let oldest = map.iter().min_by_key(|(_, at)| *at).map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                map.remove(&k);
            }
        }
        map.insert(token.to_string(), Instant::now());
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.negatives.clear();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len() + self.negatives.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.negatives.is_empty()
    }
}

// ── Admin-password hashing ──────────────────────────────────────────────────────
//
// Format: argon2id PHC string (`$argon2id$v=19$m=19456,t=2,p=1$<b64 salt>$<b64 hash>`).
// Legacy `sha2:` hashes and legacy plaintext values (no prefix) still verify and
// are transparently upgraded to argon2id on the next successful auth.
#[cfg(test)]
const ADMIN_HASH_ITERATIONS: u32 = 100_000;

/// Non-test cap on the iteration count of a legacy `sha2:` hash. An
/// attacker-controlled stored value (or a corrupted row) could otherwise
/// declare `sha2:4294967295$…` and pin a worker thread for ~hours per
/// verify. 1M is 10× the count this codebase mints (100k) — far above any
/// legitimate legacy hash, low enough to bound the worst case to ~1s.
const SHA2_LEGACY_ITERATION_CAP: u32 = 1_000_000;

pub(crate) fn hex_encode(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        use std::fmt::Write as _;
        let _ = write!(s, "{byte:02x}");
    }
    s
}

/// Hex-decode `s` (case-insensitive hex digits, even length); `None` on
/// any non-hex input or odd length. Shared with the SEC-L5 at-rest path
/// (`encrypt::decrypt_value`).
pub(crate) fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if !b.len().is_multiple_of(2) {
        return None;
    }
    (0..b.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Derive 16 salt bytes without a `rand` dependency: wall-clock nanos mixed
/// with process id and a monotonic counter through SHA-256. Test-only now
/// that production hashing is argon2id — the legacy hasher needs it for its
/// fixtures. Documented as adequate for a single-operator shared secret,
/// not high-volume storage.
#[cfg(test)]
fn admin_salt_bytes() -> [u8; 16] {
    use sha2::Digest;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut h = sha2::Sha256::new();
    h.update(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    h.update(std::process::id().to_le_bytes());
    h.update(COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let digest = h.finalize();
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&digest[..16]);
    salt
}

/// OWASP-recommended argon2id parameters (m=19456 KiB, t=2, p=1 — the
/// OWASP argon2id floor).
// LINT-3 (2026-09 sweep): static OWASP-floor KDF params — cannot fail for these values; panic =
// compile-time truth.
#[allow(clippy::expect_used)]
fn argon2id() -> Argon2<'static> {
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        argon2::Params::new(19_456, 2, 1, None).expect("valid argon2 params"),
    )
}

/// Hash `pw` with argon2id; returns the PHC string
/// `$argon2id$v=19$m=19456,t=2,p=1$<b64(32B salt)>$<b64(32B hash)>`.
///
/// ```
/// let h = zimservice::settings::hash_admin_password("admin");
/// assert!(h.starts_with("$argon2id$v=19$"));
/// assert_ne!(
///     h,
///     zimservice::settings::hash_admin_password("admin"),
///     "salted hashes must differ"
/// );
/// ```
pub fn hash_admin_password(pw: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    argon2id()
        .hash_password(pw.as_bytes(), &salt)
        .map(|h| h.to_string())
        .unwrap_or_else(|_| panic!("argon2 hashing failed"))
}

/// Legacy iterative salted SHA-256 hasher (test-only). Production emits
/// argon2id only; this exists to mint legacy-format fixtures for the
/// `verify_admin_password` upgrade-path tests.
#[cfg(test)]
fn hash_sha2_legacy(pw: &str) -> String {
    use sha2::Digest;
    let salt = admin_salt_bytes();
    let mut h = sha2::Sha256::new();
    h.update(salt);
    h.update(pw.as_bytes());
    let mut out = h.finalize();
    for _ in 0..ADMIN_HASH_ITERATIONS {
        let mut h = sha2::Sha256::new();
        h.update(salt);
        h.update(out);
        out = h.finalize();
    }
    format!(
        "sha2:{}${}${}",
        ADMIN_HASH_ITERATIONS,
        hex_encode(&salt),
        hex_encode(&out)
    )
}

/// `true` when `stored` is not a current argon2id hash — i.e. a legacy
/// `sha2:` hash or legacy plaintext. Both still verify and ride the existing
/// upgrade path (re-hashed to argon2id on the next successful auth).
///
/// ```
/// let h = zimservice::settings::hash_admin_password("pw");
/// assert!(!zimservice::settings::is_legacy_password(&h));
/// assert!(zimservice::settings::is_legacy_password("plaintext"));
/// assert!(zimservice::settings::is_legacy_password("sha2:abc"));
/// ```
pub fn is_legacy_password(stored: &str) -> bool {
    !stored.starts_with("$argon2id$")
}

/// Verify `pw` against `stored`: argon2id PHC strings run the KDF, legacy
/// `sha2:`-prefixed values run the iterative digest, anything else is
/// compared as legacy plaintext. All comparisons are constant-time
/// (argon2's verify included).
pub fn verify_admin_password(stored: &str, pw: &str) -> bool {
    if stored.starts_with("$argon2id$") {
        match PasswordHash::new(stored) {
            Ok(ph) => argon2id().verify_password(pw.as_bytes(), &ph).is_ok(),
            Err(_) => false,
        }
    } else if let Some(rest) = stored.strip_prefix("sha2:") {
        use sha2::Digest;
        let mut parts = rest.split('$');
        let iters = match parts.next().and_then(|s| s.parse::<u32>().ok()) {
            Some(i) if i <= SHA2_LEGACY_ITERATION_CAP => i,
            Some(_) => {
                // Over-cap iteration count: refuse instead of burning a
                // worker thread (see SHA2_LEGACY_ITERATION_CAP).
                tracing::warn!(
                    "legacy sha2 hash iteration count exceeds cap {} — rejecting",
                    SHA2_LEGACY_ITERATION_CAP
                );
                return false;
            }
            None => return false,
        };
        let salt_hex = parts.next().unwrap_or("");
        let hash_hex = parts.next().unwrap_or("");
        let salt = match hex_decode(salt_hex) {
            Some(s) if s.len() == 16 => s,
            _ => return false,
        };
        if hex_decode(hash_hex).is_none_or(|h| h.len() != 32) {
            return false;
        }
        let mut h = sha2::Sha256::new();
        h.update(salt.as_slice());
        h.update(pw.as_bytes());
        let mut out = h.finalize();
        for _ in 0..iters {
            let mut h = sha2::Sha256::new();
            h.update(salt.as_slice());
            h.update(out);
            out = h.finalize();
        }
        constant_time_eq(&hex_encode(&out), hash_hex)
    } else {
        constant_time_eq(pw, stored)
    }
}

/// Constant-time string comparison for token checks: does not early-exit on
/// the first differing byte. The length is compared up front and therefore
/// leaks, which is acceptable here (the password is fixed by the operator,
/// not sized by an attacker).
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── verified-token cache (TTL seam) ────────────────────────────────────

    #[test]
    fn default_uses_production_ttls() {
        // White-box: the production (Default) TTLs are the documented
        // constants — a silent edit to `TOKEN_CACHE_TTL`/`TOKEN_NEGATIVE_TTL`
        // is a behavior change and must show up here.
        let cache = VerifiedTokenCache::default();
        assert_eq!(cache.positive_ttl, TOKEN_CACHE_TTL);
        assert_eq!(cache.negative_ttl, TOKEN_NEGATIVE_TTL);
    }

    #[test]
    fn positive_entry_is_fresh_then_expires_with_injected_ttl() {
        // The TTL seam: a 1 ms positive TTL makes expiry observable without
        // a 60 s test. Fresh at record, expired after the TTL — the exact
        // window the production cache enforces at [`TOKEN_CACHE_TTL`].
        let mut cache =
            VerifiedTokenCache::with_ttl(Duration::from_millis(1), Duration::from_millis(1));
        cache.record("tok", true);
        assert!(cache.is_fresh("tok"), "a just-recorded entry is fresh");
        cache.record("tok2", true);
        assert!(cache.is_fresh("tok2"));
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            !cache.is_fresh("tok"),
            "the positive entry expired after the TTL"
        );
        assert!(!cache.is_fresh("tok2"));
    }

    #[test]
    fn negative_entry_is_fresh_then_expires_with_injected_ttl() {
        // Same seam on the negative side: the production window
        // ([`TOKEN_NEGATIVE_TTL`]) is compressed to 1 ms. A rejected token
        // suppresses KDF re-runs only while the negative entry is fresh.
        let mut cache =
            VerifiedTokenCache::with_ttl(Duration::from_millis(1), Duration::from_millis(1));
        cache.record("bad", false);
        assert!(
            cache.is_negative_fresh("bad"),
            "a just-recorded rejection is fresh"
        );
        assert!(
            !cache.is_fresh("bad"),
            "a rejection is not a positive entry"
        );
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            !cache.is_negative_fresh("bad"),
            "the negative entry expired after the TTL"
        );
    }

    #[test]
    fn ttl_seams_are_independent() {
        // The two TTLs are separate dials: a long positive window beside a
        // short negative one must not bleed into each other.
        let mut cache =
            VerifiedTokenCache::with_ttl(Duration::from_secs(60), Duration::from_millis(1));
        cache.record("good", true);
        cache.record("bad", false);
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            cache.is_fresh("good"),
            "the positive TTL is untouched by the negative one"
        );
        assert!(
            !cache.is_negative_fresh("bad"),
            "the negative TTL expired on its own"
        );
    }

    #[test]
    fn refresh_restarts_freshness_window() {
        // A re-record of an existing token restarts its freshness window
        // (the production behavior: repeat traffic keeps the entry alive —
        // the window runs from the LAST record, since `record` stamps
        // `Instant::now()` on every hit).
        //
        // Every assert runs in the "sleep outlived the TTL" direction:
        // under CI load a real sleep only ever runs LONG, so "stale after
        // sleeping past the TTL" is robust, while "still fresh after a
        // sleep" is not — the original t=20 ms fresh assert needed a 10 ms
        // sleep to land under the 20 ms TTL, and a contended coverage
        // runner stretched it past the TTL. The re-record assert below has
        // no sleep at all (timing-independent), and `record` re-stamps
        // unconditionally — fresh or not — so a re-record after expiry
        // exercises the same path as a mid-window refresh.
        let mut cache =
            VerifiedTokenCache::with_ttl(Duration::from_millis(20), Duration::from_millis(20));
        cache.record("tok", true);
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            !cache.is_fresh("tok"),
            "expired at t=60 ms (record + 20 ms TTL)"
        );
        cache.record("tok", true); // refresh: re-stamp the existing entry
        assert!(
            cache.is_fresh("tok"),
            "re-record restarts the freshness window (the original stamp is past TTL)"
        );
        std::thread::sleep(Duration::from_millis(60));
        assert!(!cache.is_fresh("tok"), "expired 20 ms after the refresh");
    }

    #[test]
    fn clear_empties_both_maps() {
        let mut cache =
            VerifiedTokenCache::with_ttl(Duration::from_secs(60), Duration::from_secs(60));
        cache.record("a", true);
        cache.record("b", false);
        assert_eq!(cache.len(), 2);
        cache.clear();
        assert!(cache.is_empty());
        assert!(!cache.is_fresh("a"));
        assert!(!cache.is_negative_fresh("b"));
    }

    #[test]
    fn eviction_respects_per_map_cap() {
        // Each map is capped independently at `TOKEN_CACHE_MAX`: filling the
        // positive map to the cap evicts the oldest positive entry on the
        // next positive record, while negatives are untouched (and vice
        // versa — the per-map cap is the SEC-M2 burst bound).
        let mut cache =
            VerifiedTokenCache::with_ttl(Duration::from_secs(60), Duration::from_secs(60));
        for i in 0..TOKEN_CACHE_MAX {
            cache.record(&format!("pos-{i}"), true);
        }
        assert_eq!(cache.entries.len(), TOKEN_CACHE_MAX);
        // Oldest positive entry gets a distinctly older timestamp so the
        // `min_by_key` eviction target is unambiguous.
        cache.entries.insert(
            "pos-0".to_string(),
            Instant::now() - Duration::from_secs(10),
        );
        cache.record("pos-new", true);
        assert_eq!(
            cache.entries.len(),
            TOKEN_CACHE_MAX,
            "cap holds after eviction"
        );
        assert!(
            !cache.entries.contains_key("pos-0"),
            "the oldest entry was evicted"
        );
        assert!(cache.entries.contains_key("pos-new"));
        // The negative map is capped separately: it can grow to its own cap
        // alongside a full positive map.
        for i in 0..TOKEN_CACHE_MAX {
            cache.record(&format!("neg-{i}"), false);
        }
        assert_eq!(cache.negatives.len(), TOKEN_CACHE_MAX);
        assert_eq!(cache.entries.len(), TOKEN_CACHE_MAX);
    }

    // ── constant_time_eq ───────────────────────────────────────────────────

    #[test]
    fn ct_eq_matches() {
        assert!(constant_time_eq("s3cret", "s3cret"));
        assert!(constant_time_eq("", ""));
    }

    #[test]
    fn ct_eq_rejects() {
        assert!(!constant_time_eq("s3cret", "s3cret1")); // length differs
        assert!(!constant_time_eq("s3cret", "x3cret")); // first byte
        assert!(!constant_time_eq("s3cret", "s3crex")); // last byte
        assert!(!constant_time_eq("", "a"));
    }

    // ── admin password hashing ─────────────────────────────────────────────

    fn assert_sha2_shape(h: &str) {
        // "sha2:100000" + 16-byte salt (32 hex) + 32-byte hash (64 hex)
        let parts: Vec<&str> = h.split('$').collect();
        assert_eq!(parts[0], "sha2:100000", "got prefix: {}", parts[0]);
        assert_eq!(parts[1].len(), 32, "salt should be 16 bytes");
        assert_eq!(parts[2].len(), 64, "hash should be 32 bytes");
        assert!(parts[1].bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(parts[2].bytes().all(|b| b.is_ascii_hexdigit()));
    }

    fn assert_argon2_shape(h: &str) {
        // "$argon2id$v=19$<params>$<b64 salt>$<b64 hash>"
        assert!(h.starts_with("$argon2id$v=19$"), "got prefix: {h}");
        let ph = PasswordHash::new(h).expect("hash must parse as PHC");
        let mut buf = [0u8; 64];
        let sn = ph
            .salt
            .expect("salt present")
            .decode_b64(&mut buf)
            .expect("salt b64");
        // argon2's default salt length is 16 bytes (plan spec said 32 —
        // corrected against observed library behavior at landing).
        assert_eq!(sn.len(), 16, "salt should be 16 bytes");
        let hn = ph.hash.expect("hash present");
        assert_eq!(hn.as_bytes().len(), 32, "hash should be 32 bytes");
    }

    #[test]
    fn hash_admin_password_shape() {
        assert_argon2_shape(&hash_admin_password("s3cret"));
    }

    #[test]
    fn hash_admin_password_verify_roundtrip() {
        let h = hash_admin_password("s3cret");
        assert!(verify_admin_password(&h, "s3cret"));
    }

    #[test]
    fn hash_admin_password_wrong_password_fails() {
        let h = hash_admin_password("s3cret");
        assert!(!verify_admin_password(&h, "wrong"));
    }

    #[test]
    fn hash_admin_password_truncated_hash_fails() {
        let h = hash_admin_password("s3cret");
        // Corrupt the hash portion (drop a char) — must not verify.
        let truncated = format!("{}x", &h[..h.len().saturating_sub(1)]);
        assert!(!verify_admin_password(&truncated, "s3cret"));
    }

    #[test]
    fn verify_admin_password_legacy_plaintext() {
        // No `$argon2id$` prefix → legacy compare (plaintext or sha2:).
        assert!(verify_admin_password("plainpw", "plainpw"));
        assert!(!verify_admin_password("plainpw", "other"));
        assert!(is_legacy_password("plainpw"));
        // sha2: rows are upgrade-eligible too.
        assert!(is_legacy_password(&hash_sha2_legacy("x")));
        assert!(!is_legacy_password(&hash_admin_password("plainpw")));
    }

    #[test]
    fn verify_admin_password_sha2_legacy_still_verifies() {
        let h = hash_sha2_legacy("s3cret");
        assert_sha2_shape(&h);
        assert!(verify_admin_password(&h, "s3cret"));
        assert!(!verify_admin_password(&h, "wrong"));
    }

    #[test]
    fn verify_admin_password_sha2_over_iteration_cap_rejected() {
        // A stored `sha2:` value is attacker/row-controlled; an over-cap
        // iteration count must be refused (with no 1M-iteration loop running)
        // instead of pinning a worker thread.
        let h = hash_sha2_legacy("pw"); // shape: sha2:100000$<salt>$<hash>
        let (prefix, rest) = h.split_once('$').expect("salt separator");
        let iters = prefix.strip_prefix("sha2:").expect("sha2 prefix");
        assert_eq!(iters, "100000");

        // Just over the cap (1_000_000) → rejected, no long loop.
        let over = format!("sha2:1000001${rest}");
        assert!(!verify_admin_password(&over, "pw"));
        // Wildly over (u32-scale) → rejected too.
        let wild = format!("sha2:999999999${rest}");
        assert!(!verify_admin_password(&wild, "pw"));
        // The original (under-cap) hash still verifies.
        assert!(verify_admin_password(&h, "pw"));
    }

    #[test]
    fn hash_admin_password_two_hashes_differ() {
        // Distinct salts → distinct outputs even for the same password.
        let a = hash_admin_password("same");
        let b = hash_admin_password("same");
        assert_ne!(a, b, "salts should differ between calls");
        // Both still verify.
        assert!(verify_admin_password(&a, "same"));
        assert!(verify_admin_password(&b, "same"));
    }
}
