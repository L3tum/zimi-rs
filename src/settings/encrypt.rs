//! At-rest encryption of the secret `settings` rows (SEC-L5).
//!
//! When `SECURITY_KEY` is set at startup, the three secret settings that
//! would otherwise sit PLAINTEXT in the `settings` table
//! ([`ENCRYPTED_AT_REST_KEYS`]) are stored as `enc:v1:` +
//! hex(nonce(12) ‖ ciphertext ‖ tag(16)) — AES-256-GCM (`ring::aead`),
//! one fresh random 12-byte nonce per encryption (GCM nonce reuse under
//! one key is catastrophic), no AAD. The KEK is derived ONCE per
//! `SettingsCache` construction (HKDF-SHA256 over the `SECURITY_KEY`
//! bytes, fixed salt/info below) and held in `SettingsInner` —
//! deliberately NOT a process global (`docs/process_globals.md`): it is
//! per-cache state like `env_snapshot`, and tests can inject different
//! keys without mutating the process env.
//!
//! `settings::cache` is the ONLY crypto boundary: the in-memory cache
//! always holds plaintext (transparent decrypt on load/reload, encrypt
//! before every write of these keys to the `settings` table), so
//! everything downstream (snapshots, redaction, poller, MCP) is
//! unchanged.
//!
//! `access.admin_password` is deliberately EXCLUDED: it is stored as an
//! argon2id KDF hash — non-reversible, so there is nothing to encrypt.
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::hkdf;

use super::auth::{hex_decode, hex_encode};
use super::defs::{KEY_ACCESS_READ_ONLY_TOKEN, KEY_EMBEDDING_API_KEY, KEY_TORRENT_PASSWORD};
use crate::error::{Error, Result};

/// Storage-format marker for at-rest-encrypted rows: `ENC_PREFIX` +
/// hex(nonce(12) ‖ ciphertext ‖ tag(16)).
pub(crate) const ENC_PREFIX: &str = "enc:v1:";

/// Every `secret: true` string setting stored PLAINTEXT at rest when
/// `SECURITY_KEY` is unset (and AES-256-GCM-encrypted when it is set).
/// `access.admin_password` is excluded on purpose: it is an argon2id KDF
/// hash — non-reversible, so there is nothing to encrypt.
pub(crate) const ENCRYPTED_AT_REST_KEYS: &[&str] = &[
    KEY_TORRENT_PASSWORD,
    KEY_EMBEDDING_API_KEY,
    KEY_ACCESS_READ_ONLY_TOKEN,
];

/// True when `key` is one of the at-rest-encrypted settings
/// ([`ENCRYPTED_AT_REST_KEYS`]).
pub(crate) fn is_encrypted_at_rest_key(key: &str) -> bool {
    ENCRYPTED_AT_REST_KEYS.contains(&key)
}

/// Fixed HKDF salt/info — a documented KDF domain for the `settings`
/// table: the same `SECURITY_KEY` always derives the same 32-byte KEK,
/// and the domain separation keeps this derivation distinct from any
/// other HKDF use in the crate.
const KEK_SALT: &[u8] = b"zimservice::settings-kek-v1";
const KEK_INFO: &[u8] = b"zimservice::settings-kek-v1";

/// Derive the 32-byte AES-256-GCM KEK from the `SECURITY_KEY` bytes
/// (HKDF-SHA256). Called once per `SettingsCache` construction; the KEK
/// is then held in `SettingsInner` for the cache's lifetime.
// LINT-3: both expects are structurally impossible — HKDF-SHA256 expand
// of 32 bytes is far below the 255×32 ceiling, and `fill` into a
// 32-byte buffer matches the requested length exactly.
#[allow(clippy::expect_used)]
pub(crate) fn derive_kek(security_key: &str) -> [u8; 32] {
    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, KEK_SALT);
    let prk = salt.extract(security_key.as_bytes());
    // `L = HKDF_SHA256` → the output length is the digest's output (32).
    let okm = prk
        .expand(&[KEK_INFO], hkdf::HKDF_SHA256)
        .expect("HKDF-SHA256 expand of a 32-byte output cannot fail");
    let mut kek = [0u8; 32];
    okm.fill(&mut kek)
        .expect("fill into a 32-byte buffer matches the requested length");
    kek
}

/// Encrypt `plaintext` for at-rest storage: `ENC_PREFIX` +
/// hex(nonce ‖ ciphertext ‖ tag), one fresh random 12-byte nonce per
/// call.
///
/// Caller convention (the only guard): `plaintext` is non-empty — empty
/// values are stored as-is by the boundary
/// (`SettingsInner::stored_value_for`), so the DB never holds an
/// `enc:v1:` row with an empty ciphertext.
// LINT-3: both expects are structurally impossible — a 32-byte key is
// always valid for AES-256, and a getrandom failure means the kernel
// CSPRNG is unavailable (a fatal environment, not a recoverable input).
#[allow(clippy::expect_used)]
pub(crate) fn encrypt_value(kek: &[u8; 32], plaintext: &str) -> String {
    let key = LessSafeKey::new(
        UnboundKey::new(&aead::AES_256_GCM, kek)
            .expect("AES-256 key construction from a 32-byte KEK cannot fail"),
    );
    let mut nonce_bytes = [0u8; 12];
    getrandom::getrandom(&mut nonce_bytes).expect("kernel CSPRNG unavailable (getrandom)");
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut ct = plaintext.as_bytes().to_vec();
    // In-place: `ct` holds the ciphertext on return; the 16-byte tag is
    // returned separately (`separate_tag` — we append it ourselves).
    let tag = key
        .seal_in_place_separate_tag(nonce, Aad::empty(), &mut ct)
        .expect("in-place GCM seal of a valid buffer cannot fail");
    let mut sealed = Vec::with_capacity(12 + ct.len() + 16);
    sealed.extend_from_slice(&nonce_bytes);
    sealed.extend_from_slice(&ct);
    sealed.extend_from_slice(tag.as_ref());
    format!("{ENC_PREFIX}{}", hex_encode(&sealed))
}

/// Decrypt a stored at-rest row value back to its plaintext.
///
/// No `ENC_PREFIX` → the value is legacy plaintext (or a never-encrypted
/// value) and is returned AS-IS. Prefix present → AES-256-GCM open,
/// FAIL-CLOSED on ANY failure (wrong/changed `SECURITY_KEY`, tampered or
/// truncated row): the [`Error::SettingsDecrypt`] names `key` and never
/// the value — the row content is a secret, and echoing it into an error
/// would put it in logs.
// LINT-3: the two expects below are structurally impossible (32-byte
// KEK → valid AES-256 key; `split_at(12)` yields an exact 12-byte
// nonce slice after the length check above).
#[allow(clippy::expect_used)]
pub(crate) fn decrypt_value(key: &str, kek: &[u8; 32], stored: &str) -> Result<String> {
    let Some(hex_body) = stored.strip_prefix(ENC_PREFIX) else {
        return Ok(stored.to_string());
    };
    let buf = hex_decode(hex_body).ok_or_else(|| Error::SettingsDecrypt {
        key: key.to_string(),
    })?;
    // Structural floor: nonce(12) ‖ ciphertext (≥1, the caller
    // convention above) ‖ tag(16).
    if buf.len() < 12 + 16 {
        return Err(Error::SettingsDecrypt {
            key: key.to_string(),
        });
    }
    let (nonce_bytes, ct_tag) = buf.split_at(12);
    let nonce = Nonce::assume_unique_for_key(
        nonce_bytes
            .try_into()
            .expect("split_at(12) yields an exact 12-byte nonce slice"),
    );
    // NB: named `less_safe` — must not shadow the `key` (the settings key
    // string) that the fail-closed error names.
    let less_safe = LessSafeKey::new(
        UnboundKey::new(&aead::AES_256_GCM, kek)
            .expect("AES-256 key construction from a 32-byte KEK cannot fail"),
    );
    let mut ct_tag = ct_tag.to_vec();
    // `open_in_place` decrypts in place and returns the PLAINTEXT PREFIX —
    // the 16 tag bytes remain at the end of the buffer — so truncate to
    // the returned slice before treating the buffer as plaintext.
    let plain_len = less_safe
        .open_in_place(nonce, Aad::empty(), &mut ct_tag)
        .map_err(|_| Error::SettingsDecrypt {
            key: key.to_string(),
        })?
        .len();
    ct_tag.truncate(plain_len);
    String::from_utf8(ct_tag).map_err(|_| Error::SettingsDecrypt {
        key: key.to_string(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_reencrypts_with_a_fresh_nonce() {
        let kek = derive_kek("test-key-1");
        let stored = encrypt_value(&kek, "s3cr3t-value");
        assert!(stored.starts_with(ENC_PREFIX));
        let plain = decrypt_value("torrent.password", &kek, &stored).unwrap();
        assert_eq!(plain, "s3cr3t-value");
        // A fresh nonce per call: two encryptions of the same plaintext
        // under the same KEK differ (the whole point of per-call nonces).
        let again = encrypt_value(&kek, "s3cr3t-value");
        assert_ne!(stored, again);
        assert_eq!(
            decrypt_value("torrent.password", &kek, &again).unwrap(),
            "s3cr3t-value"
        );
    }

    #[test]
    fn decrypt_passes_through_unprefixed_values() {
        let kek = derive_kek("k");
        for legacy in ["plain-text", "", "sha2:legacy-hash"] {
            assert_eq!(
                decrypt_value("torrent.password", &kek, legacy).unwrap(),
                legacy
            );
        }
    }

    #[test]
    fn wrong_kek_fails_closed_without_leaking_value() {
        let a = derive_kek("key-A");
        let b = derive_kek("key-B");
        let stored = encrypt_value(&a, "top-secret-value");
        let err = decrypt_value("embedding.api_key", &b, &stored).unwrap_err();
        let msg = err.to_string();
        // The failing key is named …
        assert!(msg.contains("embedding.api_key"), "{msg}");
        // … but neither the plaintext nor the ciphertext leaks.
        assert!(!msg.contains("top-secret-value"), "{msg}");
        assert!(!msg.contains(&stored), "{msg}");
    }

    #[test]
    fn tampered_ciphertext_fails_closed() {
        let kek = derive_kek("k");
        let mut stored = encrypt_value(&kek, "flip-one-hex-char");
        // Flip one hex char inside the ciphertext region (after the
        // prefix + 24 hex chars of nonce).
        let idx = ENC_PREFIX.len() + 24 + 2;
        let mut chars: Vec<char> = stored.chars().collect();
        chars[idx] = if chars[idx] == '0' { '1' } else { '0' };
        stored = chars.into_iter().collect();
        assert!(
            decrypt_value("access.read_only_token", &kek, &stored).is_err(),
            "a tampered ciphertext must fail closed"
        );
    }

    #[test]
    fn truncated_and_malformed_bodies_fail_closed() {
        let kek = derive_kek("k");
        // Below the 28-byte structural floor (nonce + 14 bytes).
        let short = format!("{ENC_PREFIX}00112233445566778899aabbccdd");
        assert!(decrypt_value("torrent.password", &kek, &short).is_err());
        // Non-hex input.
        assert!(decrypt_value("torrent.password", &kek, &format!("{ENC_PREFIX}zz")).is_err());
        // Odd-length hex.
        assert!(decrypt_value("torrent.password", &kek, &format!("{ENC_PREFIX}abc")).is_err());
    }

    #[test]
    fn kek_derivation_is_deterministic_and_key_separated() {
        assert_eq!(derive_kek("same-key"), derive_kek("same-key"));
        assert_ne!(derive_kek("key-a"), derive_kek("key-b"));
    }
}
