//! Kiwix OPDS catalog polling for ZIM auto-update detection.
//!
//! The official catalog (`https://opds.kiwix.com/opds_catalog`) is an Atom
//! feed whose entries carry `id`, `title`, `updated` and acquisition links
//! (`<link rel="http://opds-spec.org/acquisition..." href="...zim">`).
//!
//! Update detection compares each entry's ZIM name (base + trailing date
//! segment, e.g. `wikipedia_en_all_maxi_2024-06`) against the local
//! library; a newer date wins.

use quick_xml::events::Event;
use quick_xml::Reader;

use crate::error::{Error, Result};
use crate::netguard::{follow_pinned_get, PinnedResponse};
use crate::settings::SettingsCache;
use crate::torrent::poller::{build_download_client, ClientProfile};
use crate::zim::ZimManager;

use super::strip_query_fragment;

/// One `<entry>` in the OPDS feed.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct OpdsEntry {
    /// The entry's unique ID (e.g. `zim:wikipedia_en_all_maxi_2024-06`).
    pub id: Option<String>,
    /// Human-readable title of the entry.
    pub title: Option<String>,
    /// RFC 3339 timestamp from the feed, if present.
    pub updated: Option<String>,
    /// All `<link>` elements in the entry (acquisition, thumbnail, etc.).
    pub links: Vec<OpdsLink>,
}

/// A single `<link>` element from an OPDS Atom feed entry.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct OpdsLink {
    /// The `rel` attribute (e.g. `http://opds-spec.org/acquisition/open-access`).
    pub rel: Option<String>,
    /// The `href` attribute (the URL the link points to).
    pub href: Option<String>,
    /// The `type` attribute (e.g. `application/x-zim`).
    pub media_type: Option<String>,
    /// The `hash` attribute (a hex SHA-256 of the resource, when the catalog
    /// declares one). Kiwix's current catalog does not declare it; it is
    /// parsed so catalogs that DO declare digests are honored, not dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// The `digest` attribute (a base64 SHA-256 of the resource, OPDS 1.2
    /// style, when the catalog declares one).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

impl OpdsEntry {
    /// The acquisition (download) link, if any — the first link whose `rel`
    /// names acquisition, or whose href/media type identifies a ZIM. Both
    /// the URL and the declared digest are read from this one link so the
    /// pair can never drift.
    pub fn download_link(&self) -> Option<&OpdsLink> {
        self.links.iter().find(|l| {
            let Some(href) = l.href.as_deref() else {
                return false;
            };
            let is_acq = l
                .rel
                .as_deref()
                .map(|r| r.contains("acquisition"))
                .unwrap_or(false);
            let is_zim = href.ends_with(".zim")
                || l.media_type
                    .as_deref()
                    .map(|t| t.contains("zim"))
                    .unwrap_or(false);
            is_acq || is_zim
        })
    }

    /// The acquisition URL for the ZIM file, if any.
    pub fn download_url(&self) -> Option<String> {
        self.download_link().and_then(|l| l.href.clone())
    }

    /// The publisher-declared SHA-256 digest for the download, if the catalog
    /// declared one (see [`ZimDigest::parse`]).
    pub fn download_digest(&self) -> Option<ZimDigest> {
        self.download_link()
            .and_then(|l| parse_link_digest(l.hash.as_deref(), l.digest.as_deref()))
    }
}

/// A publisher-declared SHA-256 digest for a ZIM, normalized to 64 lowercase
/// hex characters. SEC: this is the content-integrity version-key — the claim
/// the queue guard re-queues on change and the direct-download finalize path
/// verifies the installed bytes against. Torrent installs are pinned by the
/// BitTorrent info-hash instead and carry no such claim.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ZimDigest {
    /// 64 lowercase hex characters (always — malformed input parses to
    /// `None`, never to a partial/garbage value).
    pub value: String,
}

impl ZimDigest {
    /// Parse a declared digest attribute pair into a normalized
    /// [`ZimDigest`].
    ///
    /// `hash` (hex) takes precedence over `digest` (base64) when both are
    /// present; if both are readable they must agree, else `None` — a
    /// self-contradicting catalog entry must not become a verified claim.
    /// Malformed values return `None`: a digest we cannot normalize is a
    /// digest we cannot verify against, so it degrades to "not declared"
    /// (structural check only) rather than being treated as a mismatch.
    pub fn parse(hash: Option<&str>, digest: Option<&str>) -> Option<Self> {
        let hex_val = hash
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(hex_sha256);
        let b64_val = digest
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(b64_decode_32)
            .map(|b| hex_bytes(&b));
        match (hex_val, b64_val) {
            (Some(a), Some(b)) => (a == b).then_some(a),
            (a, b) => a.or(b),
        }
        .map(|value| Self { value })
    }
}

/// Entry point used by [`OpdsEntry::download_digest`]: normalize the
/// `hash`/`digest` attribute pair of an acquisition link.
pub fn parse_link_digest(hash: Option<&str>, digest: Option<&str>) -> Option<ZimDigest> {
    ZimDigest::parse(hash, digest)
}

/// A 64-char (case-insensitive) hex string that is exactly 32 bytes →
/// normalized lowercase; anything else → `None`.
fn hex_sha256(s: &str) -> Option<String> {
    let lower = s.to_ascii_lowercase();
    if lower.len() != 64 || !lower.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(lower)
}

/// Standard-alphabet (RFC 4648) base64 decode. Malformed input (bad
/// characters, padding mid-string) → `None`. Self-contained — no base64
/// dependency.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    // Canonical form (RFC 4648): length must be a multiple of 4 — a 43-char
    // unpadded string is NOT a valid 32-byte encoding (that is 44 chars with
    // `==`), and accepting it would silently decode garbage tail bits.
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    // Padding may only trail: everything from the first '=' must be '='.
    let first_pad = bytes.iter().position(|&b| b == b'=');
    if let Some(pos) = first_pad {
        if bytes[pos..].iter().any(|&b| b != b'=') || bytes.len() - pos > 2 {
            return None;
        }
    }
    let mut out = Vec::with_capacity(32);
    let mut buf: u32 = 0;
    let mut bits = 0;
    for &b in &bytes {
        if b == b'=' {
            break;
        }
        let v = val(b)? as u32;
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

/// Standard-alphabet base64 → exactly 32 bytes (a SHA-256). Malformed input
/// or wrong length → `None`.
fn b64_decode_32(s: &str) -> Option<[u8; 32]> {
    let bytes = b64_decode(s)?;
    if bytes.len() != 32 {
        return None;
    }
    bytes.try_into().ok()
}

/// Bytes → 64 lowercase hex characters (the crate formats hex with local
/// helpers; no hex dependency).
fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// An available update for a locally-installed ZIM.
#[derive(Debug, Clone, serde::Serialize)]
pub struct OpdsUpdate {
    /// The local ZIM file name (without `.zim` extension).
    pub local_name: String,
    /// The catalog ZIM file name (without `.zim` extension).
    pub catalog_name: String,
    /// The catalog entry's title, if present in the feed.
    pub title: Option<String>,
    /// The direct download URL for the new ZIM file.
    pub download_url: String,
    /// The SHA-256 the catalog declared for the download (its `hash`/`digest`
    /// attributes), when present. This is the publisher's claim the queue
    /// guard version-keys on and the direct-download finalize path verifies
    /// against; `None` means the source declared nothing (structural check
    /// only — the current Kiwix catalog case).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<ZimDigest>,
}

/// Parse an OPDS Atom feed. Tolerant: malformed entries/fragments are
/// skipped, valid ones are kept.
pub fn parse_catalog(xml: &str) -> Vec<OpdsEntry> {
    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut entries = Vec::new();
    let mut cur: Option<OpdsEntry> = None;
    // Which text element we're accumulating into.
    let mut text_target: Option<&'static str> = None;

    'outer: loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = std::str::from_utf8(e.local_name().into_inner()).unwrap_or("");
                match name {
                    "entry" => cur = Some(OpdsEntry::default()),
                    "id" | "title" | "updated" if cur.is_some() => {
                        text_target = Some(match name {
                            "id" => "id",
                            "title" => "title",
                            _ => "updated",
                        });
                    }
                    _ => {}
                }
            }
            // Self-closing elements (e.g. `<link .../>`).
            Ok(Event::Empty(e)) => {
                let name = std::str::from_utf8(e.local_name().into_inner()).unwrap_or("");
                if name == "link" {
                    if let Some(entry) = cur.as_mut() {
                        let mut rel = None;
                        let mut href = None;
                        let mut media_type = None;
                        let mut hash = None;
                        let mut digest = None;
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.local_name().into_inner())
                                .unwrap_or("");
                            let val = String::from_utf8_lossy(&attr.value).into_owned();
                            match key {
                                "rel" => rel = Some(val),
                                "href" => href = Some(val),
                                "type" => media_type = Some(val),
                                "hash" => hash = Some(val),
                                "digest" => digest = Some(val),
                                _ => {}
                            }
                        }
                        if rel.is_some() || href.is_some() {
                            entry.links.push(OpdsLink {
                                rel,
                                href,
                                media_type,
                                hash,
                                digest,
                            });
                        }
                    }
                }
            }
            Ok(Event::End(e)) => {
                let name = std::str::from_utf8(e.local_name().into_inner()).unwrap_or("");
                match name {
                    "entry" => {
                        if let Some(entry) = cur.take() {
                            if entry.id.is_some() || entry.title.is_some() {
                                entries.push(entry);
                            }
                        }
                        text_target = None;
                    }
                    "id" | "title" | "updated" => text_target = None,
                    _ => {}
                }
            }
            Ok(Event::Text(e)) => {
                if let Some(target) = text_target {
                    if let Some(entry) = cur.as_mut() {
                        let s = quick_xml::escape::unescape(&String::from_utf8_lossy(&e))
                            .map(|u| u.into_owned())
                            .unwrap_or_else(|_| String::from_utf8_lossy(&e).into_owned());
                        if !s.trim().is_empty() {
                            match target {
                                "id" => entry.id = Some(s),
                                "title" => entry.title = Some(s),
                                "updated" => entry.updated = Some(s),
                                _ => {}
                            }
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break 'outer, // malformed tail — keep what we have
            _ => {}
        }
        buf.clear();
    }

    entries
}

/// Fetch and parse the OPDS catalog at `url`.
///
/// SEC-1: the catalog URL is user-influenced (like direct download URLs), so
/// redirects are followed **manually** by `follow_pinned_get`: every hop
/// — including hop 0 — is validated, re-resolved, and pinned to the exact
/// address(es) that passed the check. The initial-URL gate
/// (`validate_download_url`) is applied by the caller (`opds_check`) before
/// any HTTP I/O.
pub async fn fetch_catalog(settings: &SettingsCache, url: &str) -> Result<Vec<OpdsEntry>> {
    let settings = settings.clone();
    let provider: std::sync::Arc<dyn Fn() -> bool + Send + Sync> =
        std::sync::Arc::new(move || settings.downloads_allow_private_networks());
    let PinnedResponse { response, .. } = follow_pinned_get(
        url,
        provider,
        &[],
        &|pin| build_download_client(ClientProfile::Control, pin),
        &|c, u| c.get(u).header("Accept", "application/atom+xml"),
    )
    .await?;
    let body = response.text().await.map_err(Error::Http)?;
    Ok(parse_catalog(&body))
}

/// Split a ZIM file name (without extension) into `(base, version)`, where
/// the version is the trailing date segment Kiwix appends, e.g.
/// `wikipedia_en_all_maxi_2024-06` → `("wikipedia_en_all_maxi", Some((2024, 6)))`.
/// Accepted date forms: `_YYYY-MM`, `_YYYY-MM-DD`, `_YYYYMM`.
pub fn base_and_version(name: &str) -> (String, Option<(u32, u32)>) {
    if let Some(pos) = name.rfind('_') {
        let tail = &name[pos + 1..];
        if let Some(v) = parse_date_tag(tail) {
            return (name[..pos].to_string(), Some(v));
        }
    }
    (name.to_string(), None)
}

fn parse_date_tag(s: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = s.split('-').collect();
    match parts.as_slice() {
        [y, m] if y.len() == 4 && m.len() == 2 => Some((y.parse().ok()?, m.parse().ok()?)),
        [y, m, d] if y.len() == 4 && m.len() == 2 && d.len() == 2 => {
            Some((y.parse().ok()?, m.parse().ok()?))
        }
        [y] if y.len() == 6 => Some((y[..4].parse().ok()?, y[4..].parse().ok()?)),
        _ => None,
    }
}

/// A B10 update candidate: the newest matching catalog entry for one local
/// ZIM (rank = (date-tag or feed-updated) prefix, see `candidate_rank`).
struct UpdateCandidate<'a> {
    rank: (u32, u32),
    entry: &'a OpdsEntry,
    name: String,
    url: String,
    digest: Option<ZimDigest>,
}

/// Compare catalog entries against the local library and return available
/// updates (catalog version strictly newer than local).
///
/// `local` is a list of `(zim_name, publication_date)` pairs — the name
/// without `.zim`, and the `zims.date` value when known.
///
/// B10: a single local ZIM can match several catalog entries sharing its base
/// (e.g. both `wiki_2024-06` **and** `wiki_2025-01` are newer than a local
/// `wiki_2023-05`). We return exactly **one** update for that local ZIM — the
/// newest matching entry — instead of one per entry. `candidate_rank` orders
/// the candidates (date tag preferred, then the feed `<updated>`); exact ties
/// keep the first in feed order. `OpdsUpdate`'s shape is unchanged.
pub fn find_updates(entries: &[OpdsEntry], local: &[(String, Option<String>)]) -> Vec<OpdsUpdate> {
    let mut updates = Vec::new();
    for (local_name, local_date) in local {
        let (local_base, local_version) = base_and_version(local_name);

        // Best (newest) candidate for this local ZIM.
        let mut best: Option<UpdateCandidate<'_>> = None;

        for entry in entries {
            let url = match entry.download_url() {
                Some(u) => u,
                None => continue,
            };
            // Derive the catalog ZIM name from the download URL's file name.
            let catalog_name = match catalog_name_from_url(&url) {
                Some(n) => n,
                None => continue,
            };
            let (cat_base, cat_version) = base_and_version(&catalog_name);
            if !cat_base.eq_ignore_ascii_case(&local_base) {
                continue;
            }

            let newer = match (cat_version, local_version) {
                (Some(cv), Some(lv)) => cv > lv,
                // One side undated: fall back to the feed's <updated> vs the
                // local publication date (both compared as YYYY-MM prefixes).
                _ => match (entry.updated.as_deref(), local_date.as_deref()) {
                    (Some(u), Some(d)) => {
                        // ISO-8601 date prefix compare; floor to char
                        // boundaries so a hostile feed with non-ASCII date
                        // fields can't panic the poller.
                        let u7 = u.floor_char_boundary(u.len().min(7));
                        let d7 = d.floor_char_boundary(d.len().min(7));
                        u[..u7].cmp(&d[..d7]) == std::cmp::Ordering::Greater
                    }
                    _ => false,
                },
            };
            if !newer {
                continue;
            }

            // B10: among the newer same-base entries, keep only the newest.
            // Strict `>` means an exact rank tie keeps the first in feed order.
            let rank = candidate_rank(cat_version, entry.updated.as_deref());
            let replace = match &best {
                None => true,
                Some(b) => rank > b.rank,
            };
            if replace {
                best = Some(UpdateCandidate {
                    rank,
                    entry,
                    name: catalog_name,
                    url,
                    digest: entry.download_digest(),
                });
            }
        }

        if let Some(c) = best {
            updates.push(OpdsUpdate {
                local_name: local_name.clone(),
                catalog_name: c.name,
                title: c.entry.title.clone(),
                download_url: c.url,
                digest: c.digest,
            });
        }
    }
    updates
}

/// "Newest wins" rank (B10) for multiple catalog entries matching one local
/// ZIM: the ZIM's date tag when present, else the feed `<updated>` YYYY-MM
/// prefix, else (0, 0). Tuple order means a dated entry beats an undated one,
/// and a later (year, month) beats an earlier one.
fn candidate_rank(cat_version: Option<(u32, u32)>, updated: Option<&str>) -> (u32, u32) {
    if let Some(v) = cat_version {
        return v;
    }
    let Some(u) = updated else {
        return (0, 0);
    };
    // Floor to a char boundary so a hostile non-ASCII `<updated>` can't panic
    // the slice (mirrors the `newer` fallback above).
    let u7 = u.floor_char_boundary(u.len().min(7));
    let p: Vec<&str> = u[..u7].split('-').collect();
    match p.as_slice() {
        [y, m] if y.len() == 4 && m.len() == 2 => (y.parse().unwrap_or(0), m.parse().unwrap_or(0)),
        _ => (0, 0),
    }
}

/// Extract the ZIM file name (without `.zim`) from a download URL.
pub fn catalog_name_from_url(url: &str) -> Option<String> {
    let file = url.rsplit('/').next()?.to_string();
    // Drop any query/fragment first, then the extension (case-insensitive).
    let file = strip_query_fragment(&file);
    let lower = file.to_ascii_lowercase();
    let name = lower.strip_suffix(".zim")?;
    (!name.is_empty()).then_some(name.to_string())
}

/// Convenience: fetch the configured catalog and compare against `zims`.
pub async fn check_updates(
    settings: &SettingsCache,
    opds_url: &str,
    zims: &ZimManager,
) -> Result<Vec<OpdsUpdate>> {
    let entries = fetch_catalog(settings, opds_url).await?;
    let local: Vec<(String, Option<String>)> = zims
        .list()
        .iter()
        .map(|m| (m.name.clone(), m.date.clone()))
        .collect();
    Ok(find_updates(&entries, &local))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{
        b64_decode, base_and_version, catalog_name_from_url, find_updates, parse_catalog,
        parse_link_digest, OpdsEntry, OpdsLink, ZimDigest,
    };

    const FEED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:opds="http://opds-spec.org/2010/catalog">
  <title>Kiwix catalog</title>
  <id>opds-catalog</id>
  <updated>2024-07-01T00:00:00Z</updated>
  <entry>
    <id>zim:wikipedia_en_all_maxi_2024-06</id>
    <title type="html">Wikipedia (en) — Maximum (2024-06)</title>
    <updated>2024-07-02T10:00:00Z</updated>
    <summary>Full Wikipedia in English.</summary>
    <link rel="http://opds-spec.org/acquisition/open-access"
          href="https://download.kiwix.org/zim/wikipedia/wikipedia_en_all_maxi_2024-06.zim"
          type="application/x-zim" length="1000000000"/>
    <link rel="http://opds-spec.org/image/thumbnail" href="https://example.com/thumb.png"/>
  </entry>
  <entry>
    <id>zim:wiktionary_simple_en_2023-12</id>
    <title>Wiktionary (simple en) (2023-12)</title>
    <updated>2024-01-05T00:00:00Z</updated>
    <link rel="http://opds-spec.org/acquisition/open-access"
          href="https://download.kiwix.org/zim/wiktionary/wiktionary_simple_en_2023-12.zim"
          type="application/x-zim"/>
  </entry>
</feed>"#;

    #[test]
    fn parse_catalog_entries() {
        let entries = parse_catalog(FEED);
        assert_eq!(entries.len(), 2);

        let e0 = &entries[0];
        assert_eq!(e0.id.as_deref(), Some("zim:wikipedia_en_all_maxi_2024-06"));
        assert!(e0.title.as_deref().unwrap().contains("Wikipedia"));
        assert_eq!(e0.updated.as_deref(), Some("2024-07-02T10:00:00Z"));
        // Only the acquisition link is selected as download URL; the
        // thumbnail link is present but ignored.
        assert_eq!(e0.links.len(), 2);
        assert_eq!(
            e0.download_url().as_deref(),
            Some("https://download.kiwix.org/zim/wikipedia/wikipedia_en_all_maxi_2024-06.zim")
        );

        let e1 = &entries[1];
        assert_eq!(
            e1.download_url().as_deref(),
            Some("https://download.kiwix.org/zim/wiktionary/wiktionary_simple_en_2023-12.zim")
        );
    }

    #[test]
    fn parse_catalog_tolerates_garbage() {
        // Truncated feed: the first entry is complete, the second is cut off.
        let truncated = &FEED[..FEED.find("</entry>").unwrap() + 8];
        let entries = parse_catalog(truncated);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].download_url().is_some());
    }

    #[test]
    fn parse_catalog_empty() {
        assert!(parse_catalog("").is_empty());
        assert!(parse_catalog("<feed></feed>").is_empty());
    }

    #[test]
    fn base_and_version_parsing() {
        assert_eq!(
            base_and_version("wikipedia_en_all_maxi_2024-06"),
            ("wikipedia_en_all_maxi".to_string(), Some((2024, 6)))
        );
        assert_eq!(
            base_and_version("books_fr_all_2024-01-15"),
            ("books_fr_all".to_string(), Some((2024, 1)))
        );
        assert_eq!(
            base_and_version("stellarium_en_202311"),
            ("stellarium_en".to_string(), Some((2023, 11)))
        );
        // No date tail → base = full name.
        assert_eq!(
            base_and_version("wikipedia_en_all"),
            ("wikipedia_en_all".to_string(), None)
        );
        // A numeric-looking tail that isn't a date stays part of the base.
        assert_eq!(
            base_and_version("openstreetmap_us_2024x"),
            ("openstreetmap_us_2024x".to_string(), None)
        );
    }

    #[test]
    fn find_updates_detects_newer_version() {
        let entries = parse_catalog(FEED);
        // Local is the previous month's wikipedia, plus an unrelated ZIM.
        let local = vec![
            (
                "wikipedia_en_all_maxi_2024-05".to_string(),
                Some("2024-05-31".into()),
            ),
            ("wiktionary_simple_en_2023-12".to_string(), None), // already current
            ("other_zim".to_string(), None),
        ];
        let updates = find_updates(&entries, &local);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].local_name, "wikipedia_en_all_maxi_2024-05");
        assert_eq!(updates[0].catalog_name, "wikipedia_en_all_maxi_2024-06");
        assert!(updates[0].download_url.ends_with(".zim"));
    }

    #[test]
    fn find_updates_none_when_current() {
        let entries = parse_catalog(FEED);
        let local = vec![(
            "wikipedia_en_all_maxi_2024-06".to_string(),
            Some("2024-06-30".into()),
        )];
        assert!(find_updates(&entries, &local).is_empty());
    }

    #[test]
    fn catalog_name_from_url_variants() {
        assert_eq!(
            catalog_name_from_url("https://x/zim/wikipedia/a.zim").as_deref(),
            Some("a")
        );
        assert_eq!(
            catalog_name_from_url("https://x/a.zim?token=1").as_deref(),
            Some("a")
        );
        assert_eq!(catalog_name_from_url("https://x/nope.txt"), None);
        assert_eq!(catalog_name_from_url("https://x/"), None);
    }

    // Build an acquisition entry for a given catalog ZIM name + optional feed
    // <updated>, bypassing XML parsing so the dedup tests stay focused.
    fn entry(name: &str, updated: Option<&str>) -> OpdsEntry {
        OpdsEntry {
            id: Some(format!("zim:{name}")),
            title: None,
            updated: updated.map(|s| s.to_string()),
            links: vec![OpdsLink {
                rel: Some("http://opds-spec.org/acquisition/open-access".into()),
                href: Some(format!("https://x/{name}.zim")),
                media_type: Some("application/x-zim".into()),
                hash: None,
                digest: None,
            }],
        }
    }

    #[test]
    fn find_updates_dedups_to_newest_per_base() {
        // B10: both wiki_2024-06 and wiki_2025-01 are newer than a local
        // wiki_2023-05 → exactly one update, the newest catalog entry.
        let entries = vec![
            entry("wiki_2024-06", Some("2024-07-01T00:00:00Z")),
            entry("wiki_2025-01", Some("2025-02-01T00:00:00Z")),
        ];
        let local = vec![("wiki_2023-05".to_string(), Some("2023-05-31".into()))];
        let updates = find_updates(&entries, &local);
        assert_eq!(updates.len(), 1, "one update per local ZIM (B10)");
        assert_eq!(updates[0].local_name, "wiki_2023-05");
        assert_eq!(updates[0].catalog_name, "wiki_2025-01");

        // Local already the newest → zero updates.
        let local_newest = vec![("wiki_2025-01".to_string(), Some("2025-01-31".into()))];
        assert!(find_updates(&entries, &local_newest).is_empty());
    }

    #[test]
    fn find_updates_undated_keeps_first_in_feed_order() {
        // Two undated same-base entries tie on rank (<updated> y,m) → the
        // first in feed order is kept (deterministic).
        let mut a = entry("wiki", Some("2024-03-01T00:00:00Z"));
        a.title = Some("first".into());
        let mut b = entry("wiki", Some("2024-03-01T00:00:00Z"));
        b.title = Some("second".into());
        let entries = vec![a, b];
        let local = vec![("wiki".to_string(), Some("2024-01-01".into()))];
        let updates = find_updates(&entries, &local);
        assert_eq!(updates.len(), 1, "one update for undated same-base (B10)");
        assert_eq!(
            updates[0].title.as_deref(),
            Some("first"),
            "first in feed order wins on a rank tie"
        );
    }

    // ── Publisher-digest parsing (SEC content-integrity version-key) ────────

    /// The SHA-256 of `"abc"` (NIST vector) in the two forms a catalog may
    /// declare — hex (`hash`) and base64 (`digest`).
    const SHA256_ABC_HEX: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    const SHA256_ABC_B64: &str = "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=";

    #[test]
    fn digest_hex_normalized_to_lowercase() {
        let upper = SHA256_ABC_HEX.to_uppercase();
        let Some(d) = ZimDigest::parse(Some(upper.as_str()), None) else {
            panic!("uppercase hex must parse");
        };
        assert_eq!(
            d.value, SHA256_ABC_HEX,
            "hex claims are normalized to lowercase"
        );
    }

    #[test]
    fn digest_b64_normalizes_to_the_same_hex_claim() {
        let Some(d) = ZimDigest::parse(None, Some(SHA256_ABC_B64)) else {
            panic!("base64 digest must parse");
        };
        assert_eq!(d.value, SHA256_ABC_HEX, "b64 and hex forms must agree");
    }

    #[test]
    fn digest_both_attributes_present_must_agree() {
        assert_eq!(
            ZimDigest::parse(Some(SHA256_ABC_HEX), Some(SHA256_ABC_B64)).map(|d| d.value),
            Some(SHA256_ABC_HEX.to_string())
        );
        // A 32-byte base64 that CONTRADICTS the hex claim (not merely
        // malformed — a readable-but-different digest is the contradiction
        // the rejection rule exists for).
        let other = format!("{}=", "A".repeat(43)); // 32 zero bytes
        assert!(
            ZimDigest::parse(Some(SHA256_ABC_HEX), Some(other.as_str())).is_none(),
            "hash/digest disagreement must reject the whole claim"
        );
    }

    #[test]
    fn digest_malformed_degrades_to_not_declared() {
        assert!(ZimDigest::parse(Some("xyz"), None).is_none());
        assert!(ZimDigest::parse(Some(&"a".repeat(63)), None).is_none());
        assert!(ZimDigest::parse(Some(&"a".repeat(65)), None).is_none());
        assert!(ZimDigest::parse(
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015g"),
            None
        )
        .is_none());
        assert!(ZimDigest::parse(Some(""), None).is_none());
        assert!(ZimDigest::parse(None, Some("!!not-base64!!")).is_none());
        // 6 bytes, not 32.
        assert!(ZimDigest::parse(None, Some("Zm9vYmFy")).is_none());
        // 43 chars: a 32-byte encoding is 44 chars with `==` — the truncated
        // form must be rejected, not leniently decoded.
        assert!(ZimDigest::parse(None, Some(SHA256_ABC_B64.strip_suffix('=').unwrap())).is_none());
    }

    #[test]
    fn b64_decode_rfc4648_vectors() {
        // RFC 4648 §A.1.
        assert_eq!(b64_decode(""), Some(Vec::new()));
        assert_eq!(b64_decode("Zg=="), Some(b"f".to_vec()));
        assert_eq!(b64_decode("Zm8="), Some(b"fo".to_vec()));
        assert_eq!(b64_decode("Zm9v"), Some(b"foo".to_vec()));
        assert_eq!(b64_decode("Zm9vYg=="), Some(b"foob".to_vec()));
        assert_eq!(b64_decode("Zm9vYmE="), Some(b"fooba".to_vec()));
        assert_eq!(b64_decode("Zm9vYmFy"), Some(b"foobar".to_vec()));
        // Non-canonical inputs are rejected.
        assert!(b64_decode("Zm9=vYmFy").is_none(), "mid-string padding");
        assert!(b64_decode("Zm9vYmFy====").is_none(), "4+ pad chars");
        assert!(
            b64_decode("Zm9vYmF").is_none(),
            "length not a multiple of 4"
        );
        assert!(b64_decode("Zm9vYmF$").is_none(), "bad character");
    }

    #[test]
    fn parse_catalog_reads_digest_attributes_from_acquisition_link() {
        let hex = SHA256_ABC_HEX;
        let feed = format!(
            r#"<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>zim:wiki_2024-06</id>
    <title>Wiki</title>
    <link rel="http://opds-spec.org/acquisition/open-access"
          href="https://x/wiki_2024-06.zim" type="application/x-zim"
          hash="{hex}"/>
  </entry>
</feed>"#
        );
        let entries = parse_catalog(&feed);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.links[0].hash.as_deref(), Some(hex));
        assert_eq!(
            e.download_digest().map(|d| d.value),
            Some(hex.to_string()),
            "the claim must come from the same link the URL does"
        );

        // The live Kiwix feed shape (type + length only) declares nothing.
        let live = parse_catalog(FEED);
        assert!(live.iter().all(|e| e.download_digest().is_none()));
    }

    #[test]
    fn parse_link_digest_takes_hash_over_digest() {
        // Both present and equal → the normalized claim (hash is the primary
        // form; the result is the same either way).
        let d = parse_link_digest(Some(SHA256_ABC_HEX), Some(SHA256_ABC_B64))
            .expect("agreement must parse");
        assert_eq!(d.value, SHA256_ABC_HEX);
    }

    #[test]
    fn find_updates_propagates_the_declared_digest() {
        let hex = SHA256_ABC_HEX;
        let feed = format!(
            r#"<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>zim:wiki_2024-06</id>
    <title>Wiki</title>
    <link rel="http://opds-spec.org/acquisition/open-access"
          href="https://x/wiki_2024-06.zim" type="application/x-zim" hash="{hex}"/>
  </entry>
</feed>"#
        );
        let entries = parse_catalog(&feed);
        let local = vec![("wiki_2024-01".to_string(), Some("2024-01-31".into()))];
        let updates = find_updates(&entries, &local);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].digest.as_ref().map(|d| d.value.as_str()),
            Some(hex),
            "the update must carry the publisher's claim for the queue guard"
        );
    }

    #[test]
    fn find_updates_keeps_distinct_bases_sharing_prefix() {
        // Risk guard: `wikipedia_en_all` vs `wikipedia_en_all_maxi` are
        // different bases even though one is a prefix of the other → each
        // local ZIM must still get its own update (dedup must not merge them).
        let entries = vec![
            entry("wikipedia_en_all_2024-06", Some("2024-07-01T00:00:00Z")),
            entry(
                "wikipedia_en_all_maxi_2024-06",
                Some("2024-07-01T00:00:00Z"),
            ),
        ];
        let local = vec![
            (
                "wikipedia_en_all_2024-01".to_string(),
                Some("2024-01-31".into()),
            ),
            (
                "wikipedia_en_all_maxi_2024-01".to_string(),
                Some("2024-01-31".into()),
            ),
        ];
        let updates = find_updates(&entries, &local);
        assert_eq!(updates.len(), 2, "distinct bases must not be merged");
        let names: Vec<&str> = updates.iter().map(|u| u.catalog_name.as_str()).collect();
        assert!(names.contains(&"wikipedia_en_all_2024-06"));
        assert!(names.contains(&"wikipedia_en_all_maxi_2024-06"));
    }
}
