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

use crate::db::Pool;
use crate::error::{Error, Result};
use crate::netguard::{resolve_download_host, validate_download_url};
use crate::zim::ZimManager;

use super::poller::{build_download_client, ClientProfile, DownloadPoller};
use super::*;

/// One `<entry>` in the OPDS feed.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct OpdsEntry {
    pub id: Option<String>,
    pub title: Option<String>,
    /// RFC 3339 timestamp from the feed, if present.
    pub updated: Option<String>,
    pub links: Vec<OpdsLink>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct OpdsLink {
    pub rel: Option<String>,
    pub href: Option<String>,
    /// The `type` attribute (e.g. `application/x-zim`).
    pub media_type: Option<String>,
}

impl OpdsEntry {
    /// The acquisition URL for the ZIM file, if any.
    pub fn download_url(&self) -> Option<String> {
        self.links.iter().find_map(|l| {
            let href = l.href.as_deref()?;
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
            (is_acq || is_zim).then(|| href.to_string())
        })
    }
}

/// An available update for a locally-installed ZIM.
#[derive(Debug, Clone, serde::Serialize)]
pub struct OpdsUpdate {
    pub local_name: String,
    pub catalog_name: String,
    pub title: Option<String>,
    pub download_url: String,
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
                        for attr in e.attributes().flatten() {
                            let key = std::str::from_utf8(attr.key.local_name().into_inner())
                                .unwrap_or("");
                            let val = String::from_utf8_lossy(&attr.value).into_owned();
                            match key {
                                "rel" => rel = Some(val),
                                "href" => href = Some(val),
                                "type" => media_type = Some(val),
                                _ => {}
                            }
                        }
                        if rel.is_some() || href.is_some() {
                            entry.links.push(OpdsLink {
                                rel,
                                href,
                                media_type,
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
pub async fn fetch_catalog(http: &reqwest::Client, url: &str) -> Result<Vec<OpdsEntry>> {
    let body = http
        .get(url)
        .header("Accept", "application/atom+xml")
        .send()
        .await
        .map_err(Error::Http)?
        .text()
        .await
        .map_err(Error::Http)?;
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

        // Best (newest) candidate for this local ZIM: (rank, entry, name, url).
        let mut best: Option<((u32, u32), &OpdsEntry, String, String)> = None;

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
                Some((br, _, _, _)) => rank > *br,
            };
            if replace {
                best = Some((rank, entry, catalog_name, url));
            }
        }

        if let Some((_, entry, catalog_name, url)) = best {
            updates.push(OpdsUpdate {
                local_name: local_name.clone(),
                catalog_name,
                title: entry.title.clone(),
                download_url: url,
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
    http: &reqwest::Client,
    opds_url: &str,
    zims: &ZimManager,
) -> Result<Vec<OpdsUpdate>> {
    let entries = fetch_catalog(http, opds_url).await?;
    let local: Vec<(String, Option<String>)> = zims
        .list()
        .iter()
        .map(|m| (m.name.clone(), m.date.clone()))
        .collect();
    Ok(find_updates(&entries, &local))
}

impl DownloadPoller {
    // ── OPDS auto-update ─────────────────────────────────────────────────────

    pub(super) async fn opds_check(&self) -> Result<()> {
        // PERF-12: snapshot all poller settings once per opds_check.
        let p = self.settings.poller_params_snapshot();
        if !p.opds_auto_update {
            return Ok(());
        }
        let url = p.opds_url.clone();
        if url.is_empty() {
            return Ok(());
        }
        // SSRF guard: validate the OPDS URL before fetching, same rules as
        // direct downloads. Pin the resolved address to close the rebinding window.
        let allow_private = p.allow_private_networks;
        validate_download_url(&url, allow_private)?;
        let pin = resolve_download_host(&url, allow_private, false).await?;
        let http = build_download_client(&self.settings, ClientProfile::Control, pin)?;
        let updates = opds::check_updates(&http, &url, &self.zims).await?;
        queue_opds_updates(&self.db, &updates).await
    }
}

/// Queue OPDS auto-updates (TEST-5 seam, extracted from
/// [`DownloadPoller::opds_check`]): skip an update whose URL already has a
/// queued/in-flight/installed row (B10), insert the rest as `queued`.
///
/// Loopback URLs never reach here — the SSRF gate in `opds_check`
/// (`validate_download_url`) fires first.
async fn queue_opds_updates(db: &Pool, updates: &[opds::OpdsUpdate]) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let client = db.get().await.map_err(Error::Pool)?;
    for u in updates {
        // Second line of defense (B10): `find_updates` already dedups to
        // one (newest) update per local ZIM; this per-URL guard prevents
        // re-queueing while an older version of the same catalog is still
        // queued/downloading, so the update lands cleanly on completion.
        let pending = client
            .query_opt(
                &format!(
                    "SELECT 1 FROM downloads WHERE url = $1 AND status IN ({live})",
                    live = crate::torrent::in_list([
                        crate::torrent::DownloadStatus::Queued,
                        crate::torrent::DownloadStatus::Downloading,
                        crate::torrent::DownloadStatus::Complete,
                        crate::torrent::DownloadStatus::Seeding,
                    ])
                ),
                &[&u.download_url],
            )
            .await
            .map_err(Error::Database)?;
        if pending.is_some() {
            continue;
        }
        match client
            .execute(
                "INSERT INTO downloads (name, url, status) VALUES ($1, $2, $3)",
                &[
                    &u.catalog_name,
                    &u.download_url,
                    &crate::torrent::DownloadStatus::Queued.as_str(),
                ],
            )
            .await
        {
            Ok(_) => {}
            Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
                // Concurrent insert (e.g. manual POST /downloads) — safe to skip.
                continue;
            }
            Err(e) => return Err(Error::Database(e)),
        }
        tracing::info!(
            "OPDS: queued auto-update for {} → {} ({})",
            u.local_name,
            u.catalog_name,
            u.download_url
        );
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    use crate::settings::{KEY_TORRENT_AUTO_UPDATE, KEY_TORRENT_OPDS_URL};
    use crate::torrent::poller::test_pool;

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

    /// TEST-5: wiremock binds 127.0.0.1 and netguard hard-blocks loopback,
    /// so a loopback OPDS URL must be rejected by the SSRF gate BEFORE any
    /// HTTP I/O (DB-less: the dead pool never gets used).
    #[tokio::test]
    async fn opds_check_rejects_loopback_url_before_http() {
        use wiremock::MockServer;
        let server = MockServer::start().await;
        let mut cfg = deadpool_postgres::Config::new();
        cfg.url = Some("postgres://u:p@127.0.0.1:1/nodb".into());
        let pool = cfg
            .builder(tokio_postgres::NoTls)
            .unwrap()
            .max_size(1)
            .build()
            .unwrap();
        let mut map = crate::settings::default_settings();
        map.insert(KEY_TORRENT_AUTO_UPDATE.into(), serde_json::json!(true));
        map.insert(KEY_TORRENT_OPDS_URL.into(), serde_json::json!(server.uri()));
        let settings = crate::settings::SettingsCache::new_with_map(
            pool.clone(),
            map,
            std::collections::HashMap::new(),
        );
        let tmp = tempfile::tempdir().unwrap();
        let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());
        let poller = DownloadPoller::new(
            pool,
            settings,
            zims,
            crate::torrent::QbitClientCache::new(),
            None,
            String::new(),
            String::new(),
        );
        poller
            .opds_check()
            .await
            .expect_err("loopback OPDS URL must be rejected by the SSRF gate");
        assert!(
            server
                .received_requests()
                .await
                .unwrap_or_default()
                .is_empty(),
            "SSRF gate must fire before any HTTP I/O"
        );
    }

    /// TEST-5 (CI-DB): B10 guard — an update whose URL already has a queued
    /// row is skipped; a new URL is inserted as `queued`.
    #[tokio::test]
    async fn queue_opds_updates_queues_new_skips_pending() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let x = "http://it.example/x.zim";
        let y = "http://it.example/y.zim";
        let c = pool.get().await.expect("conn");
        c.execute("DELETE FROM downloads WHERE url IN ($1, $2)", &[&x, &y])
            .await
            .unwrap();
        c.execute(
            "INSERT INTO downloads (name, url, status) VALUES ('it-x', $1, 'queued')",
            &[&x],
        )
        .await
        .unwrap();

        let updates = vec![
            super::super::opds::OpdsUpdate {
                local_name: "x".into(),
                catalog_name: "x2".into(),
                title: None,
                download_url: x.into(),
            },
            super::super::opds::OpdsUpdate {
                local_name: "y".into(),
                catalog_name: "y2".into(),
                title: None,
                download_url: y.into(),
            },
        ];
        super::queue_opds_updates(&pool, &updates)
            .await
            .expect("queueing runs");

        let x_count: i64 = c
            .query_one("SELECT count(*) FROM downloads WHERE url = $1", &[&x])
            .await
            .unwrap()
            .get(0);
        assert_eq!(x_count, 1, "B10 guard: pending URL must not be re-queued");
        let y_status: Option<String> = c
            .query_opt("SELECT status FROM downloads WHERE url = $1", &[&y])
            .await
            .unwrap()
            .map(|r| r.get::<_, String>(0));
        assert_eq!(
            y_status.as_deref(),
            Some("queued"),
            "new URL must be queued exactly once"
        );

        // Cleanup (shared single-DB suite).
        c.execute("DELETE FROM downloads WHERE url IN ($1, $2)", &[&x, &y])
            .await
            .unwrap();
    }
}
