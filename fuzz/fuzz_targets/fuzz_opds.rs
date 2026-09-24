//! Fuzz the OPDS Atom feed XML parser — the production entry point
//! `zimservice::torrent::opds::parse_catalog` (quick-xml 0.41
//! `Reader::from_str` + `read_event_into` loop in `src/torrent/opds.rs`).
//! OPDS catalogs are fetched from operator-configured URLs (Kiwix by
//! default) and are attacker-influenced; the parser is tolerant by design,
//! but its quick-xml event walk is third-party parsing of untrusted bytes.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Mirror the production decode exactly (`fetch_catalog` in
    // src/torrent/opds.rs): feed bytes → `String::from_utf8_lossy` →
    // `parse_catalog`.
    let xml = String::from_utf8_lossy(data);
    let _ = zimservice::torrent::opds::parse_catalog(&xml);
});
