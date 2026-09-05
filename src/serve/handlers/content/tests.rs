use super::*;

// ── Range parsing ─────────────────────────────────────────────────────────

#[test]
fn range_parser_valid() {
    assert_eq!(
        parse_single_range("bytes=0-4", 100),
        Some(ParsedRange::Slice { start: 0, end: 4 })
    );
    assert_eq!(
        parse_single_range("bytes=10-", 100),
        Some(ParsedRange::Slice { start: 10, end: 99 })
    );
    assert_eq!(
        parse_single_range("bytes=-5", 100),
        Some(ParsedRange::Slice { start: 95, end: 99 })
    );
    // end clamped to last byte
    assert_eq!(
        parse_single_range("bytes=97-104", 100),
        Some(ParsedRange::Slice { start: 97, end: 99 })
    );
    assert_eq!(
        parse_single_range("bytes= 2 - 4", 100),
        Some(ParsedRange::Slice { start: 2, end: 4 })
    );
}

#[test]
fn range_parser_unsatisfiable() {
    assert_eq!(
        parse_single_range("bytes=100-", 100),
        Some(ParsedRange::Unsatisfiable)
    );
    assert_eq!(
        parse_single_range("bytes=150-200", 100),
        Some(ParsedRange::Unsatisfiable)
    );
    assert_eq!(
        parse_single_range("bytes=-0", 100),
        Some(ParsedRange::Unsatisfiable)
    );
    assert_eq!(
        parse_single_range("bytes=5-2", 100),
        Some(ParsedRange::Unsatisfiable)
    );
    // BUG-15: start offset overflowing u64 is beyond EOF → unsatisfiable (416).
    assert_eq!(
        parse_single_range("bytes=18446744073709551616-", 100),
        Some(ParsedRange::Unsatisfiable)
    );
    assert_eq!(
        parse_single_range("bytes=999999999999999999999-100", 100),
        Some(ParsedRange::Unsatisfiable)
    );
}

#[test]
fn slice_range_bytes_resolves_window() {
    // PERF-1: the closure copies exactly this window out of the mmap.
    // In-range: window is exactly [start, end+1).
    assert_eq!(
        slice_range_bytes(100, Some(ParsedRange::Slice { start: 10, end: 19 })),
        Some((10, 20))
    );
    // end >= total → clamped to the last byte (exclusive end == total).
    assert_eq!(
        slice_range_bytes(100, Some(ParsedRange::Slice { start: 5, end: 99 })),
        Some((5, 100))
    );
    // Unsatisfiable (e.g. a 0-length suffix) → no slice (416 path).
    let u = parse_single_range("bytes=-0", 100);
    assert_eq!(u, Some(ParsedRange::Unsatisfiable));
    assert_eq!(slice_range_bytes(100, u), None);
    // Multi-range → parse_single_range returns None → no slice (full body).
    let m = parse_single_range("bytes=0-4, 10-14", 100);
    assert_eq!(m, None);
    assert_eq!(slice_range_bytes(100, m), None);
}

#[test]
fn range_parser_overflow_end_clamps_to_last_byte() {
    // BUG-15 (RFC 7233 §3.1): an end offset that overflows u64 clamps to the
    // last byte — the range is satisfiable, not an error.
    assert_eq!(
        parse_single_range("bytes=5-18446744073709551616", 100),
        Some(ParsedRange::Slice { start: 5, end: 99 })
    );
    // An overflowing suffix length means "the whole entity".
    assert_eq!(
        parse_single_range("bytes=-18446744073709551616", 100),
        Some(ParsedRange::Slice { start: 0, end: 99 })
    );
}

#[test]
fn if_none_match_matches_matrix() {
    // Exact match.
    assert!(if_none_match_matches("\"123-456\"", "\"123-456\""));
    // Multi-element list containing the tag (whitespace-tolerant).
    assert!(if_none_match_matches("\"0-0\", \"123-456\"", "\"123-456\""));
    assert!(if_none_match_matches("  \"123-456\"  ", "\"123-456\""));
    // `*` matches any existing entity.
    assert!(if_none_match_matches("*", "\"123-456\""));
    // Weak (`W/…`) element never matches a strong tag.
    assert!(!if_none_match_matches("W/\"123-456\"", "\"123-456\""));
    // Mismatch → false.
    assert!(!if_none_match_matches("\"0-0\"", "\"123-456\""));
    // A different tag in the list, no match.
    assert!(!if_none_match_matches("\"0-0\", \"999-1\"", "\"123-456\""));
}

#[test]
fn range_parser_ignored() {
    // multi-range → serve full body
    assert_eq!(parse_single_range("bytes=0-4, 10-14", 100), None);
    // not the bytes unit
    assert_eq!(parse_single_range("items=0-4", 100), None);
    // garbage / malformed
    assert_eq!(parse_single_range("bytes=abc", 100), None);
    assert_eq!(parse_single_range("", 100), None);
    assert_eq!(parse_single_range("bytes=5", 100), None);
}

#[test]
fn range_parser_empty_body() {
    assert_eq!(
        parse_single_range("bytes=0-", 0),
        Some(ParsedRange::Unsatisfiable)
    );
    // a suffix range against a zero-length body is also unsatisfiable (B7)
    assert_eq!(
        parse_single_range("bytes=-5", 0),
        Some(ParsedRange::Unsatisfiable)
    );
}

// ── Title derivation ─────────────────────────────────────────────────────

#[test]
fn derive_title_from_path_derives() {
    use crate::zim::index::derive_title_from_path;
    assert_eq!(derive_title_from_path("A/Barack_Obama"), "Barack Obama");
    assert_eq!(derive_title_from_path("U/docs/intro_page"), "intro page");
    assert_eq!(derive_title_from_path("plain_title"), "plain title");
}
