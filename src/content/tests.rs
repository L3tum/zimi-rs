use super::*;

// ── Chunking ─────────────────────────────────────────────────────────────

#[test]
fn chunks_deterministic() {
    // Expected-output assertion (the old version compared two calls of
    // the same pure fn — a tautology that could never catch a real
    // regression).
    let text = "one\n\ntwo\n\nthree\n\nfour";
    let chunks = chunk_text(text, 15, 4);
    assert_eq!(chunks.len(), 2);
    assert_eq!((chunks[0].start, chunks[0].end), (0, 10));
    assert_eq!(chunks[0].text, "one\n\ntwo");
    // overlap of 4: second chunk starts 4 bytes before the first's end
    assert_eq!((chunks[1].start, chunks[1].end), (6, 21));
    assert_eq!(chunks[1].text, "wo\n\nthree\n\nfour");
}

#[test]
fn chunks_progress_and_cover() {
    // No boundaries at all: must hard-cut, advance, and reach the end.
    let text = "abcdefghij".repeat(100); // 1000 chars
    let chunks = chunk_text(&text, 30, 10);
    assert!(!chunks.is_empty());
    let mut prev_start = 0usize;
    for (i, c) in chunks.iter().enumerate() {
        assert!(c.end > c.start, "chunk must span at least one byte");
        if i > 0 {
            assert!(c.start > prev_start, "chunking must make forward progress");
        }
        assert!(
            c.end - c.start <= 30,
            "chunk must not exceed the window size"
        );
        assert_eq!(c.index, i, "chunk index must match position");
        prev_start = c.start;
    }
    let last_end = chunks.last().unwrap().end;
    assert_eq!(last_end, text.len(), "chunking must cover the whole text");
}

#[test]
fn chunks_adversarial_boundary_at_window_start() {
    // Paragraph boundary at the start of every window — the input class
    // that made the old implementation loop forever.
    let text = "A\n\n".repeat(500);
    let chunks = chunk_text(&text, 8, 2);
    let mut prev_start = 0usize;
    for (i, c) in chunks.iter().enumerate() {
        assert!(c.end > c.start);
        if i > 0 {
            assert!(c.start > prev_start);
        }
        prev_start = c.start;
    }
}

#[test]
fn chunks_leading_boundary_regression() {
    // Regression: the old implementation looped forever when a \n\n boundary
    // sat at the window start and nothing else matched in the window
    // (chunk_end == start → start = chunk_end - overlap saturates to itself).
    let text = "\n\n".to_string() + &"a".repeat(1000);
    let chunks = chunk_text(&text, 10, 2);
    assert!(!chunks.is_empty());
    let last_end = chunks.last().unwrap().end;
    assert_eq!(last_end, text.len());
}

#[test]
fn chunks_overlap_present() {
    let text = "word ".repeat(200); // 1000 chars, a space every 5
    let chunks = chunk_text(&text, 50, 20);
    assert!(chunks.len() > 1);
    let e0 = chunks[0].end;
    let s1 = chunks[1].start;
    assert!(s1 < e0, "second chunk should overlap the first");
}

#[test]
fn chunks_empty_and_zero_size() {
    assert!(chunk_text("", 100, 10).is_empty());
    assert!(chunk_text("hello", 0, 10).is_empty());
}

#[test]
fn chunks_non_ascii_no_panic() {
    // Regression: byte-indexed slicing panicked on non-ASCII text —
    // the first accented article used to take down /chunks.
    let text = "Héllo wörld. Ünïcödé tëxt. Zündörs.".repeat(50);
    for chunk_size in 1..=12 {
        for overlap in 0..3 {
            let chunks = chunk_text(&text, chunk_size, overlap);
            assert!(!chunks.is_empty());
            for c in &chunks {
                // text is a String, so always valid UTF-8; ensure it's non-empty
                assert!(!c.text.is_empty());
            }
        }
    }
    // CJK: 3-byte chars throughout, so any byte boundary falls
    // mid-character without a floor.
    let cjk = "日本語テキスト".repeat(100);
    for (chunk_size, overlap) in [(7usize, 3usize), (1, 0), (20, 5)] {
        let chunks = chunk_text(&cjk, chunk_size, overlap);
        assert!(!chunks.is_empty(), "chunk_size={chunk_size}");
        let last_end = chunks.last().unwrap().end;
        assert_eq!(last_end, cjk.len(), "must consume the whole text");
    }
}

#[test]
fn chunks_bounded_overlap_still_progresses() {
    // Regression (B8): overlap ≈ size used to allow a window advance of
    // (size - overlap) → ~len/(size-overlap) chunks; with the clamp
    // (overlap ≤ size/2) the advance is ≥ size/2, so 256 KB yields
    // ≤ ~2·len/size chunks. Feed the *clamped* params, as get_chunks does.
    let text = "word ".repeat(52_429); // 262_145 chars > MAX_READ_BYTES
    let (size, overlap) = clamp_chunk_params(100_000, 99_999);
    let chunks = chunk_text(&text, size, overlap);
    assert!(
        chunks.len() <= 2 * text.len() / size + 2,
        "clamped overlap must bound the chunk count, got {}",
        chunks.len()
    );
    // Progress + coverage invariants still hold.
    let last_end = chunks.last().expect("chunks").end;
    assert_eq!(last_end, text.len(), "must consume the whole text");
    for w in chunks.windows(2) {
        assert!(
            w[1].start >= w[0].start && w[1].start < w[0].end + size / 2 + 1,
            "windows must progress"
        );
    }
}

// ── Truncation ─────────────────────────────────────────────────────────

#[test]
fn truncate_preview_non_ascii_counts_chars_not_bytes() {
    // Emojis are multi-byte; `full_length` must be the char count, and the
    // truncated body must be exactly `max_len` chars + a `…` suffix (a byte cut
    // would corrupt the emoji).
    let text = "a😀b".repeat(10); // 30 chars
    let (content, full_len) = truncate_preview(&text, 5);
    assert_eq!(full_len, 30, "full_length is the source char count");
    assert_eq!(content, "a😀ba😀…", "5 chars + ellipsis, no broken emoji");
    assert_eq!(content.chars().count(), 6);

    // Short text is returned verbatim with no ellipsis.
    let (c2, f2) = truncate_preview("héllo😀", 100);
    assert_eq!(c2, "héllo😀");
    assert_eq!(f2, 6);
}

#[test]
fn truncate_preview_exact_boundary_no_ellipsis() {
    // Exactly `max_len` chars is NOT truncated — no `…` suffix, returned
    // verbatim (the `>` comparison, not `>=`).
    let (content, full_len) = truncate_preview("abcde", 5);
    assert_eq!(full_len, 5);
    assert_eq!(content, "abcde", "exact boundary is not cut");
    assert_eq!(content.chars().count(), 5);
}

#[test]
fn truncate_preview_zero_max_len() {
    // max_len = 0 on non-empty text → zero kept chars + the `…` suffix;
    // `full_char_count` still reports the source length.
    let (content, full_len) = truncate_preview("hello", 0);
    assert_eq!(full_len, 5);
    assert_eq!(content, "…", "nothing kept, only the ellipsis");
    assert_eq!(content.chars().count(), 1);
}

#[test]
fn default_read_max_length_caps_a_long_article() {
    // P2 (2026-09 review): pin the `/read` default intent — 8000 chars is a
    // client convenience, not a ceiling: a larger client-requested cap is
    // allowed up to the 256 KB raw-read bound.
    let text = "x".repeat(DEFAULT_READ_MAX_LENGTH + 100);
    let (content, full_len) = truncate_preview(&text, DEFAULT_READ_MAX_LENGTH);
    assert_eq!(full_len, DEFAULT_READ_MAX_LENGTH + 100);
    assert_eq!(
        content.chars().count(),
        DEFAULT_READ_MAX_LENGTH + 1, // `…` suffix when cut
    );
    assert!(content.ends_with('…'));
    // The default is a convenience, not a ceiling: a larger client-requested
    // cap is honored as long as it stays under the raw-read bound…
    assert_eq!(
        clamp_read_max_length(DEFAULT_READ_MAX_LENGTH * 10),
        DEFAULT_READ_MAX_LENGTH * 10
    );
    // …and is clamped to the 256 KB bound above it.
    assert_eq!(clamp_read_max_length(MAX_READ_BYTES * 2), MAX_READ_BYTES);
}

// ── Raw-read cap + lossy decode (BUG-11 / BUG-12) ────────────────────────

#[test]
fn cap_read_flags_oversize() {
    // One byte over the cap → capped flag set, output clamped to MAX_READ_BYTES.
    let big = vec![b'a'; MAX_READ_BYTES + 1];
    let (out, capped) = cap_read(&big);
    assert!(capped, "oversized source must set the raw_capped flag");
    assert_eq!(out.len(), MAX_READ_BYTES);

    // Small source → untouched, no flag.
    let small = b"hello";
    let (out2, capped2) = cap_read(small);
    assert!(!capped2);
    assert_eq!(out2, small);
}

#[test]
fn read_max_length_capped_at_read_cap() {
    // The `max_length` read argument is clamped at `MAX_READ_BYTES` (the
    // available source text is itself bounded by the raw-read cap), so
    // uncapped requests like `u64::MAX` can never return more content —
    // same bound for the HTTP and MCP front ends.
    assert_eq!(clamp_read_max_length(8000), 8000);
    assert_eq!(clamp_read_max_length(MAX_READ_BYTES), MAX_READ_BYTES);
    assert_eq!(clamp_read_max_length(MAX_READ_BYTES + 1), MAX_READ_BYTES);
    assert_eq!(clamp_read_max_length(usize::MAX), MAX_READ_BYTES);
}

#[test]
fn html_to_text_keeps_decodable() {
    // A single bad byte must not drop the whole body (BUG-12): the decodable
    // text around it survives.
    let html = b"<p>caf\xff</p>hi";
    let text = html_to_text(html);
    assert!(!text.is_empty());
    assert!(
        text.contains("caf"),
        "text before the bad byte must survive: {text:?}"
    );
    assert!(
        text.contains("hi"),
        "text after the bad byte must survive: {text:?}"
    );
}
