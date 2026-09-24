//! Fuzz the `zim` crate's archive open/parse path — the exact call the
//! production indexing pipeline uses (`src/zim/index.rs`,
//! `open_zim_blocking`: `zim::Zim::new(&file_path)`). `.zim` files come from
//! torrents and OPDS downloads, i.e. attacker-influenced bytes, and RUSTSEC
//! can't see this crate's robustness (see the `audit` job note in
//! .github/workflows/ci.yml).

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The `zim` crate opens archives by path, so write the fuzz bytes to a
    // temp file (single-threaded libfuzzer: one name is safe) and drive the
    // production entry point on it. The Result is intentionally ignored —
    // a clean Err is the desired behavior for a malformed archive; the
    // harness catches crashes/panics/UB instead.
    let path = std::env::temp_dir().join(format!(
        "zimservice-fuzz-zim-{}.zim",
        std::process::id()
    ));
    let _ = std::fs::write(&path, data);
    let _ = zim::Zim::new(&path);
    let _ = std::fs::remove_file(&path);
});
