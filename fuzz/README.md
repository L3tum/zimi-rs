# Fuzz workspace (cargo-fuzz)

Attacker-influenced parsers — `.zim` archives (torrent/OPDS-sourced) and OPDS
Atom feed XML — are parsed by third-party crates (`zim` 0.5, `quick-xml`
0.41) that RUSTSEC's advisory DB cannot see; this standalone cargo-fuzz
workspace is the tracking mechanism for their robustness (see the `audit`
job note in `.github/workflows/ci.yml`). It is a separate workspace (the
empty `[workspace]` table in `Cargo.toml` keeps the repo-root build
unaffected) and is **not** wired into CI (self-hosted runner constraints) —
run it locally with a nightly toolchain: `cargo +nightly fuzz run fuzz_zim`
or `cargo +nightly fuzz run fuzz_opds` (first run builds the harnesses;
each target's corpus is seeded under `fuzz/corpus/<target>/` with the
`tests/fixtures/tiny.zim` fixture / a minimal Kiwix-style feed, and new
interesting inputs accumulate there plus in `fuzz/artifacts/` on a crash).
