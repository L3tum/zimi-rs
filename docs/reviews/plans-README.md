# Plans

> **Note (2026-09):** the `plans/` directory was retired — its contents moved
> to `docs/reviews/` (this file: `docs/reviews/plans-README.md`). The rest of
> this document is the historical record of the old `plans/` directory and is
> kept as-is.

Scratch directory for the themed review/fix pipeline. Each run produces two
artifacts, kept at the top level:

- `research-findings-<timestamp>.md` — the consolidated findings from the
  parallel research subagents.
- `deep-plan-<timestamp>.md` — the approved, step-by-step fix plan.

Older runs are moved to `archive/` (created on first cleanup) so the top level
always shows only the current run's two files.

These files are **not** part of the shipped product and are consumed by no
build, docs, or runtime path (verified: no `plans/` references in `Makefile`,
`.github/`, or `src/`). They are kept only for history and cross-referencing
between pipeline runs.

## Hygiene policy (2026-09-01 review, D1)

`archive/` is **kept, not deleted**. The 2026-09-01 themed review suggested
deleting the archive as "prose with zero product value," but the project
`README.md` documents `plans/` as *the process record of design decisions and
their rationale*, and this tree is **not** under version control — deleting the
archive would be permanent and unrecoverable. So the standing policy is: superseded
runs and stray scratch files are *moved* into `archive/` (never deleted), and the
top level always shows only the current run's two artifacts.

> A `.dockerignore` keeps the build context lean (excludes `plans/`), so the
> archive never ships; its only cost is local disk.

## Known deferrals (2026-08-27 run)

Two items from the deep-plan are intentionally left uncovered; both are
blocked by the environment, not by the fix itself:

- **TEST#6 — full poller-lifecycle integration** (cancel-race, byte-cap,
  hash-rebind, recovery). Needs a live qBittorrent plus real `.zim` files, and
  `zim` 0.5 is read-only so the suite cannot author fixtures. The pure-helper
  coverage that P1/P5 *do* add stands in: completion-guard status matrix,
  stale-`.tmp` predicate, B7 cancel-before-verify ordering (asserted by code
  inspection) and the wiremock `filter=all` contract test.
- **206 Range byte-identity fixture** (`raw_content_range_206`,
  `tests/integration.rs`). Stays `#[ignore]d` — it needs a committed binary
  `.zim` fixture that must come from an external source. `parse_single_range`
  is covered by its own unit tests in `src/serve/handlers/content/`.
