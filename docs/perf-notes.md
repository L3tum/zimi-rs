# Performance notes

## C12 — trgm/prefix search plan at scale

The single `LIKE … OR similarity() > t` predicate was split into three
concurrently-run, individually indexable queries (btree prefix, GIN contains,
GiST similarity), merged by the existing `merge_results`.

The plan-shape question (index scan vs sequential scan at 100k rows) is covered by an automated plan-regression test:
`tests/integration/trgm_plan.rs` (`smoke_trgm_search_arm_no_seq_scan_100k`).
It creates a dedicated temp database (migrated fresh, created and dropped
around the test so the shared dev schema is never touched), bulk-loads 100k
generated articles into `articles` through the production
staging→`articles` upsert path, runs `ANALYZE`, then `EXPLAIN (FORMAT TEXT)`s
the exact arm SQL for the two pg_trgm-indexed arms — the contains arm (Q2,
GIN) and the similarity arm (Q3, GiST) — rebuilt through the same pure public
builders `run` uses (`trgm_contains_sql` / `trgm_similarity_sql`), so the
gate EXPLAINs the same query the server issues. It asserts (a) the plan
contains **no** `Seq Scan on articles` and (b) it references the trgm index
(`idx_articles_title_trgm` for Q2, `idx_articles_title_gist` for Q3). Like
the rest of the integration suite it is DB-gated: it skips cleanly without a
reachable Postgres and hard-fails under `ZIMSERVICE_REQUIRE_DB` (CI's `test`
job), which always runs with a Postgres service. The Q1 prefix (btree) arm is
**not** covered by the gate — its builder stays `pub(super)` — so if its plan
shape is ever in doubt, verify it manually with the recipe further down.

### The predicate

`SearchEngine::search` (trgm/fuzzy/prefix branch) runs the three split arms
concurrently (`build_trgm_arms`, `src/search/sql.rs`), each its own query with
the shared tail `ORDER BY score DESC, a.id LIMIT $n::int8`:

```sql
-- Q1 prefix (btree) — $2 = '<query_lower>%' (ESCAPE '\')
SELECT …, {trgm_weight} * GREATEST(similarity(a.title_lower, $1), 0.0) AS score
FROM articles a JOIN zims z ON z.id = a.zim_id
WHERE a.title_lower LIKE $2 ESCAPE '\'
ORDER BY score DESC, a.id LIMIT $3::int8

-- Q2 contains (GIN) — same shape, $2 = '%<query_lower>%' (ESCAPE '\')
-- Q3 similarity (GiST) — same shape, $2 = search.trgm_threshold (default 0.3, floored at 0.3):
--    WHERE a.title_lower % $1 AND similarity(a.title_lower, $1) > $2::float8
```

The Q2/Q3 arms are gated off for queries shorter than 3 chars
(`trgm_arms_enabled` — prefix-only below that), and each arm fetches up to
`min(limit + offset, 5500)` rows for the post-dedup merge.

### Relevant indexes (from `migrations/001_initial.sql` + `008`)

| Index | Type | Serves |
|---|---|---|
| `idx_articles_title_prefix` (001) | btree on `title_lower` | the `LIKE '…%'` prefix scan |
| `idx_articles_title_trgm` (001) | GIN `gin_trgm_ops` on `title_lower` | the `LIKE '%…%'` contains scan |
| `idx_articles_title_gist` (008) | GiST `gist_trgm_ops` on `title_lower` | the `%`/`similarity()` scan |

(008 also added a second btree, `idx_articles_title_lower`, which 009 dropped
as an exact duplicate of `idx_articles_title_prefix`.)

`title_lower` is `TEXT NOT NULL GENERATED ALWAYS AS (lower(title)) STORED`.

### Expected plan

For a selective term at 100k rows, each of the three split predicates should
hit its own index — `idx_articles_title_prefix` (Q1),
`idx_articles_title_trgm` (Q2), `idx_articles_title_gist` (Q3) — feeding a
top-N (`LIMIT`) sort. A **sequential scan** on any arm would indicate that
arm's predicate is not index-friendly at this scale.

### How to capture the actual plan

Manual recipe: seed a dedicated ZIM (`__itrge__`) with 100k rows in the dev
DB, then run `EXPLAIN (ANALYZE)` against each of the three split predicates:

```sql
-- Q1 prefix (btree)
EXPLAIN (ANALYZE, BUFFERS) SELECT a.id … WHERE a.title_lower LIKE 'art 42%' ESCAPE '\'
-- Q2 contains (GIN)
EXPLAIN (ANALYZE, BUFFERS) SELECT a.id … WHERE a.title_lower LIKE '%art 42%' ESCAPE '\'
-- Q3 similarity (GiST)
EXPLAIN (ANALYZE, BUFFERS) SELECT a.id … WHERE a.title_lower % 'art 42' AND similarity(a.title_lower, 'art 42') > 0.3
```

The regression-relevant result is that none of the three contains `Seq Scan
on articles`. For that plan-shape question the automated gate in
`tests/integration/trgm_plan.rs` is the regression net (it asserts
no-`Seq Scan on articles` + trgm index used for the two pg_trgm arms on a
100k-row temp DB on every integration run where a database is present). This
manual recipe is the deeper-dive tool: it adds the `EXPLAIN (ANALYZE,
BUFFERS)` timing/buffer counters the gate (plain `EXPLAIN (FORMAT TEXT)`) does
not capture, and it covers the Q1 prefix (btree) arm, which the gate does not.
Run the statements by hand (e.g. `psql`) and paste the plans below.

### Result

**Plan-shape gate: automated.** The seq-scan-vs-index question is enforced by
`tests/integration/trgm_plan.rs` in the integration suite: on every run where
a database is present — including CI's `test` job, which runs the suite
against a Postgres service with `ZIMSERVICE_REQUIRE_DB=1` (a skip there would
hard-fail) — the Q2 contains (GIN) and Q3 similarity (GiST) arms on a 100k-row
temp DB must plan an index scan, never a `Seq Scan on articles`. The gate
checks plan *shape* only (`EXPLAIN (FORMAT TEXT)`); it captures no timing.

**`EXPLAIN (ANALYZE)` timing at 100k rows: unmeasured.** No manual run against
the 100k-row fixture has been recorded — the capture metadata (Postgres
version, row count, `pg_trgm` similarity threshold) and the `EXPLAIN
(ANALYZE, BUFFERS)` output for Q1/Q2/Q3 remain to be filled in by whoever runs
the recipe above. At the time of this revision (2026-09-11) no local Postgres
was reachable at the default dev DSN (`postgres://zimservice:zimservice@127.0.0.1:5432/zimservice` — see `docker-compose.yml` /
the `test-integration` target), so a local `cargo test --test integration
trgm_plan` run was skipped; `make test-integration` boots the compose DB and
runs the gate for real.

**Verdict:** the split is implemented (DEC-5) and the plan-shape gate is
automated and enforced in the integration suite; `EXPLAIN (ANALYZE)` timing at
100k rows remains a pending manual measurement.

---

## PERF-4 (WI-36) — keep or drop `idx_articles_title_prefix` (btree)

**Question.** The Q1 prefix arm (`WHERE title_lower LIKE 'q%'`) can use the
btree `idx_articles_title_prefix`, but `gin_trgm_ops` (the GIN trgm index) can
serve `LIKE 'q%'` too. Is the btree redundant — can we drop it (migration 014)
and let the GIN index cover the prefix shape as well?

**How it is measured (manual).** Seed the 100k-row `__itrge__` fixture in the
dev DB and EXPLAIN (ANALYZE) the five shapes below, printing each plan's node:

| Shape | Predicate / ordering | Index it may use |
|-------|----------------------|------------------|
| Q1 | `title_lower LIKE 'q%'` + `ORDER BY score` | btree prefix *or* GIN |
| Q2 | `title_lower LIKE '%q%'` | GIN |
| Q3 | `title_lower % $1 AND similarity(...) > $2` | GiST |
| SUG | prefix arm (weight 1.0) | btree prefix *or* GIN |
| Q5 | `ORDER BY title_lower ASC LIMIT 20` (no predicate — hypothetical shape; no current endpoint issues it) | btree (ordering) |

Q1/Q2/Q3/SUG are the regression-relevant shapes: none should fall back to a
`Seq Scan on articles` at 100k rows. Q5 is recorded for the decision but is
**not** seq-scan-gated — a seq-scan+top-N for a pure ordering is a legal
planner choice that this decision weighs.

**Decision rule (fixed, no judgment).**
- **DROP** `idx_articles_title_prefix` (emit
  `migrations/014_articles_title_prefix_drop.sql`:
  `DROP INDEX IF EXISTS idx_articles_title_prefix;` + a `MIGRATIONS` entry
  after 013) **only if**, across *all* recorded EXPLAINs, the planner uses the
  GIN trgm index or a seq scan for the Q1 `LIKE 'q%'` prefix shape **and**
  never uses `Index Scan using idx_articles_title_prefix` for any of the five
  shapes.
- **Else KEEP** it and record the reason here.

After a drop the GIN trgm index serves the prefix shape; the Q1 no-seq-scan
shape check is the regression net — re-measure and confirm Q1 is still not a
seq scan. Precedent for the drop statement: `009:4`
`DROP INDEX IF EXISTS idx_articles_title_lower`. Non-`CONCURRENTLY` (single
transaction migrate harness; `010:16` precedent).

**Outcome.** _PENDING MANUAL MEASUREMENT._ The measurement needs Postgres (the
100k-row fixture + `EXPLAIN ANALYZE`). The keep/drop call is deferred to a
manual measurement run — CI has no perf job (the former `test-db-perf` push
job referenced a test that did not exist and was removed). **Default is KEEP**
— migration 014 is created *only* in the drop branch,
and that branch is not reached until the rule's drop condition is
measured-confirmed. _To fill in after the measurement run:_ paste the five plan
nodes
here, tick the decision rule, and either create migration 014 (drop) or record
KEEP with the reason.

---

## Deferred (with rationale)

The following performance improvements were identified during review but are
intentionally deferred to a future pass:

1. **Article-text read cache** – Caching full article content would reduce
   disk I/O for repeated reads, but adds memory-pressure complexity. The
   current mmap-backed ZIM handles are already efficient for single-instance
   deployments.
2. **`raw_content` 64 MB `to_vec` → streaming** – Streaming large article
   bodies chunk-by-chunk would reduce peak memory, but depends on the `zim`
   crate exposing a stable streaming/borrow API (it currently exposes only an
   owned `bytes()`; blocked on the crate). Revisit when it stabilises.
3. **`content_preview` payload gating** – The search API returns a 2 000-char
   preview per result. Gating this behind a query parameter would reduce
   response size for clients that only need titles, but adds API surface.
   Low priority given the small fixed size.
4. **Parallel `index_zims --all`** – Indexing multiple ZIM archives in
   parallel (e.g. via a bounded thread pool) could speed up initial ingest.
   Deferred because the I/O-bound COPY is the bottleneck and Postgres itself
   serialises concurrent COPY to the same table; gains would be marginal for
   the typical < 20 ZIM deployment.
