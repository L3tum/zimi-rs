# Performance notes

## C12 — trgm/prefix `OR similarity()` plan at scale

**Goal:** confirm whether the trigram/prefix search predicate uses its indexes
at realistic scale (100k rows), or degrades to a sequential scan.

**Resolved (DEC-5, Step 4.2):** the single `LIKE … OR similarity() > t`
predicate was split into three concurrently-run, individually indexable
queries (btree prefix, GIN contains, GiST similarity), merged by the existing
`merge_results`. The harness test `trgm_index_perf_check` now asserts none of
the three falls back to a `Seq Scan` over `articles`.

### The predicate

`SearchEngine::search` (trgm/fuzzy/prefix branch) issues, per query:

```sql
SELECT …, {trgm_weight} * GREATEST(similarity(a.title_lower, $1), 0.0) AS score
FROM articles a JOIN zims z ON z.id = a.zim_id
WHERE a.title_lower LIKE $2 ESCAPE '\'      -- $2 = '<query_lower>%'
   OR similarity(a.title_lower, $1) > $3    -- $3 = search.trgm_threshold (default 0.3)
ORDER BY score DESC LIMIT <limit_i32>
```

### Relevant indexes (from `migrations/001_initial.sql`)

| Index | Type | Serves |
|---|---|---|
| `idx_articles_title_prefix` / `idx_articles_title_lower` | btree on `title_lower` | the `LIKE '…%'` prefix scan |
| `idx_articles_title_trgm` | GIN `gin_trgm_ops` on `title_lower` | the `LIKE '%…%'` contains scan |
| `idx_articles_title_gist` | GiST `gist_trgm_ops` on `title_lower` | the `similarity() > $3` scan |

`title_lower` is `TEXT NOT NULL GENERATED ALWAYS AS (lower(title)) STORED`.

### Expected plan

For a selective term at 100k rows, the planner should produce a
`BitmapOr` (or a merge of the two index scans) over
`idx_articles_title_prefix` (for the prefix) and `idx_articles_title_trgm`
(for the similarity), feeding a top-N (`LIMIT`) sort. A **sequential scan**
would indicate the OR predicate is not index-friendly at this scale.

### How to capture the actual plan

The harness test `trgm_index_perf_check` (`tests/integration.rs`) seeds a
dedicated ZIM (`__itrge__`) with 100k rows and runs `EXPLAIN (ANALYZE)`
against each of the three split predicates:

```sql
-- Q1 prefix (btree)
EXPLAIN (ANALYZE, BUFFERS) SELECT a.id … WHERE a.title_lower LIKE 'art 42%' ESCAPE '\'
-- Q2 contains (GIN)
EXPLAIN (ANALYZE, BUFFERS) SELECT a.id … WHERE a.title_lower LIKE '%art 42%' ESCAPE '\'
-- Q3 similarity (GiST)
EXPLAIN (ANALYZE, BUFFERS) SELECT a.id … WHERE similarity(a.title_lower, 'art 42') > 0.3
```

and asserts none of the three contains `Seq Scan on articles`.

Run it against a live DB and paste the printed plans below:

```sh
make test-strict-perf   # or:
DATABASE_URL=postgres://zimservice:zimservice@127.0.0.1:5432/zimservice \
  cargo test --test integration trgm_index_perf_check -- --include-ignored --nocapture
```

### Result (fill in after `make test-strict-perf`)

_Plane captured on: ______ (Postgres version, row count, `pg_trgm` similarity
threshold)._

```
(paste the EXPLAIN (ANALYZE, BUFFERS) output for Q1/Q2/Q3 here)
```

**Verdict:** the split is implemented (DEC-5); the harness asserts each branch
is index-driven (btree / GIN / GiST) with **no** sequential scan over
`articles`.

---

## PERF-4 (WI-36) — keep or drop `idx_articles_title_prefix` (btree)

**Question.** The Q1 prefix arm (`WHERE title_lower LIKE 'q%'`) can use the
btree `idx_articles_title_prefix`, but `gin_trgm_ops` (the GIN trgm index) can
serve `LIKE 'q%'` too. Is the btree redundant — can we drop it (migration 013)
and let the GIN index cover the prefix shape as well?

**How it is measured.** `trgm_index_perf_check` (`tests/integration.rs`,
`#[ignore]`, run via `make test-strict-perf` against the 100k-row `__itrge__`
fixture) EXPLAINs five shapes and prints each plan's node:

| Shape | Predicate / ordering | Index it may use |
|-------|----------------------|------------------|
| Q1 | `title_lower LIKE 'q%'` + `ORDER BY score` | btree prefix *or* GIN |
| Q2 | `title_lower LIKE '%q%'` | GIN |
| Q3 | `title_lower % $1 AND similarity(...) > $2` | GiST |
| SUG | prefix arm (weight 1.0) | btree prefix *or* GIN |
| Q5 | `ORDER BY title_lower ASC LIMIT 20` (no predicate) | btree (ordering) |

Q1/Q2/Q3/SUG are regression-gated: none may fall back to a `Seq Scan on
articles` at 100k rows. Q5 is recorded for the decision but is **not**
seq-scan-gated — a seq-scan+top-N for a pure ordering is a legal planner choice
that this decision weighs.

**Decision rule (fixed, no judgment).**
- **DROP** `idx_articles_title_prefix` (emit
  `migrations/013_articles_title_prefix_drop.sql`:
  `DROP INDEX IF EXISTS idx_articles_title_prefix;` + a `MIGRATIONS` entry
  after 012) **only if**, across *all* recorded EXPLAINs, the planner uses the
  GIN trgm index or a seq scan for the Q1 `LIKE 'q%'` prefix shape **and**
  never uses `Index Scan using idx_articles_title_prefix` for any of the five
  shapes.
- **Else KEEP** it and record the reason here.

After a drop the GIN trgm index serves the prefix shape; the Q1 no-seq-scan
assertion is the regression net — re-run and confirm Q1 is still not a seq
scan. Precedent for the drop statement: `009:4`
`DROP INDEX IF EXISTS idx_articles_title_lower`. Non-`CONCURRENTLY` (single
transaction migrate harness; `010:16` precedent).

**Outcome.** _PENDING CI-DB MEASUREMENT._ The measurement needs Postgres (the
100k-row fixture + `EXPLAIN ANALYZE`), which is not available locally, so the
keep/drop call is deferred to `make test-strict-perf` (or the test-db-perf push
job). **Default is KEEP** — migration 013 is created *only* in the drop branch,
and that branch is not reached until the rule's drop condition is
measured-confirmed. _To fill in after the CI-DB run:_ paste the five plan nodes
here, tick the decision rule, and either create migration 013 (drop) or record
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
   crate exposing a stable streaming/borrow API (it currently requires an
   owned copy). Revisit when the crate stabilises.
   *(Re-checked 2026-08-29 against `zim` 0.5.0: `Content` still exposes no
   range/slice API — only an owned `bytes()`; this item remains blocked.)*
3. **`content_preview` payload gating** – The search API returns a 2 000-char
   preview per result. Gating this behind a query parameter would reduce
   response size for clients that only need titles, but adds API surface.
   Low priority given the small fixed size.
4. **Parallel `index_zims --all`** – Indexing multiple ZIM archives in
   parallel (e.g. via a bounded thread pool) could speed up initial ingest.
   Deferred because the I/O-bound COPY is the bottleneck and Postgres itself
   serialises concurrent COPY to the same table; gains would be marginal for
   the typical < 20 ZIM deployment.
