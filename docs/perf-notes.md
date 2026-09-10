# Performance notes

## C12 — trgm/prefix `OR similarity()` plan at scale

**Goal:** confirm whether the trigram/prefix search predicate uses its indexes
at realistic scale (100k rows), or degrades to a sequential scan.

**Resolved (DEC-5, Step 4.2):** the single `LIKE … OR similarity() > t`
predicate was split into three concurrently-run, individually indexable
queries (btree prefix, GIN contains, GiST similarity), merged by the existing
`merge_results`. Note: the repo contains **no automated plan-regression
harness** — earlier CI/docs referenced a `trgm_index_perf_check` test that
did not exist, and those references were removed. The index usage below must
be verified manually at scale (recipe further down) if the plan shape is ever
in doubt.

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
on articles`. There is no automated harness — run the statements by hand
(e.g. `psql`) and paste the plans below.

### Result (unmeasured)

**No plan has been captured.** There is no automated harness, and no manual
run against the 100k-row fixture has been recorded — the capture metadata
(Postgres version, row count, `pg_trgm` similarity threshold) and the
`EXPLAIN (ANALYZE, BUFFERS)` output for Q1/Q2/Q3 are unmeasured, and remain
to be filled in by whoever runs the recipe above.

**Verdict:** the split is implemented (DEC-5); there is no automated harness —
plan capture is pending a manual measurement.

---

## PERF-4 (WI-36) — keep or drop `idx_articles_title_prefix` (btree)

**Question.** The Q1 prefix arm (`WHERE title_lower LIKE 'q%'`) can use the
btree `idx_articles_title_prefix`, but `gin_trgm_ops` (the GIN trgm index) can
serve `LIKE 'q%'` too. Is the btree redundant — can we drop it (migration 014)
and let the GIN index cover the prefix shape as well?

**How it is measured (manual).** Seed the 100k-row `__itrge__` fixture in the
dev DB and EXPLAIN (ANALYZE) the five shapes below, printing each plan's node
(the `trgm_index_perf_check` harness referenced here previously did not exist
in the repo and its references were removed):

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
