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

**`EXPLAIN (ANALYZE)` timing at 100k rows: measured 2026-09.** Recorded
against the `trgm_plan` 100k-row corpus in a C-collation temp DB (PostgreSQL
16.15), running the *exact* production arm SQL from the `zimservice::search`
builders (probe phrase `quixotic granite`, selective: 63/100k titles
prefix-match) rather than the hand-written psql recipe above — same three
shapes, no `BUFFERS` counters. Execution times:

- Q1 prefix (`LIKE 'quixotic granite%'`) — `Index Scan using
  idx_articles_title_prefix`, **0.785 ms**
- Q2 contains (`LIKE '%quixotic granite%'`) — `Bitmap Index Scan on
  idx_articles_title_gist`, **16.238 ms**
- Q3 similarity (`% $1 AND similarity > 0.3`) — `Bitmap Index Scan on
  idx_articles_title_gist`, **47.848 ms**

None is a `Seq Scan on articles`; the prefix btree is ~20–60× faster than
the trgm arms for the prefix shape (see the PERF-4 section below for the
full five-shape record and the keep/drop decision).

**Verdict:** the split is implemented (DEC-5), the plan-shape gate is
automated and enforced in the integration suite, and the `EXPLAIN (ANALYZE)`
timing at 100k rows is recorded above (2026-09).

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

**Outcome.** **KEEP `idx_articles_title_prefix`** — measured 2026-09 (100k-row
C-collation temp DB, the `trgm_plan` corpus, `EXPLAIN (ANALYZE)`; PostgreSQL
16.15). The fixed drop rule is NOT met: the Q1 `LIKE 'q%'` prefix shape plans
`Index Scan using idx_articles_title_prefix`, so the drop condition ("planner
uses the GIN trgm index or a seq scan for Q1 **and** never uses the btree in
any of the five shapes") fails on the first clause. None of the five shapes
degrades to a `Seq Scan on articles`.

Recorded plan nodes (probe phrase `quixotic granite`, prefix in 63/100k
titles; corpus = the `trgm_plan` 100k-row fixture):

- **Q1** (`title_lower LIKE 'quixotic granite%'` + `ORDER BY score`) —
  `Index Scan using idx_articles_title_prefix`, Execution Time **0.785 ms**.
- **Q2** (`title_lower LIKE '%quixotic granite%'`) —
  `Bitmap Index Scan on idx_articles_title_gist`, Execution Time 16.238 ms.
- **Q3** (`title_lower % $1 AND similarity(...) > 0.3`) —
  `Bitmap Index Scan on idx_articles_title_gist`, Execution Time 47.848 ms.
- **SUG** (suggestion prefix arm — the same builder/SQL as Q1, weight 1.0) —
  `Index Scan using idx_articles_title_prefix`, Execution Time 0.272 ms.
- **Q5** (`ORDER BY title_lower ASC LIMIT 20`, no predicate — hypothetical
  shape, not seq-scan-gated) —
  `Index Scan using idx_articles_title_prefix`, Execution Time 0.244 ms.

Reason for KEEP: (1) the btree prefix range serves Q1/SUG ~20–60× faster
than the trgm bitmap scans serve Q2/Q3, so the btree is *not* redundant for
the prefix shape; (2) Q5 (pure ordering) also rides the btree — dropping it
would force a full 100k-row sort for any future ordering-only endpoint.
`migrations/014_articles_title_prefix_drop.sql` was never created (the KEEP
decision stood); migration number 014 was later allocated to
`migrations/014_drop_dead_schema.sql`.

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

---

## Superseded decisions

- **Sequential search arms — 2026-08-30 decision, superseded 2026-09-18
  (DROP the sequential form; KEEP the concurrent form).** On 2026-08-30 the
  arm queries ran SEQUENTIALLY on ONE pool checkout: a tokio-postgres
  client is single-in-flight-command, and parallel arms would need 4–5 of
  the pool's 20 connections per request, "not justified by the few-ms of
  sequential DB time" (the slow arm being the embed HTTP, already
  concurrent via `join!`). What changed since: Postgres moved to a remote
  host, so every arm seek now pays a network RTT (the sub-10ms local-DB
  assumption is gone), and the per-search clone/checkout churn was
  measured — the documented revisit trigger fired. Outcome (2026-09-18):
  the selected arms run CONCURRENTLY (each text arm on its own pool
  checkout, the vector arm in the same `tokio::join!`), so total latency
  is max(embed, fts, trgm×3, ann), not the sum. The in-code note lived in
  `SearchEngine::search` (src/search/mod.rs) beside the join; the 2026-09-21
  arm-construction extraction (PONY N2) moved the surviving 2026-09-18
  record to `build_search_arms` and this superseded note here.

## Closed optimizations (evaluated, no change)

- **Search candidate materialization bound — PERF L2, evaluated
  2026-09-21 (KEEP the current form).** A review flagged that search
  "fully materializes all fetched rows before dedup/limit" (transient ~20
  MB at the hard corner offset=5000, limit=500). Traced through the
  pipeline: (1) every arm's SQL is already `LIMIT min(offset+limit,
  SEARCH_FETCH_HARD_CAP)` — the offset-aware fetch
  (`branch_fetch_limit`/`vector_fetch_limit` in src/search/sql.rs, pinned
  by `branch_fetch_limit_is_offset_aware_and_capped`); (2) the trgm group
  is pre-deduped before the final merge (fixed merge positions, record in
  `SearchEngine::search`); (3) `merge_results` early-exits the collect
  scan at `offset+limit` distinct ids (pinned by
  `merge_results_serves_deep_page_from_full_pool`). The remaining
  transient is the `all` concat in `merge_results` — provably bounded at
  3×SEARCH_FETCH_HARD_CAP rows at the absolute corner, i.e. exactly the
  documented worst case, once per request. The only further reduction is a
  k-way heap merge over the three ranked arms to avoid the full concat:
  REJECTED — a large refactor for a bounded ~20 MB transient with zero
  correctness gain (the page output is identical by the top-k-of-union
  property). Revisit only if a real deployment shows the corner transient
  matters (deep pagination under memory pressure).
