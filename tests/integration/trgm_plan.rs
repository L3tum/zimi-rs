//! T-3: query-plan regression gate for the search arms.
//!
//! Proves that the search arms against `articles` do NOT degrade to a
//! sequential scan on a 100k-row corpus:
//!   - the trgm `LIKE '%…%'` contains arm — a trgm index on `title_lower`
//!     (`idx_articles_title_trgm` GIN from `migrations/001_initial.sql`, or
//!     `idx_articles_title_gist` GiST from `008_…_indexes.sql`; both operator
//!     classes serve the arm and the planner picks by cost),
//!   - the trgm `%` similarity arm — a trgm index on `title_lower` (same two
//!     candidates, same discretion),
//!   - the FTS arm — GIN tsvector index `idx_articles_fts`
//!     (`001_initial.sql`), and
//!   - the vector ANN arm — partial ANN index `idx_articles_embedding`
//!     (`WHERE embedding IS NOT NULL`, `010_…_sort.sql`; HNSW below the
//!     1M-vector threshold, the shape a 100k-row corpus gets).
//!
//! Method: a dedicated temp database (the `migrations.rs` pattern — created
//! and dropped around the test, so the shared dev schema is never touched),
//! migrated fresh, then bulk-populated with 100k articles via COPY into
//! `articles_staging` → the production tsvector upsert into `articles`
//! (exactly `bulk_insert`'s path, minus the ZIM-file extraction) plus a
//! deterministic 768-dim embedding per row so the vector arm has rows to
//! ANN-scan. After `ANALYZE`, the EXACT arm SQL from `zimservice::search`
//! (rebuilt through the same pure builders `run` uses — see their `pub`
//! docs) is `EXPLAIN (FORMAT TEXT)`-ed LIMIT-bounded, and every plan must
//! (a) contain NO `Seq Scan on articles` and (b) actually reference the
//! arm's index.
//!
//! The corpus is generated from a 40-word list as
//! `"{w1} {w2} {w3} entry {i}"`: realistic varied trigram content, with the
//! probe phrase `quixotic granite` occurring in ~63 of 100000 titles as an
//! ADJACENT pair — selective enough that ANY sane planner prefers the trgm
//! index (a seq scan would additionally have to sort all 100k rows for
//! `ORDER BY score DESC, a.id`, which no plan can win by seq-scanning). The
//! same phrase also serves the FTS arm: `websearch_to_tsquery` ANDs the two
//! words (no phrase), which occur together in only a few hundred (~0.4%)
//! of titles — index-cheaper by orders of magnitude.
//!
//! Wall clock: one ~7 MB COPY + one set-based upsert + 50 small embedding
//! UPDATEs + `ANALYZE` — a few seconds, far under the ~60 s budget.
//!
//! DB-gated like every test here: skips without a reachable `DATABASE_URL`,
//! hard-fails under `ZIMSERVICE_REQUIRE_DB=1` when the base DB is unreachable
//! (CI's `test` job). If the base user cannot `CREATE DATABASE`, the
//! `create_temp_db` `None` arm records a mid-test skip via `skip_midtest` —
//! which itself consults `REQUIRE_DB` and hard-fails in strict mode (the
//! inherited `migrations.rs` convention); CI is unaffected because its
//! service user is a superuser.
//! Holds the `DbExclusiveGuard` for the shared-server `CREATE/DROP DATABASE`.

use super::common::*;
use super::migrations::{close_and_drop, create_temp_db};
use zimservice::search::{fts_sql, trgm_contains_sql, trgm_similarity_sql, vector_sql, SqlQuery};

/// Corpus size. Deliberately 100k: below this the seq-scan-vs-index cost
/// flip is unreliable and the gate proves little.
const ROWS: usize = 100_000;

/// 40 varied words; `quixotic` + `granite` are the probe phrase (the exact
/// adjacent pair occurs in ~63 titles: `i ≡ 17 (mod 40)` for word 1 and
/// `(i/40) ≡ 7 (mod 40)` for word 2 → one row per 1600).
const WORDS: [&str; 40] = [
    "amber", "boulder", "canyon", "dune", "ember", "fjord", "glacier", "granite", "heath", "islet",
    "jungle", "lichen", "meadow", "niche", "oasis", "plateau", "quarry", "quixotic", "ridge",
    "shoal", "tundra", "upland", "valley", "wadi", "xylem", "yarrow", "zephyr", "basalt", "cinder",
    "delta", "estuary", "fissure", "gully", "habitat", "inlet", "lagoon", "moraine", "nexus",
    "outcrop", "pinnacle",
];

/// The probe phrase: long enough for both trgm arms (≥ 3 chars), selective
/// enough that the trgm index is overwhelmingly cheaper than a seq scan.
/// Also the FTS arm's probe (`websearch_to_tsquery` ANDs the two words —
/// the pair occurs in only a few hundred titles).
const PROBE: &str = "quixotic granite";

/// Fixture ZIM name — the arms JOIN `zims`, so it must exist.
const ZIM_NAME: &str = "__trgm_plan__";

/// Embedding dimension for the corpus rows. Must match the
/// `articles.embedding` column, which `migrations/001_initial.sql` creates as
/// `VECTOR(1536)`. (The *runtime* dimension is reconciled to the configured
/// model by `ensure_vector_dimension`, but this gate populates vectors
/// directly and only inspects plan shape, so it uses the migration's
/// dimension verbatim — a 768-dim literal would not fit the 1536 column.)
const EMBED_DIM: usize = 1536;

/// One embedding UPDATE batch: `EMBED_BATCH` rows share one bind (one
/// vector literal), keeping the population to 50 small statements instead
/// of 100k per-row updates.
const EMBED_BATCH: usize = 2000;

/// `EXPLAIN (FORMAT TEXT)` the arm SQL with its bound params; join the plan
/// lines for substring assertions.
async fn explain_plan(pool: &Pool, sq: &SqlQuery) -> String {
    let explain_sql = format!("EXPLAIN (FORMAT TEXT) {}", sq.sql);
    let lines: Vec<String> = zimservice::db::raw::fetch_scalar_all(pool, &explain_sql, |q| {
        let mut q = q;
        for p in &sq.params {
            q = q.bind(p);
        }
        q
    })
    .await
    .expect("EXPLAIN of the search arm SQL must run");
    lines.join("\n")
}

/// Give every corpus row a deterministic `EMBED_DIM`-dim embedding so the
/// vector arm has rows to ANN-scan (with NULL embeddings the partial ANN
/// index is empty and the vector-arm gate would plan on nothing). The
/// vector is one-hot at the coordinate derived from the row's id batch —
/// deterministic across runs; the gate checks plan SHAPE, not scores. Each
/// `EMBED_BATCH`-row batch shares one bind, cast `$1::vector` the same way
/// the search path binds `format_vector` output (`vector_sql`'s
/// `$1::vector`). Fresh table ⇒ identity ids are exactly `1..=ROWS`.
async fn populate_embeddings(pool: &Pool) {
    for start in (1..=ROWS).step_by(EMBED_BATCH) {
        let end = (start + EMBED_BATCH - 1).min(ROWS);
        let mut v = vec!["0"; EMBED_DIM];
        v[(start - 1) % EMBED_DIM] = "1";
        let sql = "UPDATE articles SET embedding = $1::vector WHERE id BETWEEN $2 AND $3";
        zimservice::db::raw::execute(pool, sql, |q| {
            q.bind(format!("[{}]", v.join(",")))
                .bind(start as i64)
                .bind(end as i64)
        })
        .await
        .expect("embedding batch");
    }
}

/// Load the 100k-row corpus every plan-shape arm shares (one fixture ZIM,
/// COPY → tsvector upsert, deterministic embeddings, `ANALYZE`); returns
/// the fixture ZIM id.
async fn load_corpus(pool: &Pool) -> i32 {
    // One fixture ZIM — the arms JOIN `zims`, so it must exist.
    let zim_id: i32 = zimservice::db::raw::fetch_scalar_optional(
        pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 100000, 100000) RETURNING id",
        |q| q.bind(ZIM_NAME),
    )
    .await
    .expect("fixture zims row")
    .expect("zims id");

    // Bulk-load 100k titles: COPY (one ~7 MB payload, not 100k INSERTs) into
    // `articles_staging` — only the columns the arms + indexes need
    // (path/title/zim_id; preview NULL, snippet/language/namespace default).
    {
        let mut client = pool.acquire().await.expect("acquire COPY connection");
        let mut copy = client
            .copy_in_raw(
                "COPY articles_staging (path, title, zim_id) FROM STDIN WITH (FORMAT text)",
            )
            .await
            .expect("COPY start");
        let mut buf = String::with_capacity(ROWS * 64);
        for i in 0..ROWS {
            let w1 = WORDS[i % 40];
            let w2 = WORDS[(i / 40) % 40];
            let w3 = WORDS[(i / 1600) % 40];
            buf.push_str(&format!(
                "A/title_{i}\t{w1} {w2} {w3} entry {i}\t{zim_id}\n"
            ));
        }
        copy.send(buf.as_bytes()).await.expect("COPY payload");
        let _rows: u64 = copy.finish().await.expect("COPY finish");
        drop(client);
    }

    // Production staging → articles upsert (tsvector computed in Postgres).
    zimservice::db::raw::execute(
        pool,
        "INSERT INTO articles (path, title, content_preview, snippet, search_vector,
                               language, namespace, zim_id)
         SELECT path, title, content_preview, snippet,
                setweight(to_tsvector('simple', title), 'A')
                || setweight(to_tsvector('simple', coalesce(content_preview, '')), 'B'),
                language, namespace, zim_id
         FROM articles_staging WHERE zim_id = $1
         ON CONFLICT (zim_id, path) DO UPDATE SET
             title = EXCLUDED.title,
             content_preview = EXCLUDED.content_preview,
             snippet = EXCLUDED.snippet,
             search_vector = EXCLUDED.search_vector,
             updated_at = now()",
        |q| q.bind(zim_id),
    )
    .await
    .expect("staging upsert");
    zimservice::db::raw::execute(
        pool,
        "DELETE FROM articles_staging WHERE zim_id = $1",
        |q| q.bind(zim_id),
    )
    .await
    .expect("staging cleanup");

    // Vector arm data (before ANALYZE, so one analyze covers every arm).
    populate_embeddings(pool).await;

    let count: i64 =
        zimservice::db::raw::fetch_scalar_optional(pool, "SELECT count(*) FROM articles", |q| q)
            .await
            .expect("count articles")
            .expect("count row");
    assert_eq!(count, ROWS as i64, "corpus must be fully populated");

    // Fresh table: no statistics until analyzed, and the index selectivity
    // estimators (tsvector + trgm) are useless without them — the EXPLAINs
    // below need both.
    zimservice::db::raw::execute(pool, "ANALYZE articles", |q| q)
        .await
        .expect("ANALYZE articles");

    zim_id
}

/// T-3: on a 100k-row `articles` corpus, every search arm must plan an
/// index scan (GIN trgm for contains, GiST trgm for similarity, GIN
/// tsvector for FTS, the partial ANN index for the vector top-k) — never a
/// sequential scan of `articles`. Runs in a dedicated temp database.
#[tokio::test]
async fn smoke_search_arms_no_seq_scan_100k() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };
    run_migrations(&pool)
        .await
        .expect("fresh apply (temp DB must migrate cleanly)");

    load_corpus(&pool).await;

    // The plan-shape assertions below are PG-16-specific: other Postgres
    // versions may plan the same query differently (and can fail here).

    // (1) Contains arm — `title_lower LIKE '%quixotic granite%'`, served by
    // the GIN trgm index (idx_articles_title_trgm).
    let contains = trgm_contains_sql(PROBE, None, None, 100, 1.0);
    let plan = explain_plan(&pool, &contains).await;
    assert!(
        !plan.contains("Seq Scan on articles"),
        "trgm contains arm must NOT degrade to a sequential scan on a \
         100k-row corpus:\n{plan}"
    );
    // Both `gin_trgm_ops` and `gist_trgm_ops` serve `LIKE '%…%'`, and the
    // planner picks between the two by cost — which depends on `ANALYZE`'s
    // random sample, so the choice can flip run-to-run. Gate on "a trgm index
    // on title_lower" (the real regression is a seq scan, asserted above).
    assert!(
        plan.contains("idx_articles_title_trgm") || plan.contains("idx_articles_title_gist"),
        "trgm contains arm should use a trgm index on title_lower (GIN or GiST):\n{plan}"
    );

    // (2) Similarity arm — `title_lower % … AND similarity(…) > 0.3`,
    // served by the GiST trgm index (idx_articles_title_gist).
    let similarity = trgm_similarity_sql(PROBE, 0.3, None, None, 100, 1.0);
    let plan = explain_plan(&pool, &similarity).await;
    assert!(
        !plan.contains("Seq Scan on articles"),
        "trgm similarity arm must NOT degrade to a sequential scan on a \
         100k-row corpus:\n{plan}"
    );
    // Same discretion as the contains arm: `similarity` is served by both trgm
    // operator classes, so accept either index (a seq scan is the regression).
    assert!(
        plan.contains("idx_articles_title_trgm") || plan.contains("idx_articles_title_gist"),
        "trgm similarity arm should use a trgm index on title_lower (GIN or GiST):\n{plan}"
    );

    // (3) FTS arm — `a.search_vector @@ websearch_to_tsquery('simple', $1)`
    // (websearch: the probe's two words are ANDed, not a phrase), served
    // by the GIN tsvector index (idx_articles_fts). The pair occurs in
    // only a few hundred of 100k titles, so the index is overwhelmingly
    // cheaper than a seq scan. Highlight off: this gate pins the
    // index-serving shape, not `ts_headline`.
    let fts = fts_sql(PROBE, false, None, None, 100, 1.0);
    let plan = explain_plan(&pool, &fts).await;
    assert!(
        !plan.contains("Seq Scan on articles"),
        "FTS arm must NOT degrade to a sequential scan on a \
         100k-row corpus:\n{plan}"
    );
    assert!(
        plan.contains("idx_articles_fts"),
        "FTS arm should use the GIN tsvector index on search_vector:\n{plan}"
    );

    // (4) Vector arm — `ORDER BY a.embedding <=> $1::vector LIMIT k`
    // top-k seek, served by the partial ANN index (idx_articles_embedding,
    // `WHERE embedding IS NOT NULL` — HNSW below the 1M-vector threshold,
    // see migrations/010). The probe is a 1536-dim one-hot literal, cast
    // `$1::vector` exactly as `vector_sql` binds the search-path vector.
    let probe: String = (0..EMBED_DIM)
        .map(|i| if i == 17 { "1" } else { "0" })
        .collect::<Vec<_>>()
        .join(",");
    let vector = vector_sql(&format!("[{probe}]"), None, None, 100, 1.0);
    let plan = explain_plan(&pool, &vector).await;
    assert!(
        !plan.contains("Seq Scan on articles"),
        "vector arm must NOT degrade to a sequential scan on a \
         100k-row corpus:\n{plan}"
    );
    assert!(
        plan.contains("idx_articles_embedding"),
        "vector arm should use the partial ANN index on embedding:\n{plan}"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}
