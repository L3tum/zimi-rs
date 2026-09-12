//! T-3: query-plan regression gate for the trigram search arms.
//!
//! Proves that the trgm search arms against `articles` (the `LIKE '%…%'`
//! contains arm served by the GIN trgm index `idx_articles_title_trgm`, and
//! the `%` similarity arm served by the GiST trgm index
//! `idx_articles_title_gist` — both on `title_lower`, created in
//! `migrations/001_initial.sql` and `008_articles_title_indexes.sql`) do NOT
//! degrade to a sequential scan on a 100k-row corpus.
//!
//! Method: a dedicated temp database (the `migrations.rs` pattern — created
//! and dropped around the test, so the shared dev schema is never touched),
//! migrated fresh, then bulk-populated with 100k articles via COPY into
//! `articles_staging` → the production tsvector upsert into `articles`
//! (exactly `bulk_insert`'s path, minus the ZIM-file extraction). After
//! `ANALYZE`, the EXACT arm SQL from `zimservice::search` (rebuilt through
//! the same pure builders `run` uses — see their `pub` docs) is
//! `EXPLAIN (FORMAT TEXT)`-ed LIMIT-bounded, and the plan must (a) contain
//! NO `Seq Scan on articles` and (b) actually reference the trgm index.
//!
//! The corpus is generated from a 40-word list as
//! `"{w1} {w2} {w3} entry {i}"`: realistic varied trigram content, with the
//! probe phrase `quixotic granite` occurring in ~63 of 100000 titles —
//! selective enough that ANY sane planner prefers the trgm index (a seq
//! scan would additionally have to sort all 100k rows for
//! `ORDER BY score DESC, a.id`, which no plan can win by seq-scanning).
//!
//! Wall clock: one ~7 MB COPY + one set-based upsert + `ANALYZE` — a few
//! seconds, far under the ~60 s budget.
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
use zimservice::search::{trgm_contains_sql, trgm_similarity_sql, SqlQuery};

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
const PROBE: &str = "quixotic granite";

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
    .expect("EXPLAIN of the trgm arm SQL must run");
    lines.join("\n")
}

/// T-3: on a 100k-row `articles` corpus, the trgm search arms must plan an
/// index scan (GIN trgm for contains, GiST trgm for similarity) — never a
/// sequential scan of `articles`. Runs in a dedicated temp database.
#[tokio::test]
async fn smoke_trgm_search_arm_no_seq_scan_100k() {
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

    // One fixture ZIM — the arms JOIN `zims`, so it must exist.
    let zim_id: i32 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ('__trgm_plan__', '__trgm_plan__', '/tmp/__trgm_plan__.zim', 0,
                 now(), 'ready', 100000, 100000) RETURNING id",
        |q| q,
    )
    .await
    .expect("fixture zims row")
    .expect("zims id");

    // Bulk-load 100k titles: COPY (one ~7 MB payload, not 100k INSERTs) into
    // `articles_staging` — only the columns the trgm index + query need
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
        &pool,
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
        &pool,
        "DELETE FROM articles_staging WHERE zim_id = $1",
        |q| q.bind(zim_id),
    )
    .await
    .expect("staging cleanup");

    let count: i64 =
        zimservice::db::raw::fetch_scalar_optional(&pool, "SELECT count(*) FROM articles", |q| q)
            .await
            .expect("count articles")
            .expect("count row");
    assert_eq!(count, ROWS as i64, "corpus must be fully populated");

    // Fresh table: no statistics until analyzed, and the trgm selectivity
    // estimators are useless without them — the EXPLAIN below needs both.
    zimservice::db::raw::execute(&pool, "ANALYZE articles", |q| q)
        .await
        .expect("ANALYZE articles");

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
    assert!(
        plan.contains("idx_articles_title_trgm"),
        "trgm contains arm should use the GIN trgm index on title_lower:\n{plan}"
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
    assert!(
        plan.contains("idx_articles_title_gist"),
        "trgm similarity arm should use the GiST trgm index on title_lower:\n{plan}"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}
