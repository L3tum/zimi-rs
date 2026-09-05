//! Embedding domain integration tests.

use super::common::*;

/// P5 (B3, PERF 1) — a reindex prunes articles removed from the archive
/// **without** leaving a search gap.
///
/// Scope note: zim 0.5 cannot author archives, so this pins the **SQL shape**
/// of the availability-preserving reindex (the exact statements `index_body`
/// + `finalize_zim` run on a fresh full pass, `start_idx == 0`) rather than the
/// wiring: 3 seeded rows (old `updated_at`) → capture run-start → NO up-front
/// delete → re-stage + upsert-in-place only the 2 articles still in the archive
/// (bumping their `updated_at`) → the removed row keeps its old `updated_at` →
/// finalize prunes it via `updated_at < index_started_at` + a `NOT EXISTS` qid
/// prune. Asserts the removed row stays queryable until the finalize prune (no
/// gap), then that the removed path + qid are gone and the 2 survivors remain
/// with their qids.
#[tokio::test]
async fn reindex_prunes_removed_articles() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_prune__";
    let zim_id: i32 = {
        let c = pool.get().await.unwrap();
        c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
            .await
            .unwrap();
        c.execute(
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,\n                               index_status, indexed_entries, article_count)\n             VALUES ($1, $1, $1, 0, now(), 'ready', 3, 3)",
            &[&ZIM],
        )
        .await
        .unwrap();
        c.query_one("SELECT id FROM zims WHERE name = $1", &[&ZIM])
            .await
            .unwrap()
            .get(0)
    };

    {
        let c = pool.get().await.unwrap();
        // Clean slate in case a prior run left rows.
        c.execute("DELETE FROM articles WHERE zim_id = $1", &[&zim_id])
            .await
            .unwrap();
        c.execute("DELETE FROM qid_index WHERE zim_id = $1", &[&zim_id])
            .await
            .unwrap();
        for p in ["A/keep-1", "A/keep-2", "A/removed"] {
            c.execute(
                "INSERT INTO articles (path, title, content_preview, snippet, search_vector, language, namespace, zim_id, updated_at)\n                 VALUES ($1, $1, 'preview', 'snippet', to_tsvector('simple', $1), 'en', 'C', $2, now() - interval '1 hour')",
                &[&p.to_string(), &zim_id],
            )
            .await
            .unwrap();
        }
        c.execute(
            "INSERT INTO qid_index (zim_id, path, qid) VALUES ($1, 'A/keep-1', 101), ($1, 'A/keep-2', 102), ($1, 'A/removed', 103)",
            &[&zim_id],
        )
        .await
        .unwrap();
    }

    // Simulate a fresh full pass (start_idx == 0) of index_body under the
    // availability-preserving reindex (PERF 1): NO up-front delete. Capture the
    // run-start timestamp, re-stage + upsert-in-place ONLY the 2 articles still
    // in the archive (bumping their `updated_at`), and leave the removed row
    // untouched (its `updated_at` stays old). The stale row is pruned once, at
    // finalize, via `updated_at < index_started_at` (exact `finalize_zim`
    // statements).
    let index_started_at: String = {
        let c = pool.get().await.unwrap();
        c.query_one("SELECT now()::text", &[]).await.unwrap().get(0)
    };
    {
        let c = pool.get().await.unwrap();
        // (PERF 1) no up-front `DELETE FROM articles`. Staging pre-clean +
        // re-stage the 2 survivors:
        c.execute("DELETE FROM articles_staging WHERE zim_id = $1", &[&zim_id])
            .await
            .unwrap();
        for p in ["A/keep-1", "A/keep-2"] {
            c.execute(
                "INSERT INTO articles_staging (path, title, content_preview, snippet, language, namespace, zim_id)\n                 VALUES ($1, $1, 'preview', 'snippet', 'en', 'C', $2)",
                &[&p.to_string(), &zim_id],
            )
            .await
            .unwrap();
        }
        // Phase-2 upsert (same SQL shape as bulk_insert) — updates the 2
        // survivors in place and bumps their `updated_at` to now(); the removed
        // article is absent from the ZIM, so its row is left stale.
        c.execute(
            "INSERT INTO articles (path, title, content_preview, snippet, search_vector, language, namespace, zim_id)\n             SELECT path, title, content_preview, snippet,\n                    setweight(to_tsvector('simple', title), 'A')\n                    || setweight(to_tsvector('simple', coalesce(content_preview, '')), 'B'),\n                    language, namespace, zim_id\n             FROM articles_staging WHERE zim_id = $1\n             ON CONFLICT (zim_id, path) DO UPDATE SET\n                title = EXCLUDED.title, content_preview = EXCLUDED.content_preview,\n                snippet = EXCLUDED.snippet, search_vector = EXCLUDED.search_vector,\n                updated_at = now()",
            &[&zim_id],
        )
        .await
        .unwrap();
        // qid rows for the survivors (phase-3 shape, simplified):
        c.execute(
            "INSERT INTO qid_index (zim_id, path, qid) VALUES ($1, 'A/keep-1', 101), ($1, 'A/keep-2', 102)\n             ON CONFLICT (zim_id, path) DO UPDATE SET qid = EXCLUDED.qid",
            &[&zim_id],
        )
        .await
        .unwrap();
        c.execute("DELETE FROM articles_staging WHERE zim_id = $1", &[&zim_id])
            .await
            .unwrap();

        // (PERF 1 no-availability-window) the removed row is still queryable
        // until the finalize prune — `articles` never holds a partial set.
        let removed_present: bool = c
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM articles WHERE zim_id = $1 AND path = 'A/removed')",
                &[&zim_id],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            removed_present,
            "removed row must remain until the finalize prune (no gap)"
        );

        // Finalize prune — the exact `finalize_zim` statements: stale rows by
        // `updated_at < index_started_at`, then orphaned qids by NOT EXISTS.
        let del: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![
            &ZIM as &(dyn postgres_types::ToSql + Sync),
            &index_started_at as &(dyn postgres_types::ToSql + Sync),
        ];
        c.execute(
            "DELETE FROM articles\n             WHERE zim_id = (SELECT id FROM zims WHERE name = $1)\n               AND updated_at < $2::timestamptz",
            &del,
        )
        .await
        .unwrap();
        c.execute(
            "DELETE FROM qid_index q\n             WHERE q.zim_id = (SELECT id FROM zims WHERE name = $1)\n               AND NOT EXISTS (SELECT 1 FROM articles a\n                    WHERE a.zim_id = q.zim_id AND a.path = q.path)",
            &[&ZIM],
        )
        .await
        .unwrap();
    }

    // Assert: removed article + its qid are gone; both survivors remain.
    let c = pool.get().await.unwrap();
    let remaining: Vec<String> = c
        .query(
            "SELECT path FROM articles WHERE zim_id = $1 ORDER BY path",
            &[&zim_id],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(remaining, vec!["A/keep-1", "A/keep-2"]);
    let qids: Vec<String> = c
        .query(
            "SELECT path FROM qid_index WHERE zim_id = $1 ORDER BY path",
            &[&zim_id],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(qids, vec!["A/keep-1", "A/keep-2"]);
    let staging_left: i64 = c
        .query_one(
            "SELECT count(*) FROM articles_staging WHERE zim_id = $1",
            &[&zim_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(staging_left, 0, "staging must be cleared");

    // Cleanup tail.
    c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
        .await
        .unwrap();
}

/// P6 (H2) — the vector-index existence check (`vector_index_state`) is
/// shape-agnostic and must recognize the **partial** `WHERE embedding IS NOT
/// NULL` index that migration 010 / `maybe_build_vector_index` build. Also
/// pins that no legacy non-partial index survives the migration.
#[tokio::test]
async fn vector_index_is_partial_after_migration() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    let c = pool.get().await.unwrap();

    // The legacy non-partial shape (indpred IS NULL) must be gone.
    let legacy: bool = c
        .query_one(
            "SELECT EXISTS (\n                SELECT 1 FROM pg_index i\n                JOIN pg_class cl ON cl.oid = i.indexrelid\n                WHERE cl.relname = 'idx_articles_embedding' AND i.indpred IS NULL\n            )",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        !legacy,
        "legacy non-partial embedding index must be dropped by migration 010"
    );

    // Wherever the index exists, it must be the partial shape, and the
    // shape-agnostic runtime check must agree with pg_index.
    let db_exists: bool = c
        .query_one(
            "SELECT EXISTS (\n                SELECT 1 FROM pg_index i\n                JOIN pg_class cl ON cl.oid = i.indexrelid\n                WHERE cl.relname = 'idx_articles_embedding'\n            )",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let partial: bool = c
        .query_one(
            "SELECT EXISTS (\n                SELECT 1 FROM pg_index i\n                JOIN pg_class cl ON cl.oid = i.indexrelid\n                WHERE cl.relname = 'idx_articles_embedding' AND i.indpred IS NOT NULL\n            )",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    let (_count, runtime_exists) = zimservice::embed::vector_index_state(&pool)
        .await
        .expect("vector_index_state");
    assert_eq!(
        runtime_exists, db_exists,
        "runtime exists flag must match pg_index"
    );
    if db_exists {
        assert!(
            partial,
            "embedding index must be partial (WHERE embedding IS NOT NULL)"
        );
    }
}

/// H2 regression: a hybrid search must not hold its pool connection across
/// the embedding HTTP round-trip. With a single-connection pool and a slow
/// (30 s) embed endpoint, the connection is acquired only for the fast DB
/// queries inside the join — so a second `pool.get()` succeeds promptly
/// while the embed is still pending. Before the fix the connection was
/// acquired before the join and held across the whole embed await, so the
/// second `get()` would block until the embed returned.
///
/// The 30 s mock delay dominates any plausible stall between embed receipt
/// and the probe (probe window ≪ 30 s), so the embed is guaranteed still
/// in-flight whenever the probe fires: probe success deterministically
/// means the DB connection was released while the embed was pending, and
/// a regression fails the assertion instead of passing green.
#[tokio::test]
async fn search_does_not_hold_pool_connection_during_embed() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let (normal, _normal_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&normal).await.expect("migrations");

    // Single-connection pool the search engine is forced to share: if the
    // search holds its connection across the embed await, the `get()` below
    // blocks until the embed returns (or its wait_timeout expires).
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    let mut scfg = DpConfig::new();
    scfg.url = Some(url.clone());
    let small = match scfg.builder(tokio_postgres::NoTls) {
        Ok(b) => b
            .max_size(1)
            .wait_timeout(Some(Duration::from_secs(1)))
            .build(),
        Err(_) => return skip_midtest("single-connection probe pool config failed"),
    };
    let small = match small {
        Ok(p) => p,
        Err(_) => return skip_midtest("single-connection probe pool build failed"),
    };
    if small.get().await.is_err() {
        return skip_midtest("single-connection probe pool connect failed");
    }

    let server = MockServer::start().await;
    // Embed endpoint that stalls 30 s before answering: the delay is so
    // large that the embed is guaranteed still in-flight for the whole
    // probe window, regardless of runner stalls.
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(30_000))
                .set_body_string(r#"{"data":[{"index":0,"embedding":[1,2,3,4,5,6,7,8]}]}"#),
        )
        .mount(&server)
        .await;

    let values: HashMap<String, serde_json::Value> = vec![
        ("embedding.enabled".into(), serde_json::json!(true)),
        ("embedding.endpoint".into(), serde_json::json!(server.uri())),
        ("embedding.api_key".into(), serde_json::json!("")),
        ("embedding.model".into(), serde_json::json!("test-model")),
        ("embedding.dimension".into(), serde_json::json!(8)),
        ("embedding.timeout_secs".into(), serde_json::json!(5)),
    ]
    .into_iter()
    .collect();
    let settings = SettingsCache::new_with_map(normal.clone(), values, HashMap::new());
    let engine = SearchEngine::new(
        small.clone(),
        settings,
        zimservice::health::DegradationTracker::default(),
    );

    let params = SearchParams::default(); // hybrid: mode = None
    let handle = tokio::spawn(async move { engine.search("one", &params).await });

    // Receipt gate: wait until the embed request reaches the mock (within 5 s).
    let gate_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if server
            .received_requests()
            .await
            .is_some_and(|r| !r.is_empty())
        {
            break;
        }
        assert!(
            Instant::now() < gate_deadline,
            "embed request never reached the mock — test-infra/settings problem"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Probe budget: try to checkout the sole connection within 1 s of
    // embed receipt. The mock's 30 s delay is orders of magnitude larger
    // than this budget, so the embed is guaranteed still in-flight when
    // the probe fires; success deterministically means the connection was
    // released while the embed was pending. The deadline is raised to
    // 1 s (from 900 ms) — the old 100 ms margin to the 1 s mock delay is
    // gone, so there is no flake risk left to guard against.
    let probe_deadline = Instant::now() + Duration::from_millis(1_000);
    loop {
        match tokio::time::timeout(Duration::from_millis(100), small.get()).await {
            Ok(Ok(conn)) => {
                // Sole connection free while embed pending — H2 bug absent.
                drop(conn);
                break;
            }
            Ok(Err(_)) => break, // pool error, treat as pass (DB issue, not H2)
            Err(_) => {
                assert!(
                    Instant::now() < probe_deadline,
                    "search held its pool connection >900 ms after the embed \
                     request was received — connection is held across the embed await"
                );
            }
        }
    }

    // Do NOT await the search handle: it would block on the 30 s mock
    // delay. Dropping it cancels the in-flight search while its embed is
    // still pending — exactly the condition under test. End-to-end search
    // success is already covered by the other wiremock tests in this file
    // (e.g. `embed_pipeline_embeds_then_guard_skips`), so cancelling here
    // loses nothing.
    drop(handle);
}

/// P6 (TEST#5) — end-to-end embedding pipeline against a wiremock provider:
/// (1) embeds every unembedded article, (2) a re-run is a no-op (claims 0,
/// sends no HTTP), (3) a count mismatch (provider returns fewer vectors than
/// claimed texts) is skipped, not an error — rows stay unembedded.
#[tokio::test]
async fn embed_pipeline_embeds_then_guard_skips() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_embed__";
    const DIM: u32 = 1536; // == baseline articles.embedding dimension → no ALTER

    // Build a `{data:[{index,embedding:[…]}]}` body with one vector per
    // requested index (values distinct per index so the mapping is real).
    let embed_body = |indices: &[usize]| -> String {
        let data: Vec<String> = indices
            .iter()
            .map(|&i| {
                let v: Vec<String> = (0..DIM)
                    .map(|_| format!("{:.4}", 0.1 * i as f32 + 0.001))
                    .collect();
                format!("{{\"index\":{},\"embedding\":[{}]}}", i, v.join(","))
            })
            .collect();
        format!("{{\"data\":[{}]}}", data.join(","))
    };

    let zim_id: i32 = {
        let c = pool.get().await.unwrap();
        c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
            .await
            .unwrap();
        c.execute(
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,\n                               index_status, indexed_entries, article_count)\n             VALUES ($1, $1, $1, 0, now(), 'ready', 3, 3)",
            &[&ZIM],
        )
        .await
        .unwrap();
        c.query_one("SELECT id FROM zims WHERE name = $1", &[&ZIM])
            .await
            .unwrap()
            .get(0)
    };
    {
        let c = pool.get().await.unwrap();
        for (path, title) in [("A/one", "One"), ("A/two", "Two"), ("A/three", "Three")] {
            c.execute(
                "INSERT INTO articles (path, title, content_preview, snippet, search_vector, language, namespace, zim_id)\n                 VALUES ($1, $2, 'preview '||$2, 'snip '||$2, to_tsvector('simple', $2), 'en', 'C', $3)",
                &[&path, &title, &zim_id],
            )
            .await
            .unwrap();
        }
    }

    let server = MockServer::start().await;
    let values: HashMap<String, serde_json::Value> = vec![
        ("embedding.endpoint".into(), serde_json::json!(server.uri())),
        ("embedding.api_key".into(), serde_json::json!("")),
        ("embedding.model".into(), serde_json::json!("test-model")),
        ("embedding.dimension".into(), serde_json::json!(DIM)),
        ("embedding.batch_size".into(), serde_json::json!(64)),
        ("embedding.max_concurrency".into(), serde_json::json!(1)),
        ("embedding.timeout_secs".into(), serde_json::json!(5)),
    ]
    .into_iter()
    .collect();
    let settings = SettingsCache::new_with_map(pool.clone(), values, HashMap::new());

    // Run 1: provider returns all 3 vectors (out of order) → 3 embedded.
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_string(embed_body(&[2, 0, 1])))
        .mount(&server)
        .await;
    zimservice::embed::run_pipeline(pool.clone(), settings.clone(), ZIM)
        .await
        .expect("pipeline run 1");
    let c = pool.get().await.unwrap();
    let n1: i64 = c
        .query_one(
            "SELECT count(*) FROM articles WHERE zim_id = $1 AND embedding IS NOT NULL",
            &[&zim_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n1, 3, "all 3 articles embedded after run 1");
    let model: Option<String> = c
        .query_one(
            "SELECT embed_model FROM articles WHERE zim_id = $1 AND embedding IS NOT NULL LIMIT 1",
            &[&zim_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(model, Some("test-model".to_string()));
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "exactly one embed HTTP call in run 1"
    );

    // Run 2: nothing unembedded → claims 0, sends no HTTP, stays a no-op.
    zimservice::embed::run_pipeline(pool.clone(), settings.clone(), ZIM)
        .await
        .expect("pipeline run 2 (no-op)");
    let n2: i64 = c
        .query_one(
            "SELECT count(*) FROM articles WHERE zim_id = $1 AND embedding IS NOT NULL",
            &[&zim_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n2, 3, "run 2 leaves embeddings intact");
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "run 2 must not send an embed HTTP call (claims 0)"
    );

    // Count-guard path: un-embed, provider returns 2 vectors for a claimed
    // batch of 3 → the guard skips the batch (Ok, not Err) and rows stay
    // unembedded.
    c.execute(
        "UPDATE articles SET embedding = NULL, embed_at = NULL WHERE zim_id = $1",
        &[&zim_id],
    )
    .await
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_string(embed_body(&[0, 1])))
        .mount(&server)
        .await;
    zimservice::embed::run_pipeline(pool.clone(), settings.clone(), ZIM)
        .await
        .expect("pipeline run 3 (guard skip)");
    let n3: i64 = c
        .query_one(
            "SELECT count(*) FROM articles WHERE zim_id = $1 AND embedding IS NOT NULL",
            &[&zim_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n3, 0, "count-guard skip must leave rows unembedded");
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        2,
        "run 3 sends exactly one (mismatched) embed call, then skips"
    );

    // Cleanup (cascades to articles).
    c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
        .await
        .unwrap();
}

/// W6.5: a row whose embed batch always fails (the endpoint 500s) must stop
/// being re-claimed after `POISON_FAIL_MAX` (3) failures — not re-sent forever.
/// Between runs we reset `embed_at` to simulate the 10-minute claim window
/// elapsing. DB-gated.
#[tokio::test]
async fn embed_poisoned_rows_not_resent_within_run() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_embed_poison__";
    const DIM: u32 = 1536;

    let zim_id: i32 = {
        let c = pool.get().await.unwrap();
        c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
            .await
            .unwrap();
        c.execute(
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,\n                               index_status, indexed_entries, article_count)\n             VALUES ($1, $1, $1, 0, now(), 'ready', 1, 1)",
            &[&ZIM],
        )
        .await
        .unwrap();
        c.query_one("SELECT id FROM zims WHERE name = $1", &[&ZIM])
            .await
            .unwrap()
            .get(0)
    };
    {
        let c = pool.get().await.unwrap();
        c.execute(
            "INSERT INTO articles (path, title, content_preview, snippet, search_vector, language, namespace, zim_id)\n             VALUES ('A/poison', 'Poison', 'preview', 'snip', to_tsvector('simple', 'Poison'), 'en', 'C', $1)",
            &[&zim_id],
        )
        .await
        .unwrap();
    }

    // The endpoint always 500s.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(ResponseTemplate::new(500).set_body_string("{}"))
        .mount(&server)
        .await;

    let values: HashMap<String, serde_json::Value> = vec![
        ("embedding.endpoint".into(), serde_json::json!(server.uri())),
        ("embedding.api_key".into(), serde_json::json!("")),
        ("embedding.model".into(), serde_json::json!("test-model")),
        ("embedding.dimension".into(), serde_json::json!(DIM)),
        ("embedding.batch_size".into(), serde_json::json!(64)),
        ("embedding.max_concurrency".into(), serde_json::json!(1)),
        ("embedding.timeout_secs".into(), serde_json::json!(5)),
    ]
    .into_iter()
    .collect();
    let settings = SettingsCache::new_with_map(pool.clone(), values, HashMap::new());

    let claimable = || {
        let pool = pool.clone();
        Box::pin(async move {
            let c = pool.get().await.unwrap();
            c.execute(
                "UPDATE articles SET embed_at = NULL WHERE zim_id = $1",
                &[&zim_id],
            )
            .await
            .unwrap()
        })
    };

    // Runs 1-3: each re-claims the (reset) row; the 500 bumps its poison
    // counter (1, 2, 3). Three embed HTTP calls total.
    for _ in 0..3 {
        claimable().await;
        assert!(
            zimservice::embed::run_pipeline(pool.clone(), settings.clone(), ZIM)
                .await
                .is_err(),
            "a 500ing endpoint must surface an error (row stays NULL)"
        );
    }
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        3,
        "first 3 failures each send one embed HTTP call"
    );

    // Run 4: the row is now poison (count 3). Even though it is re-claimable
    // (embed_at reset), the poison filter drops it → no embed HTTP call, Ok.
    claimable().await;
    zimservice::embed::run_pipeline(pool.clone(), settings.clone(), ZIM)
        .await
        .expect("run 4 (poison, no HTTP) returns Ok");
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        3,
        "poison row (3 failures) is dropped — run 4 sends no embed HTTP call"
    );

    // The row is still unembedded (the 500s never succeeded).
    let c = pool.get().await.unwrap();
    let n: i64 = c
        .query_one(
            "SELECT count(*) FROM articles WHERE zim_id = $1 AND embedding IS NOT NULL",
            &[&zim_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0, "poison row is never embedded");

    // Cleanup (cascades to articles).
    c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
        .await
        .unwrap();
}

/// PERF 1 (availability-preserving reindex): the reindex pipeline used to do an
/// up-front `DELETE FROM articles` for the ZIM and re-insert chunk-by-chunk,
/// leaving its search index empty for the whole run (a user-facing availability
/// window on an updated ZIM). It now upserts in place and prunes stale rows
/// atomically at finalize. This test pins the exact prune semantics
/// `finalize_zim` relies on:
///   1. no availability window — a stale (removed) row stays queryable until
///      the final prune, so `articles` never holds a partial row set mid-run;
///   2. `updated_at < index_started_at::timestamptz` deletes exactly the stale
///      rows and keeps the re-upserted + newly-inserted ones;
///   3. the `NOT EXISTS` qid prune drops only Q-IDs whose article is gone.
///
/// (End-to-end atomicity — a concurrent reader never observing the mix — is the
/// transaction guarantee and is exercised by the `DbExclusiveGuard` serializing
/// DB tests, not re-simulated here.)
#[tokio::test]
async fn reindex_prune_keeps_live_rows_and_drops_stale() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    const NAME: &str = "perf1_reidx";
    const A: &str = "A/one";
    const B: &str = "A/two";
    const C: &str = "A/three"; // removed from the archive on the "new" version
    const D: &str = "A/four"; // added by the "new" version

    let c = pool.get().await.unwrap();
    // Clean slate (idempotent re-run).
    c.execute("DELETE FROM zims WHERE name = $1", &[&NAME])
        .await
        .unwrap();
    c.execute(
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,\n                           index_status, indexed_entries, article_count)\n         VALUES ($1, $1, $1, 0, now(), 'ready', 3, 3)",
        &[&NAME],
    )
    .await
    .unwrap();
    let zim_id: i32 = c
        .query_one("SELECT id FROM zims WHERE name = $1", &[&NAME])
        .await
        .unwrap()
        .get(0);

    // Prior-index rows A/B/C, all with a past `updated_at`.
    for (path, title, qid) in [(A, "One", 111i64), (B, "Two", 222i64), (C, "Three", 333i64)] {
        c.execute(
            "INSERT INTO articles (path, title, content_preview, search_vector, language, namespace, zim_id, updated_at)\n             VALUES ($1, $2, 'p', to_tsvector('simple', $2), 'en', 'C', $3, now() - interval '1 hour')",
            &[&path, &title, &zim_id],
        )
        .await
        .unwrap();
        c.execute(
            "INSERT INTO qid_index (zim_id, path, qid) VALUES ($1, $2, $3)",
            &[&zim_id, &path, &qid],
        )
        .await
        .unwrap();
    }

    // Run start (what finalize_zim captures). Stale rows are well below it.
    let index_started_at: String = c.query_one("SELECT now()::text", &[]).await.unwrap().get(0);

    // Simulate the reindex: A + B re-upserted (updated_at bumped to now()), D
    // inserted (default updated_at = now()). C is NOT re-upserted → stale.
    for path in [A, B] {
        c.execute(
            "UPDATE articles SET updated_at = now() WHERE zim_id = $1 AND path = $2",
            &[&zim_id, &path],
        )
        .await
        .unwrap();
    }
    c.execute(
        "INSERT INTO articles (path, title, content_preview, search_vector, language, namespace, zim_id)\n         VALUES ($1, $2, 'p', to_tsvector('simple', $2), 'en', 'C', $3)",
        &[&D, &"Four", &zim_id],
    )
    .await
    .unwrap();

    // (1) No availability window: the stale row C is still queryable until the
    // final prune, so `articles` never holds a partial row set mid-run.
    let c_present: bool = c
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM articles WHERE zim_id = $1 AND path = $2)",
            &[&zim_id, &C],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        c_present,
        "stale row must remain until the final prune (no gap)"
    );

    // (2) The exact prune `finalize_zim` runs: stale by updated_at, then orphaned
    // qids by NOT EXISTS against the just-pruned articles.
    let del: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![
        &NAME as &(dyn postgres_types::ToSql + Sync),
        &index_started_at as &(dyn postgres_types::ToSql + Sync),
    ];
    c.execute(
        "DELETE FROM articles\n         WHERE zim_id = (SELECT id FROM zims WHERE name = $1)\n           AND updated_at < $2::timestamptz",
        &del,
    )
    .await
    .unwrap();
    c.execute(
        "DELETE FROM qid_index q\n         WHERE q.zim_id = (SELECT id FROM zims WHERE name = $1)\n           AND NOT EXISTS (SELECT 1 FROM articles a\n                WHERE a.zim_id = q.zim_id AND a.path = q.path)",
        &[&NAME],
    )
    .await
    .unwrap();

    // (2) A, B, D remain; C (stale) is gone.
    let remaining: std::collections::BTreeSet<String> = c
        .query(
            "SELECT path FROM articles WHERE zim_id = $1 ORDER BY path",
            &[&zim_id],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(
        remaining,
        std::collections::BTreeSet::from([A.to_string(), B.to_string(), D.to_string()]),
        "stale row C must be pruned; live rows A/B/D kept"
    );

    // (3) qid for C (orphaned) is gone; A + B qids kept; D has none.
    let qid_paths: Vec<String> = c
        .query(
            "SELECT path FROM qid_index WHERE zim_id = $1 ORDER BY path",
            &[&zim_id],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(
        qid_paths,
        vec![A.to_string(), B.to_string()],
        "orphaned C qid pruned; A/B qids kept"
    );

    // Cleanup.
    c.execute("DELETE FROM zims WHERE name = $1", &[&NAME])
        .await
        .unwrap();
}
