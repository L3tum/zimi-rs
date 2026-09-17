//! Search domain integration tests.

use super::common::*;

#[tokio::test]
async fn smoke_search_suggest_random_on_seed() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    let engine = seed_search_fixture(&pool).await;

    // ── 3. Search (FTS + trgm; vector is a no-op when embedding is unset) ─
    let res = engine
        .search(
            "alpine",
            &SearchParams {
                zim: Some(ZIM),
                language: None,
                mode: None,
                limit: Some(5),
                offset: None,
                highlight: false,
            },
        )
        .await
        .expect("search must run (SQL parses)");
    assert!(!res.is_empty(), "expected at least one hit for 'alpine'");
    assert!(
        res.iter().any(|r| r.title == "Alpine"),
        "Alpine not in results"
    );

    // ── 4. Suggest (regression guard for the H1 param-index bug) ───────────
    let s = engine
        .suggest("alp", Some(ZIM), Some(5))
        .await
        .expect("suggest must run (SQL parses)");
    assert!(!s.is_empty(), "expected a suggestion for 'alp'");

    // NOTE: the old "random_article SQL shape" block that inlined
    // `floor(random() * total)` OFFSET has been removed — it no longer
    // matches the production implementation (`random_id_in_range` + index
    // seek in db/random_article.rs). The real function is covered by
    // `random_article_respects_zim_filter` (40 live draws).

    // Cleanup (fixture ZIM; the suite runs single-threaded).
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}

/// `/search` returns a `results` array with `total == results.len()` (page
/// length, not a corpus count — B2.4) and honors `offset`/`limit` paging.
#[tokio::test]
async fn search_handler_shape() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_search__";
    zimservice::db::raw::execute(
        &pool,
        "DELETE FROM articles WHERE zim_id IN (SELECT id FROM zims WHERE name=$1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 3, 3)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    // Single multi-row INSERT, no binds (extended protocol — only the
    // raw-string `execute_script` path runs on the simple protocol and relies
    // on server-side script splitting; see its doc).
    zimservice::db::raw::execute(
        &pool,
        &format!(
            "INSERT INTO articles (zim_id, path, title, content_preview, search_vector) VALUES
         ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Alpha_One', 'Alpha One', 'alpha one text',
         to_tsvector('simple','alpha one')),
         ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Alpha_Two', 'Alpha Two', 'alpha two text',
         to_tsvector('simple','alpha two')),
         ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Alpha_Three', 'Alpha Three',
         'alpha three text', to_tsvector('simple','alpha three'))"
        ),
        |q| q,
    )
    .await
    .unwrap();

    let state = live_state(pool.clone()).await;
    use zimservice::serve::handlers::{search, SearchQuery};

    // Page 1: limit 2, offset 0 — all three seeded titles match `alpha`.
    let p1 = search(
        axum::extract::State(state.clone()),
        axum::extract::Query(SearchQuery {
            q: Some("alpha".into()),
            query: None,
            zim: Some(ZIM.into()),
            language: None,
            limit: Some(2),
            offset: Some(0),
            mode: Some("fts".into()),
            highlight: None,
        }),
    )
    .await
    .expect("page 1");
    assert_eq!(p1.total, p1.results.len(), "total is the page length");
    assert_eq!(p1.results.len(), 2, "limit 2 honored");

    // Page 2: limit 2, offset 2 → the third row.
    let p2 = search(
        axum::extract::State(state.clone()),
        axum::extract::Query(SearchQuery {
            q: Some("alpha".into()),
            query: None,
            zim: Some(ZIM.into()),
            language: None,
            limit: Some(2),
            offset: Some(2),
            mode: Some("fts".into()),
            highlight: None,
        }),
    )
    .await
    .expect("page 2");
    assert_eq!(p2.results.len(), 1, "offset 2 leaves one row");

    // Cleanup.
    zimservice::db::raw::execute(
        &pool,
        "DELETE FROM articles WHERE zim_id IN (SELECT id FROM zims WHERE name=$1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}

/// Deep-paging + determinism regression (DB-gated — skipped without Postgres):
/// the branch fetch must be offset-aware, so a page past 5×limit still
/// returns real rows when the fixture has enough matches (the old flat
/// `limit*2` fetch capped the merged pool at ~6×limit → empty pages). Also
/// pins page-boundary determinism: two identical consecutive requests return
/// the same `id` sequence, and consecutive pages don't overlap.
#[tokio::test]
async fn search_deep_paging_serves_rows_and_is_deterministic() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_deep__";
    zimservice::db::raw::execute(
        &pool,
        "DELETE FROM articles WHERE zim_id IN (SELECT id FROM zims WHERE name=$1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 130, 130)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    // 130 articles, all matching `deep` — enough that offset 120 (6× limit 20)
    // is a satisfiable page. One multi-row INSERT keeps it to one round-trip.
    let mut values = String::new();
    for i in 1..=130 {
        let sep = if i == 1 { String::new() } else { ",\n".into() };
        values.push_str(&format!(
            "{sep}((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Deep_{i}', 'Deep Paging Probe {i}',
             'deep paging probe text {i}', to_tsvector('simple','deep paging probe {i}'))"
        ));
    }
    zimservice::db::raw::execute(
        &pool,
        &format!(
            "INSERT INTO articles (zim_id, path, title, content_preview, search_vector) VALUES \
            {values}"
        ),
        |q| q,
    )
    .await
    .unwrap();

    let engine = zimservice::search::SearchEngine::new(
        pool.clone(),
        zimservice::settings::SettingsCache::load(
            pool.clone(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        )
        .await
        .expect("settings"),
        zimservice::health::DegradationTracker::default(),
    );
    // FTS-only keeps the assertion exact (one branch, deterministic pool).
    let params = |offset: Option<usize>| SearchParams {
        zim: Some(ZIM),
        language: None,
        mode: Some("fts"),
        limit: Some(20),
        offset,
        highlight: false,
    };

    // Page 1: limit 20 → a full page.
    let p1 = engine
        .search("deep", &params(Some(0)))
        .await
        .expect("page 1");
    assert_eq!(p1.len(), 20, "page 1 must be full (130 matches)");

    // Deep page: offset 120 > 5×limit — empty before the offset-aware fetch.
    let p_deep = engine
        .search("deep", &params(Some(120)))
        .await
        .expect("deep page");
    assert_eq!(p_deep.len(), 10, "130 matches - 120 offset = 10 rows");
    // Consecutive pages never overlap (no skips/dupes at the boundary).
    let page1_ids: std::collections::HashSet<i64> = p1.iter().map(|r| r.id).collect();
    assert!(
        p_deep.iter().all(|r| !page1_ids.contains(&r.id)),
        "deep page must not overlap page 1"
    );

    // Determinism: an identical consecutive request returns the same id
    // sequence (the `a.id` ORDER BY tiebreaker — without it, equal
    // ts_rank_cd scores could reorder rows between requests).
    let p_deep_again = engine
        .search("deep", &params(Some(120)))
        .await
        .expect("deep page again");
    let ids1: Vec<i64> = p_deep.iter().map(|r| r.id).collect();
    let ids2: Vec<i64> = p_deep_again.iter().map(|r| r.id).collect();
    assert_eq!(
        ids1, ids2,
        "identical requests must return identical id sequences"
    );

    // Cleanup.
    zimservice::db::raw::execute(
        &pool,
        "DELETE FROM articles WHERE zim_id IN (SELECT id FROM zims WHERE name=$1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}

/// P3 (H3) — per-branch soft-fail on a SHARED connection: a broken branch
/// query returns empty (warn-and-degrade) and does NOT poison the same
/// client for the other branches — this is the guarantee that lets one
/// pooled connection serve all four search arms.
#[tokio::test]
async fn search_branch_soft_fail_keeps_other_branches() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    let mut client = pool.acquire().await.expect("conn");
    let tracker = zimservice::health::DegradationTracker::default();

    // Broken branch: a query that cannot succeed.
    let broken = zimservice::search::SqlQuery {
        sql: "SELECT * FROM __no_such_table__".into(),
        params: vec![],
    };
    // Healthy branch on the SAME client: a trivial query whose nine columns
    // decode as a `SearchRow` (the shape `run_sql_on` decodes into). The score
    // column must be `float8` (a bare `1.0` is Postgres `numeric`, which sqlx
    // cannot decode as the `f64` score field).
    let healthy = zimservice::search::SqlQuery {
        sql: "SELECT 1::bigint, 1::int, 'a', 'b', 'c', NULL::text, 'd', 'e', 1.0::float8".into(),
        params: vec![],
    };

    // Run broken first — it must not leave the client in an error state.
    let r1 = zimservice::search::run_sql_on(
        &mut *client,
        &broken,
        "test broken branch",
        &tracker,
        "fts",
    )
    .await;
    assert!(
        r1.is_empty(),
        "broken branch must degrade to empty, not error"
    );

    // The same client must still serve a healthy query.
    let r2 = zimservice::search::run_sql_on(
        &mut *client,
        &healthy,
        "test healthy branch",
        &tracker,
        "fts",
    )
    .await;
    assert_eq!(
        r2.len(),
        1,
        "healthy branch on the same client must still work"
    );
    assert_eq!(r2[0].0, 1);

    // And a broken branch again (idempotent degradation).
    let r3 = zimservice::search::run_sql_on(
        &mut *client,
        &broken,
        "test broken branch 2",
        &tracker,
        "fts",
    )
    .await;
    assert!(r3.is_empty());
}

// ── Live-DB handler coverage: /snippet, /suggest, /interlanguage, /chunks ──

/// `GET /snippet` returns the indexed article's title + snippet (dead-pool 503
/// is covered by the handler unit test; here we exercise the real DB read).
#[tokio::test]
async fn snippet_live_returns_title_and_snippet() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_snip__";
    const PATH: &str = "A/article";
    const TITLE: &str = "My Snippet Title";
    const SNIP: &str = "The indexed snippet body";
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 1, 1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    let zim_id: i32 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT id FROM zims WHERE name = $1",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap()
    .expect("row present");
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO articles (path, title, content_preview, snippet, search_vector, language, \
        namespace, zim_id)
         VALUES ($1, $2, 'Some preview', $3, to_tsvector('simple', $2), 'en', 'C', $4)",
        |q| q.bind(PATH).bind(TITLE).bind(SNIP).bind(zim_id),
    )
    .await
    .unwrap();

    let state = live_state(pool.clone()).await;
    let app = zimservice::serve::build_router(state);
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/snippet?zim={ZIM}&path={PATH}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let text = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&text).unwrap();
    assert_eq!(v["title"], TITLE);
    assert_eq!(v["snippet"], SNIP);

    // Cleanup.
    zimservice::db::raw::execute(
        &pool,
        "DELETE FROM articles WHERE zim_id IN (SELECT id FROM zims WHERE name = $1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}

/// `GET /suggest` returns matching titles for a prefix query (dead-pool 503
/// is covered by the handler unit test; here the trigram prefix arm runs live).
#[tokio::test]
async fn suggest_live_returns_matching_title() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_sug__";
    const TITLE: &str = "ZebraFruitArticle";
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 1, 1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    let zim_id: i32 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT id FROM zims WHERE name = $1",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap()
    .expect("row present");
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO articles (path, title, content_preview, snippet, search_vector, language, \
        namespace, zim_id)
         VALUES ($1, $2, 'p', 's', to_tsvector('simple', $2), 'en', 'C', $3)",
        |q| q.bind("A/zebra").bind(TITLE).bind(zim_id),
    )
    .await
    .unwrap();

    let state = live_state(pool.clone()).await;
    let app = zimservice::serve::build_router(state);
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/suggest?q=ZebraFruit&zim={ZIM}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let text = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&text).unwrap();
    assert!(
        v["suggestions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s == TITLE),
        "expected a suggestion for {TITLE}, got: {}",
        v["suggestions"]
    );

    // Cleanup.
    zimservice::db::raw::execute(
        &pool,
        "DELETE FROM articles WHERE zim_id IN (SELECT id FROM zims WHERE name = $1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}

/// `GET /interlanguage` returns an empty result (200) when the article has no
/// Q-ID mapping — the common case (dead-pool 503 is covered by the unit test).
#[tokio::test]
async fn interlanguage_live_no_qid_returns_empty() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_inter__";
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 1, 1)",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap();

    let state = live_state(pool.clone()).await;
    let app = zimservice::serve::build_router(state);
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/interlanguage?zim={ZIM}&path=A/foo"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let text = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&text).unwrap();
    assert!(v["qid"].is_null(), "qid must be null, got: {}", v["qid"]);
    assert_eq!(v["languages"], serde_json::json!([]));

    // Cleanup.
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}

/// `GET /interlanguage` resolves a real Q-ID and lists the cross-language
/// articles that share it — the only live `/interlanguage` test that asserts a
/// non-null QID and a populated `languages` array (the no-QID empty case is
/// covered by `interlanguage_live_no_qid_returns_empty`).
///
/// Two single-language ZIMs share one Wikidata Q-ID; querying from `FR`
/// must return the Q-ID string and the `EN` entry in `languages`. The zim
/// names are improbable two-letter fixtures (not real language codes) so this
/// test's `DELETE FROM zims` can never clobber a real ZIM row on a dev DB.
#[tokio::test]
async fn interlanguage_live_returns_qid_and_languages() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const EN: &str = "zz";
    const FR: &str = "qy";
    const EN_PATH: &str = "A/foo";
    const FR_PATH: &str = "A/foo_fr";
    const QID: i64 = 12345;
    for name in [EN, FR] {
        zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(name))
            .await
            .unwrap();
        zimservice::db::raw::execute(
            &pool,
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                               index_status, indexed_entries, article_count)
             VALUES ($1, $1, $1, 0, now(), 'ready', 1, 1)",
            |q| q.bind(name),
        )
        .await
        .unwrap();
    }
    let en_id: i32 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT id FROM zims WHERE name = $1",
        |q| q.bind(EN),
    )
    .await
    .unwrap()
    .expect("row present");
    let fr_id: i32 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT id FROM zims WHERE name = $1",
        |q| q.bind(FR),
    )
    .await
    .unwrap()
    .expect("row present");
    // Articles (so the LEFT JOIN yields a title) + the shared Q-ID rows.
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO articles (path, title, content_preview, search_vector, language, namespace, \
        zim_id)
         VALUES ($1, $2, 'p', to_tsvector('simple', $2), 'en', 'C', $3)",
        |q| q.bind(EN_PATH).bind("Foo (en)").bind(en_id),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO articles (path, title, content_preview, search_vector, language, namespace, \
        zim_id)
         VALUES ($1, $2, 'p', to_tsvector('simple', $2), 'fr', 'C', $3)",
        |q| q.bind(FR_PATH).bind("Foo (fr)").bind(fr_id),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO qid_index (zim_id, path, qid) VALUES ($1, $2, $3)",
        |q| q.bind(en_id).bind(EN_PATH).bind(QID),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO qid_index (zim_id, path, qid) VALUES ($1, $2, $3)",
        |q| q.bind(fr_id).bind(FR_PATH).bind(QID),
    )
    .await
    .unwrap();

    let state = live_state(pool.clone()).await;
    let app = zimservice::serve::build_router(state);

    // From `FR`: the shared Q-ID resolves and `EN` is the cross-language
    // entry.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/interlanguage?zim={FR}&path={FR_PATH}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let text = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&text).unwrap();
    assert_eq!(
        v["qid"],
        serde_json::json!("Q12345"),
        "qid must be a string"
    );
    let langs = v["languages"]
        .as_array()
        .expect("languages must be an array");
    assert_eq!(langs.len(), 1, "exactly the other language, got: {langs:?}");
    assert_eq!(langs[0]["zim"], EN, "cross-language entry must be `en`");
    assert_eq!(langs[0]["path"], EN_PATH);
    assert_eq!(langs[0]["title"], "Foo (en)");

    // Symmetric: from `EN` the Q-ID resolves and the unfiltered
    // cross-language list includes `FR`.
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/interlanguage?zim={EN}&path={EN_PATH}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let text = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&text).unwrap();
    assert_eq!(v["qid"], serde_json::json!("Q12345"));
    let langs = v["languages"]
        .as_array()
        .expect("languages must be an array");
    assert!(
        langs.iter().any(|e| e["zim"] == FR),
        "unfiltered list must include `fr`, got: {langs:?}"
    );

    // Cleanup (qid_index + articles cascade off zims; delete the zims).
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(EN))
        .await
        .unwrap();
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(FR))
        .await
        .unwrap();
}

/// `GET /chunks` opens the live ZIM, extracts the article text, and returns it
/// as RAG chunks (bad-ZIM 404 is covered by the handler unit test; here the
/// real `tiny.zim` fixture is chunked).
#[tokio::test]
async fn chunks_live_returns_chunked_article() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    let state = fixture_state(pool.clone()).await;
    let app = zimservice::serve::build_router(state);
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/chunks?zim=tiny&path=main.html")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let text = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&text).unwrap();
    let n = v["chunk_count"].as_u64().unwrap_or(0);
    assert!(n >= 1, "expected at least one chunk, got: {}", v);
    let chunks = v["chunks"].as_array().unwrap();
    assert!(!chunks[0]["text"].as_str().unwrap_or("").is_empty());

    // Cleanup: drop the fixture's zims row so the harness stays idempotent.
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| {
        q.bind("tiny")
    })
    .await
    .unwrap();
}

/// Tests (2026-09 review round 2): direct (function-level) DB test for
/// `db::qid::interlanguage_json` — the HTTP route tests
/// (`interlanguage_live_no_qid_returns_empty`, `interlanguage_live_returns_qid_
/// and_languages`) pin the same shapes through the handler; this pins the
/// function's contract itself so a route refactor can't silently change the
/// wire shape. Both arms: null QID (`{"qid": null, "languages": []}`) and a
/// populated cross-language list.
#[tokio::test]
async fn interlanguage_json_direct_function_shape() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "il1";
    const OTHER: &str = "il2";
    const PATH: &str = "A/bar";
    const QID: i64 = 777;

    for name in [ZIM, OTHER] {
        zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(name))
            .await
            .unwrap();
        zimservice::db::raw::execute(
            &pool,
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime, \
             index_status, indexed_entries, article_count) \
             VALUES ($1, $1, $1, 0, now(), 'ready', 1, 1)",
            |q| q.bind(name),
        )
        .await
        .unwrap();
    }
    let id1: i32 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT id FROM zims WHERE name = $1",
        |q| q.bind(ZIM),
    )
    .await
    .unwrap()
    .expect("row present");
    let id2: i32 = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT id FROM zims WHERE name = $1",
        |q| q.bind(OTHER),
    )
    .await
    .unwrap()
    .expect("row present");

    // (1) No qid_index row: the null-QID shape.
    let v = zimservice::db::qid::interlanguage_json(&pool, ZIM, "A/no-qid")
        .await
        .unwrap();
    assert_eq!(v, serde_json::json!({"qid": null, "languages": []}));

    // (2) Shared Q-ID + the other ZIM's article title via the LEFT JOIN.
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO articles (path, title, content_preview, search_vector, language, \
         namespace, zim_id) VALUES ($1, $2, 'p', to_tsvector('simple', $2), 'en', 'C', $3)",
        |q| q.bind(PATH).bind("Bar (en)").bind(id1),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO articles (path, title, content_preview, search_vector, language, \
         namespace, zim_id) VALUES ($1, $2, 'p', to_tsvector('simple', $2), 'fr', 'C', $3)",
        |q| q.bind("A/bar_fr").bind("Bar (fr)").bind(id2),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO qid_index (zim_id, path, qid) VALUES ($1, $2, $3)",
        |q| q.bind(id1).bind(PATH).bind(QID),
    )
    .await
    .unwrap();
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO qid_index (zim_id, path, qid) VALUES ($1, $2, $3)",
        |q| q.bind(id2).bind("A/bar_fr").bind(QID),
    )
    .await
    .unwrap();

    let v = zimservice::db::qid::interlanguage_json(&pool, ZIM, PATH)
        .await
        .unwrap();
    assert_eq!(v["qid"], serde_json::json!("Q777"));
    let langs = v["languages"].as_array().expect("languages array");
    assert_eq!(langs.len(), 1, "only the OTHER zim, got: {langs:?}");
    assert_eq!(langs[0]["zim"], OTHER);
    assert_eq!(langs[0]["path"], "A/bar_fr");
    assert_eq!(langs[0]["title"], "Bar (fr)");

    // Cleanup (articles + qid_index cascade off zims).
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(OTHER))
        .await
        .unwrap();
}
