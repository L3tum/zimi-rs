//! Zims domain integration tests.

use super::common::*;

// ── T7: DB-backed MCP happy paths ─────────────────────────────────────────
// One test per tool asserting the success envelope shape and a non-empty
// result against the seeded fixture ZIM. Skipped when DB unreachable.
#[tokio::test]
async fn mcp_tools_db_backed() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    // Dedicated fixture ZIM so this test's inserts/deletes can't collide with
    // `smoke_search_suggest_random_on_seed`'s `__itest__` rows (suite runs
    // single-threaded, but the fixtures must still be disjoint).
    const ZIM: &str = "__itest_mcp__";

    // Seed fixture ZIM + articles.
    {
        let c = pool.get().await.unwrap();
        c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
            .await
            .unwrap();
        c.execute(
            "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                               index_status, indexed_entries, article_count)
             VALUES ($1, $1, $1, 0, now(), 'ready', 3, 3)",
            &[&ZIM],
        )
        .await
        .unwrap();
        c.batch_execute(&format!(
            "INSERT INTO articles (zim_id, path, title, content_preview, search_vector, title_lower) VALUES
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Alpine', 'Alpine',
              'The Alps are mountains.', to_tsvector('simple','alpine peaks europe'), 'alpine'),
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Baltic', 'Baltic Sea',
              'The Baltic is a sea.', to_tsvector('simple','baltic sea'), 'baltic sea'),
             ((SELECT id FROM zims WHERE name='{ZIM}'), 'A/Andes', 'Andes',
              'The Andes are mountains in South America.',
              to_tsvector('simple','andes mountains south america'), 'andes')"
        ))
        .await
        .unwrap();
    }

    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");
    let zims = ZimManager::new(std::path::PathBuf::from("/nonexistent-mcp"), pool.clone());
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool.clone(), settings.clone(), zims, search);

    // 1. search → envelope with results array.
    let res = zimservice::testing::mcp_call_tool(
        &state,
        "search",
        &serde_json::json!({"query": "alpine", "zim": ZIM}),
    )
    .await
    .expect("mcp search");
    assert!(
        res.get("content")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("text"))
            .is_some(),
        "search envelope should have content[0].text"
    );
    let text = res["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("Alpine"),
        "search result should mention Alpine: {text}"
    );

    // 2. random → envelope with an article.
    let res =
        zimservice::testing::mcp_call_tool(&state, "random", &serde_json::json!({"zim": ZIM}))
            .await
            .expect("mcp random");
    let text = &res["content"][0]["text"];
    assert!(text.as_str().is_some(), "random should return article text");
    let body: serde_json::Value = serde_json::from_str(text.as_str().unwrap()).unwrap();
    assert!(body.get("title").is_some(), "random article has title");
    assert!(body.get("path").is_some(), "random article has path");

    // 3. suggest → envelope with suggestions.
    let res = zimservice::testing::mcp_call_tool(
        &state,
        "suggest",
        &serde_json::json!({"query": "alp", "zim": ZIM}),
    )
    .await
    .expect("mcp suggest");
    let text = &res["content"][0]["text"];
    let sugg: serde_json::Value = serde_json::from_str(text.as_str().unwrap()).unwrap();
    assert!(sugg.is_array(), "suggest returns an array");
    assert!(
        !sugg.as_array().unwrap().is_empty(),
        "suggest should have at least one hit"
    );

    // 4. list_sources → envelope with ZIM list.
    let res = zimservice::testing::mcp_call_tool(&state, "list_sources", &serde_json::json!({}))
        .await
        .expect("mcp list_sources");
    let text = &res["content"][0]["text"];
    let sources: serde_json::Value = serde_json::from_str(text.as_str().unwrap()).unwrap();
    assert!(sources.is_array(), "list_sources returns an array");

    // Cleanup.
    let c = pool.get().await.unwrap();
    c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
        .await
        .unwrap();
}

// ── T8: SettingsCache update success path ─────────────────────────────────
// H3 regression: an unauthenticated PUT /settings returns a 200 for a
// non-sensitive key, but the response's topology keys must be redacted (the
// write path must not leak the internal qB/embed URLs that GET already hides).
#[tokio::test]
async fn put_settings_unauthenticated_response_redacts_topology() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    // Seed a topology key so the unauthenticated response has something to
    // redact. Captured so we can restore it on cleanup.
    let original: Option<String> = {
        let c = pool.get().await.unwrap();
        c.query_one(
            "SELECT value::text FROM settings WHERE key = $1",
            &[&"torrent.url"],
        )
        .await
        .ok()
        .map(|r| r.get::<_, String>(0))
    };
    {
        let c = pool.get().await.unwrap();
        c.execute(
            "INSERT INTO settings (key, value) VALUES ('torrent.url', $1)\n             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            &[&serde_json::json!("http://qb-host:8080").to_string()],
        )
        .await
        .unwrap();
    }

    let state = live_state(pool.clone()).await;

    let res = zimservice::serve::handlers::put_settings(
        axum::extract::State(state.clone()),
        axum::http::HeaderMap::new(),
        axum::Json(serde_json::json!({"embedding.model": "itest-redact"})),
    )
    .await
    .expect("put_settings");
    assert_eq!(res.updated, 1, "one non-sensitive key updated");
    assert!(
        res.errors.as_array().map(|a| a.is_empty()).unwrap_or(false),
        "no errors expected: {:?}",
        res.errors
    );
    assert_eq!(
        res.settings["torrent"]["url"]["value"],
        serde_json::json!("[redacted]"),
        "unauthenticated response must redact topology"
    );

    // Restore torrent.url to its prior value (usually the empty seed).
    let c = pool.get().await.unwrap();
    match original {
        Some(v) => c
            .execute(
                "UPDATE settings SET value = $1 WHERE key = 'torrent.url'",
                &[&v],
            )
            .await
            .unwrap(),
        None => c
            .execute("DELETE FROM settings WHERE key = 'torrent.url'", &[])
            .await
            .unwrap(),
    };
}

// ── T8: SettingsCache update success path ─────────────────────────────────
// Validates the full write-through: update → DB row changed + in-memory
// cache reflects the new value + reload() round-trips.
#[tokio::test]
async fn settings_update_roundtrip() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");

    // 1) Config-only key is rejected (nothing reaches the DB).
    let mut host_updates = HashMap::new();
    host_updates.insert("general.host".to_string(), serde_json::json!("127.0.0.1"));
    let host_errors = settings
        .update(&host_updates, true)
        .await
        .expect("update returns Ok with per-key errors");
    assert!(
        host_errors
            .iter()
            .any(|e| e.contains("set via config file")),
        "general.host should be rejected as config-only: {host_errors:?}"
    );

    // 2) A mutable key writes through: no errors, cache updated, DB row
    //    changed, reload() round-trips.
    let mut updates = HashMap::new();
    updates.insert(
        "embedding.model".to_string(),
        serde_json::json!("itest-model"),
    );
    let model_errors = settings.update(&updates, true).await.expect("update");
    assert!(
        model_errors.is_empty(),
        "no errors expected: {model_errors:?}"
    );
    assert_eq!(
        settings.get("embedding.model").unwrap(),
        serde_json::json!("itest-model")
    );
    {
        let c = pool.get().await.unwrap();
        let val: String = c
            .query_one(
                "SELECT value::text FROM settings WHERE key = $1",
                &[&"embedding.model"],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(val, "\"itest-model\"");
    }
    settings.reload().await.expect("reload");
    assert_eq!(
        settings.get("embedding.model").unwrap(),
        serde_json::json!("itest-model")
    );

    // 3) API-immutable key is rejected even when otherwise well-formed.
    let mut locked = HashMap::new();
    locked.insert("access.mode".to_string(), serde_json::json!("password"));
    let locked_errors = settings
        .update(&locked, true)
        .await
        .expect("update (may have errors)");
    assert!(
        locked_errors
            .iter()
            .any(|e| e.contains("not changeable via API")),
        "access.mode should be rejected as API-immutable: {locked_errors:?}"
    );

    // 4) Cleanup: drop the seeded row so the next reload falls back to the
    //    seed default (leaves the dev DB as we found it).
    pool.get()
        .await
        .unwrap()
        .execute("DELETE FROM settings WHERE key = $1", &[&"embedding.model"])
        .await
        .unwrap();
}

// ── B1.2: random article must respect the ZIM filter ────────────────────
// Regression test: the forward/backward index seeks used to carry no ZIM
// filter, so `?zim=A` could return an article from ZIM B.
#[tokio::test]
async fn random_article_respects_zim_filter() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM_A: &str = "__itest_rand__";
    const ZIM_B: &str = "__itest_rand2__";
    let cleanup = |pool: &Pool| {
        let pool = pool.clone();
        async move {
            let c = pool.get().await.unwrap();
            c.execute(
                "DELETE FROM articles WHERE zim_id IN (SELECT id FROM zims WHERE name IN ($1,$2))",
                &[&ZIM_A, &ZIM_B],
            )
            .await
            .unwrap();
            c.execute("DELETE FROM zims WHERE name IN ($1,$2)", &[&ZIM_A, &ZIM_B])
                .await
                .unwrap();
        }
    };
    {
        let c = pool.get().await.unwrap();
        for name in [ZIM_A, ZIM_B] {
            c.execute(
                "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,\n                               index_status, indexed_entries, article_count)\n             VALUES ($1, $1, $1, 0, now(), 'ready', 5, 5)",
                &[&name],
            )
            .await
            .unwrap();
            c.batch_execute(&format!(
                "INSERT INTO articles (zim_id, path, title, content_preview) VALUES\n             ((SELECT id FROM zims WHERE name='{name}'), 'A/a1', 'Rand {name} 1', 'x'),\n             ((SELECT id FROM zims WHERE name='{name}'), 'A/a2', 'Rand {name} 2', 'x'),\n             ((SELECT id FROM zims WHERE name='{name}'), 'A/a3', 'Rand {name} 3', 'x'),\n             ((SELECT id FROM zims WHERE name='{name}'), 'A/a4', 'Rand {name} 4', 'x'),\n             ((SELECT id FROM zims WHERE name='{name}'), 'A/a5', 'Rand {name} 5', 'x')"
            ))
            .await
            .unwrap();
        }
    }
    // Sanity: ZIM_B articles exist, so an unfiltered seek would find them.
    {
        let c = pool.get().await.unwrap();
        let n: i64 = c
            .query_one(
                "SELECT count(*) FROM articles a JOIN zims z ON z.id=a.zim_id WHERE z.name=$1",
                &[&ZIM_B],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            n, 5,
            "ZIM_B must have 5 articles for the regression to bite"
        );
    }
    // 20 scoped draws — every result must come from ZIM_A.
    for i in 0..20 {
        let art =
            match zimservice::db::random_article::fetch_random_article(&pool, Some(ZIM_A)).await {
                Ok(a) => a,
                Err(e) => {
                    cleanup(&pool).await;
                    panic!("draw {i}: {e}")
                }
            };
        assert_eq!(
            art.zim, ZIM_A,
            "draw {i}: scoped to {ZIM_A} but got {}",
            art.zim
        );
    }
    // 20 unfiltered draws — hermetic guard. Unfiltered mode samples the
    // GLOBAL id range by design: a populated DB may contain other ZIMs, so we
    // cannot claim membership in {ZIM_A, ZIM_B} (the old assertion was the
    // original failure condition). We only assert each result is a real, live
    // article row (its id exists and its `zim` resolves to its `zim_id`). The
    // scoped-draws block above is the actual filter regression guard.
    for i in 0..20 {
        let art = match zimservice::db::random_article::fetch_random_article(&pool, None).await {
            Ok(a) => a,
            Err(e) => {
                cleanup(&pool).await;
                panic!("draw {i}: {e}")
            }
        };
        let c = pool.get().await.unwrap();
        let exists: bool = c
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM articles a\n                 JOIN zims z ON z.id = a.zim_id\n                 WHERE a.id = $1 AND z.name = $2)",
                &[&art.id, &art.zim],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            exists,
            "unfiltered draw {i} is not a live article row: id={} zim={}",
            art.id, art.zim
        );
    }
    cleanup(&pool).await;
}

// ── B1.3: `.zim` detection must strip query *and* fragment ───────────────
// Regression: the SQL predicate `split_part(url, '?', 1)` left `#fragment`
// in the path, so `...file.zim#frag` was not recognised as a direct download.
#[tokio::test]
async fn direct_zim_fragment() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "__itest_frag__";
    {
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
    }
    let c = pool.get().await.unwrap();
    let cases = [
        // (url, should be a direct .zim row)
        ("https://x/file.zim#frag", true),
        ("https://x/FILE.ZIM?token=1#frag", true),
        ("https://x/file.txt#frag", false),
        ("https://x/file.zim.txt#frag", false),
    ];
    for (url, expected) in cases {
        // TEST-13: use the production ZIM_URL_PREDICATE constant so the test
        // actually verifies parity with production code (previously the SQL
        // was inlined here and could drift).
        let sql = format!(
            "SELECT 1 WHERE {} LIKE '%.zim'",
            zimservice::torrent::poller::ZIM_URL_PREDICATE.replace("url", "$1")
        );
        let row = c.query_opt(&sql, &[&url]).await.unwrap();
        assert_eq!(
            row.is_some(),
            expected,
            "SQL predicate disagrees with Rust is_direct_zim_url for {url}"
        );
    }
    c.execute("DELETE FROM zims WHERE name = $1", &[&ZIM])
        .await
        .unwrap();
}

/// BUG-3 (migration 011): an ACTIVE row for a given `name` blocks another
/// active row with the same name — even a different URL — because the direct
/// `.part` file is `{name}.part` and must have one owner. Terminal statuses
/// do not block, and a distinct name never conflicts.
#[tokio::test]
async fn enqueue_rejects_same_name_active_row() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    let c = pool.get().await.unwrap();
    c.execute(
        "DELETE FROM downloads WHERE name LIKE 'itest_dup%' OR name = 'itest_other.zim'",
        &[],
    )
    .await
    .unwrap();

    // First queued row for the name.
    let id = insert_download(
        &pool,
        "itest_dup.zim",
        "http://testhost:1/dup.zim",
        "queued",
    )
    .await
    .unwrap();
    // Same name, DIFFERENT url → 23505 from uq_downloads_active_name (003
    // alone would not fire: the URLs differ).
    let err = insert_download(
        &pool,
        "itest_dup.zim",
        "http://testhost:2/dup.zim",
        "downloading",
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.code(),
        Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION),
        "same-name active row must be rejected by 011"
    );
    // Terminal rows do not block a new active row for the same name…
    c.execute("DELETE FROM downloads WHERE id = $1", &[&id])
        .await
        .unwrap();
    c.execute(
        "INSERT INTO downloads (name, url, status) VALUES ('itest_dup.zim', 'http://testhost:1/done.zim', 'complete')",
        &[],
    )
    .await
    .unwrap();
    let _id2 = insert_download(
        &pool,
        "itest_dup.zim",
        "http://testhost:1/dup.zim",
        "queued",
    )
    .await
    .unwrap();
    // …and a distinct name never conflicts.
    let _id3 = insert_download(
        &pool,
        "itest_other.zim",
        "http://testhost:3/other.zim",
        "queued",
    )
    .await
    .unwrap();

    c.execute(
        "DELETE FROM downloads WHERE name LIKE 'itest_dup%' OR name = 'itest_other.zim'",
        &[],
    )
    .await
    .unwrap();
}

/// BUG-16c (CI-DB): cancelling a `complete` row is a 409 (the row exists but
/// is not cancellable), while a non-existent id is a 404. An active row is
/// cancelled normally (200).
#[tokio::test]
async fn cancel_download_non_cancellable_is_409() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    use zimservice::serve::handlers::cancel_download;
    let state = live_state(pool.clone()).await;

    let complete_id = insert_download(
        &pool,
        "itest_cancel_done.zim",
        "http://testhost:9/done.zim",
        "complete",
    )
    .await
    .expect("insert complete row");
    let queued_id = insert_download(
        &pool,
        "itest_cancel_q.zim",
        "http://testhost:9/q.zim",
        "queued",
    )
    .await
    .expect("insert queued row");

    // Terminal (complete) row → 409 Conflict.
    let err = cancel_download(
        axum::extract::State(state.clone()),
        axum::extract::Path(complete_id),
    )
    .await
    .expect_err("a complete row is not cancellable");
    assert!(
        matches!(
            err,
            zimservice::error::Error::Conflict(ref m)
                if m == "download {complete_id} is not cancellable (status: complete)"
        ),
        "expected 409 Conflict, got: {err:?}"
    );

    // Non-existent id → 404 NotFound.
    let missing = cancel_download(
        axum::extract::State(state.clone()),
        axum::extract::Path(9_999_999),
    )
    .await
    .expect_err("a missing id is not found");
    assert!(
        matches!(
            missing,
            zimservice::error::Error::NotFound(ref m) if m.contains("not found")
        ),
        "expected 404 NotFound, got: {missing:?}"
    );

    // Active (queued) row → 200 cancelled.
    let res = cancel_download(
        axum::extract::State(state.clone()),
        axum::extract::Path(queued_id),
    )
    .await
    .expect("an active row cancels fine");
    assert_eq!(res.0.id, queued_id);

    let c = pool.get().await.unwrap();
    c.execute(
        "DELETE FROM downloads WHERE name IN ('itest_cancel_done.zim', 'itest_cancel_q.zim')",
        &[],
    )
    .await
    .unwrap();
}

/// Two concurrent inserts of the same URL with an *active* status race the
/// `idx_downloads_active_url` partial unique index: exactly one succeeds, the
/// other fails with SQLSTATE 23505 (the same `error.rs` mapping the API uses
/// to surface 409).
#[tokio::test]
async fn active_download_unique_race() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const NAME: &str = "__itest_race__";
    const URL: &str = "http://seed.example/__itest_race__.zim";
    {
        let c = pool.get().await.unwrap();
        c.execute("DELETE FROM downloads WHERE url = $1", &[&URL])
            .await
            .unwrap();
    }

    let (r1, r2) = tokio::join!(
        insert_download(&pool, NAME, URL, "queued"),
        insert_download(&pool, NAME, URL, "queued"),
    );

    // Exactly one succeeds.
    let (winner, loser_err) = match (r1, r2) {
        (Ok(a), Ok(b)) => panic!("both inserts must not succeed: {a} {b}"),
        (Ok(a), Err(e)) => (a, e),
        (Err(e), Ok(b)) => (b, e),
        (Err(a), Err(b)) => panic!("both inserts failed: {a} {b}"),
    };
    assert!(winner > 0, "winner should have an id");

    // Loser: unique_violation (SQLSTATE 23505).
    let db_err = loser_err
        .as_db_error()
        .expect("loser must be a Postgres error");
    assert_eq!(
        *db_err.code(),
        tokio_postgres::error::SqlState::UNIQUE_VIOLATION,
        "expected 23505, got {:?}",
        db_err.code()
    );

    // Exactly one active row remains.
    let c = pool.get().await.unwrap();
    let n: i64 = c
        .query_one(
            "SELECT count(*) FROM downloads WHERE url = $1 AND status IN ('queued','downloading')",
            &[&URL],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 1, "exactly one active row should remain");

    c.execute("DELETE FROM downloads WHERE url = $1", &[&URL])
        .await
        .unwrap();
}

/// PERF 5 (was M-poller-n1 / PONY-S4): the batched stats flush writes every
/// changed row in a single `UPDATE … FROM unnest(…)` statement (all stats +
/// `updated_at` advanced) and leaves a no-change row untouched (its
/// `updated_at` is not bumped, so the missing-torrent grace clock keeps
/// counting on it).
#[tokio::test]
async fn stats_update_writes_changed_only() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const TAG: &str = "__itest_batch__";
    let urls = [
        format!("http://seed.example/{TAG}_a.zim"),
        format!("http://seed.example/{TAG}_b.zim"),
        format!("http://seed.example/{TAG}_c.zim"),
    ];

    // Clean up leftovers, seed three `downloading` rows (id ascending).
    {
        let c = pool.get().await.unwrap();
        for u in urls.iter().map(|s| s.as_str()) {
            c.execute("DELETE FROM downloads WHERE url = $1", &[&u])
                .await
                .unwrap();
        }
    }
    let ids = [
        insert_download(&pool, TAG, &urls[0], "downloading")
            .await
            .unwrap(),
        insert_download(&pool, TAG, &urls[1], "downloading")
            .await
            .unwrap(),
        insert_download(&pool, TAG, &urls[2], "downloading")
            .await
            .unwrap(),
    ];

    // Record each row's pre-update `updated_at` (ordered by id).
    let before: Vec<chrono::DateTime<chrono::Utc>> = {
        let c = pool.get().await.unwrap();
        c.query(
            "SELECT updated_at FROM downloads WHERE id IN ($1, $2, $3) ORDER BY id",
            &[&ids[0], &ids[1], &ids[2]],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, chrono::DateTime<chrono::Utc>>(0))
        .collect()
    };

    // Rows 0 and 1 changed; row 2 did not → omit it from the batch.
    let changed: Vec<zimservice::torrent::poller::StatsRow> = vec![
        (ids[0], 0.42f32, 102_400, Some(3600), Some(1.25), 40_960, 7),
        (ids[1], 0.75f32, 0, None, Some(2.0), 0, 3),
    ];
    zimservice::torrent::poller::apply_stats_batch(&pool, &changed)
        .await
        .unwrap();

    let c = pool.get().await.unwrap();
    for (i, id) in ids.iter().enumerate() {
        let r = c
            .query_one(
                "SELECT progress, speed_bps, eta_secs, ratio, up_speed_bps, num_seeds, updated_at \
                 FROM downloads WHERE id = $1",
                &[id],
            )
            .await
            .unwrap();
        let progress: f32 = r.get(0);
        let speed_bps: Option<i64> = r.get(1);
        let eta_secs: Option<i64> = r.get(2);
        let ratio: Option<f32> = r.get(3);
        let up_speed_bps: Option<i64> = r.get(4);
        let num_seeds: Option<i64> = r.get(5);
        let updated: chrono::DateTime<chrono::Utc> = r.get(6);

        if i < 2 {
            let (_, np, ns, ne, nr, nu, nn) = changed[i];
            assert_eq!(progress, np, "row {i} progress");
            assert_eq!(speed_bps, Some(ns), "row {i} speed_bps");
            assert_eq!(eta_secs, ne, "row {i} eta_secs");
            assert_eq!(ratio, nr, "row {i} ratio");
            assert_eq!(up_speed_bps, Some(nu), "row {i} up_speed_bps");
            assert_eq!(num_seeds, Some(nn), "row {i} num_seeds");
            assert!(updated >= before[i], "row {i} updated_at advanced");
        } else {
            // Untouched: still the seeded defaults (progress 0.0, NULL stats)
            // and the exact same `updated_at` (no write touched it).
            assert_eq!(progress, 0.0, "row 2 progress unchanged");
            assert_eq!(speed_bps, None, "row 2 speed_bps still NULL");
            assert_eq!(num_seeds, None, "row 2 num_seeds still NULL");
            assert_eq!(updated, before[2], "row 2 updated_at unchanged");
        }
    }

    // Clean up.
    for u in urls.iter().map(|s| s.as_str()) {
        c.execute("DELETE FROM downloads WHERE url = $1", &[&u])
            .await
            .unwrap();
    }
}

/// `seeding` is deliberately *not* in the 003 partial index, so two seeding
/// rows with the same URL coexist (no false re-download block while seeding),
/// and the 006 seeding columns round-trip.
#[tokio::test]
async fn seeding_rows_not_constrained_and_visible() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const NAME: &str = "__itest_race_seed__";
    const URL: &str = "http://seed.example/__itest_race_seed__.zim";
    {
        let c = pool.get().await.unwrap();
        c.execute("DELETE FROM downloads WHERE url = $1", &[&URL])
            .await
            .unwrap();
    }

    // (a) Two seeding rows, same URL — both insert (no false 23505).
    let a = insert_download(&pool, NAME, URL, "seeding")
        .await
        .expect("first seeding row");
    let b = insert_download(&pool, NAME, URL, "seeding")
        .await
        .expect("second seeding row");
    assert_ne!(a, b, "two distinct seeding rows");

    // (b) Seeding columns (006) round-trip.
    let c = pool.get().await.unwrap();
    c.execute(
        "UPDATE downloads SET ratio = 2.5, up_speed_bps = 1024, num_seeds = 7 WHERE id = $1",
        &[&a],
    )
    .await
    .unwrap();
    let row = c
        .query_one(
            "SELECT ratio, up_speed_bps, num_seeds FROM downloads WHERE id = $1",
            &[&a],
        )
        .await
        .unwrap();
    assert!((row.get::<_, f32>(0) - 2.5).abs() < f32::EPSILON);
    assert_eq!(row.get::<_, i64>(1), 1024);
    assert_eq!(row.get::<_, i64>(2), 7);

    c.execute("DELETE FROM downloads WHERE url = $1", &[&URL])
        .await
        .unwrap();
}

/// `collections` CRUD happy path: create → appears in list → delete → absent.
/// Pure DB against a live pool.
#[tokio::test]
async fn collections_crud() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const NAME: &str = "__itest_coll__";
    const LABEL: &str = "ITest Coll";
    {
        let c = pool.get().await.unwrap();
        c.execute("DELETE FROM collections WHERE name = $1", &[&NAME])
            .await
            .unwrap();
    }

    let state = live_state(pool.clone()).await;
    use zimservice::serve::handlers::{
        create_collection, delete_collection, list_collections, CreateCollectionBody,
    };

    // Create.
    let (_, created) = create_collection(
        axum::extract::State(state.clone()),
        axum::extract::Json(CreateCollectionBody {
            name: NAME.into(),
            label: LABEL.into(),
            zim_names: vec![],
            is_favorite: false,
        }),
    )
    .await
    .expect("create");
    let id = created.id;
    assert!(id > 0);

    // Appears in the list.
    let list = list_collections(axum::extract::State(state.clone()))
        .await
        .expect("list");
    assert!(
        list.collections
            .iter()
            .any(|c| c.id == id && c.name == NAME),
        "created collection should be listed"
    );

    // BUG-9: a second create with the same name → 23505 pre-mapped to a
    // domain-specific Conflict message.
    let dup = create_collection(
        axum::extract::State(state.clone()),
        axum::extract::Json(CreateCollectionBody {
            name: NAME.into(),
            label: LABEL.into(),
            zim_names: vec![],
            is_favorite: false,
        }),
    )
    .await
    .expect_err("duplicate name must conflict");
    assert!(
        matches!(
            dup,
            zimservice::error::Error::Conflict(ref m) if m == "collection __itest_coll__ already exists"
        ),
        "expected a duplicate-name Conflict, got: {dup:?}"
    );

    // Delete → absent.
    let del = delete_collection(axum::extract::State(state.clone()), axum::extract::Path(id))
        .await
        .expect("delete");
    assert!(del.ok, "delete should report ok");
    let list2 = list_collections(axum::extract::State(state.clone()))
        .await
        .expect("list after delete");
    assert!(
        !list2.collections.iter().any(|c| c.id == id),
        "deleted collection should be absent"
    );
}
