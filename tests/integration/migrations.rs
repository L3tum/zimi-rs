//! Migrations domain integration tests.

use super::common::*;

#[tokio::test]
async fn smoke_migrations_apply_idempotent() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    // ── Migrations: apply + idempotent re-run ────────────────────────────
    run_migrations(&pool).await.expect("first migration run");
    run_migrations(&pool)
        .await
        .expect("second run must be a no-op");

    {
        let c = pool.get().await.unwrap();
        let names: Vec<String> = c
            .query("SELECT name FROM schema_migrations ORDER BY name", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        assert!(names.contains(&"001_initial.sql".to_string()));
        assert!(names.contains(&"002_fixes.sql".to_string()));

        // 002 dropped search_history + qid_cache.
        let dropped: Vec<String> = c
            .query(
                "SELECT table_name FROM information_schema.tables
                 WHERE table_schema='public' AND table_name IN ('search_history','qid_cache')",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        assert!(
            dropped.is_empty(),
            "002 should have dropped these tables: {dropped:?}"
        );

        // 002 dropped the zims embed-progress columns; 004 dropped uuid.
        let cols: Vec<String> = c
            .query(
                "SELECT column_name FROM information_schema.columns WHERE table_name='zims'",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        assert!(!cols.contains(&"embed_status".to_string()));
        assert!(!cols.contains(&"embed_progress".to_string()));
        assert!(!cols.contains(&"uuid".to_string()));

        // 002 created the partial un-embedded index.
        let idx: i64 = c
            .query_one(
                "SELECT count(*) FROM pg_indexes WHERE indexname='idx_articles_unembedded_zim'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(idx, 1, "idx_articles_unembedded_zim missing");

        // 013 created the downloads (status, updated_at) composite index for
        // the poller's status-scoped scans (10-min error retry + per-tick
        // early-exit count). Pins the index's existence so a migration edit
        // that drops/renames it is caught.
        let idx13: i64 = c
            .query_one(
                "SELECT count(*) FROM pg_indexes WHERE indexname='idx_downloads_status_updated'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(idx13, 1, "idx_downloads_status_updated missing");
    }
}

#[tokio::test]
async fn smoke_migration_drift_detection() {
    let (shared, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    // Fully isolated: the hash tamper and the drift error both run in a
    // dedicated temporary database that is dropped at the end — the shared
    // dev DB's `schema_migrations` is never modified, so no restore is needed
    // and the suite no longer needs single-threaded serialization for this
    // test (the `trgm_index_perf_check` temp-DB pattern).
    let (pool, tmp_db) = match perf_temp_db(&shared).await {
        Some(p) => p,
        None => return,
    };

    // 001's hash must exist (idempotent no-op when already applied).
    run_migrations(&pool).await.expect("migrations");

    let c = pool.get().await.unwrap();

    // Tamper 001's recorded hash; the next `run_migrations` must refuse to run
    // (the migration body changed after it was applied).
    c.execute(
        "UPDATE schema_migrations SET hash = 'bogus' WHERE name = '001_initial.sql'",
        &[],
    )
    .await
    .unwrap();
    let err = run_migrations(&pool)
        .await
        .expect_err("tampered hash must be a startup error");
    assert!(
        format!("{err}").contains("modified after being applied"),
        "unexpected error: {err}"
    );

    perf_drop_temp_db(&shared, &tmp_db).await;
}

#[tokio::test]
async fn smoke_legacy_tracking_upgrade() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    // Legacy (version INTEGER) tracking → hard error (PONY-D2).
    // Skipped when the role cannot CREATE DATABASE. Never touches the dev DB:
    // the legacy table shape is created in a fresh database and the whole
    // experiment is dropped afterwards.
    {
        let c = pool.get().await.unwrap();
        let can_create: bool = c
            .query_one(
                "SELECT has_database_privilege(current_user, 'template1', 'CREATE')",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if !can_create {
            eprintln!("skipping legacy-upgrade check (role cannot CREATE DATABASE)");
        } else {
            let tmp_db = "zimservice_itest_legacy";
            // No bind parameter: Postgres forbids parameters in DROP DATABASE,
            // so the identifier is interpolated (a fixed test-DB name).
            let _ = c
                .execute(
                    &format!("DROP DATABASE IF EXISTS \"{tmp_db}\" WITH (FORCE)"),
                    &[],
                )
                .await;
            c.execute("CREATE DATABASE \"zimservice_itest_legacy\"", &[])
                .await
                .unwrap();

            let mut legacy_url = url::Url::parse(
                &std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.into()),
            )
            .unwrap();
            legacy_url.set_path(&format!("/{tmp_db}"));

            let mut lcfg = DpConfig::new();
            lcfg.url = Some(legacy_url.to_string());
            let lpool = lcfg
                .builder(tokio_postgres::NoTls)
                .unwrap()
                .max_size(2)
                .build()
                .unwrap();
            let lc = lpool.get().await.unwrap();

            // Legacy-shaped tracking table claiming migration 3 done.
            lc.batch_execute(
                "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY); INSERT INTO schema_migrations (version) VALUES (3);",
            )
            .await
            .unwrap();

            // PONY-D2: legacy (version INTEGER) schema must be a hard error,
            // not an auto-upgrade.
            let err = run_migrations(&lpool).await.expect_err(
                "legacy (version INTEGER) schema must be a hard error, not an auto-upgrade",
            );
            let msg = format!("{err}");
            assert!(
                msg.contains("legacy schema_migrations (version INTEGER)"),
                "expected legacy detection message, got: {msg}"
            );
            assert!(
                msg.contains("pg_dump"),
                "expected manual-migration recipe in message, got: {msg}"
            );

            // Detection fired before any migration: the `zims` table must not
            // exist.
            let zims_exists: bool = lc
                .query_one("SELECT to_regclass('zims') IS NOT NULL", &[])
                .await
                .unwrap()
                .get(0);
            assert!(
                !zims_exists,
                "no migration should have been applied on a legacy schema"
            );

            drop(lc);
            drop(lpool);
            let _ = c
                .execute(
                    &format!("DROP DATABASE IF EXISTS \"{tmp_db}\" WITH (FORCE)"),
                    &[],
                )
                .await;
        }
    }
}

#[ignore]
#[tokio::test]
async fn trgm_index_perf_check() {
    let (shared, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    // Fully isolated: the 100k-row seed and `ANALYZE` run in a dedicated
    // temporary database that is dropped below — the shared dev DB is
    // neither seeded nor `ANALYZE`d by this test.
    let (pool, tmp_db) = match perf_temp_db(&shared).await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    const R: &str = "__itrge__"; // dedicated fixture ZIM for the EXPLAIN run
    let c = pool.get().await.unwrap();
    c.execute("DELETE FROM zims WHERE name = $1", &[&R])
        .await
        .unwrap();
    c.execute(
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 100000, 100000)",
        &[&R],
    )
    .await
    .unwrap();
    // 100k rows in one statement.
    c.batch_execute(&format!(
        "INSERT INTO articles (zim_id, path, title, content_preview, search_vector)
         SELECT (SELECT id FROM zims WHERE name='{R}'),
                'A/Art_' || g, 'Art ' || g, 'row ' || g,
                to_tsvector('simple', 'article ' || g)
         FROM generate_series(1, 100000) AS g"
    ))
    .await
    .unwrap();

    // Fresh planner statistics so EXPLAIN reflects the 100k-row scale.
    c.batch_execute("ANALYZE articles; ANALYZE zims")
        .await
        .unwrap();

    let mut all_lines: Vec<String> = Vec::new();

    // Q1: btree prefix  (WHERE title_lower LIKE 'q%')
    let q1: Vec<String> = c
        .query(
            "EXPLAIN (ANALYZE, BUFFERS)
             SELECT a.id, a.zim_id, a.path, a.title, a.snippet, a.content_preview, a.language, z.name as zim_name,
                    GREATEST(similarity(a.title_lower, $1), 0.0) as score
             FROM articles a JOIN zims z ON z.id = a.zim_id
             WHERE a.title_lower LIKE $2 ESCAPE '\\'
             ORDER BY score DESC LIMIT 20",
            &[&"art 42", &"art 42%"],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    eprintln!("\n── C12 Q1 prefix (btree, 100k rows) ────");
    for line in &q1 {
        eprintln!("{line}");
        all_lines.push(line.clone());
    }

    // Q2: GIN contains  (WHERE title_lower LIKE '%q%')
    let q2: Vec<String> = c
        .query(
            "EXPLAIN (ANALYZE, BUFFERS)
             SELECT a.id, a.zim_id, a.path, a.title, a.snippet, a.content_preview, a.language, z.name as zim_name,
                    GREATEST(similarity(a.title_lower, $1), 0.0) as score
             FROM articles a JOIN zims z ON z.id = a.zim_id
             WHERE a.title_lower LIKE $2 ESCAPE '\\'
             ORDER BY score DESC LIMIT 20",
            &[&"art 42", &"%art 42%"],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    eprintln!("\n── C12 Q2 contains (GIN, 100k rows) ────");
    for line in &q2 {
        eprintln!("{line}");
        all_lines.push(line.clone());
    }

    // Q3: GiST similarity  (production shape post-BUG-1: `%` + typed
    // threshold conjunct — `similarity() > text` had no operator and the arm
    // soft-failed dead). The `%` conjunct is the index-usable part; pin the
    // GUC so the `%` pre-scan threshold equals the conjunct's.
    c.batch_execute("SET pg_trgm.similarity_threshold = 0.3")
        .await
        .unwrap();
    let q3: Vec<String> = c
        .query(
            "EXPLAIN (ANALYZE, BUFFERS)
             SELECT a.id, a.zim_id, a.path, a.title, a.snippet, a.content_preview, a.language, z.name as zim_name,
                    GREATEST(similarity(a.title_lower, $1), 0.0) as score
             FROM articles a JOIN zims z ON z.id = a.zim_id
             WHERE a.title_lower % $1 AND similarity(a.title_lower, $1) > $2::float8
             ORDER BY score DESC LIMIT 20",
            &[&"art 42", &0.3f64],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    eprintln!("\n── C12 Q3 similarity (GiST, 100k rows) ────");
    for line in &q3 {
        eprintln!("{line}");
        all_lines.push(line.clone());
    }

    // ── Suggest split (B2.2) ─────────────────────────────────────────────
    // `suggest()` no longer runs one OR query; it runs the same three
    // index-friendly shapes as search (weight 1.0 = raw-similarity score).
    // The prefix arm below pins that shape: the `1.0 * GREATEST(...)` score
    // must still take the btree path; the other two arms are shape-identical
    // to Q2/Q3 above.
    let q4: Vec<String> = c
        .query(
            "EXPLAIN (ANALYZE, BUFFERS)
             SELECT a.id, a.zim_id, a.path, a.title, a.snippet, a.content_preview, a.language, z.name as zim_name,
                    1.0 * GREATEST(similarity(a.title_lower, $1), 0.0) as score
             FROM articles a JOIN zims z ON z.id = a.zim_id
             WHERE a.title_lower LIKE $2 ESCAPE '\\'
             ORDER BY score DESC LIMIT 20",
            &[&"art 42", &"art 42%"],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    eprintln!("\n── C12 SUG prefix arm (btree, 100k rows) ────");
    for line in &q4 {
        eprintln!("{line}");
        all_lines.push(line.clone());
    }

    // Q5 (PERF-4 / WI-36 decision): a *pure* `ORDER BY title_lower ASC LIMIT`
    // shape — the one query that can legitimately ride the btree
    // `idx_articles_title_prefix` for **ordering** (the Q1 prefix predicate is
    // the index-usable part, but `ORDER BY score` forces a top-N sort anyway).
    // Collected separately from `all_lines` and **not** subject to the
    // no-seq-scan assertion below: whether the planner picks the btree or a
    // seq-scan+top-N here is exactly what the keep/drop decision in
    // `docs/perf-notes.md` weighs, and a seq-scan for this shape is a legal
    // planner choice, not a regression.
    let q5: Vec<String> = c
        .query(
            "EXPLAIN (ANALYZE, BUFFERS)
             SELECT a.id, a.zim_id, a.path, a.title, a.snippet, a.content_preview, a.language, z.name as zim_name,
                    a.title_lower
             FROM articles a JOIN zims z ON z.id = a.zim_id
             ORDER BY a.title_lower ASC LIMIT 20",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    eprintln!("\n── C12 Q5 title_lower ORDER BY (btree candidate, 100k rows) ────");
    for line in &q5 {
        eprintln!("{line}");
    }
    eprintln!("─────────────────────────────────");

    // DEC-5 assertion: none of the split queries (search or suggest) may
    // fall back to a
    // sequential scan over `articles` at this scale — that is the regression
    // the split exists to prevent.
    // Reset the per-connection GUC and drop the temp DB **before** the
    // assertions: if an assert panics, the pooled conn is returned without
    // the stale threshold, the shared dev DB was never touched, and the
    // temp DB is swept by the next run (B5.8 idiom; dropping the whole DB
    // subsumes the fixture cleanup).
    c.execute("RESET pg_trgm.similarity_threshold", &[])
        .await
        .unwrap();
    drop(c);
    drop(pool);
    perf_drop_temp_db(&shared, &tmp_db).await;

    for line in &all_lines {
        assert!(
            !line.contains("Seq Scan on articles"),
            "a split trgm query hit a sequential scan over articles:\n  {line}\n\nfull plans:\n{}",
            all_lines.join("\n")
        );
    }
}

/// TEST-8 (WI-52) — 10k-row PR canary for trgm search perf. A 1/10-scale
/// mirror of the `#[ignore]`d push-only `trgm_index_perf_check`: three search
/// arms (fts / trgm-prefix / trgm-similarity) at 10k rows, each measured
/// against a 2 s wall-clock budget, with the two trgm shapes additionally
/// plan-checked (no `Seq Scan on articles`). Runs on every PR — the name must
/// NOT contain the substring `trgm_index_perf_check` (the CI PR gate's
/// `--skip trgm_index_perf_check` would otherwise silently filter this out).
#[tokio::test]
async fn trgm_index_perf_canary() {
    let (shared, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    // Fully isolated: the 10k-row seed and `ANALYZE` run in a dedicated
    // temporary database that is dropped below — the shared dev DB is
    // neither seeded nor `ANALYZE`d by this test (it runs on every PR).
    let (pool, tmp_db) = match perf_temp_db(&shared).await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    const R: &str = "__itrgc__"; // dedicated fixture ZIM for the canary run
    let c = pool.get().await.unwrap();
    c.execute("DELETE FROM zims WHERE name = $1", &[&R])
        .await
        .unwrap();
    c.execute(
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                           index_status, indexed_entries, article_count)
         VALUES ($1, $1, $1, 0, now(), 'ready', 10000, 10000)",
        &[&R],
    )
    .await
    .unwrap();
    // 10k rows in one statement (the 100k test's insert, count 100_000 →
    // 10_000; `title_lower` values are 5+ chars so all three arms are
    // index-eligible).
    c.batch_execute(&format!(
        "INSERT INTO articles (zim_id, path, title, content_preview, search_vector)
         SELECT (SELECT id FROM zims WHERE name='{R}'),
                'A/Art_' || g, 'Art ' || g, 'row ' || g,
                to_tsvector('simple', 'article ' || g)
         FROM generate_series(1, 10000) AS g"
    ))
    .await
    .unwrap();

    // Fresh planner statistics so EXPLAIN reflects the 10k-row scale. Safe:
    // this test runs entirely in a dedicated temporary database, so
    // `ANALYZE` only rewrites statistics there — the shared dev DB's planner
    // statistics are never touched.
    c.batch_execute("ANALYZE articles; ANALYZE zims")
        .await
        .unwrap();

    const BUDGET: Duration = Duration::from_secs(2);
    let mut fts_rows: i64 = 0;
    let mut timings: Vec<(&str, Duration)> = Vec::new();
    let mut plan_lines: Vec<String> = Vec::new();

    // The wall-clock budget is a soft signal and a real flake vector on
    // slow/loaded runners; the *deterministic* guard is the plan-shape check
    // below (no `Seq Scan on articles`), asserted strictly against the final
    // attempt. So if a timed pass exceeds the budget we re-run the three timed
    // arms once before failing (Tests Major #3) rather than flaking a clean
    // index.
    'attempts: for _ in 0..2 {
        timings.clear();
        plan_lines.clear();

        // Arm 1 — fts (production shape: the seeded GIN column + websearch
        // query). Wall-clock budget only: this arm's plan is index-driven, not
        // trgm-shaped, so it is not subject to the no-seq-scan plan check.
        let started = Instant::now();
        fts_rows = c
            .query_one(
                "SELECT count(*)
                 FROM articles a JOIN zims z ON z.id = a.zim_id
                 WHERE a.search_vector @@ websearch_to_tsquery('simple', $1)",
                &[&"article 42"],
            )
            .await
            .unwrap()
            .get(0);
        timings.push(("fts", started.elapsed()));

        // Arm 2 — trgm-prefix (btree `title_lower LIKE 'q%'`; the 100k test's
        // Q1 production shape). `EXPLAIN (ANALYZE, BUFFERS)` measures AND
        // plan-checks in one call.
        let started = Instant::now();
        let prefix_plan: Vec<String> = c
            .query(
                "EXPLAIN (ANALYZE, BUFFERS)
                 SELECT a.id, a.zim_id, a.path, a.title, a.snippet, a.content_preview, a.language, z.name as zim_name,
                        GREATEST(similarity(a.title_lower, $1), 0.0) as score
                 FROM articles a JOIN zims z ON z.id = a.zim_id
                 WHERE a.title_lower LIKE $2 ESCAPE '\\'
                 ORDER BY score DESC LIMIT 20",
                &[&"art 42", &"art 42%"],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        timings.push(("trgm-prefix", started.elapsed()));
        eprintln!("\n── CANARY trgm-prefix (btree, 10k rows) ────");
        for line in &prefix_plan {
            eprintln!("{line}");
            plan_lines.push(line.clone());
        }

        // Arm 3 — trgm-similarity (production shape post-BUG-1: `%` + typed
        // threshold conjunct). Pin the GUC so the `%` pre-scan threshold
        // equals the conjunct's, same as the 100k test's Q3.
        c.batch_execute("SET pg_trgm.similarity_threshold = 0.3")
            .await
            .unwrap();
        let started = Instant::now();
        let sim_plan: Vec<String> = c
            .query(
                "EXPLAIN (ANALYZE, BUFFERS)
                 SELECT a.id, a.zim_id, a.path, a.title, a.snippet, a.content_preview, a.language, z.name as zim_name,
                        GREATEST(similarity(a.title_lower, $1), 0.0) as score
                 FROM articles a JOIN zims z ON z.id = a.zim_id
                 WHERE a.title_lower % $1 AND similarity(a.title_lower, $1) > $2::float8
                 ORDER BY score DESC LIMIT 20",
                &[&"art 42", &0.3f64],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        timings.push(("trgm-similarity", started.elapsed()));
        eprintln!("\n── CANARY trgm-similarity (GiST, 10k rows) ────");
        for line in &sim_plan {
            eprintln!("{line}");
            plan_lines.push(line.clone());
        }
        eprintln!("─────────────────────────────────");

        // All arms within budget → done; otherwise loop once (the retry).
        if timings.iter().all(|(_, e)| *e <= BUDGET) {
            break 'attempts;
        }
        eprintln!("canary: timed pass exceeded {BUDGET:?} for one or more arms — re-running once");
    }

    // Reset the per-connection GUC and drop the temp DB **before** the
    // assertions (the 100k test's B5.8 idiom): if an assert panics, the
    // pooled conn is returned without the stale threshold, the shared dev DB
    // was never touched, and the temp DB is swept by the next run (dropping
    // the whole DB subsumes the fixture cleanup).
    c.execute("RESET pg_trgm.similarity_threshold", &[])
        .await
        .unwrap();
    drop(c);
    drop(pool);
    perf_drop_temp_db(&shared, &tmp_db).await;

    assert_eq!(
        fts_rows, 1,
        "fts arm must match exactly the seeded 'article 42' row at 10k scale"
    );
    // Deterministic guard: plan shape (no sequential scan) — strict, no retry.
    for line in &plan_lines {
        assert!(
            !line.contains("Seq Scan on articles"),
            "a canary trgm arm hit a sequential scan over articles:\n  {line}\n\nfull plans:\n{}",
            plan_lines.join("\n")
        );
    }
    // Soft signal: the wall-clock budget, asserted only after the single retry.
    for (name, elapsed) in &timings {
        assert!(
            *elapsed <= BUDGET,
            "canary arm '{name}' took {elapsed:?} at 10k rows (budget {BUDGET:?}) even after one retry — trgm index regressed?"
        );
    }
}

/// P0 — migration 010: the embedding index is PARTIAL (`WHERE embedding IS NOT
/// NULL`) after applying, a legacy non-partial shape is swapped out, and
/// `idx_downloads_created_at` exists. Re-running migrations is a no-op.
#[tokio::test]
async fn migration_010_embedding_index_shape() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };

    // Force the legacy NON-partial shape first (simulating an older runtime's
    // index) so this run exercises the swap path: the DO block must drop it
    // and recreate the partial one. (A non-partial HNSW index over all rows
    // is only buildable while the articles table holds no NULL embeddings, so
    // it may not be creatable on a dev DB with embedded rows — in that case
    // fall through and assert the end state only.)
    let c = pool.get().await.expect("conn");
    let legacy_exists: bool = c
        .query_opt(
            "SELECT EXISTS (SELECT 1 FROM pg_index i
                JOIN pg_class cl ON cl.oid = i.indexrelid
                WHERE cl.relname = 'idx_articles_embedding' AND i.indpred IS NULL)",
            &[],
        )
        .await
        .expect("query legacy")
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);
    if !legacy_exists {
        // Drop any partial index, then try to rebuild the legacy shape. A
        // failure here just means the swap path can't be simulated on this
        // DB (NULL embeddings present) — the migration still converges to
        // the partial shape, which is what we assert below.
        if c.query_opt(
            "SELECT EXISTS (SELECT 1 FROM pg_index i
                    JOIN pg_class cl ON cl.oid = i.indexrelid
                    WHERE cl.relname = 'idx_articles_embedding')",
            &[],
        )
        .await
        .expect("query any")
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false)
        {
            c.batch_execute("DROP INDEX idx_articles_embedding")
                .await
                .expect("drop index");
        }
        let _ = c
            .batch_execute(
                "CREATE INDEX idx_articles_embedding
                 ON articles USING hnsw (embedding vector_cosine_ops)",
            )
            .await;
    }

    run_migrations(&pool).await.expect("migrations");

    // (a) End state: partial HNSW index present, downloads sort index present.
    let c = pool.get().await.expect("conn");
    let partial: Option<bool> = c
        .query_opt(
            "SELECT indpred IS NOT NULL FROM pg_index i
             JOIN pg_class cl ON cl.oid = i.indexrelid
             WHERE cl.relname = 'idx_articles_embedding'",
            &[],
        )
        .await
        .expect("query partial")
        .map(|r| r.get::<_, bool>(0));
    assert!(
        partial == Some(true),
        "idx_articles_embedding should exist and be partial after 010"
    );
    let dl: bool = c
        .query_opt(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_downloads_created_at')",
            &[],
        )
        .await
        .expect("query dl index")
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);
    assert!(dl, "idx_downloads_created_at should exist after 010");

    // (b) Re-run is a no-op (hash stable, shape unchanged).
    run_migrations(&pool)
        .await
        .expect("second run must be a no-op");
    let c = pool.get().await.expect("conn");
    let partial2: Option<bool> = c
        .query_opt(
            "SELECT indpred IS NOT NULL FROM pg_index i
             JOIN pg_class cl ON cl.oid = i.indexrelid
             WHERE cl.relname = 'idx_articles_embedding'",
            &[],
        )
        .await
        .expect("query partial again")
        .map(|r| r.get::<_, bool>(0));
    assert!(
        partial2 == Some(true),
        "index should still be partial after re-run"
    );

    // (c) ≥1M branch: cannot seed 1M vectors in an integration test. Covered
    // logically — the DO block skips the inline create at n_vectors >= 1_000_000
    // (leaving the drop applied) so the runtime's ivfflat branch of
    // maybe_build_vector_index takes over. See the plan's scope note.
}
