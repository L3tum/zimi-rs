//! T3 (2026-09 review): sustained-concurrency soak of the three-pool split
//! (`db` / `db_read` / `db_bg` — see `AppState`'s field docs and
//! `src/db/pool.rs` `create_pool` / `create_read_pool` / `create_bg_pool`).
//!
//! DB-gated like every sibling module: `pool_or_skip()` is the sole entry
//! point (cross-process `DbExclusiveGuard` + clean skip when no database
//! is reachable).

use super::common::*;

/// (T3, 2026-09 review): the pool-split design under SUSTAINED
/// concurrency — nothing in the suite exercised the split before this.
///
/// The three pools are built exactly as production does: one `Config`
/// drives `create_pool` / `create_read_pool` / `create_bg_pool`
/// (mirroring `startup::build_state`, which calls the same three
/// builders in sequence). The read URL is the same primary DSN — no
/// replica exists in the test environment, and the property under test
/// is the split + queuing, not replication. Sizes stay small
/// (primary/read 6, bg pinned at `BG_POOL_MAX_CONNECTIONS` = 4) so that
/// 50 concurrent tasks per wave must genuinely queue on `acquire`; the
/// 10 s acquire timeout (`build_pool`, fast 503 instead of sqlx's 30 s
/// default) is the bound the queue may not exceed.
///
/// Each task round-robins the three production roles: a foreground read
/// through `db_read_or_primary()` (the replica pool when set, else
/// primary), a background read via `db_bg`, and a write via the primary
/// `db` pool (all writes stay on one pool by design — the scratch table
/// is the real write target, created + dropped by this test under the
/// guard's serialization).
///
/// Property pinned: bounded-concurrency sustained load across the split
/// pools completes WITHOUT pool-exhaustion/timeout errors and WITHOUT
/// losing writes. Deliberately NO wall-clock assertion — CI runners
/// vary, and a time bound would be flaky.
#[tokio::test]
async fn pool_split_sustained_concurrency_completes_without_exhaustion() {
    // The suite pool + cross-process guard (the sole entry point for
    // DB-gated tests); it also fixes the DSN the split is built on.
    let (suite_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&suite_pool).await.expect("migrations");
    let dsn = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    // The three-pool split, built exactly as `startup::build_state` does:
    // one `Config` → the three `db::pool::create_*` builders.
    let config = zimservice::config::Config {
        database_url: dsn.clone(),
        read_database_url: Some(dsn),
        db_pool_size: 6,
        ..Default::default()
    };
    let db = zimservice::db::pool::create_pool(&config)
        .await
        .expect("primary pool");
    let db_read = zimservice::db::pool::create_read_pool(&config)
        .await
        .expect("read pool (same validation path as the primary)")
        .expect("read pool present (read_database_url set)");
    let db_bg = zimservice::db::pool::create_bg_pool(&config)
        .await
        .expect("bg pool");

    // Scratch write target, scoped to this test. The guard serializes us
    // with every other DB test; the leading `DROP IF EXISTS` keeps reruns
    // idempotent after a failed run.
    const TABLE: &str = "__itest_pool_soak__";
    zimservice::db::raw::execute(&db, &format!("DROP TABLE IF EXISTS {TABLE}"), |q| q)
        .await
        .unwrap();
    zimservice::db::raw::execute(
        &db,
        &format!("CREATE TABLE {TABLE} (v text NOT NULL)"),
        |q| q,
    )
    .await
    .unwrap();

    // AppState the way `common.rs` does, then swap in the split's pools
    // (that harness's `db_read: None` / `db_bg: db.clone()` stand-ins are
    // exactly what production's split replaces).
    let mut state = live_state(db.clone()).await;
    state.db_read = Some(db_read);
    state.db_bg = db_bg;

    const WAVES: usize = 4;
    const TASKS: usize = 50;
    /// One round-robin pass = 3 ops: fg read, bg read, and exactly ONE
    /// write (the `op % 3 == 2` arm).
    const OPS_PER_TASK: usize = 3;
    let expected_writes = WAVES * TASKS; // one write op per task per wave

    for wave in 0..WAVES {
        let handles: Vec<_> = (0..TASKS)
            .map(|task| {
                let state = state.clone();
                tokio::spawn(async move {
                    for op in 0..OPS_PER_TASK {
                        match op % 3 {
                            // Foreground read: the replica pool when set
                            // (always, here), else the primary.
                            0 => {
                                let one: i32 = zimservice::db::raw::fetch_scalar_optional(
                                    state.db_read_or_primary(),
                                    "SELECT 1",
                                    |q| q,
                                )
                                .await?
                                .expect("SELECT 1 returns one row");
                                assert_eq!(one, 1);
                            }
                            // Background read: the dedicated bg pool.
                            1 => {
                                let one: i32 = zimservice::db::raw::fetch_scalar_optional(
                                    &state.db_bg,
                                    "SELECT 1",
                                    |q| q,
                                )
                                .await?
                                .expect("SELECT 1 returns one row");
                                assert_eq!(one, 1);
                            }
                            // Write: the primary pool — a real row in the
                            // scratch table, so the write pool is
                            // exercised for real and loss is assertable.
                            _ => {
                                zimservice::db::raw::execute(
                                    &state.db,
                                    &format!("INSERT INTO {TABLE} (v) VALUES ($1)"),
                                    |q| q.bind(format!("w{wave}-t{task}-o{op}")),
                                )
                                .await?;
                            }
                        }
                    }
                    Result::<(), zimservice::error::Error>::Ok(())
                })
            })
            .collect();
        for h in handles {
            // A pool-acquire timeout (the 10 s `build_pool` bound) or any
            // DB error surfaces here — the exhaustion failure mode this
            // test pins.
            h.await
                .expect("soak task must not panic")
                .expect("soak task completed without a pool exhaustion/timeout error");
        }
    }

    // No writes lost: the primary pool must hold every row.
    let count: i64 = zimservice::db::raw::fetch_scalar_optional(
        &state.db,
        &format!("SELECT COUNT(*) FROM {TABLE}"),
        |q| q,
    )
    .await
    .expect("count must run")
    .expect("count row present");
    assert_eq!(count, expected_writes as i64, "no writes may be lost");

    // Cleanup + prove the scratch table is actually gone.
    zimservice::db::raw::execute(&state.db, &format!("DROP TABLE {TABLE}"), |q| q)
        .await
        .unwrap();
    let gone: bool = zimservice::db::raw::fetch_scalar_optional(
        &state.db,
        &format!("SELECT to_regclass('{TABLE}') IS NULL"),
        |q| q,
    )
    .await
    .expect("regclass probe must run")
    .expect("regclass row present");
    assert!(gone, "scratch table must be dropped");
}
