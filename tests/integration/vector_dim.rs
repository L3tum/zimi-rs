//! The `articles.embedding` pgvector dimension reconciliation guard
//! (`src/embed/column.rs`) — the only code path that may `ALTER` the column's
//! dimension. `AGENTS.md` records a past bug in exactly this probe (reading
//! `atttypmod - 4` yielded dim 1520 instead of 1536 and broke inserts), so
//! the probe and the data-loss guard (never ALTER while vectors are stored)
//! are pinned directly here.
//!
//! DB-gated like every test here: temp DB (migrated fresh), dropped after.

use super::common::{pool_or_skip, skip_midtest};
use super::migrations::close_and_drop;
use zimservice::db::raw;
use zimservice::embed::column::{ensure_vector_dimension, stored_embedding_dimension};
use zimservice::error::Error;

/// A one-hot vector literal of `dim` dimensions (deterministic, cheap to
/// build in Rust — mirrors `trgm_plan`'s embedding population style).
fn one_hot(dim: usize) -> String {
    let body = (0..dim)
        .map(|i| if i == 0 { "1" } else { "0" })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{body}]")
}

/// The atttypmod probe reads the stored dimension *directly* (no header
/// offset — the documented past bug read `atttypmod - 4`).
#[tokio::test]
async fn smoke_embedding_dimension_probe_and_alter_guard() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match super::migrations::create_temp_db(&base_pool).await {
        Some(t) => t,
        None => {
            skip_midtest("base user cannot CREATE DATABASE");
            return;
        }
    };
    zimservice::db::migrate::run_migrations(&pool)
        .await
        .expect("fresh apply (temp DB must migrate cleanly)");

    // 001 creates the column as VECTOR(1536).
    assert_eq!(
        stored_embedding_dimension(&pool).await.expect("probe"),
        Some(1536),
        "001's vector(1536) column must probe as dimension 1536 (atttypmod \
         IS the dimension — no header offset; AGENTS.md records the -4 bug)"
    );

    // Same dimension → no-op (no ALTER, column untouched).
    ensure_vector_dimension(&pool, 1536)
        .await
        .expect("matching dimension must be a no-op");
    assert_eq!(
        stored_embedding_dimension(&pool).await.expect("probe"),
        Some(1536)
    );

    // One stored vector: switching models must NOT alter (data-loss guard) —
    // the warn path keeps the column and the rows.
    // articles.zim_id is an FK: seed the fixture ZIM first.
    raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime, \
         index_status, indexed_entries, article_count) \
         VALUES ('__vec_dim__', '__vec_dim__', '__vec_dim__', 0, now(), 'ready', 1, 1)",
        |q| q,
    )
    .await
    .expect("fixture zims row");
    let zim_id: i32 = raw::fetch_scalar_optional(
        &pool,
        "SELECT id FROM zims WHERE name = '__vec_dim__'",
        |q| q,
    )
    .await
    .expect("fixture zims id query")
    .expect("fixture zims id");
    let stored: i64 = raw::fetch_scalar_optional(
        &pool,
        "INSERT INTO articles (path, title, zim_id, search_vector, embedding) \
         VALUES ('A/x', 'x', $1, ''::tsvector, $2::vector) RETURNING id",
        |q| q.bind(zim_id).bind(one_hot(1536)),
    )
    .await
    .expect("seed a stored vector")
    .expect("seed row id");
    assert!(stored > 0, "seed row must exist");
    ensure_vector_dimension(&pool, 8)
        .await
        .expect("mismatch with stored vectors must warn, not error");
    assert_eq!(
        stored_embedding_dimension(&pool).await.expect("probe"),
        Some(1536),
        "the guard must NOT alter while vectors are stored"
    );

    // Vectors cleared: the alter now proceeds.
    raw::execute(&pool, "UPDATE articles SET embedding = NULL", |q| q)
        .await
        .expect("clear vectors");
    ensure_vector_dimension(&pool, 8)
        .await
        .expect("mismatch with no stored vectors must alter");
    assert_eq!(
        stored_embedding_dimension(&pool).await.expect("probe"),
        Some(8),
        "after the guarded alter the column must be vector(8)"
    );

    // Column absent: the probe returns None and the reconcile is NotFound.
    raw::execute(&pool, "ALTER TABLE articles DROP COLUMN embedding", |q| q)
        .await
        .expect("drop the column");
    assert_eq!(
        stored_embedding_dimension(&pool).await.expect("probe"),
        None,
        "dropped column must probe as None (not 0, not -4 garbage)"
    );
    let err = ensure_vector_dimension(&pool, 8)
        .await
        .expect_err("absent column must be NotFound");
    assert!(
        matches!(err, Error::NotFound(_)),
        "absent column must surface as NotFound, got {err:?}"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}
