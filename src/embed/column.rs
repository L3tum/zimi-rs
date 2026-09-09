//! `articles.embedding` column dimension reconciliation (pgvector typmod).

use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::{Error, Result};

/// Probe the stored dimension of the `articles.embedding` column
/// (pgvector stores the dimension as typmod - VARHDRSZ (4)), or `None` when
/// the column does not exist. Factored out of [`ensure_vector_dimension`]
/// so the pipeline's fail-fast dimension check reuses the same probe.
///
/// pg_catalog probe (the generic helpers still serve it via db::raw).
pub(crate) async fn stored_embedding_dimension(pool: &Pool) -> Result<Option<u32>> {
    let typmod: Option<i32> = raw::fetch_scalar_optional(
        pool,
        "SELECT atttypmod FROM pg_attribute
         WHERE attrelid = 'articles'::regclass AND attname = 'embedding'",
        |q| q,
    )
    .await?;
    Ok(typmod.map(|t| t.saturating_sub(4) as u32))
}

/// Reconcile the `articles.embedding` column dimension with the configured
/// model dimension. pgvector columns have a fixed dimension (`vector(N)`),
/// so switching models of a different size requires an ALTER.
///
/// The alter is only performed when no vectors are stored yet — otherwise the
/// data belongs to a different model and would be silently destroyed, so we
/// warn instead (the caller, `run_pipeline`, then fails fast so the
/// mismatch can never poison a ZIM's rows).
pub async fn ensure_vector_dimension(pool: &Pool, dimension: u32) -> Result<()> {
    let Some(current) = stored_embedding_dimension(pool).await? else {
        return Err(Error::NotFound(
            "articles.embedding column not found".into(),
        ));
    };
    if current == dimension {
        return Ok(());
    }

    let stored: i64 = raw::fetch_scalar_optional(
        pool,
        "SELECT COUNT(*) FROM articles WHERE embedding IS NOT NULL",
        |q| q,
    )
    .await?
    .unwrap_or(0);

    if stored > 0 {
        tracing::warn!(
            "articles.embedding is vector({current}) but configured dimension is {dimension}; \
             {stored} existing vectors kept — run `zimservice embed` after clearing them if you want to switch models"
        );
        return Ok(());
    }

    tracing::info!("altering articles.embedding from vector({current}) to vector({dimension})");
    // Column-type DDL (`ALTER TABLE … TYPE vector(N)`), raw SQL.
    let sql = format!("ALTER TABLE articles ALTER COLUMN embedding TYPE vector({dimension})");
    raw::execute(pool, &sql, |q| q).await?;
    Ok(())
}
