// The 10k-article bench fixture builder — the SINGLE definition, included
// (not `mod`-linked) from two consumers:
//
// - `benches/common/mod.rs` (the criterion benches' `setup()`/`cleanup()`),
// - `tests/integration/search_10k.rs` (the DB-gated behavioral search test
//   at 10k rows).
//
// Why `include!` and not a normal module: a bench target cannot `use` code
// from `tests/` (test and bench are separate compilation units), and the
// third, natural home — `src/testing.rs` — is owned by the parallel review
// track, so the seam that keeps ONE definition without touching a third
// crate is a textually-included module. Both consumers live at a known
// relative distance from this file (`benches/common/` and
// `tests/integration/`), so the `include!` paths are stable.
//
// Regular `//` comments on purpose: `include!` inlines the text into a
// `mod` body, where a `//!` inner doc comment would be a hard parse error
// (E0753) — this file is not a standalone module.
//
// # Corpus shape
//
// One dedicated ZIM (`FIXTURE_ZIM`) with 10,000 deterministic articles
// (the `tests/integration/trgm_plan.rs` 100k corpus shape at 10k — the same
// 40-word grid from `zimservice::testing::trgm_corpus`):
//
// - `path`:   `A/bench_00000` … `A/bench_09999`
// - `title`:  `"{w1} {w2} {w3} entry {i}"` with `w1 = WORDS[i % 40]`,
//   `w2 = WORDS[(i / 40) % 40]`, `w3 = WORDS[(i / 1600) % 40]` — the probe
//   phrase `quixotic granite` occurs in exactly 7 titles
//   (`i ≡ 297 (mod 1600)`): `A/bench_00297`, `A/bench_01897`,
//   `A/bench_03497`, `A/bench_05097`, `A/bench_06697`, `A/bench_08297`,
//   `A/bench_09897`
// - `content_preview` / `snippet`: deterministic per-row text
// - `embedding`: a deterministic 1536-dim one-hot per 2,000-row batch —
//   the live `articles.embedding` column dimension is read from the
//   catalog (`atttypmod`, as-is — see AGENTS.md "Reading the vector
//   column"); every 10th row keeps `embedding = NULL`, a realistic
//   mid-embed tail for the embed-claim bench
//
// The bulk load reuses the PRODUCTION path: COPY into `articles_staging`
// followed by `zim::index::UPSERT_ARTICLES_FROM_STAGING_SQL`, so
// `search_vector` is built by the exact production tsvector expression.
// Seeding is idempotent — the fixture ZIM row (and its articles, via
// `ON DELETE CASCADE`) is dropped and recreated at the start of every
// seed, so re-runs measure identical data.

use zimservice::db::pool::Pool;
use zimservice::db::raw;
use zimservice::testing::trgm_corpus::{EMBED_BATCH, WORDS};
use zimservice::zim::index::UPSERT_ARTICLES_FROM_STAGING_SQL;

/// The fixture ZIM name — the shared DB footprint of the bench + the 10k
/// search test (clearly named; dropped by the next seed and by
/// `cleanup`-style deletes).
pub const FIXTURE_ZIM: &str = "bench_fixture";

/// Article count in the fixture. 10k keeps a full bench run in minutes;
/// `tests/integration/trgm_plan.rs` proves the same corpus shape at 100k.
pub const ROWS: usize = 10_000;

/// Read the live `articles.embedding` column dimension from the catalog.
/// `atttypmod` is used as-is (AGENTS.md: no `vector_dims()` translation —
/// the seed must bind what the column actually stores). A plain `vector`
/// column (no dimension, `atttypmod = -1`) is a hard seed failure — the
/// one-hot seeding has nothing to bind against.
async fn probe_embed_dim(pool: &Pool) -> Result<i32, String> {
    let dim: i32 = raw::fetch_scalar_optional::<i32, _, _>(
        pool,
        "SELECT atttypmod FROM pg_attribute \
         WHERE attrelid = 'articles'::regclass AND attname = 'embedding'",
        |q| q,
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "articles.embedding column missing".to_string())?;
    if dim <= 0 {
        return Err(format!(
            "articles.embedding has no fixed dimension (atttypmod={dim}) — \
             one-hot seeding needs a vector(N) column"
        ));
    }
    Ok(dim)
}

/// Seed (idempotently) the `bench_fixture` ZIM, 10k deterministic articles,
/// and batch one-hot embeddings. Returns the fixture zims id and the live
/// embedding dimension.
pub async fn seed_fixture(pool: &Pool) -> Result<(i32, i32), String> {
    // Idempotent drop (the cascade covers the previous fixture's articles).
    raw::execute(pool, "DELETE FROM zims WHERE name = $1", |q| {
        q.bind(FIXTURE_ZIM)
    })
    .await
    .map_err(|e| e.to_string())?;

    // Fresh zims row. `file_path` points nowhere: search never opens it.
    let zim_id: i32 = raw::fetch_scalar_optional(
        pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime, \
                 index_status, indexed_entries, article_count) \
         VALUES ($1, $2, $1, 0, now(), 'ready', $3, $3) RETURNING id",
        |q| q.bind(FIXTURE_ZIM).bind("Bench Fixture").bind(ROWS as i64),
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "fixture zims INSERT returned no id".to_string())?;

    // Bulk-load ROWS articles: COPY into `articles_staging` (one ~3 MB
    // payload, not 10k INSERTs) through the production staging path, then
    // the production upsert (`search_vector` built in Postgres by the exact
    // production tsvector expression).
    {
        let mut client = pool.acquire().await.map_err(|e| e.to_string())?;
        let mut copy = client
            .copy_in_raw(
                "COPY articles_staging (path, title, content_preview, snippet, language, \
                 namespace, zim_id) FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut buf = String::with_capacity(ROWS * 320);
        for i in 0..ROWS {
            let w1 = WORDS[i % 40];
            let w2 = WORDS[(i / 40) % 40];
            let w3 = WORDS[(i / 1600) % 40];
            let path = format!("A/bench_{i:05}");
            let title = format!("{w1} {w2} {w3} entry {i}");
            // ~300 chars of deterministic body text (weight-B in the
            // production tsvector expression; the probe words repeat so FTS
            // ranking has real signal).
            let preview = format!(
                "{w1} {w2} {w3} entry {i} body text. The {w2} beside the {w3} repeats for \
                 full-text matching and ranking. "
            );
            let snippet = format!("Snippet of {w1} {w2} {w3} entry {i}.");
            copy_escape(&mut buf, &path);
            buf.push('\t');
            copy_escape(&mut buf, &title);
            buf.push('\t');
            copy_escape(&mut buf, &preview);
            buf.push('\t');
            copy_escape(&mut buf, &snippet);
            buf.push_str("\ten\tC\t");
            buf.push_str(&zim_id.to_string());
            buf.push('\n');
        }
        copy.send(buf.as_bytes()).await.map_err(|e| e.to_string())?;
        copy.finish().await.map_err(|e| e.to_string())?;
        // Drop the client so the COPY connection returns to the pool before
        // the upsert takes its own checkout.
        drop(client);
    }

    raw::execute(pool, UPSERT_ARTICLES_FROM_STAGING_SQL, |q| q.bind(zim_id))
        .await
        .map_err(|e| e.to_string())?;
    raw::execute(
        pool,
        "DELETE FROM articles_staging WHERE zim_id = $1",
        |q| q.bind(zim_id),
    )
    .await
    .map_err(|e| e.to_string())?;

    // Deterministic embeddings: each EMBED_BATCH-row batch shares one
    // one-hot vector at the batch's index (0..4) — the ANN probe
    // (one-hot at coordinate 2) then has ~1800 distance-0 rows and the
    // HNSW index real rows to seek. Every 10th row (global `id % 10 = 0`)
    // is left NULL: the embed-claim bench's mid-embed tail (~1000 rows).
    let dim = probe_embed_dim(pool).await?;
    let (min_id, max_id): (i64, i64) = raw::fetch_optional::<(i64, i64), _, _>(
        pool,
        "SELECT min(id), max(id) FROM articles WHERE zim_id = $1",
        |q| q.bind(zim_id),
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "no fixture articles after upsert".to_string())?;

    let mut v = vec!["0"; dim as usize];
    for (batch, start) in (min_id..=max_id).step_by(EMBED_BATCH).enumerate() {
        let end = (start + EMBED_BATCH as i64 - 1).min(max_id);
        v.fill("0");
        v[batch % dim as usize] = "1";
        // RAW-OK: bench-only fixture seeding — dynamic dimension +
        // per-batch one-hot literal; no production equivalent.
        raw::execute(
            pool,
            "UPDATE articles SET embedding = $1::vector WHERE zim_id = $2 \
              AND id BETWEEN $3 AND $4 AND id % 10 <> 0",
            |q| {
                q.bind(format!("[{}]", v.join(",")))
                    .bind(zim_id)
                    .bind(start)
                    .bind(end)
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    }

    // Fresh planner statistics for the new 10k-row corpus (the shared dev
    // DB otherwise plans against whatever the last test run left).
    raw::execute(pool, "ANALYZE articles", |q| q)
        .await
        .map_err(|e| e.to_string())?;

    Ok((zim_id, dim))
}

/// Append `s` to `buf`, escaping backslashes/tabs/newlines/CRs for the
/// COPY text format (the fixture text never contains them, but the escape
/// keeps the payload honest if the corpus shape changes).
fn copy_escape(buf: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '\\' => buf.push_str("\\\\"),
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            c => buf.push(c),
        }
    }
}
