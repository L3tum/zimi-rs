//! Wikidata Q-ID lookup for cross-language article discovery.
//!
//! Resolves the `(zim, path)` Q-ID and the articles that share it, as the JSON
//! served by `GET /interlanguage` and the MCP `article_languages` tool.
use tokio_postgres::Row;

use crate::db::pool::Pool;
use crate::error::{Error, Result};

/// Look up the Wikidata Q-ID for `(zim, path)` and the cross-language articles
/// that share it, as the JSON object served by `GET /interlanguage` and the MCP
/// `article_languages` tool: `{"qid": "Q<n>" | null, "languages": [...]}`.
///
/// Shared by both call sites so the QID / interlanguage SQL lives in one place
/// (previously duplicated verbatim in `serve/handlers.rs` and `mcp/mod.rs`).
pub async fn interlanguage_json(pool: &Pool, zim: &str, path: &str) -> Result<serde_json::Value> {
    let client = pool.get().await.map_err(Error::Pool)?;
    let qid_row = client
        .query_opt(
            "SELECT qid FROM qid_index \
             WHERE zim_id = (SELECT id FROM zims WHERE name = $1) AND path = $2",
            &[&zim, &path],
        )
        .await
        .map_err(Error::Database)?;
    let Some(qid_row) = qid_row else {
        return Ok(serde_json::json!({ "qid": null, "languages": [] }));
    };
    let qid: i64 = qid_row.get(0);
    let rows: Vec<Row> = client
        .query(
            "SELECT z.name, q.path, a.title FROM qid_index q \
             JOIN zims z ON z.id = q.zim_id \
             LEFT JOIN articles a ON a.zim_id = q.zim_id AND a.path = q.path \
             WHERE q.qid = $1 AND z.name != $2 \
             ORDER BY z.language",
            &[&qid, &zim],
        )
        .await
        .map_err(Error::Database)?;
    let languages: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "zim": r.get::<_, String>(0),
                "path": r.get::<_, String>(1),
                "title": r.get::<_, Option<String>>(2),
            })
        })
        .collect();
    Ok(serde_json::json!({ "qid": format!("Q{qid}"), "languages": languages }))
}
