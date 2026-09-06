//! Wikidata Q-ID lookup for cross-language article discovery.
//!
//! Resolves the `(zim, path)` Q-ID and the articles that share it, as the JSON
//! served by `GET /interlanguage` and the MCP `article_languages` tool.
use crate::db::pool::Pool;
use crate::db::raw;
use crate::error::Result;

/// Look up the Wikidata Q-ID for `(zim, path)` and the cross-language articles
/// that share it, as the JSON object served by `GET /interlanguage` and the MCP
/// `article_languages` tool: `{"qid": "Q<n>" | null, "languages": [...]}`.
///
/// Shared by both call sites so the QID / interlanguage SQL lives in one place
/// (previously duplicated verbatim in `serve/handlers.rs` and `mcp/mod.rs`).
pub async fn interlanguage_json(pool: &Pool, zim: &str, path: &str) -> Result<serde_json::Value> {
    let qid: Option<i64> = raw::fetch_scalar_optional::<Option<i64>, _, _>(
        pool,
        "SELECT qid FROM qid_index \
         WHERE zim_id = (SELECT id FROM zims WHERE name = $1) AND path = $2",
        |q| q.bind(zim).bind(path),
    )
    .await?
    .flatten();
    let Some(qid) = qid else {
        return Ok(serde_json::json!({ "qid": null, "languages": [] }));
    };
    let rows: Vec<(String, String, Option<String>)> = raw::fetch_all(
        pool,
        "SELECT z.name, q.path, a.title FROM qid_index q \
         JOIN zims z ON z.id = q.zim_id \
         LEFT JOIN articles a ON a.zim_id = q.zim_id AND a.path = q.path \
         WHERE q.qid = $1 AND z.name != $2 \
         ORDER BY z.language",
        |q| q.bind(qid).bind(zim),
    )
    .await?;
    let languages: Vec<serde_json::Value> = rows
        .iter()
        .map(|(z, p, t)| {
            serde_json::json!({
                "zim": z,
                "path": p,
                "title": t,
            })
        })
        .collect();
    Ok(serde_json::json!({ "qid": format!("Q{qid}"), "languages": languages }))
}
