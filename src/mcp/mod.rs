//! MCP (Model Context Protocol) server over stdio.
//!
//! Hand-rolled JSON-RPC 2.0 with newline-delimited JSON messages.
//! No external MCP crate needed — the protocol is simple.

use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::search::SearchParams;
use crate::AppState;

const PROTOCOL_VERSION: &str = "2025-03-26";

/// Run the MCP server on stdio. Blocks until stdin is closed.
pub async fn run(state: Arc<AppState>) -> crate::error::Result<()> {
    run_io(state, tokio::io::stdin(), tokio::io::stdout()).await
}

/// Check whether MCP access is authorized. In "open" mode, no password is
/// required. In "password" mode, the `MCP_AUTH_PASSWORD` env var must be set
/// and must match the configured password (constant-time compare).
/// Fail-closed: missing or mismatched credentials result in an error.
pub fn mcp_auth_ok(
    mode: &str,
    configured: &str,
    provided: Option<&str>,
) -> crate::error::Result<()> {
    if mode == crate::settings::ACCESS_MODE_OPEN {
        return Ok(());
    }
    // Password mode: require the env var and a constant-time match.
    let provided = provided.ok_or_else(|| {
        crate::error::Error::Mcp(
            "MCP_AUTH_PASSWORD environment variable is required when access.mode=\"password\""
                .into(),
        )
    })?;
    if !crate::settings::verify_admin_password(configured, provided) {
        return Err(crate::error::Error::Mcp(
            "MCP_AUTH_PASSWORD does not match the configured password".into(),
        ));
    }
    Ok(())
}

/// The stdio message loop, generic over the input/output streams so tests
/// can drive it with in-memory pipes.
async fn run_io<R, W>(state: Arc<AppState>, reader: R, writer: W) -> crate::error::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut writer = writer;

    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader
            .read_line(&mut line)
            .await
            .map_err(|e| crate::error::Error::Mcp(format!("stdin read: {e}")))?;

        if bytes_read == 0 {
            // EOF — client disconnected
            break;
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                let err = json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32700, "message": format!("Parse error: {e}") }
                });
                if let Err(e) = writer.write_all(format!("{err}\n").as_bytes()).await {
                    tracing::debug!("MCP stdio: failed to send parse-error reply: {e}");
                }
                continue;
            }
        };

        let response = match msg.get("method").and_then(|m| m.as_str()) {
            Some(method) => {
                let id = msg.get("id").cloned();
                let params = msg.get("params").cloned().unwrap_or(json!({}));
                let result = dispatch(&state, method, &params).await;
                // Notifications (no id) don't get a response
                if id.is_none() {
                    None
                } else {
                    Some(match result {
                        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                        Err((code, message)) => json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": { "code": code, "message": message },
                        }),
                    })
                }
            }
            // Not a request or notification (e.g. a JSON-RPC batch — which we
            // do not support): reply with a single error object.
            None => Some(json!({
                "jsonrpc": "2.0",
                "id": msg.get("id").cloned(),
                "error": { "code": -32600, "message": "Invalid Request: expected a JSON-RPC 2.0 request or notification" },
            })),
        };

        if let Some(resp) = response {
            if let Err(e) = writer.write_all(format!("{resp}\n").as_bytes()).await {
                tracing::debug!("MCP stdio: failed to send response: {e}");
            }
        }
    }

    Ok(())
}

/// Dispatch a JSON-RPC method to its handler.
async fn dispatch(state: &AppState, method: &str, params: &Value) -> Result<Value, (i32, String)> {
    match method {
        // MCP handshake
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": "zimservice",
                "version": env!("CARGO_PKG_VERSION")
            }
        })),

        "notifications/initialized" => Ok(Value::Null),

        // MCP methods
        "tools/list" => Ok(json!({
            "tools": tool_definitions()
        })),

        "tools/call" => {
            let tool_name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            call_tool(state, tool_name, &args).await
        }

        "ping" => Ok(json!({})),

        _ => Err((-32601, format!("Method not found: {method}"))),
    }
}

/// Get the list of available tools.
fn tool_definitions() -> Value {
    json!([
        {
            "name": "search",
            "description": "Search across all ZIM archives. Hybrid by default (full-text + fuzzy); set mode to use one engine only.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search query" },
                    "zim": { "type": "string", "description": "Filter to specific ZIM" },
                    "language": { "type": "string", "description": "Filter by language" },
                    "mode": { "type": "string", "description": "Engine: fts (full-text), trgm (fuzzy/prefix), vector (semantic), or hybrid (default)" },
                    "limit": { "type": "integer", "description": "Max results (default 10)" }
                },
                "required": ["query"]
            }
        },
        {
            "name": "read",
            "description": "Read an article as plain text",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "zim": { "type": "string", "description": "ZIM name" },
                    "path": { "type": "string", "description": "Article path" },
                    "max_length": { "type": "integer", "description": "Max characters (default 8000)" }
                },
                "required": ["zim", "path"]
            }
        },
        {
            "name": "suggest",
            "description": "Title autocomplete",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "zim": { "type": "string" },
                    "limit": { "type": "integer" }
                },
                "required": ["query"]
            }
        },
        {
            "name": "list_sources",
            "description": "List all available ZIM sources",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "random",
            "description": "Get a random article",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "zim": { "type": "string" }
                }
            }
        },
        {
            "name": "get_chunks",
            "description": "Get RAG-ready text chunks from an article",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "zim": { "type": "string" },
                    "path": { "type": "string" },
                    "size": { "type": "integer" },
                    "overlap": { "type": "integer" }
                },
                "required": ["zim", "path"]
            }
        },
        {
            "name": "deep_search",
            "description": "Search and auto-read top results for full context",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "zim": { "type": "string" },
                    "language": { "type": "string" },
                    "max_results": { "type": "integer" }
                },
                "required": ["query"]
            }
        },
        {
            "name": "article_languages",
            "description": "Find translations of an article via Q-ID index",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "zim": { "type": "string" },
                    "path": { "type": "string" }
                },
                "required": ["zim", "path"]
            }
        },
        {
            "name": "list_collections",
            // Note (B6.9): unlike the HTTP GET /collections endpoint (which
            // returns a structured `{ "collections": [...] }` body), the MCP
            // envelope serializes that same payload as a JSON string in
            // `content[0].text` — parse it as JSON before use.
            "description": "List user collections. Returns the collection list as a JSON string in content[0].text.",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}

// ─── Tool result helpers ─────────────────────────────────────────────────────

/// Wrap a payload in the MCP tool-result envelope (`content` is a list of
/// `type: "text"` blocks per the spec).
fn tool_result(value: &Value) -> Value {
    json!({
        "content": [ { "type": "text", "text": value.to_string() } ],
        "isError": false,
    })
}

/// Wrap a runtime failure in the tool-result envelope with `isError: true` so
/// the calling model can read the message and adapt (vs. a JSON-RPC error,
/// which is reserved for protocol-level problems).
fn tool_error(message: String) -> Value {
    json!({
        "content": [ { "type": "text", "text": format!("error: {message}") } ],
        "isError": true,
    })
}

/// Extract a required string argument from tool arguments.
fn req_str(args: &Value, key: &str) -> Result<String, (i32, String)> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or((-32602, format!("missing required argument '{key}'")))
}

/// SEC-L5 parity: cap the query length on the MCP path to match the HTTP
/// search/suggest endpoints (`MAX_QUERY_CHARS = 500` in `serve/handlers/search`).
/// Without this, `tool_search`/`tool_suggest`/`tool_deep_search` accept an
/// uncapped `query` that the HTTP API would reject — a DoS asymmetry between
/// the two front-ends.
const MCP_MAX_QUERY_CHARS: usize = 500;

/// Extract a required, length-capped query argument (see
/// [`MCP_MAX_QUERY_CHARS`]).
fn req_query_str(args: &Value) -> Result<String, (i32, String)> {
    let q = req_str(args, "query")?;
    if q.chars().count() > MCP_MAX_QUERY_CHARS {
        return Err((
            -32602,
            format!("query exceeds {MCP_MAX_QUERY_CHARS} characters"),
        ));
    }
    Ok(q)
}

// ─── Tool implementations ────────────────────────────────────────────────────

/// Execute a tool call. `Err` is a protocol error (unknown tool, bad args);
/// runtime failures are encoded as `isError: true` results.
///
/// Crate-internal by design: the public MCP surface is `run` (the stdio
/// server) and `mcp_auth_ok`. Integration tests exercise this pipeline via
/// the `#[doc(hidden)]` `testing::mcp_call_tool` seam rather than a public
/// tool-dispatch entry point.
pub(crate) async fn call_tool(
    state: &AppState,
    name: &str,
    args: &Value,
) -> Result<Value, (i32, String)> {
    match name {
        "search" => tool_search(state, args).await,
        "read" => tool_read(state, args).await,
        "suggest" => tool_suggest(state, args).await,
        "list_sources" => Ok(tool_list_sources(state)),
        "random" => tool_random(state, args).await,
        "get_chunks" => tool_get_chunks(state, args).await,
        "deep_search" => tool_deep_search(state, args).await,
        "article_languages" => tool_article_languages(state, args).await,
        "list_collections" => Ok(tool_list_collections(state).await),
        _ => Err((-32601, format!("Unknown tool: {name}"))),
    }
}

async fn tool_search(state: &AppState, args: &Value) -> Result<Value, (i32, String)> {
    let query = req_query_str(args)?;
    let results = match state
        .search
        .search(
            &query,
            &SearchParams {
                zim: args.get("zim").and_then(|z| z.as_str()),
                language: args.get("language").and_then(|l| l.as_str()),
                mode: args.get("mode").and_then(|m| m.as_str()),
                limit: args
                    .get("limit")
                    .and_then(|l| l.as_u64())
                    .map(|l| l as usize),
                ..Default::default()
            },
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return Ok(tool_error(e.to_string())),
    };
    Ok(tool_result(
        &json!({ "results": results, "total": results.len() }),
    ))
}

async fn tool_read(state: &AppState, args: &Value) -> Result<Value, (i32, String)> {
    let zim = req_str(args, "zim")?;
    let path = req_str(args, "path")?;
    let max_len = args
        .get("max_length")
        .and_then(|l| l.as_u64())
        .unwrap_or(8000) as usize;
    match crate::content::read_article_payload(state, &zim, &path, max_len).await {
        Ok(payload) => Ok(tool_result(&json!({
            "title": payload.title,
            "zim": payload.zim,
            "path": payload.path,
            "content": payload.content,
            "truncated": payload.truncated,
            "full_length": payload.full_length,
            "source": payload.source,
        }))),
        Err(e) => Ok(tool_error(e.to_string())),
    }
}

async fn tool_suggest(state: &AppState, args: &Value) -> Result<Value, (i32, String)> {
    let query = req_query_str(args)?;
    let limit = args
        .get("limit")
        .and_then(|l| l.as_u64())
        .map(|l| l as usize);
    let results = match state
        .search
        .suggest(&query, args.get("zim").and_then(|z| z.as_str()), limit)
        .await
    {
        Ok(r) => r,
        Err(e) => return Ok(tool_error(e.to_string())),
    };
    Ok(tool_result(&json!({
        "suggestions": results.iter().map(|r| &r.title).collect::<Vec<_>>(),
    })))
}

fn tool_list_sources(state: &AppState) -> Value {
    let zims = state.zims.list();
    json!({
        "sources": zims.iter().map(|z| json!({
            "name": z.name,
            "title": z.display_title,
            "language": z.language,
            "category": z.category,
            "entries": z.entry_count,
            "indexed": z.indexed_entries,
            "index_status": z.index_status,
        })).collect::<Vec<_>>(),
    })
}

async fn tool_random(state: &AppState, args: &Value) -> Result<Value, (i32, String)> {
    let zim = args.get("zim").and_then(|z| z.as_str()).map(str::to_string);
    let article =
        match crate::db::random_article::fetch_random_article(&state.db, zim.as_deref()).await {
            Ok(a) => a,
            Err(e) => return Ok(tool_error(e.to_string())),
        };
    Ok(tool_result(&json!({
        "path": article.path,
        "title": article.title,
        "snippet": article.snippet,
        "zim": article.zim,
    })))
}

async fn tool_get_chunks(state: &AppState, args: &Value) -> Result<Value, (i32, String)> {
    let zim = req_str(args, "zim")?;
    let path = req_str(args, "path")?;
    let size = args.get("size").and_then(|s| s.as_u64()).unwrap_or(1000) as usize;
    let overlap = args.get("overlap").and_then(|s| s.as_u64()).unwrap_or(200) as usize;
    // Same clamp as the HTTP /chunks handler (B8): bounds size to [10, 100k]
    // and overlap to size-1 and size/2 (bounded chunk count).
    let (size, overlap) = crate::content::clamp_chunk_params(size, overlap);
    let text = match crate::content::read_zim_text(state, &zim, &path).await {
        Ok(t) => t,
        Err(e) => return Ok(tool_error(e.to_string())),
    };
    let chunks = crate::content::chunk_text(&text, size, overlap);
    Ok(tool_result(&json!({
        "zim": zim,
        "path": path,
        "chunk_count": chunks.len(),
        "chunks": chunks,
    })))
}

async fn tool_deep_search(state: &AppState, args: &Value) -> Result<Value, (i32, String)> {
    let query = req_query_str(args)?;
    let max_results = args
        .get("max_results")
        .and_then(|v| v.as_u64())
        .unwrap_or(5)
        .min(10) as usize;
    let results = match state
        .search
        .search(
            &query,
            &SearchParams {
                zim: args.get("zim").and_then(|z| z.as_str()),
                language: args.get("language").and_then(|l| l.as_str()),
                limit: Some(max_results),
                ..Default::default()
            },
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return Ok(tool_error(e.to_string())),
    };
    let articles: Vec<serde_json::Value> =
        futures::future::join_all(results.iter().map(|r| async move {
            let full = crate::content::read_article_payload(state, &r.zim_name, &r.path, 4000)
                .await
                .ok();
            json!({
                "title": r.title,
                "path": r.path,
                "zim": r.zim_name,
                "score": r.score,
                "snippet": r.snippet,
                "content": full
                    .as_ref()
                    .map(|v| Value::String(v.content.clone()))
                    .unwrap_or(Value::Null),
                "truncated": full.as_ref().map(|v| v.truncated),
            })
        }))
        .await;
    Ok(tool_result(
        &json!({ "query": query, "count": articles.len(), "articles": articles }),
    ))
}

async fn tool_article_languages(state: &AppState, args: &Value) -> Result<Value, (i32, String)> {
    let zim = req_str(args, "zim")?;
    let path = req_str(args, "path")?;
    let body = match crate::db::qid::interlanguage_json(&state.db, &zim, &path).await {
        Ok(v) => v,
        Err(e) => return Ok(tool_error(e.to_string())),
    };
    Ok(tool_result(&body))
}

async fn tool_list_collections(state: &AppState) -> Value {
    let rows = match sqlx::query_as::<
        _,
        (
            String,
            String,
            bool,
            chrono::DateTime<chrono::Utc>,
            Vec<String>,
        ),
    >(
        "SELECT c.name, c.label, c.is_favorite, c.created_at,
                COALESCE(ARRAY_AGG(z.name) FILTER (WHERE z.id IS NOT NULL), '{}')
         FROM collections c
         LEFT JOIN zims z ON z.id = ANY(c.zim_ids)
         GROUP BY c.id
         ORDER BY c.name",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(r) => r,
        Err(e) => return tool_error(e.to_string()),
    };
    let collections: Vec<Value> = rows
        .iter()
        .map(|(name, label, is_favorite, created_at, zims)| {
            json!({
                "name": name,
                "label": label,
                "is_favorite": is_favorite,
                "zims": zims,
                "created_at": created_at.to_rfc3339(),
            })
        })
        .collect();
    tool_result(&json!({ "collections": collections }))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn test_state() -> Arc<AppState> {
        Arc::new(crate::testing::test_state())
    }

    async fn run_session(state: &Arc<AppState>, requests: &[&str]) -> Vec<Value> {
        let (server_in, mut client_out) = tokio::io::duplex(64 * 1024);
        let (server_out, client_in) = tokio::io::duplex(64 * 1024);
        let task = tokio::spawn(run_io(state.clone(), server_in, server_out));

        for req in requests {
            client_out.write_all(req.as_bytes()).await.unwrap();
            client_out.write_all(b"\n").await.unwrap();
        }
        drop(client_out); // close write side → EOF for server
        task.await.unwrap().unwrap();

        let mut input = BufReader::new(client_in);
        let mut line = String::new();
        let mut responses = Vec::new();
        loop {
            line.clear();
            let n = input.read_line(&mut line).await.unwrap();
            if n == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            responses.push(serde_json::from_str(trimmed).unwrap());
        }
        responses
    }

    #[tokio::test]
    async fn handshake_and_tools_list() {
        let state = test_state();
        let responses = run_session(
            &state,
            &[
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","clientInfo":{"name":"test","version":"0"}}}"#,
                r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
                r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
                r#"{"jsonrpc":"2.0","id":3,"method":"ping"}"#,
            ],
        )
        .await;

        assert_eq!(responses.len(), 3);

        let init = &responses[0];
        assert_eq!(init["id"], 1);
        assert_eq!(init["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(init["result"]["serverInfo"]["name"], "zimservice");
        assert!(init["result"]["capabilities"].get("tools").is_some());

        let tools = &responses[1];
        assert_eq!(tools["id"], 2);
        let tool_list = tools["result"]["tools"].as_array().unwrap();
        assert_eq!(tool_list.len(), 9);
        let names: Vec<&str> = tool_list
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        for expected in [
            "search",
            "read",
            "suggest",
            "list_sources",
            "random",
            "get_chunks",
            "deep_search",
            "article_languages",
            "list_collections",
        ] {
            assert!(names.contains(&expected), "missing tool {expected}");
        }
        for t in tool_list {
            assert!(
                t["description"].is_string(),
                "tool {} has no description",
                t["name"]
            );
            assert_eq!(t["inputSchema"]["type"], "object");
        }

        assert_eq!(responses[2]["id"], 3);
        assert_eq!(responses[2]["result"], json!({}));
    }

    #[tokio::test]
    async fn parse_error_and_unknown_method() {
        let state = test_state();
        let responses = run_session(
            &state,
            &[
                "this is not json",
                r#"{"jsonrpc":"2.0","id":9,"method":"bogus/method"}"#,
            ],
        )
        .await;
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["error"]["code"], -32700);
        assert!(responses[0]["id"].is_null());
        assert_eq!(responses[1]["error"]["code"], -32601);
        assert_eq!(responses[1]["id"], 9);
    }

    #[tokio::test]
    async fn invalid_request_gets_error_response() {
        let state = test_state();
        let responses =
            run_session(&state, &[r#"[{"jsonrpc":"2.0","id":1,"method":"ping"}]"#]).await;
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0]["error"]["code"], -32600);
    }

    #[tokio::test]
    async fn unknown_tool_and_missing_args() {
        let state = test_state();
        let responses = run_session(
            &state,
            &[
                r#"{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"nope","arguments":{}}}"#,
                r#"{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"search","arguments":{}}}"#,
                r#"{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"read","arguments":{"zim":"x"}}}"#,
            ],
        )
        .await;
        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0]["error"]["code"], -32601);
        assert_eq!(responses[1]["error"]["code"], -32602);
        assert!(responses[1]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("query"));
        assert_eq!(responses[2]["error"]["code"], -32602);
        assert!(responses[2]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("path"));
    }

    #[tokio::test]
    async fn tool_runtime_failure_uses_is_error_envelope() {
        let state = test_state();
        let responses = run_session(
            &state,
            &[
                r#"{"jsonrpc":"2.0","id":20,"method":"tools/call","params":{"name":"random","arguments":{}}}"#,
            ],
        )
        .await;
        assert_eq!(responses.len(), 1);
        let result = &responses[0]["result"];
        assert_eq!(result["isError"], true);
        assert_eq!(result["content"][0]["type"], "text");
        let text = result["content"][0]["text"].as_str().unwrap();
        assert!(text.starts_with("error:"), "got: {text}");
    }

    // ── mcp_auth_ok (Step 3.4 / DEC-2) ────────────────────────────────────

    #[test]
    fn mcp_auth_ok_open_mode_needs_no_password() {
        assert!(mcp_auth_ok("open", "secret", None).is_ok());
        assert!(mcp_auth_ok("open", "secret", Some("whatever")).is_ok());
    }

    #[test]
    fn mcp_auth_ok_password_mode_missing_env_fails() {
        let err = mcp_auth_ok("password", "secret", None).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("MCP_AUTH_PASSWORD"), "got: {msg}");
    }

    #[test]
    fn mcp_auth_ok_password_mode_wrong_password_fails() {
        let err = mcp_auth_ok("password", "secret", Some("wrong")).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("does not match"), "got: {msg}");
    }

    #[test]
    fn mcp_auth_ok_password_mode_correct_password_passes() {
        assert!(mcp_auth_ok("password", "secret", Some("secret")).is_ok());
    }

    #[test]
    fn mcp_auth_ok_password_mode_hashed_configured_passes() {
        // A `sha2:`-hashed stored value must still verify against the plaintext
        // provided password — otherwise, after one successful HTTP auth
        // transparently upgrades the stored value, MCP auth could never match
        // again (silent runtime regression).
        let hashed = crate::settings::hash_admin_password("secret");
        assert!(crate::settings::is_legacy_password("secret"));
        assert!(!crate::settings::is_legacy_password(&hashed));
        assert!(mcp_auth_ok("password", &hashed, Some("secret")).is_ok());
        // Wrong password against the hashed value still fails.
        assert!(mcp_auth_ok("password", &hashed, Some("wrong")).is_err());
    }
}
