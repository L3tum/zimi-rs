//! Embedding endpoint configuration + OpenAI-compatible HTTP client.
//!
//! Owns the `/v1/embeddings` wire format (request/response types), the
//! SSRF-guarded `reqwest` client, and pgvector literal formatting.

use std::fmt::Write as _;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::settings::{
    SettingsCache, EMBED_DEFAULT_DIMENSION, EMBED_DEFAULT_MODEL, KEY_EMBEDDING_API_KEY,
    KEY_EMBEDDING_BATCH_SIZE, KEY_EMBEDDING_DIMENSION, KEY_EMBEDDING_ENDPOINT,
    KEY_EMBEDDING_MAX_CONCURRENCY, KEY_EMBEDDING_MODEL, KEY_EMBEDDING_TIMEOUT_SECS,
};

/// Configuration for the embedding client.
#[derive(Debug, Clone)]
pub struct EmbedConfig {
    /// Base URL of the OpenAI-compatible `/v1/embeddings` endpoint.
    pub endpoint: String,
    /// API key, sent as `Authorization: Bearer` when non-empty.
    pub api_key: String,
    /// Model name to request in each embeddings call.
    pub model: String,
    /// Expected vector dimension; must match the pgvector column.
    pub dimension: u32,
    /// Number of texts per request batch.
    pub batch_size: usize,
    /// Max batches in flight at once (HTTP + write, held under a semaphore).
    pub max_concurrency: usize,
    /// Per-request HTTP timeout, in seconds.
    pub timeout_secs: u64,
}

impl EmbedConfig {
    /// Build the config from the runtime settings table.
    ///
    /// Returns `None` only when `embedding.endpoint` is unset/empty — the
    /// other fields fall back to the canonical defaults (see
    /// `EMBED_DEFAULT_*`). Values are floored at 1 for the numeric fields.
    pub fn from_settings(settings: &SettingsCache) -> Option<Self> {
        let endpoint = settings.get_typed::<String>(KEY_EMBEDDING_ENDPOINT)?;
        if endpoint.is_empty() {
            return None;
        }
        Some(Self {
            endpoint,
            api_key: settings
                .get_typed(KEY_EMBEDDING_API_KEY)
                .unwrap_or_default(),
            model: settings
                .get_typed(KEY_EMBEDDING_MODEL)
                .unwrap_or_else(|| EMBED_DEFAULT_MODEL.into()),
            dimension: settings
                .get_typed(KEY_EMBEDDING_DIMENSION)
                .unwrap_or(EMBED_DEFAULT_DIMENSION),
            batch_size: settings
                .get_typed(KEY_EMBEDDING_BATCH_SIZE)
                .unwrap_or(64)
                .max(1),
            max_concurrency: settings
                .get_typed(KEY_EMBEDDING_MAX_CONCURRENCY)
                .unwrap_or(4)
                .max(1),
            timeout_secs: settings
                .get_typed(KEY_EMBEDDING_TIMEOUT_SECS)
                .unwrap_or(60)
                .max(1),
        })
    }
}

/// Max bytes of a single embeddings response body (the wire budget for
/// `resp` in `EmbedClient::embed`). Every other external read in this crate
/// is size-capped (256 KB article reads, 64 MB downloads, 10 MiB OPDS, 1 MB
/// MCP); the embeddings response was the one unbounded `.json()`. A
/// well-formed response is `batch_size × dimension × ~12` JSON bytes
/// (default 64 × 768 ≈ 600 KB; the max dimension 4096 × max batch 64 ≈ 3.2
/// MiB), so 16 MiB bounds the honest case by ~5× while still catching a
/// rogue endpoint (the operator-configured, DNS-pinned target of this
/// client) serving a pathological payload.
const MAX_EMBED_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

/// OpenAI-compatible embeddings client.
#[derive(Clone)]
pub struct EmbedClient {
    http: reqwest::Client,
    config: EmbedConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmbedRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedData>,
}

#[derive(Debug, Deserialize)]
struct EmbedData {
    index: usize,
    embedding: Vec<f32>,
}

impl EmbedClient {
    /// `pin` (host, addr) fixes the resolved address of the endpoint's
    /// initial host (DNS-rebinding guard); redirect hops are re-resolved and
    /// re-validated by the client's policy. Operator-configured endpoint:
    /// this built-in re-validation is accepted with its residual sub-second
    /// rebinding window on a redirect to a *different* host — user-influenced
    /// URLs (direct downloads, OPDS) instead follow redirects manually with
    /// per-hop resolve + pin (`netguard::follow_pinned_get`).
    pub fn new(config: EmbedConfig, pin: Option<(String, std::net::SocketAddr)>) -> Result<Self> {
        // SSRF guard: block metadata IPs and private ranges (loopback is
        // admitted for local Ollama). Every redirect hop is re-validated.
        let http = crate::netguard::build_guarded_client(
            &config.endpoint,
            /* allow_private */ false,
            /* allow_loopback */ true,
            pin,
        )?
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .map_err(|e| Error::Embedding(format!("failed to build HTTP client: {e}")))?;
        Ok(Self { http, config })
    }

    /// Generate embeddings for a batch of texts.
    pub async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut all_embeddings = Vec::new();

        for chunk in texts.chunks(self.config.batch_size) {
            let mut req = self
                .http
                .post(format!(
                    "{}/embeddings",
                    self.config.endpoint.trim_end_matches('/')
                ))
                .json(&EmbedRequest {
                    model: self.config.model.clone(),
                    input: chunk.to_vec(),
                });

            if !self.config.api_key.is_empty() {
                req = req.bearer_auth(&self.config.api_key);
            }

            let resp: EmbedResponse = {
                let resp = req.send().await.map_err(Error::Http)?;
                // Non-2xx → `Error::Http` carrying the status (the pre-cap
                // `.json()` behaved this way via reqwest's decode path).
                let resp = resp.error_for_status().map_err(Error::Http)?;
                // Size budget: a `Content-Length` over the cap is rejected
                // before reading (the common case — providers send it);
                // the post-read check below covers chunked/lying bodies.
                let over_cl = resp
                    .headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|cl| cl > MAX_EMBED_RESPONSE_BYTES);
                if over_cl {
                    return Err(Error::Embedding(format!(
                        "embedding endpoint response exceeds the {MAX_EMBED_RESPONSE_BYTES}-byte \
                         body budget (Content-Length)"
                    )));
                }
                let body = resp.bytes().await.map_err(Error::Http)?;
                if (body.len() as u64) > MAX_EMBED_RESPONSE_BYTES {
                    return Err(Error::Embedding(format!(
                        "embedding endpoint response is {} bytes (budget {MAX_EMBED_RESPONSE_BYTES})",
                        body.len()
                    )));
                }
                serde_json::from_slice(&body)?
            };

            // Sort by index to maintain order
            let mut data = resp.data;
            data.sort_by_key(|d| d.index);
            // Reject duplicate/gap/out-of-range indices before extending — a
            // misaligned batch would attach the wrong vector to an article.
            check_embed_indices(&data)?;
            all_embeddings.extend(data.into_iter().map(|d| d.embedding));
        }

        Ok(all_embeddings)
    }
}

/// Format an embedding as a Postgres vector literal: `[0.1,0.2,...]`.
pub fn format_vector(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 12 + 2);
    s.push('[');
    for (i, f) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{f:.7}");
    }
    s.push(']');
    s
}

/// Verify a **sorted** batch of embed results has exactly the contiguous
/// index set `0..data.len()` — i.e. `data[i].index == i` for all `i`.
///
/// The API is expected to echo back one entry per input text, in order. A
/// well-formed response (even when the provider returns entries out of
/// order) sorts to `0,1,2,…`. A duplicate (`[0,1,1]`), a gap (`[0,2]`), or a
/// shifted range (`[1,2,3]`) means the provider dropped or duplicated an
/// entry: extending `all_embeddings` from such a batch would silently attach
/// the wrong vector to the wrong article. Rejecting the whole batch is safer
/// than writing a misaligned vector.
///
/// Module-private (not `pub(crate)`) because it takes the private
/// [`EmbedData`] type; a `pub(crate)` signature would trip `private_interfaces`.
fn check_embed_indices(sorted: &[EmbedData]) -> Result<()> {
    for (i, d) in sorted.iter().enumerate() {
        if d.index != i {
            return Err(Error::Embedding(format!(
                "embedding API returned misaligned indices: position {i} has index {} (expected \
                exactly 0..{})",
                d.index,
                sorted.len()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::testing::dead_pool;

    #[test]
    fn format_vector_produces_pgvector_literal() {
        assert_eq!(
            format_vector(&[0.5, -1.0, 2.25]),
            "[0.5000000,-1.0000000,2.2500000]"
        );
    }

    #[test]
    fn format_vector_empty() {
        assert_eq!(format_vector(&[]), "[]");
    }

    #[test]
    fn format_vector_single() {
        assert_eq!(format_vector(&[0.0]), "[0.0000000]");
    }

    #[test]
    fn embed_config_empty_endpoint_returns_none() {
        let values = vec![(KEY_EMBEDDING_ENDPOINT.into(), serde_json::json!(""))]
            .into_iter()
            .collect();
        let settings = crate::settings::SettingsCache::new_with_map(
            dead_pool(),
            values,
            std::collections::HashMap::new(),
        );
        assert!(EmbedConfig::from_settings(&settings).is_none());
    }

    #[test]
    fn embed_config_missing_endpoint_returns_none() {
        let values: std::collections::HashMap<_, _> = vec![].into_iter().collect();
        let settings = crate::settings::SettingsCache::new_with_map(
            dead_pool(),
            values,
            std::collections::HashMap::new(),
        );
        assert!(EmbedConfig::from_settings(&settings).is_none());
    }

    #[test]
    fn embed_config_defaults_applied() {
        let values = vec![(
            KEY_EMBEDDING_ENDPOINT.into(),
            serde_json::json!("http://localhost:11434/api/embed"),
        )]
        .into_iter()
        .collect();
        let settings = crate::settings::SettingsCache::new_with_map(
            dead_pool(),
            values,
            std::collections::HashMap::new(),
        );
        let cfg = EmbedConfig::from_settings(&settings).unwrap();
        assert_eq!(cfg.endpoint, "http://localhost:11434/api/embed");
        assert_eq!(cfg.api_key, "");
        assert_eq!(cfg.model, EMBED_DEFAULT_MODEL);
        assert_eq!(cfg.dimension, EMBED_DEFAULT_DIMENSION);
        assert_eq!(cfg.batch_size, 64);
        assert_eq!(cfg.max_concurrency, 4);
        assert_eq!(cfg.timeout_secs, 60);
    }

    #[test]
    fn embed_config_explicit_overrides() {
        let values = vec![
            (KEY_EMBEDDING_ENDPOINT.into(), serde_json::json!("http://x")),
            (KEY_EMBEDDING_API_KEY.into(), serde_json::json!("sk-123")),
            (KEY_EMBEDDING_MODEL.into(), serde_json::json!("bge-m3")),
            (KEY_EMBEDDING_DIMENSION.into(), serde_json::json!(1024)),
            (KEY_EMBEDDING_BATCH_SIZE.into(), serde_json::json!(32)),
            (KEY_EMBEDDING_MAX_CONCURRENCY.into(), serde_json::json!(8)),
            (KEY_EMBEDDING_TIMEOUT_SECS.into(), serde_json::json!(120)),
        ]
        .into_iter()
        .collect();
        let settings = crate::settings::SettingsCache::new_with_map(
            dead_pool(),
            values,
            std::collections::HashMap::new(),
        );
        let cfg = EmbedConfig::from_settings(&settings).unwrap();
        assert_eq!(cfg.endpoint, "http://x");
        assert_eq!(cfg.api_key, "sk-123");
        assert_eq!(cfg.model, "bge-m3");
        assert_eq!(cfg.dimension, 1024);
        assert_eq!(cfg.batch_size, 32);
        assert_eq!(cfg.max_concurrency, 8);
        assert_eq!(cfg.timeout_secs, 120);
    }

    // ── check_embed_indices ─────────────────────────────────────────────────

    /// A config pointed at a local (loopback-allowed) endpoint with the
    /// smallest honest shape.
    fn budget_config(endpoint: &str) -> EmbedConfig {
        EmbedConfig {
            endpoint: endpoint.into(),
            api_key: String::new(),
            model: EMBED_DEFAULT_MODEL.into(),
            dimension: 2,
            batch_size: 1,
            max_concurrency: 1,
            timeout_secs: 5,
        }
    }

    // ── response-size budget (P1: the one unbounded `.json()`) ────────

    #[tokio::test]
    async fn embed_rejects_oversized_response_body() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        // Just over the 16 MiB budget (560k entries × ~30 JSON bytes).
        let big = format!(
            "{{\"data\":[{}]}}",
            vec!["{\"index\":0,\"embedding\":[0.0]}"; 560_000].join(",")
        );
        assert!((big.len() as u64) > MAX_EMBED_RESPONSE_BYTES);
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(big, "application/json"))
            .mount(&server)
            .await;
        let client = EmbedClient::new(budget_config(&server.uri()), None).unwrap();
        let err = client.embed(&["x".into()]).await.unwrap_err();
        assert!(
            err.to_string().contains("budget"),
            "oversized body must be rejected as a size-budget error: {err}"
        );
    }

    #[tokio::test]
    async fn embed_rejects_oversized_chunked_response() {
        // Chunked (no `Content-Length`): the pre-read gate cannot see the
        // size, so the post-read byte check is the only gate. wiremock 0.6
        // always serves a `Full` body with `Content-Length`, so this speaks
        // hand-rolled chunked HTTP over raw TCP (the idiom
        // `stream_part_cancel_mid_stream_removes_part_no_error` uses for
        // wiremock-inexpressible behavior).
        let body = format!(
            "{{\"data\":[{}]}}",
            vec!["{\"index\":0,\"embedding\":[0.0]}"; 560_000].join(",")
        );
        assert!((body.len() as u64) > MAX_EMBED_RESPONSE_BYTES);
        // One chunk frame: `<hex len>\r\n<body>\r\n0\r\n\r\n`.
        let frame = format!("{:x}\r\n{body}\r\n0\r\n\r\n", body.len());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            // Read the request head (everything up to the blank line).
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = sock.read(&mut buf).await.expect("read head");
                if n == 0 {
                    return;
                }
                head.extend_from_slice(&buf[..n]);
                if head.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let _ = sock
                .write_all(
                    // Byte-exact response head (single literal: adjacent
                    // literals do not concatenate across lines).
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n",
                )
                .await;
            let _ = sock.write_all(frame.as_bytes()).await;
            let _ = sock.shutdown().await;
        });
        let client = EmbedClient::new(budget_config(&format!("http://{addr}/v1")), None).unwrap();
        let err = client.embed(&["x".into()]).await.unwrap_err();
        assert!(
            err.to_string().contains("budget"),
            "oversized chunked body must be rejected by the post-read check: {err}"
        );
    }

    #[tokio::test]
    async fn embed_accepts_sized_response() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [{ "index": 0, "embedding": [0.1, 0.2] }]
            })))
            .mount(&server)
            .await;
        let client = EmbedClient::new(budget_config(&server.uri()), None).unwrap();
        let vecs = client.embed(&["x".into()]).await.unwrap();
        assert_eq!(vecs, vec![vec![0.1, 0.2]]);
    }

    fn embed_data(indices: &[usize]) -> Vec<EmbedData> {
        indices
            .iter()
            .map(|i| EmbedData {
                index: *i,
                embedding: vec![0.0],
            })
            .collect()
    }

    #[test]
    fn check_embed_indices_sorted_ok() {
        // A well-formed batch returned out of order sorts to a contiguous
        // 0..n and passes.
        let mut v = embed_data(&[2, 0, 1]);
        v.sort_by_key(|d| d.index);
        assert!(check_embed_indices(&v).is_ok());
    }

    #[test]
    fn check_embed_indices_duplicate_err() {
        // [0,1,1]: index 2 missing, 1 duplicated.
        assert!(check_embed_indices(&embed_data(&[0, 1, 1])).is_err());
    }

    #[test]
    fn check_embed_indices_gap_err() {
        // [0,2]: index 1 missing.
        assert!(check_embed_indices(&embed_data(&[0, 2])).is_err());
    }

    #[test]
    fn check_embed_indices_shifted_err() {
        // [1,2,3]: starts at 1, not 0 — position 0 has index 1.
        assert!(check_embed_indices(&embed_data(&[1, 2, 3])).is_err());
    }

    #[test]
    fn check_embed_indices_empty_ok() {
        assert!(check_embed_indices(&[]).is_ok());
    }
}
