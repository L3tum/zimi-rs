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

            let resp: EmbedResponse = req
                .send()
                .await
                .map_err(Error::Http)?
                .json()
                .await
                .map_err(Error::Http)?;

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
                "embedding API returned misaligned indices: position {i} has index {} (expected exactly 0..{})",
                d.index,
                sorted.len()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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
