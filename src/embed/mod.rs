//! Optional embedding pipeline for semantic search.
//!
//! Connects to an OpenAI-compatible /v1/embeddings endpoint to generate
//! vector embeddings for article snippets, stored in pgvector.
//!
//! Module tree (same pattern as `torrent::poller`):
//!
//! - `client` — endpoint config + HTTP client, vector-literal formatting,
//!   batch-index guard.
//! - `column` — `articles.embedding` dimension probe + reconciliation.
//! - `pipeline` — per-ZIM claim → embed → bulk-write pipeline (with the
//!   W6.5 bounded poison-row guard).
//! - `vector_index` — `idx_articles_embedding` catalog state + the
//!   `CREATE INDEX CONCURRENTLY` build paths.
//! - `auto_loop` — the 60 s background auto-embed loop and the shared
//!   10-minute build-probe backoff gate.
//!
//! Everything below is re-exported so the external surface
//! (`zimservice::embed::…`) is unchanged for callers outside this module;
//! child modules import each other's items via `crate::embed::<sibling>`.

mod auto_loop;
mod client;
pub mod column;
mod pipeline;
mod vector_index;

pub use auto_loop::{auto_embed_loop, list_embeddable_zims};
pub use client::{format_vector, EmbedClient, EmbedConfig};
pub use column::ensure_vector_dimension;
pub use pipeline::run_pipeline;
pub use vector_index::{
    index_state, maybe_build_vector_index, vector_index_state, VectorIndexState,
    VECTOR_INDEX_MIN_ROWS,
};
// /diagnostic degradation note: crate-internal (serve handler), not part of
// the public embed surface.
pub(crate) use vector_index::vector_index_degradation_note;
