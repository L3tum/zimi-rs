//! Search handlers: full-text / fuzzy / semantic search, title suggestions,
//! random articles, and cross-language (Q-ID) links.
use axum::extract::{Query, State};
use axum::response::Json;
use serde::Deserialize;

use crate::search::{SearchParams, SearchResult};
use crate::serve::openapi::ErrorResponse;
use crate::AppState;

/// Max characters for the `q` query parameter. Bounds the LIKE/trgm/FTS work a
/// single request can trigger (a 10 MB `q` would build a huge pattern and run
/// a full-scan trgm similarity pass). 500 is far beyond any real query; the
/// 10 MB body limit stays as a backstop for other parameters.
const MAX_QUERY_CHARS: usize = 500;

/// Once-per-process rate limit for the deprecated-`query`-parameter warning:
/// the first request using the old `query` alias logs a `warn!`, every
/// subsequent one a `debug!` — one warn per process is enough for the
/// operator to notice, and it stops a client pinned to the alias from
/// flooding the log on every request.
static DEPRECATION_WARNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn log_deprecated_query_param() {
    if DEPRECATION_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::debug!("query parameter 'query' is deprecated, use 'q'");
    } else {
        tracing::warn!("query parameter 'query' is deprecated, use 'q'");
    }
}

// ─── Search + random + interlanguage: response DTOs (OpenAPI schemas) ────────

/// `GET /search` response: the merged multi-engine result page.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct SearchResponse {
    /// The query string as received.
    pub query: String,
    /// Matches on this page.
    pub results: Vec<SearchResult>,
    /// Number of results in this page (after offset/limit). Hybrid search
    /// merges capped per-branch results, so this is the page size — not a
    /// total match count across the corpus.
    pub total: usize,
    /// Branches currently degraded (≥ 3 consecutive failures), WI-5.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub degraded: Vec<String>,
}

/// `GET /suggest` response: title suggestions for a query prefix.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct SuggestResponse {
    /// The prefix string as received.
    pub query: String,
    /// Suggested titles.
    pub suggestions: Vec<String>,
    /// Full result objects for each suggestion.
    pub results: Vec<SearchResult>,
}

/// `GET /random` response: one randomly chosen article.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct RandomArticleResponse {
    /// Row id of the matched `articles` record.
    pub id: i64,
    /// DB id of the ZIM the article belongs to.
    pub zim_id: i32,
    /// Article path within the ZIM.
    pub path: String,
    /// Article title.
    pub title: String,
    /// Stored FTS snippet.
    pub snippet: String,
    /// ZIM name.
    pub zim: String,
}

/// One cross-language link to the same article in another ZIM.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct InterlanguageLink {
    /// Name of the ZIM containing the linked article.
    pub zim: String,
    /// Path of the linked article within that ZIM.
    pub path: String,
    /// Title of the linked article, when known.
    pub title: Option<String>,
}

/// Response for `GET /article/{path}/interlanguage` — the Wikidata Q-ID and
/// cross-language links for the article.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct InterlanguageResponse {
    /// Wikidata Q-ID (null when the article has no Q-ID).
    pub qid: Option<String>,
    /// Cross-language links for the same article.
    pub languages: Vec<InterlanguageLink>,
}

// ─── Search ───────────────────────────────────────────────────────────────────

/// Query parameters for `GET /search`.
#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    /// Search query (required unless `query` is given).
    pub q: Option<String>,
    /// Deprecated alias of `q`.
    pub query: Option<String>,
    /// Restrict to one ZIM by name.
    pub zim: Option<String>,
    /// Restrict to one language code.
    pub language: Option<String>,
    /// Max results (default from settings).
    pub limit: Option<usize>,
    /// Result offset for pagination.
    pub offset: Option<usize>,
    /// Engine(s) to use: "fts", "trgm" (a.k.a. "fuzzy"/"prefix"), or "hybrid" (default).
    pub mode: Option<String>,
    /// When true, wrap matched terms in the snippet with <b>…</b> via ts_headline.
    pub highlight: Option<bool>,
}

/// `GET /search` — multi-engine article search (FTS / trigram / vector) with
/// score merge; `mode` selects the engine(s) and pagination via limit/offset.
#[utoipa::path(
    get,
    path = "/search",
    params(
        ("q" = Option<String>, Query, description = "Search query (required unless 'query' is given)"),
        ("query" = Option<String>, Query, description = "Alias of 'q'"),
        ("zim" = Option<String>, Query, description = "Restrict to one ZIM by name"),
        ("language" = Option<String>, Query, description = "Restrict to one language code"),
        ("mode" = Option<String>, Query, description = "Engine: fts, trgm (fuzzy/prefix), vector (semantic), or hybrid (default)"),
        ("highlight" = Option<bool>, Query, description = "Wrap matched terms in snippet with <b> via ts_headline"),
        ("limit" = Option<usize>, Query, description = "Max results (default from settings)"),
        ("offset" = Option<usize>, Query, description = "Result offset for pagination")
    ),
    responses(
        (status = 200,
            description = "Search results; `total` is the number of results in this page (after offset/limit), not a corpus-wide match count",
            body = SearchResponse),
        (status = 400, description = "Missing 'q' parameter", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, crate::error::Error> {
    if params.q.is_none() && params.query.is_some() {
        log_deprecated_query_param();
    }
    let query = params.q.or(params.query).ok_or_else(|| {
        crate::error::Error::InvalidInput("query parameter 'q' is required".into())
    })?;

    // Cap the query length (see MAX_QUERY_CHARS) before any DB work.
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err(crate::error::Error::InvalidInput(format!(
            "query parameter 'q' exceeds {MAX_QUERY_CHARS} characters"
        )));
    }

    // A blank/whitespace query matches everything in the LIKE/similarity
    // predicates — short-circuit to empty results instead of a full-table scan.
    if query.trim().is_empty() {
        return Ok(Json(SearchResponse {
            query,
            results: Vec::<crate::search::SearchResult>::new(),
            total: 0,
            degraded: Vec::new(),
        }));
    }

    let results = state
        .search
        .search(
            &query,
            &SearchParams {
                zim: params.zim.as_deref(),
                language: params.language.as_deref(),
                mode: params.mode.as_deref(),
                limit: params.limit,
                offset: params.offset,
                highlight: params.highlight.unwrap_or(false),
            },
        )
        .await?;

    let total = results.len();
    Ok(Json(SearchResponse {
        query,
        results,
        total,
        degraded: state
            .degradation
            .degraded_snapshot()
            .into_iter()
            .map(|(name, _)| name)
            .collect(),
    }))
}

// ─── Suggest ──────────────────────────────────────────────────────────────────

/// Query parameters for `GET /suggest`.
#[derive(Debug, Deserialize)]
pub struct SuggestQuery {
    /// Title prefix (required unless `query` is given).
    pub q: Option<String>,
    /// Deprecated alias of `q`.
    pub query: Option<String>,
    /// Restrict to one ZIM by name.
    pub zim: Option<String>,
    /// Max suggestions (default 10, max 20).
    pub limit: Option<usize>,
}

/// `GET /suggest` — title suggestions for a query prefix.
#[utoipa::path(
    get,
    path = "/suggest",
    params(
        ("q" = Option<String>, Query, description = "Title prefix (required unless 'query' is given)"),
        ("query" = Option<String>, Query, description = "Alias of 'q'"),
        ("zim" = Option<String>, Query, description = "Restrict to one ZIM by name"),
        ("limit" = Option<usize>, Query, description = "Max suggestions (default 10, max 20)")
    ),
    responses(
        (status = 200, description = "Title suggestions", body = SuggestResponse),
        (status = 400, description = "Missing 'q' parameter", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn suggest(
    State(state): State<AppState>,
    Query(params): Query<SuggestQuery>,
) -> Result<Json<SuggestResponse>, crate::error::Error> {
    if params.q.is_none() && params.query.is_some() {
        log_deprecated_query_param();
    }
    let query = params.q.or(params.query).ok_or_else(|| {
        crate::error::Error::InvalidInput("query parameter 'q' is required".into())
    })?;

    // Cap the query length (see MAX_QUERY_CHARS) before any DB work.
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err(crate::error::Error::InvalidInput(format!(
            "query parameter 'q' exceeds {MAX_QUERY_CHARS} characters"
        )));
    }

    // A blank/whitespace query would ILIKE '%%' over the whole table —
    // short-circuit to empty suggestions instead.
    if query.trim().is_empty() {
        return Ok(Json(SuggestResponse {
            query,
            suggestions: Vec::<String>::new(),
            results: Vec::<crate::search::SearchResult>::new(),
        }));
    }

    let results = state
        .search
        .suggest(&query, params.zim.as_deref(), params.limit)
        .await?;

    let suggestions: Vec<String> = results.iter().map(|r| r.title.clone()).collect();
    Ok(Json(SuggestResponse {
        query,
        suggestions,
        results,
    }))
}

// ─── Random ───────────────────────────────────────────────────────────────────

/// Query parameters for `GET /random` (a random article).
#[derive(Debug, Deserialize)]
pub struct RandomQuery {
    /// Restrict to one ZIM by name.
    pub zim: Option<String>,
}

/// `GET /random` — a random article, optionally restricted to one ZIM.
#[utoipa::path(
    get,
    path = "/random",
    params(
        ("zim" = Option<String>, Query, description = "Restrict to one ZIM by name")
    ),
    responses(
        (status = 200, description = "A random article", body = RandomArticleResponse),
        (status = 404, description = "No articles found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn random_article(
    State(state): State<AppState>,
    Query(params): Query<RandomQuery>,
) -> Result<Json<RandomArticleResponse>, crate::error::Error> {
    let article =
        crate::db::random_article::fetch_random_article(&state.db, params.zim.as_deref()).await?;

    Ok(Json(RandomArticleResponse {
        id: article.id,
        zim_id: article.zim_id,
        path: article.path,
        title: article.title,
        snippet: article.snippet,
        zim: article.zim,
    }))
}

// ─── Interlanguage ────────────────────────────────────────────────────────────

/// Query parameters for `GET /interlanguage` (cross-language links via Q-ID).
#[derive(Debug, Deserialize)]
pub struct InterlangQuery {
    /// Name of the ZIM containing the article.
    pub zim: String,
    /// Article path within the ZIM.
    pub path: String,
}

/// `GET /interlanguage` — cross-language links for an article via its
/// Wikidata Q-ID (empty when the article has no Q-ID).
#[utoipa::path(
    get,
    path = "/interlanguage",
    params(
        ("zim" = String, Query, description = "ZIM name"),
        ("path" = String, Query, description = "Article path within the ZIM")
    ),
    responses(
        (status = 200, description = "Cross-language links via Wikidata Q-ID (empty when the article has no Q-ID)", body = InterlanguageResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn interlanguage(
    State(state): State<AppState>,
    Query(params): Query<InterlangQuery>,
) -> Result<Json<serde_json::Value>, crate::error::Error> {
    let body = crate::db::qid::interlanguage_json(&state.db, &params.zim, &params.path).await?;
    Ok(Json(body))
}
