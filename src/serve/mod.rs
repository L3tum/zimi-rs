//! HTTP serving: the axum router plus its middleware, handlers, and OpenAPI.
//!
//! `build_router(state)` assembles the route tree (auth middleware, rate
//! limiting, JSON/HTML handlers, and the OpenAPI spec) from a shared
//! `AppState`.
pub mod handlers;
#[cfg(test)]
mod handlers_test;
pub mod middleware;
pub mod openapi;
pub mod ratelimit;

use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::settings::KEY_GENERAL_CORS_ORIGINS;
use crate::AppState;

/// Maximum accepted request body size (10 MiB) — the hard cap on any single request body.
pub(crate) const MAX_REQUEST_BODY_BYTES: usize = 10 * 1024 * 1024;

/// Build the application router with all routes and middleware.
pub fn build_router(state: AppState) -> Router {
    // CORS is config-only by design (SETTING_DEFS: config_only=true). Changing
    // origins requires a restart — this is intentional, not a limitation.
    //
    // CORS: explicit origin allowlist from `general.cors_origins` (comma-
    // separated, e.g. "https://a.example,https://b.example"). Built at
    // startup — changing it requires a restart. With an empty list no
    // Access-Control-Allow-Origin header is emitted at all, so browsers
    // enforce same-origin while non-browser clients (curl, the embedded UI,
    // MCP) are unaffected.
    let raw_origins = state
        .settings
        .get_typed::<String>(KEY_GENERAL_CORS_ORIGINS)
        .unwrap_or_default();
    let origins: Vec<axum::http::HeaderValue> = raw_origins
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<axum::http::HeaderValue>().ok())
        .collect();
    let cors = if origins.is_empty() {
        if !raw_origins.is_empty() {
            tracing::warn!(
                "general.cors_origins is set but contains no parseable origins — CORS disabled"
            );
        }
        CorsLayer::new()
    } else {
        tracing::info!(
            "CORS origins: {:?}",
            origins
                .iter()
                .map(|o| o.to_str().unwrap_or("?"))
                .collect::<Vec<_>>()
        );
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::PUT,
                axum::http::Method::DELETE,
                axum::http::Method::OPTIONS,
            ])
            .allow_headers([
                axum::http::header::AUTHORIZATION,
                axum::http::header::CONTENT_TYPE,
                axum::http::header::RANGE,
            ])
    };

    // Auth layer must be added after the routes it should wrap (axum layers
    // only apply to routes defined earlier in the chain).
    let auth_state = state.clone();
    let rate_limit_state = state.clone();

    Router::new()
        // Health & info
        .route("/health", axum::routing::get(handlers::health))
        .route("/list", axum::routing::get(handlers::list_zims))
        // Search
        .route("/search", axum::routing::get(handlers::search))
        .route("/suggest", axum::routing::get(handlers::suggest))
        // Content
        .route("/read", axum::routing::get(handlers::read_article))
        // Raw content is served cross-origin so the web UI's plain-anchor
        // article links and any third-party reader can open `/w/…` without a
        // same-origin requirement. Cross-origin *simple* GETs (plain fetch,
        // new-tab navigation) need no CORS headers at all. For cross-origin
        // fetch with preflight (custom headers, non-simple methods), operators
        // add the reader's origin to `general.cors_origins` — the global CORS
        // layer then handles it. A separate route-level CorsLayer here would
        // conflict with the global layer (duplicate
        // `Access-Control-Allow-Origin` headers), so it is intentionally
        // omitted (SEC-L1).
        .route(
            "/w/{zim}/{*path}",
            axum::routing::get(handlers::raw_content),
        )
        .route("/chunks", axum::routing::get(handlers::get_chunks))
        .route("/snippet", axum::routing::get(handlers::get_snippet))
        .route("/random", axum::routing::get(handlers::random_article))
        // Interlanguage
        .route(
            "/interlanguage",
            axum::routing::get(handlers::interlanguage),
        )
        // Settings
        .route(
            "/settings",
            axum::routing::get(handlers::get_settings).put(handlers::put_settings),
        )
        .route(
            "/settings/zim/{name}",
            axum::routing::get(handlers::get_zim_settings).put(handlers::put_zim_settings),
        )
        // Downloads
        .route(
            "/downloads",
            axum::routing::get(handlers::list_downloads).post(handlers::add_download),
        )
        .route(
            "/downloads/{id}",
            axum::routing::delete(handlers::cancel_download),
        )
        // Web UI
        .route("/", axum::routing::get(handlers::web_index))
        .route("/search.html", axum::routing::get(handlers::web_search))
        .route("/settings.html", axum::routing::get(handlers::web_settings))
        .route("/common.js", axum::routing::get(handlers::web_common_js))
        // OpenAPI
        .route("/openapi.json", axum::routing::get(openapi::openapi_json))
        // Collections
        .route(
            "/collections",
            axum::routing::get(handlers::list_collections).post(handlers::create_collection),
        )
        .route(
            "/collections/{id}",
            axum::routing::put(handlers::update_collection).delete(handlers::delete_collection),
        )
        // Auth (shared password; only active when access.mode == "password")
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            middleware::auth_middleware,
        ))
        // Rate limit sits outside auth so unauthenticated requests are counted too.
        .layer(axum::middleware::from_fn_with_state(
            rate_limit_state,
            ratelimit::rate_limit,
        ))
        // Layer middleware — custom span strips access_token from logged URIs
        .layer(TraceLayer::new_for_http().make_span_with(
            |req: &axum::http::Request<axum::body::Body>| {
                let uri = crate::serve::middleware::sanitize_uri_for_logs(req.uri());
                tracing::debug_span!(
                    "request",
                    method = %req.method(),
                    uri = %uri,
                    version = ?req.version(),
                )
            },
        ))
        .layer(cors)
        .layer(RequestBodyLimitLayer::new(MAX_REQUEST_BODY_BYTES)) // 10 MiB body limit
        .with_state(state)
}
