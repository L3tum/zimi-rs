//! OpenAPI 3.1 document, generated at compile time by utoipa from the
//! `#[utoipa::path]` attributes on the axum handlers in `serve/handlers/`.
//!
//! Served at `GET /openapi.json`.

use axum::Json;
use utoipa::OpenApi;

use crate::search::SearchResult;
use crate::zim::ZimMeta;

/// Standard error body returned by every failing endpoint.
#[derive(utoipa::ToSchema)]
pub struct ErrorResponse {
    /// Human-readable, client-safe error message.
    pub error: String,
}

/// Adds the shared-password bearer scheme used by mutating endpoints.
///
/// Auth is only enforced at runtime when `access.mode == "password"`, but the
/// scheme is documented unconditionally so clients know what to send.
struct AddSecuritySchemes;

impl utoipa::Modify for AddSecuritySchemes {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};

        openapi
            .components
            .get_or_insert_with(utoipa::openapi::Components::default)
            .add_security_scheme(
                "bearer_auth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .description(Some(
                            "Only active when access.mode=password. Send `Authorization: Bearer <password>` on mutating requests (POST/PUT/DELETE). On read-only requests (GET/HEAD/OPTIONS) `?access_token=<password>` is also accepted; it is ignored on mutating requests (query-string tokens leak via logs and Referer headers — prefer Bearer).",
                        ))
                        .build(),
                ),
            );
    }
}

#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "zimservice API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Cross-ZIM search and serve platform: full-text, fuzzy and semantic \
                       search over ZIM archives, with download management and content serving."
    ),
    paths(
        crate::serve::handlers::health,
        crate::serve::handlers::list_zims,
        crate::serve::handlers::search,
        crate::serve::handlers::suggest,
        crate::serve::handlers::read_article,
        crate::serve::handlers::raw_content,
        crate::serve::handlers::get_chunks,
        crate::serve::handlers::get_snippet,
        crate::serve::handlers::random_article,
        crate::serve::handlers::interlanguage,
        crate::serve::handlers::get_settings,
        crate::serve::handlers::put_settings,
        crate::serve::handlers::get_zim_settings,
        crate::serve::handlers::put_zim_settings,
        crate::serve::handlers::list_downloads,
        crate::serve::handlers::add_download,
        crate::serve::handlers::cancel_download,
        crate::serve::handlers::list_collections,
        crate::serve::handlers::create_collection,
        crate::serve::handlers::update_collection,
        crate::serve::handlers::delete_collection,
    ),
    components(schemas(
        ErrorResponse,
        ZimMeta,
        SearchResult,
        crate::serve::handlers::Collection,
        crate::serve::handlers::ListCollectionsResponse,
        crate::serve::handlers::CreateCollectionBody,
        crate::serve::handlers::UpdateCollectionBody,
        crate::serve::handlers::AddDownloadBody,
        crate::serve::handlers::HealthResponse,
        crate::serve::handlers::ListZimsResponse,
        crate::serve::handlers::SearchResponse,
        crate::serve::handlers::SuggestResponse,
        crate::content::ReadResponse,
        crate::content::TextChunk,
        crate::content::ChunksResponse,
        crate::serve::handlers::SnippetResponse,
        crate::serve::handlers::RandomArticleResponse,
        crate::serve::handlers::InterlanguageLink,
        crate::serve::handlers::InterlanguageResponse,
        crate::serve::handlers::Download,
        crate::serve::handlers::ListDownloadsResponse,
        crate::serve::handlers::DownloadQueuedResponse,
        crate::serve::handlers::SettingsUpdateResponse,
        crate::serve::handlers::PutZimSettingsResponse,
        crate::serve::handlers::OkResponse,
        crate::serve::handlers::OkIdResponse,
        crate::serve::handlers::CreatedIdResponse,
    )),
    modifiers(&AddSecuritySchemes),
)]
/// utoipa `OpenApi` root: the compile-time-generated OpenAPI 3.1 spec
/// (served at `GET /openapi.json`).
pub struct ApiDoc;

/// `GET /openapi.json` — the generated spec.
///
/// The spec is built once and cached: it is static for the life of the
/// process (the utoipa macro expands at compile time), so rebuilding it on
/// every request was pure waste (PERF-L2).
pub async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    static SPEC: std::sync::LazyLock<utoipa::openapi::OpenApi> =
        std::sync::LazyLock::new(ApiDoc::openapi);
    Json(SPEC.clone())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn openapi_paths_match_route_table() {
        // Two-sided pinning, side 1 (ARCH-4): the utoipa `paths(...)` macro
        // (21 handler refs collapsing to 16 unique path keys) must stay in
        // sync with the axum route table in `serve::build_router`. The 5
        // routes intentionally absent from `paths(...)` are the 4 web-UI
        // pages + `/openapi.json` itself.
        use std::collections::BTreeSet;
        let doc = ApiDoc::openapi();
        let actual: BTreeSet<String> = doc.paths.paths.keys().cloned().collect();
        let expected: BTreeSet<String> = [
            "/health",
            "/list",
            "/search",
            "/suggest",
            "/random",
            "/interlanguage",
            "/read",
            "/w/{zim}/{path}",
            "/chunks",
            "/snippet",
            "/settings",
            "/settings/zim/{name}",
            "/downloads",
            "/downloads/{id}",
            "/collections",
            "/collections/{id}",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(
            actual, expected,
            "OpenAPI path set drifted from the route table"
        );
    }

    #[test]
    fn spec_builds_and_contains_key_paths() {
        let doc = ApiDoc::openapi();
        for path in [
            "/health",
            "/list",
            "/search",
            "/read",
            "/chunks",
            "/settings",
            "/collections",
            "/collections/{id}",
            "/downloads",
        ] {
            assert!(doc.paths.paths.contains_key(path), "missing path {path}");
        }
        // Security scheme registered by the modifier.
        let components = doc.components.as_ref().expect("components present");
        assert!(components.security_schemes.contains_key("bearer_auth"));
        // The spec must serialize to valid JSON.
        let json = serde_json::to_value(&doc).expect("spec serializes");
        assert_eq!(json["openapi"], "3.1.0");
        assert_eq!(json["info"]["title"], "zimservice API");
    }

    #[test]
    fn spec_lists_search_params_and_modes() {
        let json = serde_json::to_value(ApiDoc::openapi()).expect("spec serializes");
        let params = json["paths"]["/search"]["get"]["parameters"]
            .as_array()
            .expect("search has parameters");
        let names: Vec<&str> = params.iter().filter_map(|p| p["name"].as_str()).collect();
        for expected in [
            "q",
            "query",
            "zim",
            "language",
            "mode",
            "highlight",
            "limit",
            "offset",
        ] {
            assert!(names.contains(&expected), "missing param {expected}");
        }
        // Mutating endpoints carry the bearer security requirement.
        let mut_req = &json["paths"]["/collections"]["post"]["security"][0]["bearer_auth"];
        assert!(mut_req.is_array());
    }

    /// Mirrors `examples/dump_spec.rs` (the `cargo run --example dump_spec >
    /// openapi.json` path) so that exact code path is exercised in the suite,
    /// not only compile-checked: build the doc, pretty-serialize it, and
    /// confirm the output is valid, non-empty JSON (Tests Minor #9).
    #[test]
    fn dump_spec_example_path_serializes_to_valid_json() {
        let doc = ApiDoc::openapi();
        let pretty = serde_json::to_string_pretty(&doc).expect("OpenAPI doc serializes");
        assert!(
            !pretty.trim().is_empty(),
            "spec must not serialize to an empty string"
        );
        // Re-parse to prove the pretty output is well-formed JSON (what a
        // `> openapi.json` redirect would persist).
        let reparsed: serde_json::Value =
            serde_json::from_str(&pretty).expect("pretty output is valid JSON");
        assert_eq!(reparsed["openapi"], "3.1.0");
    }
}
