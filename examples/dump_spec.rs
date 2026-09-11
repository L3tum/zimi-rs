//! Dump the generated OpenAPI 3.1 spec to stdout.
//!
//! Usage: `cargo run --example dump_spec > openapi.json`
//!
//! LINT-3 (2026-09 sweep): dev-only tooling — a panic on a serialization
//! failure is the intended behavior, so expect_used is grandfathered here.
#![allow(clippy::expect_used)]

use utoipa::OpenApi;

fn main() {
    let doc = zimservice::serve::openapi::ApiDoc::openapi();
    println!(
        "{}",
        serde_json::to_string_pretty(&doc).expect("OpenAPI doc serializes")
    );
}
