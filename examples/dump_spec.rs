//! Dump the generated OpenAPI 3.1 spec to stdout.
//!
//! Usage: `cargo run --example dump_spec > openapi.json`

use utoipa::OpenApi;

fn main() {
    let doc = zimservice::serve::openapi::ApiDoc::openapi();
    println!(
        "{}",
        serde_json::to_string_pretty(&doc).expect("OpenAPI doc serializes")
    );
}
