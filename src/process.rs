//! Process-mode flags: the shared live readers of the two process-mode
//! opt-out env vars (`ZIMSERVICE_ALLOW_MULTI_INSTANCE`,
//! `ZIMSERVICE_ALLOW_MULTI_DB`).
//!
//! Why this module exists (2026-10 project-wide review layering fix):
//! `src/settings/` and `src/serve/` must not depend on `src/startup/` —
//! startup owns orchestration and itself depends on settings, so a
//! settings/ or serve/ reference into it would invert the graph (the edge
//! is pinned by rule P5 in `scripts/check-boundaries.sh`). The two opt-out
//! readers were exactly that inversion (settings/cache.rs and the `/health`
//! handler read them through `crate::startup`), so they live here: a leaf
//! module with no crate-internal dependencies that both lower layers can
//! depend on. `startup/` keeps calling the readers directly and re-exports
//! nothing from here.
//!
//! The readers read the env **at call time** on purpose: the tests set the
//! env at runtime and the `/health` snapshot must reflect live values, so
//! the flags must stay re-readable after process start (not just at
//! `Config::load()` time). The `ZIMSERVICE_ALLOW_MULTI_DB` **parsing** in
//! `Config::load` remains in `src/config.rs` — only the live readers moved
//! here.

/// M1: whether this process started with the full single-instance opt-out
/// (`ZIMSERVICE_ALLOW_MULTI_INSTANCE=1`) — the one place that reading of the
/// env var lives so the guard sites, the `/health` flag, and the settings-
/// divergence warnings all agree.
///
/// In that mode every instance keeps its own in-memory caches (settings,
/// rate limiter, ZIM metadata); connected serve processes invalidate each
/// other over `LISTEN`/`NOTIFY` (`crate::db::notify`), so the residual gap
/// is narrow — a one-shot CLI instance (which runs no listener) or any
/// process whose listener is offline stays stale until its own resync or
/// restart. The callers use this to surface that residual divergence
/// continuously instead of only via the one startup `tracing::warn!`.
///
/// This is the **single, deliberate** reader of
/// `ZIMSERVICE_ALLOW_MULTI_INSTANCE`, kept outside `Config` on purpose: it
/// must stay re-readable at **any point after process start** (startup
/// guards, `/health`, the settings-divergence warnings), not just at
/// `Config::load()` time. Note that `SettingsCache` (`src/settings/cache.rs`)
/// calls this at construction and **snapshots the result once** — callers
/// that need the live value must call this fn directly, not the cached copy.
pub fn multi_instance_allowed() -> bool {
    matches!(std::env::var("ZIMSERVICE_ALLOW_MULTI_INSTANCE"), Ok(v) if v == "1")
}

/// m-7 partial opt-out: live reader of `ZIMSERVICE_ALLOW_MULTI_DB` (exact
/// "1" — the same parse [`crate::config::Config::load`] applies). Like
/// [`multi_instance_allowed`], a deliberate env reader kept outside `Config`
/// so `/health` can surface the process's degraded single-instance guarantee
/// without a state field.
pub fn multi_db_allowed() -> bool {
    matches!(std::env::var("ZIMSERVICE_ALLOW_MULTI_DB"), Ok(v) if v == "1")
}

#[cfg(test)]
mod tests {
    use super::multi_instance_allowed;

    /// M1: `multi_instance_allowed` mirrors the process env exactly.
    /// Mutating the process env in a parallel test would race every other
    /// test, so (like the DB-gated tests in `startup`) this asserts against
    /// the env as found: the unset/non-`1` arm unconditionally, the `1` arm
    /// only when a developer has exported the opt-out.
    #[test]
    fn multi_instance_allowed_mirrors_env() {
        match std::env::var("ZIMSERVICE_ALLOW_MULTI_INSTANCE") {
            Ok(v) if v == "1" => {
                assert!(multi_instance_allowed(), "the `1` opt-out must be honored")
            }
            _ => assert!(
                !multi_instance_allowed(),
                "unset (or non-`1`) must mean single-instance mode"
            ),
        }
    }
}
