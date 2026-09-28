//! `/diagnostic` response shapes + collector (M-diag, 2026-10 review).
//!
//! The operator-facing introspection payload
//! ([`DiagnosticResponse`](crate::serve::diagnostics::DiagnosticResponse)) and
//! all probe collection live here — NOT in the `zims` handler module. The
//! `diagnostic()` handler in `handlers::zims` is a thin auth + delegate
//! shell over [`diagnostic_snapshot`](crate::serve::diagnostics::diagnostic_snapshot)
//! (the one place every probe is
//! gathered). The DTOs are re-exported from `handlers::zims`
//! (`pub use crate::serve::diagnostics::*`) so the
//! `crate::serve::handlers::PoolHealth` paths in `openapi.rs` keep
//! resolving, and the OpenAPI component names are unchanged.

use crate::AppState;

// ─── DTOs (OpenAPI schemas) ─────────────────────────────────────────────────

/// `GET /diagnostic` → `pool`: shared Postgres pool saturation signal
/// (Architecture M1). A long reindex `COPY` holding every pooled connection is
/// invisible until queued acquires hit the pool's 10 s acquire timeout and
/// surface as 503s; this snapshot is the pre-503 signal an operator can poll.
///
/// Semantics (sqlx 0.8 `Pool` — the crate exposes no `pending_acquires`):
/// `size` is the number of connections currently **active, idle included**
/// (`Pool::size`), `idle` the idle count (`Pool::num_idle`), so
/// `checked_out = size - idle`; `max_size` is the configured ceiling
/// (`PoolOptions::get_max_connections`, i.e. `db_pool_size` clamped by
/// `db::pool::effective_pool_size`). Saturation is readable as
/// `checked_out == max_size && idle == 0`.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct PoolHealth {
    /// Active connections (checked-out + idle); `Pool::size()`.
    pub size: u32,
    /// Idle (not in use) connections; `Pool::num_idle()`.
    pub idle: u32,
    /// Configured maximum connection count (`PoolOptions::get_max_connections`).
    pub max_size: u32,
    /// `size - idle` — connections currently held by in-flight queries.
    pub checked_out: u32,
}

impl PoolHealth {
    /// Saturation predicate — mirrors `db::pool::PoolSaturation::saturated`
    /// on the DTO (with `idle == 0`, `checked_out == size`, so
    /// `size == max_size` ⟺ `checked_out == max_size`): a full pool with
    /// zero idle connections — the point at which further acquires queue
    /// (10 s → 503).
    pub fn saturated(&self) -> bool {
        self.checked_out == self.max_size && self.idle == 0 && self.max_size > 0
    }
}

/// `GET /diagnostic` → `vector_index`: the partial `idx_articles_embedding`
/// degradation state. A missing (or invalid) vector index makes every
/// vector search a brute-force sequential cosine scan — the most expensive
/// query shape in the system, invisible from `/health`; the `degraded` note
/// (≥ 10k embedded rows, no valid index) is the operator-facing signal.
/// Omitted entirely when the catalog probe itself fails (DB unreachable —
/// `/health`'s `db_connected` already reports that).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct VectorIndexDiagnostic {
    /// `articles` rows with a non-NULL embedding.
    pub embedded_rows: i64,
    /// Catalog state of `idx_articles_embedding`: `absent`, `present`
    /// (valid and usable), or `present_invalid` (a failed/in-progress
    /// `CONCURRENTLY` build that must be dropped before a fresh build takes
    /// effect).
    pub index: crate::embed::VectorIndexState,
    /// Present only when `index` is not `present` and `embedded_rows >=
    /// 10 000`: the planner cannot use a vector index, so vector search is
    /// a full sequential cosine scan over `embedded_rows` rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded: Option<String>,
    /// Unix seconds when `embedded_rows`/`index` were last measured. The
    /// auto-embed loop publishes the probe at most every 60 s, so this can
    /// trail the pull time by up to `VECTOR_INDEX_SNAPSHOT_TTL` (5 min);
    /// a pull that finds the snapshot stale pays the exact probe, so the
    /// value is never older than the TTL.
    pub count_at: u64,
}

/// `GET /diagnostic` → `checkout_wait`: explicit `pool.acquire()` wait times
/// since process start (Architecture M1). The number behind the "the shared
/// 20-connection pool is the main scalability limiter" revisit decision (the
/// PERF-10 trigger in `search::SearchEngine::search`): a non-trivial
/// `max_us`/`avg_us` under sustained search QPS is the signal to move the
/// search arms onto separate connections. `by_site` attributes each explicit
/// checkout to its call site (the `&'static str` label each site passes to
/// `db::pool::acquire_timed`). Only explicit checkouts are measured (the
/// `search` DB arm + vector ANN seek, `suggest`, the `ensure_trgm` slow path,
/// and the `/health` db probe); failed acquires (10 s timeout → 503) are not
/// — those surface via the `pool` saturation snapshot instead.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct CheckoutWait {
    /// Explicit checkouts completed since process start.
    pub count: u64,
    /// Longest single checkout wait since start, microseconds.
    pub max_us: u64,
    /// Average checkout wait since start, microseconds (0 when `count == 0`).
    pub avg_us: u64,
    /// Per-call-site breakdown (sorted by `site` for stable output): which
    /// explicit checkout is actually holding the pool. Omitted until the
    /// first explicit checkout has been recorded.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub by_site: Vec<CheckoutWaitBySite>,
}

/// One per-site row of `GET /diagnostic` → `checkout_wait.by_site`.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct CheckoutWaitBySite {
    /// Call-site label (`&'static str` passed to `db::pool::acquire_timed`).
    pub site: String,
    /// Explicit checkouts completed by this site since process start.
    pub count: u64,
    /// Longest single checkout wait by this site since start, microseconds.
    pub max_us: u64,
    /// Average checkout wait by this site since start, microseconds (0 when
    /// `count == 0`).
    pub avg_us: u64,
}

/// One per-ZIM integrity row of `GET /diagnostic` →
/// `content_integrity.zims` (full digests; the web UI shows a prefix + "…").
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ZimIntegrityRow {
    /// ZIM name.
    pub name: String,
    /// Observed SHA-256 of the installed bytes (64 lowercase hex). Absent
    /// when the install predates integrity recording.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    /// Publisher-declared SHA-256 verified against the installed bytes at
    /// install time. Present only when the source declared one (the current
    /// Kiwix catalog shape declares none) — always equal to
    /// `content_sha256` when both are present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publisher_sha256: Option<String>,
    /// Drift: a later install of the SAME identity (source URL / torrent
    /// info-hash) produced bytes differing from the identity's previously
    /// observed digest. Flag, never auto-rejected — the operator's judgment
    /// call (a publisher republishing the same URL).
    pub digest_drift: bool,
}

/// `GET /diagnostic` → `content_integrity`: per-ZIM content-integrity
/// record (SEC). Which ZIMs have an observed digest, which were verified
/// against a publisher claim, and which are drifted (same identity served
/// different bytes at a later install — flagged, never auto-rejected). Rows
/// with no integrity record at all (pre-feature installs) do not appear.
/// Omitted entirely when the probe fails (DB unreachable — `/health`'s
/// `db_connected` already reports that).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ContentIntegrityDiagnostic {
    /// ZIM rows carrying at least one integrity field (observed digest,
    /// verified publisher claim, or drift flag), sorted by name.
    pub zims: Vec<ZimIntegrityRow>,
    /// Rows with a verified publisher claim (`publisher_sha256 IS NOT NULL`).
    pub publisher_verified: i64,
    /// Rows currently flagged drifted.
    pub drift: i64,
}

/// `GET /diagnostic` response: operator-facing introspection that `/health`
/// intentionally does not carry (it is an unauthenticated, rate-limit-exempt
/// LB probe, so it must not disclose which settings rows are corrupt).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct DiagnosticResponse {
    /// Version of the running crate.
    pub version: String,
    /// Settings keys whose stored value does not match its expected JSON type
    /// (ARCH Major #3): each silently runs on its default, and the operator
    /// needs the signal. Empty (omitted) when every value deserializes.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub settings_mismatches: Vec<String>,
    /// Primary Postgres pool saturation snapshot (Architecture M1): the
    /// pre-503 signal for a long query (e.g. a reindex `COPY`) holding the
    /// pool at its ceiling. Always present (additive).
    pub pool: PoolHealth,
    /// Read-replica pool saturation snapshot — present only when a read
    /// replica is configured (`DATABASE_URL_READ`). Foreground reads
    /// (search, suggest, snippet, random, the article-read DB fallback,
    /// the `/health` db probe) check out from this pool when it is set, so
    /// replica saturation is the pre-503 signal for the read path (mirrors
    /// `pool` for the primary). Additive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_read: Option<PoolHealth>,
    /// Dedicated background pool saturation snapshot (PERF-10): the pool on
    /// the primary URL capped at `db::pool::BG_POOL_MAX_CONNECTIONS` that
    /// the auto-embed loop and the vector-index builds check out from —
    /// a saturated background pool is the pre-stall signal for background
    /// work (it can no longer consume foreground checkouts). Always present
    /// (additive).
    pub pool_bg: PoolHealth,
    /// Partial vector-index degradation state (Architecture M1): the
    /// embedded row count, the `idx_articles_embedding` catalog state, and —
    /// when no valid index exists over ≥ 10k embedded rows — an explicit
    /// note that vector search is a sequential cosine scan. Omitted when
    /// the catalog probe itself fails (DB unreachable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_index: Option<VectorIndexDiagnostic>,
    /// Explicit pool checkout-wait metric since process start (Architecture
    /// M1): the number behind the pool-size revisit decision, with a
    /// per-call-site breakdown (`by_site`). Always present (additive).
    pub checkout_wait: CheckoutWait,
    /// Query-embedding FIFO cache entry count (operator signal for cache
    /// effectiveness). Always present (additive).
    pub query_embed_cache_entries: usize,
    /// Per-ZIM content-integrity record (SEC): observed SHA-256, verified
    /// publisher claim (when declared), and the drift flag. Omitted when
    /// the probe fails (DB unreachable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_integrity: Option<ContentIntegrityDiagnostic>,
    /// Cross-process invalidation-listener liveness (LISTEN/NOTIFY,
    /// `db::notify`): session state + notification/resync counters. Omitted
    /// when no listener is running (non-serve processes, and a serve whose
    /// startup could not reach the database — a `reconnecting` session is
    /// reported by the field's own `state`, an *absent* listener is omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<crate::db::notify::NotifyStatusSnapshot>,
}

// ─── Probes (pure reads + DTO mapping) ──────────────────────────────────────

/// Map the shared pure pool read (`db::pool::pool_saturation`, M-diag,
/// 2026-10 review) onto the `/diagnostic` DTO. No connection is opened, so
/// this is safe on a dead/unreachable pool and costs nothing (called only
/// by the operator-pulled `/diagnostic` collector).
fn pool_health(pool: &crate::db::Pool) -> PoolHealth {
    let s = crate::db::pool::pool_saturation(pool);
    PoolHealth {
        size: s.size,
        idle: s.idle,
        max_size: s.max_size,
        checked_out: s.checked_out,
    }
}

/// Snapshot the process-global explicit-checkout-wait metric (Architecture
/// M1). Pure atomic reads — no connection is opened, so this is safe on a
/// dead/unreachable pool and costs nothing (called only by the
/// operator-pulled `/diagnostic` collector, like [`pool_health`]).
fn checkout_wait_snapshot() -> CheckoutWait {
    let s = crate::db::pool::checkout_wait_stats();
    CheckoutWait {
        count: s.count,
        max_us: s.max_us,
        avg_us: s.avg_us(),
        by_site: crate::db::pool::checkout_wait_stats_by_site()
            .into_iter()
            .map(|(site, s)| CheckoutWaitBySite {
                site,
                count: s.count,
                max_us: s.max_us,
                avg_us: s.avg_us(),
            })
            .collect(),
    }
}

/// Probe the per-ZIM content-integrity record for `GET /diagnostic` (SEC):
/// observed SHA-256, verified publisher claim, and the drift flag. Rows
/// with no integrity record at all (pre-feature installs) do not appear.
/// Called only by the authenticated, operator-pulled `/diagnostic`
/// collector — one scan over the integrity columns is fine there.
async fn content_integrity_snapshot(
    zims: &crate::zim::ZimManager,
) -> crate::error::Result<ContentIntegrityDiagnostic> {
    // The SQL lives in the ZIM data layer (raw-SQL boundary: handlers must
    // not own SQL). One indexed scan over the integrity columns, operator-
    // pulled and authenticated.
    let rows = zims.integrity_snapshot_rows().await?;
    let zims = rows
        .into_iter()
        .map(
            |(name, content_sha256, publisher_sha256, digest_drift)| ZimIntegrityRow {
                name,
                content_sha256,
                publisher_sha256,
                digest_drift,
            },
        )
        .collect::<Vec<_>>();
    let drift = zims.iter().filter(|z| z.digest_drift).count() as i64;
    let publisher_verified = zims.iter().filter(|z| z.publisher_sha256.is_some()).count() as i64;
    Ok(ContentIntegrityDiagnostic {
        zims,
        publisher_verified,
        drift,
    })
}

// ─── Collector ──────────────────────────────────────────────────────────────

/// Collect the full [`DiagnosticResponse`] (M-diag, 2026-10 review): the
/// ONE place every `/diagnostic` probe is gathered — pool saturation
/// (primary + read + background, with a saturation warning log per pool),
/// the vector-index degradation state (fresh in-memory snapshot vs. exact
/// probe, via `embed::vector_index_diagnostic`), the explicit
/// checkout-wait metric, the query-embedding cache size, the per-ZIM
/// content-integrity record, and the LISTEN/NOTIFY listener status.
///
/// The `diagnostic()` handler in `handlers::zims` is a thin auth + delegate
/// shell over this; nothing here runs on a hot path (operator-pulled,
/// authenticated), and no DB call here opens a connection except the
/// vector-index exact-probe fallback (one bounded checkout) and the
/// content-integrity scan.
pub async fn diagnostic_snapshot(state: &AppState) -> DiagnosticResponse {
    // Architecture M1: surface pool saturation while it is still a warning,
    // not a 503. Emitted here (operator-pulled, authenticated) rather than
    // on the hot path: no per-request logging on the routes that would queue
    // on the pool. sqlx 0.8 exposes no pending-acquire count, so "saturated"
    // is the observable state of a full pool with zero idle connections —
    // the point at which further acquires queue and then time out
    // (10 s → 503).
    let pool = pool_health(&state.db);
    if pool.saturated() {
        tracing::warn!(
            size = pool.size,
            max_size = pool.max_size,
            "db pool saturated: {}/{} in use, 0 idle — queued acquires will hit the \
             10s acquire timeout and surface as 503s (Architecture M1)",
            pool.size,
            pool.max_size
        );
    }
    // PERF-10 / read-replica finding: the other two pools' saturation too —
    // warning-log each (M-diag, 2026-10 review): a saturated background
    // pool means background work is stalling, and replica saturation is the
    // read-path pre-503 signal. `pool_read` is present only when
    // `DATABASE_URL_READ` is set.
    let pool_read = state.db_read.as_ref().map(pool_health);
    if let Some(read) = &pool_read {
        if read.saturated() {
            tracing::warn!(
                size = read.size,
                max_size = read.max_size,
                "read-replica pool saturated: {}/{} in use, 0 idle — foreground read \
                 checkouts will queue (PERF-10)",
                read.size,
                read.max_size
            );
        }
    }
    let pool_bg = pool_health(&state.db_bg);
    if pool_bg.saturated() {
        tracing::warn!(
            size = pool_bg.size,
            max_size = pool_bg.max_size,
            "background pool saturated: {}/{} in use, 0 idle — background work \
             (auto-embed, vector-index builds) is stalling (PERF-10)",
            pool_bg.size,
            pool_bg.max_size
        );
    }

    // Architecture M1 (vector index) + SEC (content integrity): the two
    // independent DB probes run CONCURRENTLY (PERF, 2026-10 review) — the
    // vector-index exact-probe fallback (one bounded checkout) and the
    // content-integrity scan used to serialize; they are read-only and
    // touch disjoint state, so one `tokio::join!` round of pool work
    // replaces two sequential rounds. The fresh/stale snapshot policy for
    // the vector-index probe lives in the embed layer —
    // `embed::vector_index_diagnostic` (M-diag, 2026-10 review). On probe
    // failure the corresponding field is omitted (the DB is unreachable —
    // `/health`'s `db_connected` already reports that).
    let (vi_res, ci_res) = tokio::join!(
        crate::embed::vector_index_diagnostic(&state.vector_index_snapshot, &state.db),
        content_integrity_snapshot(&state.zims)
    );
    let vector_index = vi_res.map(|d| VectorIndexDiagnostic {
        embedded_rows: d.embedded_rows,
        index: d.index,
        degraded: d.degraded,
        count_at: d.count_at,
    });
    let content_integrity = match ci_res {
        Ok(c) => Some(c),
        Err(e) => {
            tracing::warn!("content integrity diagnostic probe failed: {e}");
            None
        }
    };

    // Architecture M1 (checkout wait): pure atomic reads — no connection.
    let checkout_wait = checkout_wait_snapshot();

    DiagnosticResponse {
        version: env!("CARGO_PKG_VERSION").into(),
        settings_mismatches: state.settings.type_mismatches(),
        pool,
        pool_read,
        pool_bg,
        vector_index,
        checkout_wait,
        query_embed_cache_entries: state.search.query_embed_cache_len(),
        content_integrity,
        notify: state.notify_status(),
    }
}

// ─── Tests (M-diag, 2026-10 review) ────────────────────────────────────────
//
// The extraction moved the pure probe fns out of `handlers/zims.rs`; this
// module pins them so no untested pure logic is left behind. `pool_health`'s
// live-pool checkout behavior is covered by `db::pool::tests::
// pool_saturation_live_pool_checked_out`.

#[cfg(test)]
mod tests {
    use super::*;

    /// `pool_health` on a never-connected (dead) pool: pure atomic reads, all
    /// counters zero, and the `checked_out == size - idle` invariant holds.
    #[test]
    fn pool_health_dead_pool_is_all_zero() {
        let pool = crate::testing::dead_pool();
        let h = pool_health(&pool);
        assert_eq!(h.size, 0);
        assert_eq!(h.idle, 0);
        assert!(h.max_size > 0, "a pool always has a size ceiling");
        assert_eq!(h.checked_out, 0);
        assert_eq!(h.checked_out, h.size.saturating_sub(h.idle));
    }

    /// `checkout_wait_snapshot` maps the process-global metric losslessly:
    /// `avg_us` is zero with no checkouts and never exceeds `max_us`.
    /// (Other tests may have recorded explicit checkouts on the same process,
    /// so `count` is asserted conditionally, not absolutely.)
    #[test]
    fn checkout_wait_snapshot_invariants() {
        let s = checkout_wait_snapshot();
        if s.count == 0 {
            assert_eq!(s.avg_us, 0, "no checkouts → zero average");
            assert!(s.by_site.is_empty(), "no checkouts → no per-site rows");
        } else {
            assert!(s.avg_us <= s.max_us, "average cannot exceed the max");
        }
        // The per-site rows must agree with the aggregate: every site's
        // count is part of the total.
        let site_sum: u64 = s.by_site.iter().map(|r| r.count).sum();
        assert!(site_sum <= s.count, "per-site counts sum to ≤ the total");
    }
}
