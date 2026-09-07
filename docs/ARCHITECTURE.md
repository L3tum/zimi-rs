# zimservice Architecture

zimservice is a single-instance, cross-ZIM search and serve platform written in
Rust (`README.md`, `src/lib.rs`). One process: an axum HTTP server (REST API +
embedded static web UI), Postgres (full-text search, pg_trgm, optional pgvector),
ZIM archive reading (the `zim` crate, mmap-backed), SIMD HTML→text extraction
(`deformat`), rayon-parallel indexing, a qBittorrent download poller with
hardlink-based file sharing, an OpenAI-compatible embedding pipeline, and an MCP
(stdio) server for AI agents. The CLI (`src/main.rs`) exposes `serve`, `list
[--sync]`, `status`, `index`, `mcp`, and `embed`.

## Module map

- **`src/config.rs`** — startup/process configuration: compiled-in defaults
  (`/zims`, `postgres://localhost:5432/zimservice`, pool size 20, loopback
  bind) overridden by env vars. Owns the env→settings lock/snapshot helpers
  that seed and lock runtime settings from the environment; runtime tunables
  deliberately live in the `settings` table, not here.

- **`src/startup.rs`** — startup orchestration and the single-instance guards
  (moved from `src/main.rs` so the guard order around the mutating
  `build_state` is a testable unit). `StartupMode::{Serve, Mutating, ReadOnly}`
  selects whether the ZIM cache is reconciled with disk; `build_state` assembles
  the `AppState` (pool → migrations → settings → ZIM scan → search engine →
  qBittorrent client cache) and never acquires the instance lock itself —
  `cmd_serve` does via `acquire_instance_guard` before state is built, so the
  startup resync (a DELETE/INSERT cycle on `zims`) can never interleave between
  two starting instances.

- **`src/state.rs`** — `AppState`: the 9-field shared state passed to every
  axum handler (db pool, settings, ZIM manager, search engine, qBittorrent
  cache, rate limiter, probes, lockout, degradation); sub-state regrouping is
  re-evaluated on every field change (documented active trigger) and kept flat.

- **`src/db/`** — the persistence layer: `pool.rs` (the shared
  `sqlx::PgPool`, TLS mode from the DSN, 10 s acquire timeout), `migrate.rs`
  (applies `migrations/*.sql`), `raw` (the raw-SQL data-access helpers, see
  Persistence), and domain query modules: `collections.rs`, `downloads.rs`,
  `downloads_lifecycle.rs` (the download status state-machine row helpers),
  `qid.rs` (Wikidata Q-ID / interlanguage lookups), `random_article.rs`.

- **`src/zim/`** — `ZimManager` (`mod.rs`): on-disk discovery, per-file
  (mtime, size) snapshots for resync early-out, an LRU cache of open ZIM
  handles (cap 16, stat invalidation), and reconciliation of the `zims` table
  with disk. `discovery.rs` watches `zim_dir` for changes; `index.rs` is the
  indexing pipeline (see Data flow).

- **`src/search/`** — `SearchEngine`: runs the configured branches (Postgres
  FTS via `websearch_to_tsquery`, pg_trgm prefix/contains/similarity arms on
  `title_lower`, optional pgvector semantic arm), dedups by `(zim, path)`
  keeping the highest score, and interleaves by score. SQL builders in `sql.rs`
  are pure and unit-tested. The pg_trgm availability flag is cached with a
  60 s background re-probe so arms recover if the extension is installed
  without a restart (`src/main.rs`).

- **`src/embed/`** — optional semantic-search pipeline. `run_pipeline` embeds
  article snippets in batches through an OpenAI-compatible `/v1/embeddings`
  endpoint and stores pgvector rows in `articles.embedding`; `auto_embed_loop`
  is the background task spawned by `serve` that picks up newly indexed
  articles; a vector index (hnsw/ivfflat, threshold-driven) is built as row
  counts cross their thresholds. Dimension changes `ALTER` the column.

- **`src/torrent/`** — qBittorrent integration: `mod.rs` (authenticated REST
  client with fingerprint-keyed runtime cache, the `DownloadStatus` state
  machine and its allowed-transition table), `files.rs` (locate/verify/install
  — hardlink preferred, copy fallback; `verify_zim` is structural-only),
  `opds.rs` (Kiwix OPDS catalog auto-updates), and `poller/` (background
  download lifecycle: `reconcile`, `inflight`, `direct` for plain `.zim` HTTP
  downloads, `complete`, `stats`).

- **`src/serve/`** — the HTTP layer. `mod.rs` builds the axum router (routes +
  layers); `handlers/` (search, content, downloads, settings, zims, web UI
  pages); `middleware.rs` (shared-password auth, `access_token` log
  sanitization, per-IP auth-failure lockout); `openapi.rs` (utoipa-generated
  OpenAPI 3.1 served at `/openapi.json`); `ratelimit.rs` (service-wide token
  bucket, live-reloaded from settings).

- **`src/settings/`** — Postgres-backed runtime settings: `defs.rs` (setting
  table, seeds, security-sensitive classification), `cache.rs`
  (`SettingsCache`: in-memory, env-locked writes, secret redaction, TTL-cached
  token verification), `auth.rs` (argon2id hashing, legacy sha2/plaintext
  verification), `snapshots.rs` (read snapshots for search/poller params).

- **`src/mcp/`** — MCP server over stdio: hand-rolled JSON-RPC 2.0,
  newline-delimited; tools: `search`, `read`, `suggest`, `list_sources`,
  `random`, `get_chunks`, `deep_search`, `article_languages`,
  `list_collections`. In password mode it requires `MCP_AUTH_PASSWORD` to match
  the configured admin password (fail-closed, `mcp_auth_ok`).

- **`src/netguard.rs`** — shared SSRF/network guards for all outbound HTTP
  (downloads, OPDS, embeddings): scheme allowlist, always-blocked
  link-local/metadata ranges, private ranges gated by
  `downloads.allow_private_networks`, per-redirect-hop re-validation, and host
  pinning to close the DNS-rebinding window.

- **`src/content.rs`** — the shared content service (article reading with
  live-ZIM + DB-preview fallback, entry reads, chunking, response DTOs)
  consumed by both the HTTP handlers and the MCP tools so neither
  presentation layer depends on the other; HTTP glue (extractors, ranges,
  ETag/304, security headers) stays in `src/serve/handlers/content.rs`.

- **`src/health.rs`** — `/health` probes: only the qBittorrent ping is
  memoized (2 s TTL); the Postgres `SELECT 1` probe is deliberately not.
  `DegradationTracker` records which search capabilities are degraded.

- **`src/util.rs`** — `redact_url`: rewrites `user:pass@` in a URL's authority
  to `***@` so credentials never reach logs (re-exported at the crate root).

- **`src/testing.rs`** — `#[doc(hidden)]` internal test support: `dead_pool`,
  `test_state`, the MCP tool seam, and `DbExclusiveGuard` (see Testing).

## Data flow

**Downloads → library → index.** `POST /downloads` inserts a `queued` row in
the `downloads` table. `serve` spawns the `DownloadPoller`
(`src/torrent/poller/mod.rs`), which on each `torrent.poll_secs` tick:
reconciles rows against live qBittorrent state (startup), promotes `queued`
rows to qBittorrent torrents (subject to `torrent.max_active`) or to direct
`.zim` HTTP downloads, refreshes progress/ETA, and on completion applies the
seed-ratio cap, locates the `.zim` in the torrent's content dir, runs
`verify_zim` (structural parse check only), installs it into `zim_dir`
(hardlink or copy), resyncs the library, and kicks off background indexing.
The OPDS arm periodically checks the Kiwix catalog and queues auto-updates
when `torrent.auto_update` is enabled. Every status write goes through the
`DownloadStatus` machine in `src/torrent/mod.rs`.

**Indexing.** `src/zim/index.rs`: open the ZIM, enumerate the C-namespace
entry range, extract text/snippet/Q-ID per entry in rayon, then bulk-insert in
chunks — **COPY** into the UNLOGGED `articles_staging` table (via
`PgConnection::copy_in_raw`), then
upsert staging rows into `articles`, with the `search_vector` tsvector
computed in Postgres. A pid-scoped checkpoint file under the temp dir makes a
run resumable; finalization prunes stale rows atomically with the
`index_status = 'ready'` flip so a reindex never leaves the ZIM search-empty.

**Embedding.** `auto_embed_loop` (spawned by `serve`; no-op when embedding is
disabled) picks up newly indexed articles, batch-calls the endpoint, and
writes pgvector rows; `run_pipeline` (the `embed` subcommand) does the same on
demand, building the vector index when row counts cross thresholds.

**Request path.** `serve::build_router` (`src/serve/mod.rs`) assembles routes
from `AppState`; axum layers apply innermost-first: auth middleware (shared
password; only active in password mode) → rate limit (token bucket,
service-wide; unauthenticated requests count too, `/health` exempt) →
`TraceLayer` (spans with `access_token`-sanitized URIs) → CORS (allowlist from
`general.cors_origins`, built once at startup — changing it requires a
restart) → 10 MiB request-body limit. Content handlers read live ZIM entries
through `src/content.rs`; search handlers run the `SearchEngine` branch merge.

## Single-instance invariant

zimservice is designed as a single instance per database *and* per `zim_dir`.
A second `serve` must fail before binding the port because two live servers
would corrupt the shared in-process state — the settings cache, the rate
limiter, the qBittorrent client cache, and the ZIM LRU (`src/main.rs` bail,
`README.md` "Single-instance deployment").

Enforcement (`src/startup.rs`):

1. **Per-database advisory lock.**
   `pg_try_advisory_lock(hashtext('zimservice:instance'))` —
   `INSTANCE_LOCK_SQL` — taken on a *dedicated non-pooled* connection held for
   the process lifetime by `SingleInstanceGuard` (a pooled connection could be
   recycled and silently release the lock). `cmd_serve` refuses to start when
   the lock is already held.
2. **Per-`zim_dir` PID lockfile.** `<zim_dir>/.zimservice.lock` records the
   holder PID; a live holder blocks startup, a dead holder's stale file is
   stolen, and the guard unlinks it on drop. The advisory lock is
   per-database and cannot exclude a different-database deployment on the same
   disk, so the PID lock covers that case (a one-time startup warning
   documents the remaining risk).
3. **Liveness monitor.** A background task probes the lock connection with
   `SELECT 1` every 30 s; if it dies mid-run (Postgres restart, network drop —
   Postgres silently releases the lock) the process exits non-zero
   fail-closed, because a running server whose uniqueness guarantee evaporated
   must not keep serving.
4. **Warn-only `pg_locks` probe.** `build_state` checks
   `pg_locks`/`pg_stat_activity` with a predicate matching the same 1-argument
   advisory lock (`ADVISORY_LOCK_MATCH`) and warns for guardless paths
   (read-only subcommands, opt-outs).
5. **Mutating-CLI guard.** `index`, `embed`, and `list --sync` take the same
   advisory lock via `MutatingGuard` *before* any mutation and refuse when a
   running server holds it, since a server reconciles its in-memory caches
   with Postgres only at startup and would otherwise serve stale data with no
   signal.

Opt-outs: `ZIMSERVICE_ALLOW_MULTI_DB=1` keeps the PID lock but makes the
advisory lock best-effort (different-database deployments sharing a `zim_dir`);
`ZIMSERVICE_ALLOW_MULTI_INSTANCE=1` disables all guards. Read-only subcommands
(`status`, `mcp`, plain `list`) never take the lock — warn-only.

## Persistence layer

Postgres through a single shared `sqlx::PgPool` (`src/db/pool.rs`; default 20
connections, 10 s acquire timeout, TLS mode derived from the DSN's `sslmode`
or `+tls` scheme). All subsystems — HTTP API, poller, embedding, index COPY —
share this one pool.

**Pure sqlx.** Application-table queries go through the `db::raw` helpers
(`src/db/mod.rs`) against the shared pool — no ORM, no extra connections, the
same pool and TLS mode as everything else. The helper shapes (`execute`,
`fetch_optional`, `fetch_all`, `fetch_scalar_optional`, `fetch_scalar_all`)
take a plain SQL string, a `|q| q.bind(a).bind(b)` bind closure, and an
executor (`&pool`, `&mut conn`, or `&mut *tx` for transactions). Postgres-
specific column types have no first-class sqlx types: `articles.search_vector`
(tsvector) and `articles.embedding` (pgvector) are bound/decoded as `String`.

**Raw-SQL policy.** All SQL is raw, but it is confined: application SQL lives
in `src/db/` (which owns the `db::raw` helpers and is therefore exempt from
`scripts/check-raw-sql.sh`); any other direct `sqlx::query*` call site must
carry a `// RAW-OK: <reason>` marker on the call line (session-level advisory
locks, batch DDL — migration files are multi-statement; sqlx has no
`batch_execute`, so statements are split and executed individually —, catalog
probes (`pg_locks`, `pg_stat_activity`), the `COPY` bulk insert, and
Postgres-specific operators — `websearch_to_tsquery`/tsvector, pg_trgm
`similarity()`, pgvector distance operators, and `$n::vector` / `::tsvector`
casts). The SQLSTATE/HTTP mapping in `Error` redacts DB details; 23505
unique-violations are domain duplicates (409), not DB faults (503).

**Migrations.** Numbered `.sql` files in `migrations/` (001–013) are embedded
with `include_str!` and applied by `src/db/migrate.rs` at **every** startup,
in every subcommand mode: tracked in `schema_migrations` by filename + content
hash, idempotent, and serialized by a session-level advisory lock so
concurrent startups cannot interleave DDL. DDL does not live in Rust: even
runtime DDL such as the vector-dimension `ALTER` (`src/embed/mod.rs`) and
invalid-index cleanup (`drop_invalid_indexes`) go through the migration/raw
path.

## Error containment contract

`src/error.rs` defines the crate-wide `Error` enum and its HTTP mapping
(`status_and_message`):

- **Client-error variants** (`NotFound`, `Forbidden`, `InvalidInput`,
  `Conflict`) return their text to the client.
- **`Error::Internal(anyhow::Error)`** is the containment boundary: it
  deliberately has *no* `From<anyhow::Error>` impl, so anyhow errors from
  CLI/startup code cannot leak into the HTTP path; it always maps to a
  redacted `500 "internal server error"` with the detail logged only.
- **SQLSTATE mapping** (`Error::Database`): `23505`/`23503` → 409 (the
  23505 message is derived from the constraint name via `duplicate_field`,
  never raw DB text), `23502`/`23514` → 400, everything else → 503. Pool
  checkout timeouts → `503 "database unavailable"`; connection-level failures
  → 503.
- **Upstream failures** (`Torrent`, `Http`) → 502 with a generic message;
  raw upstream text is logged, never returned.
- **DSN redaction**: `util::redact_url` strips `user:pass@` before any URL
  reaches logs or CLI output; `sanitize_uri_for_logs` (serve middleware)
  strips `access_token` from logged request URIs.

## Non-goals

Explicitly declared out of scope:

- **High availability / clustering.** Single instance by design: at most one
  `serve` per database *and* per `zim_dir` (`src/main.rs`, `src/startup.rs`,
  `README.md` "Single-instance deployment"). The guards exist so a violation
  fails fast, not to enable it.
- **Per-client rate limiting.** The token bucket is service-wide by design
  (single-operator model); one heavy client can 429 every other client
  (`src/serve/ratelimit.rs`, `README.md` "Threat model").
- **Content authentication of downloaded ZIMs.** `verify_zim` checks
  structural integrity only — the `zim` crate exposes no checksum/signature
  API. Trust is the source (torrent tracker, OPDS feed), not the file
  (`src/torrent/files.rs`, `README.md`).
- **TLS in-process.** The server speaks plain HTTP; terminate TLS at a reverse
  proxy (`README.md`; startup warning in `src/main.rs`).
- **Unauthenticated reads on network-facing binds.** In password mode, reads
  are open by default on a **loopback** bind (local trust), but **gated by
  default on non-loopback binds**: `access.require_auth_for_reads` starts as
  `true` there unless `REQUIRE_AUTH_FOR_READS` is set explicitly (M-1
  hardening — the bind-based startup default is injected into the env
  snapshot in `src/startup.rs` via
  `config::Config::apply_require_reads_default`; the all-zero CIDR refusal
  (M-3) for `general.trusted_proxy_cidrs` is enforced in `src/main.rs`).
  `README.md` "Security model".
- **`?access_token=` query-string token transport.** Auth is a single shared
  admin password presented as `Authorization: Bearer <password>` (preferred)
  or `?access_token=<password>` — the query-string form is accepted **only on
  read-only GET/HEAD/OPTIONS requests** (`src/serve/middleware.rs`, SEC-M3).
  In-app request logs are sanitized (`sanitize_uri_for_logs` strips the token,
  including percent-encoded forms), but that is in-app only: a fronting
  **reverse proxy's access log, browser history, and `Referer` headers capture
  the full query string**, which the app cannot control. Consequence:
  `?access_token=` is a last-resort channel (it exists for same-tab anchor
  navigations that cannot carry a header) — prefer `Authorization: Bearer`
  wherever possible, and if a proxy fronts the service, configure it to
  omit/scrub `access_token` from its access log. `README.md` "Security model".
- **Public API stability.** The crate is 0.x: no stable public API; `testing`
  is internal test support (`src/lib.rs`).

## Testing architecture

- **DB gating.** Tests that need a live Postgres connect to
  `DATABASE_URL` (default the dev loopback instance) and *skip cleanly* when
  it is unreachable; setting `ZIMSERVICE_REQUIRE_DB=1` turns a skip into a
  hard failure — that is the strict/CI gate (`Makefile`: `test-strict`,
  `test-strict-ci`). `src/testing.rs` provides `dead_pool()` (lazy pool to an
  unreachable URL, 250 ms acquire timeout) and `test_state()` (full in-memory
  `AppState`, no DB) so unit tests never need a database.
- **`DbExclusiveGuard`.** Every DB-gated test takes this guard: an
  in-process mutex slot plus a per-`DATABASE_URL` cross-process lockfile
  (atomic `create_new`, FNV-1a-scoped name, 120 s stale-steal) — so the suite
  is safe under parallel `--test-threads` *across* the lib and integration
  test binaries, which otherwise interleave migrations and fixture writes
  against the shared dev DB.
- **wiremock.** `tests/wiremock.rs` exercises the qBittorrent client against
  `MockServer` instances on loopback (the SSRF guard admits loopback for
  this); no real qBittorrent is needed.
- **CI mirror.** `make test-strict-ci` is the local twin of the CI test-db
  job (strict integration); the Makefile requires
  the two selections to stay in sync.
- **Coverage ratchet.** The CI `coverage` job runs `cargo llvm-cov` against a
  Postgres service (`.github/workflows/ci.yml`) over the same selection as
  `test-db` with `ZIMSERVICE_REQUIRE_DB=1`, so DB-gated code counts toward the
  line-coverage floor in `.github/coverage_floor.txt`. The floor can only
  effectively rise: the job fails if measured coverage drops below it, so it
  should be bumped to the newly measured value after coverage gains land.
