# Process-global statics — inventory

The codebase is fully dependency-injected: everything that varies is built
in `AppState` (or a test) and passed down. A small set of `static`
process globals exists as a **documented exception** to that standard. This
file is the inventory that keeps the exception enforceable: anything not
listed here is a violation until it is justified and added.

## The standard (the "DI exception" rule)

A new process global is justified **only** when the state must hold across
the *whole process* — every request handler, middleware layer, and
background task — rather than per instance (the wording in
`KDF_SEM`'s doc, `src/settings/cache.rs`). In addition, a qualifying global
must be:

- a **constant** (no config dependency), or a **bounded, self-healing
  metric/counter**;
- read/written by **one** function (nothing else may touch it);
- **documented with its limit** — a reset-on-restart bound, an overflow
  cap, or an explicit revisit trigger.

Add a new global and this inventory in the **same commit**, with the file,
purpose, and revisit condition. Prefer DI; the bar for a global is
"per-instance would be *wrong*", not "less boilerplate".

## Mutable process globals (the DI exceptions)

| Static | Location | Purpose | Documented limit / revisit condition |
|---|---|---|---|
| `KDF_SEM` | `src/settings/cache.rs` | `LazyLock` tokio semaphore (16) capping concurrent admin-password KDF verifications. Kept outside `SettingsCache`'s fields because the cap must hold process-wide, and it is a constant. | Nothing else may read or write it. No explicit trigger; the global lives as long as the cap must be process-wide and unconfigured. |
| `EMBED_FAILS` | `src/embed/pipeline.rs` | `LazyLock<Mutex<HashMap<i64, u32>>>` per-row embed-failure counter (W6.5): drops poison rows from claims after 3 failures. | Resets on restart (≤ 3 wasted cycles, self-healing); fail-open map cap (100k) clears on overflow. Durable alternative (`embed_fail_count` column = a new migration) was deliberately not adopted — see the 014 note below. |
| `CHECKOUT_WAIT` | `src/db/pool.rs` | Process-global aggregate of explicit pool-checkout waits (Architecture M1), behind the "shared 20-connection pool is the main scalability limiter" decision. | **PERF-10 revisit trigger:** a non-trivial max/avg checkout wait under sustained search QPS is the signal to move the search arms onto separate connections (`src/search/mod.rs`). |
| `CHECKOUT_WAIT_BY_SITE` | `src/db/pool.rs` | Per-call-site buckets of the same metric, keyed by `&'static str` labels, so `/diagnostic` can attribute the wait to the checkout that caused it. | Label set is fixed by construction (finite explicit checkout sites — never grows under load). Same PERF-10 trigger as `CHECKOUT_WAIT`. |
| `TMP_SEQ` | `src/torrent/files.rs` | `AtomicU64` making install tmp names unique per call (`tmp-<pid>-<n>`); a deterministic `tmp-<pid>` name let a concurrent install of the same ZIM delete the first install's in-flight tmp (B5). | Resets to 0 on restart — safe because names also carry the pid and stale-tmp cleanup is mtime-gated (`STALE_TMP_AGE`) and skips the current call's tmp. |
| `DEPRECATION_WARNED` | `src/serve/handlers/search.rs` | `AtomicBool` once-per-process warn for the deprecated `query` parameter alias (subsequent uses log `debug!`), so a pinned client can't flood the log. | Resets on restart (≤ 1 extra warn per process). Retire when the `query` alias is removed. |

## Memoized statics (not shared mutable state)

Built once, immutable, deterministic — no cross-instance or cross-test
state. Listed so a grep for `static` stays auditable:

- `QID_RE` — `src/zim/index.rs` — `OnceLock<regex::Regex>`, regex memo.
- `SETTING_DEFS` — `src/settings/defs.rs` — `LazyLock` settings table, the
  single source of truth for settings policy (constant data, not state).
- `DEFAULTS` (fn-local) — `src/settings/defs.rs` — `LazyLock` memo of the
  seed-defaults map.
- `SPEC` / `SPEC_JSON` (fn-local) — `src/serve/openapi.rs` — `LazyLock`
  memo of the OpenAPI spec.
- `STAMPED` (fn-local) — `src/serve/handlers/web.rs` — `LazyLock` memo of
  the asset-stamped web page.

## Test-only statics (out of the DI scope)

`LIB_SKIPPED`, `DB_LOCK`, `DB_CV` (`src/testing.rs`), `EMBED_TESTS_LOCK`
(`src/embed/pipeline.rs`), `COUNTER` (`src/settings/auth.rs`,
`#[cfg(test)]`), and the `A`/`B` flags in `src/startup.rs` test modules.
These exist only under `cfg(test)` / test support and are not DI
exceptions.

## The deferred 014 migration

Migration number 014 is reserved by the deferred PERF-4 decision
(`migrations/014_articles_title_prefix_drop.sql`, default KEEP — see
`docs/perf-notes.md`). So any durable replacement for a global above —
e.g. persisting `EMBED_FAILS` as an `embed_fail_count` column — is
migration 015 or later, not 014.
