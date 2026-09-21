# AGENTS.md — notes for agent runs in this repo

## Tooling gotcha: secrets can arrive masked as `***`

**Symptom.** A command that embeds a credential as a `user:password@` token in a
connection string (e.g. `postgres://user:password@host/db`) can reach the child
process with the password replaced by a **literal `***`** (`[42,42,42]`). The
process then fails with a spurious
`password authentication failed` — which is **not** a Postgres, network, or
wrong-password problem. The credential in `pg_hba`/the server is fine; the
*delivered* value was mangled by the command-execution layer before `exec`.

**How to recognize it (do this before chasing the DB).** Verify at the byte
level what the process actually received — have the process print the value, e.g.:

```
python3 -c 'import os; print(repr(os.environ.get("DATABASE_URL")))'
# or, for a password held separately: print(list(pw.encode()))
```

If you see `***` / `[42,42,42]` where the real password should be, the tooling
masked it — stop diagnosing the server and use the workaround below.

**Reliable workaround.** Do not depend on the password surviving verbatim in the
command text:
- Construct the DSN **in-process** from components (scheme, host, user, port, db
  + a password read from a file/secret the process opens directly), so the
  `user:password@` pattern is never a literal in the tool command; **or**
- Read the credential from a file (e.g. `~/.pgpass` for `psql`, or a local secret
  file the driver opens) rather than passing it inline.

Setting `os.environ["DATABASE_URL"] = "postgres://user:pass@host/db"` from inside
a `python3 -c` driver *has* worked in this repo; prefer the file/secret form for
anything sensitive, and always confirm via the byte-level diagnostic when auth
fails.

**Context.** Observed while configuring the local Postgres (see the `zimservice`
role below) for the `cargo test` DB gate.

## Test gotcha: `start_paused` + a spawned background task only works on a local DB

**Symptom.** A `#[tokio::test(start_paused = true)]` (mock time) test that
*spawns* a background task — e.g. the auto-embed loop, which does
`tokio::time::sleep(tick)` between passes — **times out / never ticks on a
*remote* (slow-RTT) database**, while it passes on a *local* (loopback) one. The
test looks flaky and environment-dependent.

**Root cause.** tokio's paused-clock *auto-advance* does **not** reliably wake a
**spawned** task's timer. The virtual clock can jump far past the spawned task's
`sleep` deadline, yet the spawned task is never woken — so the loop never ticks.
On a *local* DB this is **masked**: fast loopback I/O keeps the worker busy enough
that the spawned task happens to get scheduled. On a *remote* DB the slow real I/O
lets the test's wait budget expire before the spawned loop ever ticks. (A
*main-task* `sleep`/`timeout` is fine — auto-advance handles those correctly; only
*spawned* timers are affected.)

**How to recognize it.** A scratch repro: spawn a task that does
`tokio::time::sleep(60s)` then prints, and drive the clock in the main task. The
clock (read via `tokio::time::Instant::now()`) advances far past 60 s, but the
spawned task never prints. That is the signature: the clock moved, the spawned
timer didn't fire.

**Reliable workaround.** Run such tests on a **real clock** (`#[tokio::test]`,
not paused) and make the cadence a *parameter*: the production loop takes
`tick: Duration` (production passes `60s`; tests pass `~50ms`). With a real clock
the loop's `sleep`, its I/O, and the test's poll waits are all wall-clock, so there
is no paused-clock/auto-advance interaction — green on a local *or* remote DB. A
`tokio::time::resume()`/`pause()` guard around *just* the connect is **not**
enough: the loop's own I/O is still under the paused clock.

**Related latent bug (pgvector dimension).** pgvector stores a `vector(N)`
column's dimension **directly** as `atttypmod` — there is **no** `− 4` (VARHDRSZ)
offset. Reading it as `atttypmod − 4` yields a wrong dimension (e.g. `1520`
instead of `1524`), and inserts fail with `expected 1524 dimensions, not 1520`.
Read `atttypmod` as-is.

**Context.** Discovered while making the `auto_embed_loop` lifecycle tests pass
against a remote dev DB. The two other `start_paused` tests in the crate
(`search/mod.rs`) are pure timeout-mechanism tests with no spawned task and no
real DB, so they are unaffected.

## Running the DB-gated tests

Most DB tests skip silently unless a database is reachable and required:

```
DATABASE_URL=postgres://zimservice:zimservice@<host>:5432/zimservice \
ZIMSERVICE_REQUIRE_DB=1 cargo test
```

**Where the URL lives.** The repo deliberately ships no real host — the
`<host>` above is a placeholder. On the dev machine the concrete URL is in
`dev-database.sh` (gitignored, never commit); source it with
`set -a; source dev-database.sh; set +a` before `cargo test`. That target is
`192.168.0.38:5432/zimservice` (LAN).

**The dev DB is test-only — disposable.** The suites wipe, rewrite, and
re-encrypt its rows freely (the SEC-L5 at-rest-encryption tests sweep and
restore the three secret `settings` keys; the temp-DB test modules create and
drop databases). Treat it as scratch: never point a real deployment at it, and
never expect data written there (real tokens, qBittorrent passwords, …) to
survive a test run.

`zimservice` must be a **superuser**: the migrations run
`CREATE EXTENSION vector`, and pgvector's `vector` extension is `trusted = false`
on the local server, so only a superuser can create it. In a reset/provision
script that recreates the role, use `ALTER ROLE zimservice SUPERUSER;` (or
`CREATE ROLE zimservice LOGIN SUPERUSER PASSWORD 'zimservice';`).

## Gotcha: trailing whitespace is load-bearing — never bulk-strip it

Several multi-line Rust string literals use the `\` line-continuation form
with a **trailing space before the backslash** (e.g. `startup.rs`,
`src/torrent/poller/*.rs`): `"...text \"` — that space is part of the
continuation's emitted text. A bulk "strip trailing whitespace" pass (editor
setting, `sed -i 's/ $//'`, formatter) will **silently corrupt** SQL and log
strings by deleting spaces inside the literal. `cargo fmt` does not touch it
and eslint does not flag the web side — this is intentional, not sloppiness.
If a diff removes a space before a `\` continuation, restore it.

## Gates: what must be green before pushing

The CI gate is the contract; the local mirror is `make test-strict-ci`
(strict DB: `ZIMSERVICE_REQUIRE_DB=1` — a missing Postgres is a hard failure,
never a vacuous pass) plus the non-DB jobs. **Pre-push policy:
`make all` (fmt + clippy + `make test`) AND `make test-strict-ci`** — a
`cargo test` run without a reachable DB passes vacuously by design (loud
WARNING banner, DB-gated tests skip), so the strict mirror is the gate that
means something. Web-side: `web/lint` (eslint, `ZIMSERVICE_WEB_CHECK_STRICT=1`)
and the rustdoc gate (`RUSTDOCFLAGS="-D warnings" cargo doc`) are red on
intra-doc link breakage and >100-col `max-len` lines in `web/*.js` — both
have burned a green-local/red-CI merge before; run them, don't skip them.

**Performance gate policy:** CI has no timing ratchet (the `trgm_plan`
EXPLAIN gate pins plan SHAPE only, and plan-shape gates demonstrably don't
catch everything — see the retracted 2026-09 perf F1). The weekly non-blocking
`bench` job (`cargo bench` on the 10k fixture) is the trend detector: a
sustained regression in its output is a signal to investigate, not an
automatic block. If a hot path changes, run `cargo bench` locally and compare
before pushing.
