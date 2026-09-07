# zimservice — Quality & Development Makefile
# Run with `make` or `make <target>`

CARGO ?= cargo
BIN := zimservice

# The compose file requires POSTGRES_PASSWORD (a `:?` hard-fail) so a real
# deployment can't boot Postgres with the default credential. For the loopback
# dev DB targets below (test-integration / test-strict*), which connect with the
# hardcoded `zimservice:zimservice` URL, default it to `zimservice` — but only
# when the operator hasn't already set it in the environment.
export POSTGRES_PASSWORD ?= zimservice

# DSN for the loopback dev Postgres (user/password default to
# zimservice:zimservice, matching the compose setup). Overridable via the
# environment. Used by every DB-gated test target below.
DEV_DSN ?= postgres://zimservice:zimservice@127.0.0.1:5432/zimservice

WAIT_PG := @ok=0; for i in $$(seq 1 30); do docker compose exec -T postgres pg_isready -U zimservice >/dev/null 2>&1 && ok=1 && break; sleep 1; done; [ $$ok = 1 ] || { echo "postgres failed to become ready" >&2; exit 1; }

# Shared boilerplate for every DB-gated dev target (a make macro, invoked as
# $(call DB_WRAP,<commands>)): boot the compose Postgres, wait for it, run
# the caller's commands ($1), then tear the container down. NOTE: a naive
# recipe where <commands> is a separate line would skip the final
# `docker compose stop` when <commands> fails (leaking the running container);
# instead the stop is chained after $1 on the same shell line via
# `st=$?; ...; exit $st`, so stop runs even on failure while the target still
# reports the original failure status.
# Use only inside recipe lines.
define DB_WRAP
docker compose up -d postgres
$(WAIT_PG)
$1; st=$$?; docker compose stop postgres; exit $$st
endef

.PHONY: all help check fmt fmt-check clippy raw-sql-lint test test-fast test-integration test-strict test-strict-ci build release install uninstall doc run clean web-check web-fmt web-test web-lint

# Quick pre-commit checks
check:
	$(CARGO) check --all-targets --all-features

fmt: web-fmt
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

# M2: raw-SQL boundary lint — flags sqlx::query*/query_as/query_scalar call
# sites outside src/db/ (the module owning the sanctioned db::raw escape
# hatch) that lack a `// RAW-OK: <reason>` marker on the call line. POSIX sh
# + grep only so it runs on the dev box and in CI alike.
raw-sql-lint:
	@sh scripts/check-raw-sql.sh

# The integration half skips cleanly with no Postgres; `--nocapture` surfaces
# the one-shot "DB unreachable — suite skipped" banner (a fully-skipped run
# otherwise reports a misleading "N passed"). CI sets ZIMSERVICE_REQUIRE_DB=1,
# where a skip is a hard failure, so this only affects local `make test`/`all`.
#
# Thread model: the unit half (lib/bins/wiremock/doc) runs with the default
# parallel --test-threads — it holds no shared process state (per-test
# tempfile::tempdir, ephemeral-port wiremock MockServers, read-only
# tests/fixtures, thread-safe statics) and its DB-gated tests skip without a
# database (see src/testing.rs). The integration half is *safe* under the
# default parallel --test-threads: every DB-gated test is serialized by
# DbExclusiveGuard (cross-process lockfile + in-process slot), and
# smoke_migration_drift_detection (tests/integration/migrations.rs) now runs in a
# dedicated temp DB it drops instead of tampering the shared
# schema_migrations. Every target here (local and CI) therefore runs with the
# default parallel --test-threads.
test:
	$(CARGO) test --lib --bins --test wiremock
	$(CARGO) test --doc
	$(CARGO) test --test integration -- --nocapture

test-fast:
	$(CARGO) test --lib --bins

# Integration tests against the compose Postgres (boots it, runs, tears it
# down). Idempotent — safe against an existing dev DB. This target requires
# docker.
test-integration:
	$(call DB_WRAP,DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --test integration --)

# Strict integration: DB must be reachable or tests hard-fail.
test-strict:
	$(call DB_WRAP,DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --test integration)

# Local mirror of the CI PR gate (P13): the CI `coverage` job in
# .github/workflows/ci.yml is this target's CI twin — strict integration, now
# run under `cargo llvm-cov` as a superset of this selection (it adds
# --all-features and the wiremock suite). Keep the two in sync: any change to
# the selection of lib vs integration halves (flags) needs to land in both
# this target and the CI job. Both halves
# run with the default parallel --test-threads: the lib's DB-gated tests and
# the integration suite share one dev DB, but every DB-gated test is
# serialized by DbExclusiveGuard (cross-process lockfile + in-process slot),
# and the migration drift check
# now runs in a dedicated temp DB it drops.
test-strict-ci:
	$(call DB_WRAP,DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --lib --bins && \
	    DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --test integration)

# JS syntax check for the embedded web UI (web/*.js — the pages carry no
# inline <script> blocks; the `pages_have_no_inline_scripts` Rust unit test
# guards that). Requires node; skips with a warning when node is absent.
# Set ZIMSERVICE_WEB_CHECK_STRICT=1 to fail without node.
web-check:
	@command -v node >/dev/null 2>&1 || { \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-check: node not found (strict mode)" >&2; exit 1; \
	  fi; echo "web-check: node not found — skipping JS syntax checks"; exit 0; }
	@node --check web/common.js web/index.js web/search.js web/settings.js
	@echo "web-check: OK"

# Behavioral unit tests for the pure helpers in web/common.js (node --test,
# no external dependencies). Requires node; skips with a warning when node is
# absent, same policy as web-check. Set ZIMSERVICE_WEB_CHECK_STRICT=1 to fail
# without node.
web-test:
	@command -v node >/dev/null 2>&1 || { \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-test: node not found (strict mode)" >&2; exit 1; \
	  fi; echo "web-test: node not found — skipping web UI unit tests"; exit 0; }
	node --test tests/web/*.test.mjs

# Real lint (eslint) of the embedded web UI: web/common.js + the per-page
# scripts (web/index.js, web/search.js, web/settings.js). Requires
# `npm install` first (populates node_modules). Skips with a warning when
# eslint isn't installed; strict mode (CI) fails instead.
web-lint:
	@command -v node >/dev/null 2>&1 || { \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-lint: node not found (strict mode)" >&2; exit 1; \
	  fi; echo "web-lint: node not found — skipping JS lint"; exit 0; }
	@[ -x node_modules/.bin/eslint ] || { \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-lint: eslint not installed (run: npm install; strict mode)" >&2; exit 1; \
	  fi; echo "web-lint: eslint not installed (npm install) — skipping JS lint"; exit 0; }
	./node_modules/.bin/eslint web/common.js web/index.js web/search.js web/settings.js
	@echo "web-lint: OK"

# eslint --fix for the embedded web UI (the JS half of `make fmt`): auto-fixes
# fixable rules in web/common.js and the per-page scripts — all real files,
# so fixes are written back. Same prerequisites/policy as web-lint: needs
# node + `npm install` (node_modules); skips with a warning when eslint
# isn't installed. Set ZIMSERVICE_WEB_CHECK_STRICT=1 to fail without
# node/eslint.
web-fmt:
	@command -v node >/dev/null 2>&1 || { \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-fmt: node not found (strict mode)" >&2; exit 1; \
	  fi; echo "web-fmt: node not found — skipping JS auto-fix"; exit 0; }
	@[ -x node_modules/.bin/eslint ] || { \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-fmt: eslint not installed (run: npm install; strict mode)" >&2; exit 1; \
	  fi; echo "web-fmt: eslint not installed (npm install) — skipping JS auto-fix"; exit 0; }
	./node_modules/.bin/eslint --fix web/common.js web/index.js web/search.js web/settings.js
	@echo "web-fmt: OK"

# Full pre-merge check suite: type-check, format check, lint, full test run,
# and the web UI checks (syntax, unit tests, eslint).
all: check fmt-check clippy raw-sql-lint test web-check web-test web-lint

help:
	@echo "zimservice — make targets"
	@echo ""
	@echo "  make              Default: runs 'make check' (cargo check). Use 'make all' for the full gate"
	@echo "  make all          Full quality gate: check + fmt-check + clippy + test + web-check + web-test + web-lint"
	@echo "  make check        cargo check (pre-commit)"
	@echo "  make fmt          Format Rust (cargo fmt) + JS (eslint --fix; see web-fmt)"
	@echo "  make fmt-check    Check formatting (CI)"
	@echo "  make clippy       Lint (warnings as errors)"
	@echo "  make raw-sql-lint  Flag unmarked raw sqlx::query* sites outside src/db/ (M2)"
	@echo "  make test         Run full test suite"
	@echo "  make test-fast    Run unit tests only (lib + bins; no integration/doctests)"
	@echo "  make test-integration  Boot compose Postgres, run DB integration tests"
	@echo "  make test-strict      Strict mode: DB required (missing DB is a hard failure)"
	@echo "  make test-strict-ci    Mirrors the CI PR gate: strict integration (DB required)"
	@echo "  make web-check    JS syntax check of the embedded web UI (web/*.js; needs node; skips if absent)"
	@echo "  make web-test     Behavioral unit tests for web/common.js helpers (node --test; skips if absent)"
	@echo "  make web-fmt      eslint --fix for the web UI (JS half of make fmt; needs npm install; skips if absent)"
	@echo "  make web-lint     JS lint of web UI via eslint (needs npm install; skips if absent)"
	@echo "  make doc          Build docs"
	@echo "  make build        Build (debug)"
	@echo "  make release      Build (optimized, LTO)"
	@echo "  make install      Install $(BIN) to ~/.local/bin"
	@echo "  make uninstall    Remove $(BIN) from ~/.local/bin"
	@echo "  make run          Run $(BIN) serve (debug build first)"
	@echo "  make clean        Remove target dir"

doc:
	$(CARGO) doc --no-deps

# ─── Build targets ──────────────────────────────────────────────────────
build:
	$(CARGO) build

release:
	$(CARGO) build --release

# ─── Install targets ────────────────────────────────────────────────────
install: release
	@mkdir -p $(HOME)/.local/bin
	install -m 755 target/release/$(BIN) $(HOME)/.local/bin/$(BIN)
	@echo "Installed $(HOME)/.local/bin/$(BIN)"

uninstall:
	rm -f $(HOME)/.local/bin/$(BIN)

run:
	$(CARGO) build
	./target/debug/$(BIN) serve

clean:
	rm -rf target
