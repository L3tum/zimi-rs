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

.PHONY: all help check fmt fmt-check clippy test test-fast test-integration test-strict test-strict-perf test-strict-ci build release install uninstall doc run clean web-check web-test web-lint

# Quick pre-commit checks
check:
	$(CARGO) check --all-targets --all-features

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

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
# smoke_migration_drift_detection (tests/integration.rs) now runs in a
# dedicated temp DB it drops instead of tampering the shared
# schema_migrations. The --test-threads=1 flags still present on the local
# dev targets (test / test-integration / test-strict*) are valid but no
# longer required; the CI and test-strict-ci gates now run parallel.
test:
	$(CARGO) test --lib --bins --test wiremock
	$(CARGO) test --doc
	$(CARGO) test --test integration -- --test-threads=1 --nocapture

test-fast:
	$(CARGO) test --lib --bins

# Integration tests against the compose Postgres (boots it, runs, tears it
# down). Idempotent — safe against an existing dev DB. This target requires
# docker.
test-integration:
	docker compose up -d postgres
	$(WAIT_PG)
	DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --test integration -- --test-threads=1
	docker compose stop postgres

# Strict integration: DB must be reachable or tests hard-fail.
test-strict:
	docker compose up -d postgres
	$(WAIT_PG)
	DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --test integration -- --include-ignored --test-threads=1
	docker compose stop postgres

# DB-gated performance checks (seeds 100k rows, runs EXPLAIN (ANALYZE); slow).
# Runs only the ignored perf tests, e.g. trgm_index_perf_check.
test-strict-perf:
	docker compose up -d postgres
	$(WAIT_PG)
	DATABASE_URL=$(DEV_DSN) $(CARGO) test --test integration trgm_index_perf_check -- --ignored
	docker compose stop postgres

# Mirrors the CI PR gate (P13): strict integration minus the slow 100k-row
# perf test, so the perf guard never blocks the correctness gate. Both halves
# run with the default parallel --test-threads: the lib's DB-gated tests and
# the integration suite share one dev DB, but every DB-gated test is
# serialized by DbExclusiveGuard (cross-process lockfile + in-process slot),
# and the migration drift check now runs in a dedicated temp DB it drops.
test-strict-ci:
	docker compose up -d postgres
	$(WAIT_PG)
	DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --lib
	DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --test integration -- --include-ignored --skip trgm_index_perf_check
	docker compose stop postgres

# JS syntax check for the embedded web UI (web/*.js + inline <script> blocks).
# Requires node; skips with a warning when node is absent. Set
# ZIMSERVICE_WEB_CHECK_STRICT=1 to fail without node.
web-check:
	@command -v node >/dev/null 2>&1 || { \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-check: node not found (strict mode)" >&2; exit 1; \
	  fi; echo "web-check: node not found — skipping JS syntax checks"; exit 0; }
	@node --check web/common.js
	@for f in web/index.html web/search.html web/settings.html; do \
	  node -e 'const fs=require("fs"),vm=require("vm");const s=fs.readFileSync(process.argv[1],"utf8");const re=/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi;let m,n=0;while((m=re.exec(s))){n++;try{new vm.Script(m[1])}catch(e){console.error(process.argv[1]+" script#"+n+": "+e.message);process.exit(1)}}if(!n){console.error(process.argv[1]+": no inline scripts found");process.exit(1)}console.log(process.argv[1]+": "+n+" inline script(s) OK");' "$$f" \
	  || exit 1; done
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

# Real lint (eslint) of the embedded web UI: web/common.js + the inline
# <script> blocks extracted by tests/web/extract-inline.mjs. Requires
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
	node tests/web/extract-inline.mjs
	./node_modules/.bin/eslint web/common.js .web-lint-tmp/*.js
	@echo "web-lint: OK"

# Full pre-merge check suite (type-check, format check, lint, full test run)
all: check fmt-check clippy test web-check web-test web-lint

help:
	@echo "zimservice — make targets"
	@echo ""
	@echo "  make              Default: runs 'make check' (cargo check). Use 'make all' for the full gate"
	@echo "  make all          Full quality gate: check + fmt-check + clippy + test + web-check"
	@echo "  make check        cargo check (pre-commit)"
	@echo "  make fmt          Format code"
	@echo "  make fmt-check    Check formatting (CI)"
	@echo "  make clippy       Lint (warnings as errors)"
	@echo "  make test         Run full test suite"
	@echo "  make test-fast    Run unit tests only (lib + bins; no integration/doctests)"
	@echo "  make test-integration  Boot compose Postgres, run DB integration tests"
	@echo "  make test-strict      Strict mode: DB required, run ignored tests too"
	@echo "  make test-strict-perf  DB-gated perf checks (EXPLAIN, 100k rows; slow)"
	@echo "  make test-strict-ci    Mirrors the CI PR gate: strict integration minus the perf test"
	@echo "  make web-check    JS syntax check of embedded web UI (needs node; skips if absent)"
	@echo "  make web-test     Behavioral unit tests for web/common.js helpers (node --test; skips if absent)"
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
