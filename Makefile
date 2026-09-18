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

# Shared boilerplate for every web-UI dev target (a make macro, invoked as
# $(call WEB_WRAP,<target>,<eslint>,<skip-label>,<commands>)): guard node
# presence, and — when <eslint> is non-empty (only the targets that read
# node_modules) — guard eslint presence. Either guard fails hard under
# ZIMSERVICE_WEB_CHECK_STRICT=1, else skips with a warning. <skip-label> is
# the human phrase after "skipping" in the warning.
#
# IMPORTANT (the fix for the old skip quirk): make runs each recipe line in
# its OWN shell, so a guard on one line followed by <commands> on the next
# line cannot be stopped by the guard's `exit 0` — the command line still ran
# in a fresh shell and failed. So the guard(s) and <commands> are chained on a
# SINGLE logical line (backslash continuations) with `&&`: everything runs in
# one shell, and a guard's `exit 0`/`exit 1` genuinely prevents <commands>.
# The leading `@` silences the whole one-line command (call sites must NOT
# re-prefix <commands> with `@`). Use only inside recipe lines.
define WEB_WRAP
@{ if ! command -v node >/dev/null 2>&1; then \
  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
    echo "$1: node not found (strict mode)" >&2; exit 1; \
  else echo "$1: node not found — skipping $3"; fi; exit 0; \
  fi; } \
$(if $2,&& { if [ ! -x node_modules/.bin/eslint ]; then \
  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
    echo "$1: eslint not installed (run: npm install; strict mode)" >&2; exit 1; \
  else echo "$1: eslint not installed (npm install) — skipping $3"; fi; exit 0; \
  fi; },) \
&& $4 && echo "$1: OK"
endef

.PHONY: all help check fmt fmt-check clippy raw-sql-lint test test-fast test-integration test-strict test-strict-ci build release install uninstall doc run clean web-check web-fmt web-test web-lint

# Quick pre-commit checks
check:
	$(CARGO) check --all-targets

fmt: web-fmt
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --all-targets -- -D warnings

# M2: raw-SQL boundary lint — two rules: (1) flags sqlx::query* call sites
# outside src/db/ (the module owning the sanctioned db::raw escape hatch)
# that lack a `// RAW-OK: <reason>` marker on the call line; (2) flags
# db::raw::* call sites in the presentation layer (src/serve/handlers/) —
# handlers must not own SQL (move it into a named src/db/ helper).
# POSIX sh + grep only so it runs on the dev box and in CI alike.
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
# M4: `--nocapture` surfaces the one-shot "INTEGRATION SUITE SKIPPED" banner
# (printed by tests/integration/common.rs, pinned by SKIP_BANNER there) so we
# can detect a vacuous-green run: when the banner is present (no reachable
# Postgres) we print a prominent red warning at the end — we do NOT hard-fail
# (local non-strict behavior is unchanged), but the skip is made loud. The run
# streams live to the terminal (tee) while a copy lands in a temp file that is
# grepped afterwards; `bash -c` + `set -o pipefail` is needed so the exit
# status is cargo's, not tee's (POSIX sh pipelines report the last command).
test:
	$(CARGO) test --lib --bins --test wiremock
	$(CARGO) test --doc
	@bash -c 'set -o pipefail; \
	  out=$$(mktemp); trap "rm -f $$out" EXIT; \
	  $(CARGO) test --test integration -- --nocapture 2>&1 | tee "$$out"; st=$$?; \
	  if grep -qF "INTEGRATION SUITE SKIPPED" "$$out"; then \
	    printf "\033[1;31m\n>>> WARNING: DB-backed integration tests were SKIPPED (no reachable Postgres). <<<\033[0m\n"; \
	    printf "\033[1;31mThe pass above is VACUOUS for the DB paths — no database behavior was verified.\033[0m\n"; \
	    printf "\033[1;31mFor a real DB run: make test-integration (boots compose Postgres) or make test-strict.\033[0m\n"; \
	  fi; \
	  exit $$st'

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

# Local mirror of the CI `test` job (.github/workflows/ci.yml): strict DB
# (ZIMSERVICE_REQUIRE_DB=1), lib + bins + wiremock, then the integration
# suite. Both halves run with the default parallel --test-threads: every
# DB-gated test is serialized by DbExclusiveGuard (cross-process lockfile +
# in-process slot), and the migration drift check runs in a dedicated temp
# DB it drops.
test-strict-ci:
	$(call DB_WRAP,DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --lib --bins --test wiremock && \
	    DATABASE_URL=$(DEV_DSN) ZIMSERVICE_REQUIRE_DB=1 $(CARGO) test --test integration)

# JS syntax check for the embedded web UI (web/*.js — the pages carry no
# inline <script> blocks; the `pages_have_no_inline_scripts` Rust unit test
# guards that). Also syntax-checks the embedded CSS: web/style.css plus
# every page's inline <style> block. web/check-css.mjs parses with
# css-tree — the sanctioned CSS parser dependency (mirroring jsdom, the
# sanctioned exception for web-test; it replaced the old hand-rolled
# parser). Requires node; skips with a warning when node is absent. With
# node but css-tree missing (no `npm install` yet) the CSS half warns +
# skips while the JS half still runs. Set ZIMSERVICE_WEB_CHECK_STRICT=1 to
# fail without node/css-tree. The node guard and the run stay chained on
# ONE logical line (make runs each recipe line in its own shell, so an
# `exit 0` on a separate guard line would not stop the check run — same
# reason WEB_WRAP uses one-line chaining).
web-check:
	@if ! command -v node >/dev/null 2>&1; then \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-check: node not found (strict mode)" >&2; exit 1; \
	  fi; echo "web-check: node not found — skipping web syntax checks"; exit 0; \
	else \
	  node --check web/common.js web/index.js web/search.js web/settings.js && \
	  { if ! node -e "require('css-tree')" >/dev/null 2>&1; then \
	    if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	      echo "web-check: css-tree not installed (run: npm install; strict mode)" >&2; exit 1; \
	    fi; echo "web-check: css-tree not installed (run: npm install) — skipping CSS check"; exit 0; \
	  fi; } && \
	  node web/check-css.mjs web/style.css web/index.html web/search.html web/settings.html; \
	fi

# Behavioral unit tests for the web UI helpers + page scripts (node --test).
# Single real-DOM harness: every suite (common/index/search/settings/smoke)
# boots the page scripts in jsdom against the minimal HTML they touch
# (tests/web/jsdom.mjs). Requires node; jsdom is a declared devDependency.
#
# WHY jsdom (and not a hand-rolled DOM shim): the suites assert on
# browser-PARSED element trees (class/attribute querySelectorAll, bubbling
# Event, real <select> option matching) and run the page scripts as top-level
# classic scripts in document load order. A fake-DOM shim does not parse
# innerHTML or run scripts — reproducing that means rebuilding an HTML parser
# + script runtime (a browser). jsdom is the only reasonable way to get real
# parsed-DOM behavior, and it is ONE declared devDependency that CI installs
# via `npm install` anyway, so there is no extra cost to always using it.
#
# Consequence: jsdom is required to run the web tests at all. Without node we
# warn + skip (fail under ZIMSERVICE_WEB_CHECK_STRICT=1); with node but
# jsdom missing (no `npm install` yet) we warn + skip ALL web tests (strict
# mode fails). The node guard and the run stay chained on ONE logical line:
# make runs each recipe line in its own shell, so an `exit 0` on a separate
# guard line would not stop the test run (same reason WEB_WRAP uses one-line
# chaining).
web-test:
	@if ! command -v node >/dev/null 2>&1; then \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-test: node not found (strict mode)" >&2; exit 1; \
	  fi; echo "web-test: node not found — skipping web UI unit tests"; exit 0; \
	elif ! node -e "import('jsdom')" >/dev/null 2>&1; then \
	  if [ "$${ZIMSERVICE_WEB_CHECK_STRICT:-0}" = "1" ]; then \
	    echo "web-test: jsdom not installed (run: npm install; strict mode)" >&2; exit 1; \
	  fi; echo "web-test: jsdom not installed (run: npm install) — skipping all web UI unit tests"; \
	else \
	  node --test tests/web/*.test.mjs; \
	fi

# Real lint (eslint) of the embedded web UI: web/common.js + the per-page
# scripts (web/index.js, web/search.js, web/settings.js), plus the web-UI test
# suite (tests/web/*.mjs). Requires `npm install` first (populates
# node_modules). Skips with a warning when eslint isn't installed; strict
# mode (CI) fails instead.
web-lint:
	$(call WEB_WRAP,web-lint,yes,JS lint,\
	./node_modules/.bin/eslint web/common.js web/index.js web/search.js web/settings.js tests/web/*.mjs)

# eslint --fix for the embedded web UI (the JS half of `make fmt`): auto-fixes
# fixable rules in web/common.js and the per-page scripts — all real files,
# so fixes are written back. Same prerequisites/policy as web-lint: needs
# node + `npm install` (node_modules); skips with a warning when eslint
# isn't installed. Set ZIMSERVICE_WEB_CHECK_STRICT=1 to fail without
# node/eslint.
web-fmt:
	$(call WEB_WRAP,web-fmt,yes,JS auto-fix,\
	./node_modules/.bin/eslint --fix web/common.js web/index.js web/search.js web/settings.js)

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
	@echo "  make raw-sql-lint  Flag unmarked raw SQL: sqlx::query* outside src/db/, db::raw::* call sites in handlers (M2)"
	@echo "  make test         Run full test suite"
	@echo "  make test-fast    Run unit tests only (lib + bins; no integration/doctests)"
	@echo "  make test-integration  Boot compose Postgres, run DB integration tests"
	@echo "  make test-strict      Strict mode: DB required (missing DB is a hard failure)"
	@echo "  make test-strict-ci    Mirrors the CI test job: strict DB, lib+bins+wiremock+integration"
	@echo "  make web-check    JS syntax + CSS syntax check of the embedded web UI (needs node; skips if absent)"
	@echo "  make web-test     Behavioral unit tests for web UI helpers + page scripts (node --test, jsdom real-DOM harness)"
	@echo "  make web-fmt      eslint --fix for the web UI (JS half of make fmt; needs npm install; skips if absent)"
	@echo "  make web-lint     JS lint of web UI + tests/web via eslint (needs npm install; skips if absent)"
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
