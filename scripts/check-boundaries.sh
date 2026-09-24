#!/bin/sh
# M2: module-boundary lint (the use-graph half of the boundary contract).
#
# Companion to check-raw-sql.sh, which owns the raw-SQL *call-site* rules
# (sqlx::query* markers + db::raw::* in src/serve/handlers/). This script
# enforces the module edges the raw-SQL lint does not see — a small
# use-graph/denylist check (grep over `crate::<module>` references and
# call shapes), same style: POSIX sh + grep only, a self-test that pins
# the detection, and a `// RAW-OK: <reason>` marker convention shared with
# check-raw-sql.sh where it applies.
#
# Rule P1 (presentation-layer separation): src/mcp/ and src/serve/ are the
# two presentation layers; neither may depend on the other. ARCHITECTURE.md
# (the `src/content.rs` entry): content.rs exists "so neither presentation
# layer depends on the other". A `crate::serve::` reference under
# src/mcp/ (or `use crate::serve;`) or a `crate::mcp::` reference under
# src/serve/ (outside pure comment lines) is a violation.
#
# Rule P2 (no raw-SQL literals in presentation layers): a string literal in
# src/serve/handlers/** or src/mcp/** that STARTS with an uppercase SQL
# statement keyword (SELECT/INSERT/UPDATE/DELETE/CREATE/ALTER/DROP/
# TRUNCATE/COPY/WITH + whitespace) is a violation — presentation layers
# must not carry SQL text at all. Case-sensitive on purpose: SQL in this
# codebase is uppercase, and a case-insensitive match would flag doc
# strings like "Update result". Additive vs check-raw-sql.sh (which sees
# sqlx::query* call sites): this catches SQL text with or without a call
# site, i.e. earlier.
#
# Rule P3 (no pool creation in presentation layers): src/serve/** and
# src/mcp/** must not create a database pool (`db::pool::create_pool` /
# `*Pool::connect`) — pools are owned by startup (src/startup.rs, the
# three-pool topology in ARCHITECTURE.md "Persistence layer") and handed
# over via AppState; a presentation layer opening its own pool would
# bypass that contract.
#
# Rule P4 (serve-layer files own no SQL — extends check-raw-sql.sh rule 2):
# rule 2 of check-raw-sql.sh covers src/serve/handlers/ only; the same
# "presentation layer owns no SQL" contract extends to the remaining
# serve-layer files (mod.rs, middleware.rs, openapi.rs, ...): a
# `db::raw::*` call site there must carry the same `// RAW-OK: <reason>`
# marker (on the call line or the line directly above it — identical
# marker contract to check-raw-sql.sh, pinned by its self-test).
#
# Exit status: 0 when all rules are clean, 1 when a violation is found in
# any rule (a self-test failure also exits 1).

set -u

# SQL statement keywords that may open a raw-SQL string literal (P2).
SQL_KW='SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|COPY|WITH'

# is_comment LINE → true (0) when LINE is a pure comment line (first
# non-whitespace char starts //, /* or * — the block-comment filler form).
is_comment() {
  case "${1#"${1%%[![:space:]]*}"}" in
    //* | /** | \**) return 0 ;;
    *) return 1 ;;
  esac
}

# suppressed_lines FILE → line numbers carrying a RAW-OK marker (the marker
# line itself, and the call line directly below it). Same contract and same
# helper shape as check-raw-sql.sh.
suppressed_lines() {
  grep -n 'RAW-OK' "$1" 2>/dev/null | grep -oE '^[0-9]+' |
    while read -r m; do
      printf '%s\n%s\n' "$m" "$((m + 1))"
    done
}

# p1_violations ROOT → cross-references between the two presentation
# layers. ROOT is the walk root: `.` for the real run (the script runs from
# the repo root), the scratch root for the self-test; the walked tree is
# `src/...` under it (same relative-path contract as check-raw-sql.sh's
# unmarked_query_sites, which is why the pinned self-test paths work in
# both modes).
p1_violations() {
  (cd "$1" && {
    grep -rnE 'crate::serve(::|;)' src/mcp --include='*.rs'
    grep -rnE 'crate::mcp(::|;)' src/serve --include='*.rs'
  } 2>/dev/null) |
    while IFS=: read -r f l rest; do
      is_comment "$rest" && continue
      printf '%s:%s\n' "$f" "$l"
    done
}

# p2_violations ROOT → string literals starting with an uppercase SQL
# statement keyword in the presentation layers (handlers + mcp).
p2_violations() {
  (cd "$1" && grep -rnE "\"[[:space:]]*(${SQL_KW})[[:space:]]" \
    src/serve/handlers src/mcp --include='*.rs' 2>/dev/null) |
    while IFS=: read -r f l rest; do
      is_comment "$rest" && continue
      printf '%s:%s\n' "$f" "$l"
    done
}

# p3_violations ROOT → pool creation in the presentation layers.
p3_violations() {
  (cd "$1" && grep -rnE 'db::pool::create_pool|[A-Za-z_]*Pool::connect' \
    src/serve src/mcp --include='*.rs' 2>/dev/null) |
    while IFS=: read -r f l rest; do
      is_comment "$rest" && continue
      printf '%s:%s\n' "$f" "$l"
    done
}

# p4_violations ROOT → db::raw::* call sites in serve-layer files OUTSIDE
# src/serve/handlers/ (which check-raw-sql.sh rule 2 already covers)
# lacking a RAW-OK marker.
p4_violations() {
  (cd "$1" && grep -rnE 'db::raw::[A-Za-z0-9_]+[^(]*\(' src/serve \
    --include='*.rs' 2>/dev/null) |
    while IFS=: read -r f l rest; do
      is_comment "$rest" && continue
      case "$f" in
        src/serve/handlers/*) continue ;;
      esac
      if ! suppressed_lines "$1/$f" | grep -xq "$l"; then
        printf '%s:%s\n' "$f" "$l"
      fi
    done
}

# --- self-test (pins one positive + the suppression exemptions per rule) --
selftest_dir=$(mktemp -d)
trap 'rm -rf "$selftest_dir"' EXIT
mkdir -p "$selftest_dir/src/serve/handlers" "$selftest_dir/src/mcp"
{
  # P1: mcp → serve reference (must be reported)
  printf '%s\n' 'let x = crate::serve::state::AppState;'
  # P1: mcp → serve in a comment line (must NOT be reported)
  printf '%s\n' '// mirrors the router in crate::serve::mod'
} > "$selftest_dir/src/mcp/a.rs"
{
  # P1: serve → mcp reference (must be reported)
  printf '%s\n' 'let x = crate::mcp::run;'
} > "$selftest_dir/src/serve/b.rs"
{
  # P2: SQL string literal in a handler (must be reported)
  printf '%s\n' 'let q = "SELECT 1";'
  # P2: lowercase-starting doc string (must NOT be reported — case pin)
  printf '%s\n' 'let d = "Update result";'
  # P2: SQL literal in a comment line (must NOT be reported)
  printf '%s\n' '// let q = "DELETE FROM x";'
} > "$selftest_dir/src/serve/handlers/c.rs"
{
  # P3: pool creation in the serve layer (must be reported)
  printf '%s\n' 'let pool = db::pool::create_pool(&cfg).await;'
} > "$selftest_dir/src/serve/d.rs"
{
  # P3: pool creation in mcp (must be reported — the PgPool::connect form)
  printf '%s\n' 'let pool = sqlx::PgPool::connect(&url).await;'
} > "$selftest_dir/src/mcp/e.rs"
{
  # P4: unmarked db::raw::* call in a non-handler serve file (report)
  printf '%s\n' 'let x = db::raw::fetch_all(&p, "q1", |q| q.bind(1));'
  # P4: marker on the line directly above the call (suppressed)
  printf '%s\n' '// RAW-OK: self-test marker-above contract'
  printf '%s\n' 'let y = db::raw::execute(&p, "q2", |q| q.bind(2));'
} > "$selftest_dir/src/serve/middleware.rs"
{
  # P4: db::raw::* call INSIDE handlers/ (out of scope — check-raw-sql.sh
  # rule 2 owns it; must NOT be reported here)
  printf '%s\n' 'let z = db::raw::execute(&p, "q3", |q| q.bind(3));'
} > "$selftest_dir/src/serve/handlers/z.rs"

got=$(
  {
    p1_violations "$selftest_dir"
    p2_violations "$selftest_dir"
    p3_violations "$selftest_dir"
    p4_violations "$selftest_dir"
  } | sort
)
want="src/mcp/a.rs:1
src/mcp/e.rs:1
src/serve/b.rs:1
src/serve/d.rs:1
src/serve/handlers/c.rs:1
src/serve/middleware.rs:1"
if [ "$got" != "$want" ]; then
  echo "boundary lint: SELF-TEST FAILED — detection drifted from its pinned" >&2
  echo "expected:" >&2
  printf '%s\n' "$want" >&2
  echo "got:" >&2
  printf '%s\n' "$got" >&2
  exit 1
fi

# --- rule P1 ----------------------------------------------------------------
v=$(p1_violations .)
if [ -n "$v" ]; then
  echo "boundary lint: P1 — presentation layers must not depend on each other (src/mcp/ <-> src/serve/): the shared content service is src/content.rs; route new shared needs through it:" >&2
  printf '%s\n' "$v" >&2
  exit 1
fi

# --- rule P2 ----------------------------------------------------------------
v=$(p2_violations .)
if [ -n "$v" ]; then
  echo "boundary lint: P2 — raw-SQL string literal in a presentation layer (src/serve/handlers/, src/mcp/): SQL text belongs in src/db/ (move it into a named db::raw helper — see check-raw-sql.sh):" >&2
  printf '%s\n' "$v" >&2
  exit 1
fi

# --- rule P3 ----------------------------------------------------------------
v=$(p3_violations .)
if [ -n "$v" ]; then
  echo "boundary lint: P3 — pool creation in a presentation layer (src/serve/, src/mcp/): pools are owned by startup (src/startup.rs) and passed via AppState — never open a pool in the presentation layer:" >&2
  printf '%s\n' "$v" >&2
  exit 1
fi

# --- rule P4 ----------------------------------------------------------------
v=$(p4_violations .)
if [ -n "$v" ]; then
  echo "boundary lint: P4 — db::raw::* call site in a serve-layer file outside src/serve/handlers/ (extend of the check-raw-sql.sh rule-2 contract: presentation layer files own no SQL — move it into a named src/db/ helper; a // RAW-OK: <reason> marker is a last resort, not the fix):" >&2
  printf '%s\n' "$v" >&2
  exit 1
fi

echo "boundary lint: OK — self-test passed; presentation layers separated (mcp<->serve), no raw-SQL literals or pool creation in presentation layers, no db::raw::* call sites in serve-layer files outside handlers/"
