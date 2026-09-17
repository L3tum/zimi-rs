#!/bin/sh
# M2: raw-SQL boundary lint (two rules).
#
# Rule 1 (sqlx::query* sites): the `db::raw` helpers in `src/db/` are the
# data-access layer; every direct `sqlx::query` / `sqlx::query_as` /
# `sqlx::query_scalar` call site is a boundary crossing and must carry a
# `// RAW-OK: <reason>` marker ON THE CALL LINE OR ON THE LINE DIRECTLY
# ABOVE IT (a one-line-or-few justification future readers — and this lint —
# can audit). The marker above the call must be a comment line with nothing
# but the call (and its continuation lines) after it — the marker is
# anchored to the line where the call starts, so a call whose first line is
# two lines below the marker is NOT marked. `src/db/` is exempt because it
# owns the raw helpers.
#
# Why not only on the call line: the call line itself is often too long for
# a trailing comment to fit inside the 100-column budget (e.g. the
# `pg_extension` probe), so the contract also accepts the immediately
# preceding line. Both forms are pinned by the self-test below.
#
# Rule 2 (db::raw::* call sites in the presentation layer): the HTTP
# handlers under `src/serve/handlers/` are the presentation layer and must
# not own SQL — that is the presentation<->persistence boundary. Any line
# under `src/serve/handlers/` that calls a `db::raw::*` helper (a `(`
# after the helper name on the line; pure `//` comment lines are ignored)
# and lacks a `// RAW-OK: <reason>` marker is a violation. The intended
# fix is to move the SQL into a named helper in `src/db/` and call that —
# after which no marker is needed. Background layers (`src/torrent`,
# `src/zim`, `src/search`, `src/embed`, `src/startup`) are EXEMPT from
# rule 2: they ARE the data-access/workers, and their ~60 `db::raw` call
# sites are the intended path.
#
# Exit status: 0 when both rules are clean, 1 when an unmarked call site is
# found in either rule (a self-test failure also exits 1). grep + POSIX
# shell only (no awk, no bashisms, no dependencies) — runs on the dev box
# and in CI alike.

set -u

# Rule 1 call-site pattern. The optional `::<…>` group is the turbofish
# generic-argument list (`sqlx::query_as::<_, Row>(…)` — the runtime-SQL
# hybrid branch in src/search uses exactly this form); `[^)]*` bounds it to
# the line (a generic list never contains `)`).
QUERY_RE='sqlx::query(_as|_scalar)?[[:space:]]*(::[[:space:]]*<[^)]*>)?[[:space:]]*\('

# suppressed_lines FILE → line numbers carrying a RAW-OK marker (the marker
# line itself, and the call line directly below it).
suppressed_lines() {
  grep -n 'RAW-OK' "$1" 2>/dev/null | grep -oE '^[0-9]+' |
    while read -r m; do
      printf '%s\n%s\n' "$m" "$((m + 1))"
    done
}

# unmarked_query_sites DIR → list every sqlx::query* call site under DIR
# (relative paths) that is neither in src/db/ nor marker-suppressed.
unmarked_query_sites() {
  (cd "$1" && grep -rnE "$QUERY_RE" . --include='*.rs') |
    while IFS=: read -r f l _; do
      f=${f#./}
      # `db/*` — the real lint runs with `src` as the walk root
      # (paths are `db/mod.rs`); `src/db/*` — the self-test walks its own
      # scratch root (paths are `src/db/y.rs`).
      case "$f" in db/* | src/db/*) continue ;; esac
      if ! suppressed_lines "$1/$f" | grep -xq "$l"; then
        printf '%s:%s\n' "$f" "$l"
      fi
    done
}

# --- self-test (pins both marker positions AND the turbofish form) --------
selftest_dir=$(mktemp -d)
trap 'rm -rf "$selftest_dir"' EXIT
mkdir -p "$selftest_dir/src/db"
{
  # line 1: UNMARKED plain call            → must be reported
  printf '%s\n' 'fn a() { sqlx::query("SELECT 1"); }'
  # line 2: UNMARKED turbofish call        → must be reported (regex pin)
  printf '%s\n' 'fn b() { sqlx::query_as::<_, R>(x); }'
  # line 3: turbofish + trailing marker    → OK
  printf '%s\n' 'fn c() { sqlx::query_as::<_, R>(y); } // RAW-OK: self-test'
  # line 4: marker on the line directly above the call → OK
  printf '%s\n' '// RAW-OK: self-test marker-above contract'
  printf '%s\n' 'fn d() { sqlx::query_scalar("SELECT 2"); }'
  # line 8: marker two lines above the call → must be reported
  printf '%s\n' '// RAW-OK: self-test too-far marker'
  printf '%s\n' '// a comment line between marker and call'
  printf '%s\n' 'fn e() { sqlx::query("SELECT 3"); }'
} > "$selftest_dir/src/x.rs"
# line 1 of src/db/y.rs: unmarked call inside src/db/ → exempt
printf '%s\n' 'fn f() { sqlx::query("SELECT 4"); }' > "$selftest_dir/src/db/y.rs"

got=$(unmarked_query_sites "$selftest_dir")
want="src/x.rs:1
src/x.rs:2
src/x.rs:8"
if [ "$got" != "$want" ]; then
  echo "raw-sql lint: SELF-TEST FAILED — detection drifted from its pinned" >&2
  echo "expected:" >&2
  printf '%s\n' "$want" >&2
  echo "got:" >&2
  printf '%s\n' "$got" >&2
  exit 1
fi

# --- rule 1 ----------------------------------------------------------------
unmarked=$(unmarked_query_sites src)
if [ -n "$unmarked" ]; then
  echo "raw-sql lint: unmarked sqlx::query* call site(s) outside src/db/ (add a // RAW-OK: <reason> marker on the call line or the line directly above it, or route the query through db::raw):" >&2
  printf '%s\n' "$unmarked" >&2
  exit 1
fi

# --- rule 2 ----------------------------------------------------------------
unmarked_handler=$(grep -rnE 'db::raw::[A-Za-z0-9_]+[^(]*\(' src/serve/handlers --include='*.rs' \
  | grep -vE '^[^:]*:[0-9]+:[[:space:]]*//' \
  | grep -v 'RAW-OK')

if [ -n "$unmarked_handler" ]; then
  echo "raw-sql lint: db::raw::* call site(s) in the presentation layer (src/serve/handlers/) — handlers must not own SQL: move the SQL into a named helper in src/db/ (a // RAW-OK: <reason> marker on the call line is a last resort, not the fix):" >&2
  printf '%s\n' "$unmarked_handler" >&2
  exit 1
fi

echo "raw-sql lint: OK — self-test passed; no unmarked sqlx::query* sites outside src/db/, no db::raw::* call sites in src/serve/handlers/"
