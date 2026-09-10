#!/bin/sh
# M2: raw-SQL boundary lint (two rules).
#
# Rule 1 (sqlx::query* sites): the `db::raw` helpers in `src/db/` are the
# data-access layer; every direct `sqlx::query` / `sqlx::query_as` /
# `sqlx::query_scalar` call site is a boundary crossing and must carry a
# `// RAW-OK: <reason>` marker ON THE CALL LINE (a one-line justification
# future readers — and this lint — can audit). `src/db/` is exempt because
# it owns the raw helpers.
#
# Rule 2 (db::raw::* call sites in the presentation layer): the HTTP
# handlers under `src/serve/handlers/` are the presentation layer and must
# not own SQL — that is the presentation↔persistence boundary. Any line
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
# found in either rule. grep only (no awk, no bashisms, no dependencies) —
# runs on the dev box and in CI alike.
set -u

unmarked=$(grep -rnE 'sqlx::query(_as|_scalar)?[[:space:]]*\(' src --include='*.rs' \
  | grep -v 'RAW-OK' \
  | grep -v '^src/db/')

if [ -n "$unmarked" ]; then
  echo "raw-sql lint: unmarked sqlx::query* call site(s) outside src/db/ (add a // RAW-OK: <reason> marker ON THE CALL LINE, or route the query through db::raw):" >&2
  printf '%s\n' "$unmarked" >&2
  exit 1
fi

unmarked_handler=$(grep -rnE 'db::raw::[A-Za-z0-9_]+[^(]*\(' src/serve/handlers --include='*.rs' \
  | grep -vE '^[^:]*:[0-9]+:[[:space:]]*//' \
  | grep -v 'RAW-OK')

if [ -n "$unmarked_handler" ]; then
  echo "raw-sql lint: db::raw::* call site(s) in the presentation layer (src/serve/handlers/) — handlers must not own SQL: move the SQL into a named helper in src/db/ (a // RAW-OK: <reason> marker on the call line is a last resort, not the fix):" >&2
  printf '%s\n' "$unmarked_handler" >&2
  exit 1
fi

echo "raw-sql lint: OK — no unmarked sqlx::query* sites outside src/db/, no db::raw::* call sites in src/serve/handlers/"
