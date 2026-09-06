#!/bin/sh
# M2: raw-SQL boundary lint.
#
# The sanctioned escape hatch for SQL the SeaORM query builder cannot express
# lives in `src/db/` (the `db::raw` module in `src/db/mod.rs`); every other
# direct `sqlx::query` / `sqlx::query_as` / `sqlx::query_scalar` call site is
# a boundary violation and must carry a `// RAW-OK: <reason>` marker ON THE
# CALL LINE (a one-line justification future readers — and this lint — can
# audit). `src/db/` is exempt because it owns the escape hatch.
#
# Queries routed through the `db::raw` helpers (`raw::fetch_*`,
# `raw::execute`, ...) are the intended path and are not flagged.
#
# Exit status: 0 when clean, 1 when an unmarked call site is found.
# grep only (no awk, no bashisms, no dependencies) — runs on the dev box and
# in CI alike.
set -u

unmarked=$(grep -rnE 'sqlx::query(_as|_scalar)?[[:space:]]*\(' src --include='*.rs' \
  | grep -v 'RAW-OK' \
  | grep -v '^src/db/')

if [ -n "$unmarked" ]; then
  echo "raw-sql lint: unmarked sqlx::query* call site(s) outside src/db/ (add a // RAW-OK: <reason> marker ON THE CALL LINE, or route the query through db::raw):" >&2
  printf '%s\n' "$unmarked" >&2
  exit 1
fi

echo "raw-sql lint: OK — no unmarked sqlx::query* sites outside src/db/"
