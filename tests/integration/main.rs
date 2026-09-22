#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Integration tests against a live Postgres (the compose DB).
//!
//! **Safe to run with the default parallel `--test-threads`**: every
//! DB-gated test acquires the [`zimservice::testing::DbExclusiveGuard`]
//! (via `common::pool_or_skip`), which serializes the shared-fixture mutation
//! in-process and, through its cross-process lockfile, across concurrent
//! `cargo test` binaries. `smoke_migration_drift_detection` runs in a
//! dedicated temporary database (dropped afterwards), so it no longer
//! tampers the shared `schema_migrations` — the old reason for a
//! `--test-threads=1` pin is gone.
//!
//! Reads `DATABASE_URL` (default: the compose URL) and **skips cleanly when
//! the DB is unreachable**, so a plain `cargo test` stays green on any
//! machine. Run explicitly with:
//!
//! ```sh
//! make test-integration          # boots compose, runs, tears down
//! # or manually:
//! DATABASE_URL=postgres://zimservice:zimservice@127.0.0.1:5432/zimservice
//!   cargo test --test integration
//! ```
//!
//! If `DATABASE_URL` is set in the environment (even if unreachable), the suite
//! **panics instead of skipping**: an explicit URL is a signal of intent —
//! silently skipping would report a vacuous "N passed" where zero database
//! behavior was actually verified.
//!
//! The harness is idempotent: migrations are applied in place, fixtures use a
//! dedicated ZIM name that is cleaned up, and the migration checks (drift +
//! legacy refusal, `migrations.rs`) run in temp DBs they drop — so it is safe
//! to run against an existing dev database.

mod auth_tokens;
mod common;
mod constraints;
mod embed_claim_fifo;
mod embed_claim_plan;
mod embedding;
mod health;
mod integrity;
mod invalid_index;
mod migrations;
mod raw;
mod search;
mod serve;
mod settings_auth;
mod tls;
mod trgm_plan;
mod vector_dim;
mod zims;
