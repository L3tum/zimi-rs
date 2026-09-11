//! Access-control policy objects shared across layers (M-B).
//!
//! This module holds the access-control *policy* objects that `AppState`
//! owns live at the serve layer's level: the global token-bucket rate
//! limiter (`ratelimit`), the per-IP auth-failure lockout tracker
//! (`lockout`), and the trusted-proxy CIDR / `X-Forwarded-For` client-IP
//! resolution logic (`cidr`).
//!
//! This follows the [crate::health] precedent: policy objects that the
//! `serve` layer *applies* (as HTTP middleware / request gates) live here,
//! one level **below** `serve`, rather than inside `serve` itself. Keeping
//! them out of `serve` lets non-serve layers (`state`, `startup`, `testing`)
//! construct and reference them without depending on the HTTP-serving module
//! (M-B layering inversion), while `serve` sits above `access` and consumes
//! these objects as middleware.

/// Trusted-proxy CIDR validation + `X-Forwarded-For` client-IP resolution.
pub mod cidr;
/// Per-source-IP auth-failure lockout (SEC-M1).
pub mod lockout;
/// Global token-bucket rate limiting policy (the limiter + settings handle).
pub mod ratelimit;
