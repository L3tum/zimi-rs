# Multi-stage build for zimservice
# The builder base must not have a newer glibc than the runtime base below
# (debian:bookworm-slim = glibc 2.36): bare `rust:1.96.0-slim` tracks the
# "current" Debian (trixie, glibc 2.41, since 2026-06) and produces a
# binary that dies at container start with `GLIBC_2.38 not found`. Pin the
# base variant explicitly; the build-time check after `cargo build` enforces
# the invariant so a future base drift fails the build, not the boot.
FROM rust:1.96.0-slim-bookworm AS builder
WORKDIR /build
# Dependency-layer caching: fetch the manifest-only layer first so the
# Cargo.lock-driven download layer stays valid across source-only changes.
# The manifest declares explicit [[bench]] targets (search, retrieval) and
# relies on target autodiscovery for [lib]/[bin]; cargo refuses to parse it
# until every target path exists — so stub them here (empty: `cargo fetch`
# never compiles) and let `COPY . .` below replace the stubs with the real
# sources.
COPY Cargo.toml Cargo.lock .
RUN mkdir -p src benches && touch src/lib.rs src/main.rs benches/search.rs benches/retrieval.rs
RUN cargo fetch
COPY . .
RUN cargo build --release --locked
# Build-time glibc guard: the binary's max GLIBC requirement must fit the
# bookworm runtime (2.36). Fails the build (with a clear message) instead of
# failing at container start if the builder base ever outruns the runtime.
RUN max_glibc=$(objdump -T target/release/zimservice | grep -oE 'GLIBC_[0-9.]+' | sed 's/GLIBC_//' | sort -Vu | tail -1) \
    && dpkg --compare-versions "$max_glibc" le 2.36 \
    && echo "glibc check: binary needs GLIBC_${max_glibc} (runtime is bookworm, 2.36)"

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -r zimservice

COPY --from=builder /build/target/release/zimservice /usr/local/bin/

# Default ZIM and download directories
RUN mkdir -p /zims /downloads && chown zimservice:zimservice /zims /downloads
WORKDIR /app
USER zimservice

ENV ZIM_DIR=/zims
ENV PORT=8899

EXPOSE 8899
ENTRYPOINT ["zimservice"]
CMD ["serve"]
