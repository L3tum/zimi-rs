# Multi-stage build for zimservice
FROM rust:1.96.0-slim AS builder
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
