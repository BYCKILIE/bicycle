# Build stage - compiles the job
FROM rust:1.75-bookworm AS builder

WORKDIR /build

# Create a minimal workspace just for the job
RUN mkdir -p crates/api/src crates/api-macros/src test-jobs/wordcount/src

# Create workspace Cargo.toml (minimal, just what the job needs)
COPY <<'EOF' Cargo.toml
[workspace]
resolver = "2"
members = [
  "crates/api",
  "crates/api-macros",
  "test-jobs/wordcount",
]

[workspace.package]
edition = "2021"
license = "Apache-2.0"

[workspace.dependencies]
anyhow = "1"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "1"
EOF

# Copy only the crates needed for the job
COPY crates/api/Cargo.toml crates/api/
COPY crates/api-macros/Cargo.toml crates/api-macros/
COPY test-jobs/wordcount/Cargo.toml test-jobs/wordcount/

# Create dummy source files for dependency caching
RUN touch crates/api/src/lib.rs \
    && touch crates/api-macros/src/lib.rs \
    && echo "fn main() {}" > test-jobs/wordcount/src/main.rs

# Build dependencies (cached layer)
RUN cargo build --release -p wordcount 2>/dev/null || true

# Copy actual source
COPY crates/api crates/api/
COPY crates/api-macros crates/api-macros/
COPY test-jobs/wordcount test-jobs/wordcount/

# Touch to invalidate cache
RUN touch crates/*/src/*.rs test-jobs/wordcount/src/*.rs

# Build the job (release mode)
RUN cargo build --release -p wordcount

# Runtime stage - minimal Debian image with the binary
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/wordcount /usr/local/bin/wordcount

# Default ports
ENV SOURCE_PORT=9999
ENV SINK_PORT=9998

EXPOSE 9999 9998

ENTRYPOINT ["wordcount"]
