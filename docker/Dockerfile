# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.88-slim AS builder

WORKDIR /build

# Install only the packages needed to compile SQLite (bundled by sqlx) and
# to link the final binary. No OpenSSL is required — the project uses rustls.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Cache dependency compilation separately from application source.
# Copy workspace manifests and lock file first, then build a dummy binary so
# that Cargo downloads and compiles all dependencies. Subsequent rebuilds that
# only touch src/ will reuse this cache layer.
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml core/Cargo.toml
COPY server/Cargo.toml server/Cargo.toml

# Create stub lib/main so `cargo build` has something to compile.
RUN mkdir -p core/src server/src \
    && echo "pub fn lib_stub() {}" > core/src/lib.rs \
    && echo "fn main() {}" > server/src/main.rs \
    && cargo build --release --locked \
    && rm -rf core/src server/src

# Now copy the real source and rebuild only the application crates.
COPY core/ core/
COPY server/ server/

# Touch lib.rs / main.rs so Cargo sees them as newer than the cached artifacts.
RUN touch core/src/lib.rs server/src/main.rs \
    && cargo build --release --locked

# ── Runtime stage ─────────────────────────────────────────────────────────────
# gcr.io/distroless/cc-debian12 provides glibc and libgcc — enough to run a
# dynamically-linked Rust binary compiled against the default glibc target.
# It contains no shell, package manager, or other tooling, minimising the
# attack surface.
FROM gcr.io/distroless/cc-debian12 AS runtime

# Run as a non-root user. distroless/cc ships uid 65532 ("nonroot").
USER nonroot:nonroot

COPY --from=builder /build/target/release/modelpointer /usr/local/bin/modelpointer

# Gateway API
EXPOSE 8080
# Prometheus metrics
EXPOSE 29000

ENTRYPOINT ["/usr/local/bin/modelpointer"]
CMD ["serve"]
