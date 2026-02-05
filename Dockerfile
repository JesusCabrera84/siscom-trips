# Multi-stage build para optimizar tamaño
FROM rust:bookworm as builder

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy manifest files
COPY Cargo.toml Cargo.lock build.rs ./
COPY siscom.proto ./

# Build dependencies (cached layer)
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Copy source code
COPY src/ src/
COPY schema.sql .

# Build application
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder stage
COPY --from=builder /app/target/release/siscom-trips /usr/local/bin/siscom-trips

# Create log directory
RUN mkdir -p /var/log/siscom-trips

# Create non-root user
RUN useradd -r -s /bin/false siscom && \
    chown -R siscom:siscom /var/log/siscom-trips

USER siscom

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pidof siscom-trips || exit 1

CMD ["siscom-trips"]
