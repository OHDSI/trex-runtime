## Stage 1: Build the FHIR trexsql extension
FROM rust:1.82-bookworm AS builder

WORKDIR /build

# Copy workspace Cargo files
COPY Cargo.toml Cargo.lock ./
COPY plugins/fhir/ plugins/fhir/
COPY src/ src/

# Build the extension in release mode
RUN cargo build --release -p fhir

## Stage 2: Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled extension
COPY --from=builder /build/target/release/libfhir.so /app/fhir.trex

# Copy FHIR R4 data files (embedded at compile time, but useful for reference)
COPY plugins/fhir/data/ /app/data/

# Startup script
COPY plugins/fhir/docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

EXPOSE 8080

VOLUME /data

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENV FHIR_HOST=0.0.0.0
ENV FHIR_PORT=8080
ENV TREXSQL_PATH=/data/fhir.db

ENTRYPOINT ["/app/docker-entrypoint.sh"]
