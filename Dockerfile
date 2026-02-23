# Stage 1: Build the trex binary
FROM debian:trixie-slim AS builder

ARG TREXSQL_VERSION=v1.4.4-trex
ARG CHDB_VERSION=v3.6.0

RUN apt-get update && apt-get install -y curl unzip wget gcc libc6-dev && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.88.0
ENV PATH="/root/.cargo/bin:${PATH}"

# Download libtrexsql from GitHub release
RUN mkdir -p /opt/trexsql && \
    wget -O /tmp/libtrexsql.zip \
      https://github.com/p-hoffmann/trexsql-rs/releases/download/${TREXSQL_VERSION}/libtrexsql-linux-amd64.zip && \
    unzip /tmp/libtrexsql.zip -d /opt/trexsql && \
    rm /tmp/libtrexsql.zip && \
    ln -sf /opt/trexsql/libtrexsql.so /opt/trexsql/libduckdb.so

ENV TREXSQL_LIB_DIR=/opt/trexsql
ENV TREXSQL_INCLUDE_DIR=/opt/trexsql

# Download libchdb from GitHub release
RUN cd /tmp && \
    wget -O libchdb.tar.gz \
      https://github.com/chdb-io/chdb/releases/download/${CHDB_VERSION}/linux-x86_64-libchdb.tar.gz && \
    tar -xzf libchdb.tar.gz && \
    mv libchdb.so /opt/ && \
    rm -f libchdb.tar.gz chdb.h

# Cache dependency build: copy manifests first, build with dummy src, then replace
COPY Cargo.toml Cargo.lock /usr/src/trexsql/
WORKDIR /usr/src/trexsql
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs && \
    cargo build --release && \
    rm -rf src target/release/trex target/release/libtrexsql_engine* \
      target/release/deps/trexsql* target/release/.fingerprint/trexsql-*

COPY src/ /usr/src/trexsql/src/
RUN cargo build --release

# Stage 2: Runtime
FROM node:20-trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      libssl3 libgomp1 ca-certificates libvulkan1 curl && \
    rm -rf /var/lib/apt/lists/*

# Copy trex binary, libtrexsql, libchdb, and libtrexsql_engine
COPY --from=builder /usr/src/trexsql/target/release/trex /usr/bin/
COPY --from=builder /opt/trexsql/libtrexsql.so /usr/lib/
COPY --from=builder /opt/libchdb.so /usr/lib/
COPY --from=builder /usr/src/trexsql/target/release/libtrexsql_engine.so /usr/lib/
RUN ldconfig

WORKDIR /usr/src

# Install extensions via npm
COPY package.json package-lock.json .npmrc deno.json ./
RUN npm install

# Collect extension files from node_modules into extensions dir
RUN mkdir -p /usr/lib/trexsql/extensions && \
    find node_modules/@trex -name "*.trex" -exec cp {} /usr/lib/trexsql/extensions/ \; && \
    find node_modules/@trex -name "*.duckdb_extension" -exec cp {} /usr/lib/trexsql/extensions/ \;

# Download official trexsql extensions for offline use
ENV DUCKDB_VERSION=1.4.4
RUN mkdir -p /usr/share/trexsql/extensions/v${DUCKDB_VERSION}/linux_amd64 && \
    cd /usr/share/trexsql/extensions/v${DUCKDB_VERSION}/linux_amd64 && \
    for lib in avro aws delta ducklake fts httpfs icu iceberg inet json mysql_scanner parquet postgres_scanner spatial sqlite sqlite_scanner vss; do \
        curl -sfO https://extensions.duckdb.org/v${DUCKDB_VERSION}/linux_amd64/${lib}.duckdb_extension.gz && \
        gzip -d ${lib}.duckdb_extension.gz; \
    done && \
    for lib in bigquery; do \
        curl -sfO https://community-extensions.duckdb.org/v${DUCKDB_VERSION}/linux_amd64/${lib}.duckdb_extension.gz && \
        gzip -d ${lib}.duckdb_extension.gz; \
    done

# Override npm extensions with CI-built ones (no-op locally since dir only has .gitkeep)
COPY extensions/ /usr/lib/trexsql/extensions/

# Download and extract Shinylive assets for analytics dashboards
ARG SHINYLIVE_VERSION=0.10.7
RUN curl -sLO https://github.com/posit-dev/shinylive/releases/download/v${SHINYLIVE_VERSION}/shinylive-${SHINYLIVE_VERSION}.tar.gz && \
    tar -xzf shinylive-${SHINYLIVE_VERSION}.tar.gz && \
    mv shinylive-${SHINYLIVE_VERSION} shinylive && \
    rm shinylive-${SHINYLIVE_VERSION}.tar.gz

# Create plugins directory for plugin installs
RUN mkdir -p ./plugins

# Copy core (overridden by volume mount in development)
COPY core/ ./core/

# Copy functions (overridden by volume mount in development)
COPY functions/ ./functions/

# Build documentation site
COPY core/docs/ ./core/docs/
RUN cd core/docs && npm ci && npm run build

ENV SCHEMA_DIR=/usr/src/core/schema
ENV DUCKDB_EXTENSION_DIRECTORY=/usr/share/trexsql/extensions

EXPOSE 8001
USER node
ENTRYPOINT ["trex"]
