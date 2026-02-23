# ── Stage 1: Build the trex binary ──────────────────────────────────────────
FROM debian:trixie-slim AS trex-builder

ARG TREXSQL_VERSION=v1.4.4-trex
ARG CHDB_VERSION=v3.6.0

RUN apt-get update && apt-get install -y curl unzip wget gcc libc6-dev && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.85.1
ENV PATH="/root/.cargo/bin:${PATH}"

RUN mkdir -p /opt/trexsql && \
    wget -O /tmp/libtrexsql.zip \
      https://github.com/p-hoffmann/trexsql-rs/releases/download/${TREXSQL_VERSION}/libtrexsql-linux-amd64.zip && \
    unzip /tmp/libtrexsql.zip -d /opt/trexsql && \
    rm /tmp/libtrexsql.zip && \
    ln -sf /opt/trexsql/libtrexsql.so /opt/trexsql/libduckdb.so

ENV TREXSQL_LIB_DIR=/opt/trexsql
ENV TREXSQL_INCLUDE_DIR=/opt/trexsql

RUN cd /tmp && \
    wget -O libchdb.tar.gz \
      https://github.com/chdb-io/chdb/releases/download/${CHDB_VERSION}/linux-x86_64-libchdb.tar.gz && \
    tar -xzf libchdb.tar.gz && \
    mv libchdb.so /opt/ && \
    rm -f libchdb.tar.gz chdb.h

COPY Cargo.toml Cargo.lock /usr/src/trexsql/
WORKDIR /usr/src/trexsql
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs && \
    cargo build --release && \
    rm -rf src target/release/trex target/release/libtrexsql_engine* \
      target/release/deps/trexsql* target/release/.fingerprint/trexsql-*

COPY src/ /usr/src/trexsql/src/
RUN cargo build --release

# ── Stage 2: Build pg_trex extension ─────────────────────────────────────────
FROM postgres:17 AS pg-trex-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gcc libc6-dev pkg-config libssl-dev make \
      postgresql-server-dev-17 \
    && rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.88.0

RUN . /root/.cargo/env && cargo install cargo-pgrx --version 0.16.1 --locked
RUN . /root/.cargo/env && cargo pgrx init --pg17=$(which pg_config)

COPY --from=trex-builder /opt/trexsql /opt/trexsql
RUN ln -sf /opt/trexsql/libtrexsql.so /usr/lib/libduckdb.so && ldconfig
ENV TREXSQL_LIB_DIR=/opt/trexsql
ENV TREXSQL_INCLUDE_DIR=/opt/trexsql

WORKDIR /usr/src/pg_trex
COPY plugins/pg_trex/Cargo.toml plugins/pg_trex/Cargo.lock ./
RUN . /root/.cargo/env && \
    mkdir -p src/bin && echo '#[allow(warnings)] fn main(){}' > src/bin/pgrx_embed_pg_trex.rs && \
    echo '' > src/lib.rs && \
    (cargo build --release 2>/dev/null || true) && \
    rm -rf src target/release/libpg_trex* target/release/deps/pg_trex* \
      target/release/.fingerprint/pg_trex-*

COPY plugins/pg_trex/src/ ./src/
RUN . /root/.cargo/env && cargo build --release

# ── Stage 3: Collect npm extensions + official DuckDB extensions ──────────
FROM node:20-trixie-slim AS npm-deps

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src
COPY package.json package-lock.json .npmrc deno.json ./
RUN npm install

RUN mkdir -p /usr/lib/trexsql/extensions && \
    find node_modules/@trex -name "*.trex" -exec cp {} /usr/lib/trexsql/extensions/ \; && \
    find node_modules/@trex -name "*.duckdb_extension" -exec cp {} /usr/lib/trexsql/extensions/ \;

ENV DUCKDB_VERSION=1.4.4
RUN mkdir -p /root/.duckdb/extensions/v${DUCKDB_VERSION}/linux_amd64 && \
    cd /root/.duckdb/extensions/v${DUCKDB_VERSION}/linux_amd64 && \
    for lib in avro aws delta ducklake fts httpfs icu iceberg inet json mysql_scanner parquet postgres_scanner spatial sqlite sqlite_scanner vss; do \
        curl -sfO http://extensions.duckdb.org/v${DUCKDB_VERSION}/linux_amd64/${lib}.duckdb_extension.gz && \
        gzip -d ${lib}.duckdb_extension.gz; \
    done && \
    for lib in bigquery; do \
        curl -sfO http://community-extensions.duckdb.org/v${DUCKDB_VERSION}/linux_amd64/${lib}.duckdb_extension.gz && \
        gzip -d ${lib}.duckdb_extension.gz; \
    done

# CI override (no-op locally since dir only has .gitkeep)
COPY extensions/ /usr/lib/trexsql/extensions/

# ── Stage 4: Build documentation site ─────────────────────────────────────
FROM node:20-trixie-slim AS docs-builder

WORKDIR /usr/src/docs
COPY core/docs/ .
RUN npm ci && npm run build

# ── Stage 5: Runtime (postgres:17 + everything) ───────────────────────────
FROM postgres:17

# ── System dependencies ──
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libssl3 libgomp1 libvulkan1 curl gnupg && \
    rm -rf /var/lib/apt/lists/*

# ── Node.js 20 (needed for trexas / core server) ──
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

# ── libtrexsql for pg_trex (into PG lib dir) ──
COPY --from=trex-builder /opt/trexsql/libtrexsql.so /usr/lib/postgresql/17/lib/libtrexsql.so.1.4
RUN ln -sf libtrexsql.so.1.4 /usr/lib/postgresql/17/lib/libtrexsql.so && \
    ln -sf libtrexsql.so.1.4 /usr/lib/postgresql/17/lib/libduckdb.so && \
    ln -sf libtrexsql.so.1.4 /usr/lib/postgresql/17/lib/libduckdb.so.1.4 && \
    echo "/usr/lib/postgresql/17/lib" >> /etc/ld.so.conf.d/pg_trex.conf

# ── Libraries for trex binary + versioned soname for pg_trex ──
COPY --from=trex-builder /opt/trexsql/libtrexsql.so /usr/lib/libtrexsql.so.1.4
RUN ln -sf libtrexsql.so.1.4 /usr/lib/libtrexsql.so && \
    ln -sf libtrexsql.so.1.4 /usr/lib/libduckdb.so.1.4 && \
    ln -sf libtrexsql.so.1.4 /usr/lib/libduckdb.so
COPY --from=trex-builder /opt/libchdb.so /usr/lib/
COPY --from=trex-builder /usr/src/trexsql/target/release/libtrexsql_engine.so /usr/lib/
RUN ldconfig

# ── trex binary ──
COPY --from=trex-builder /usr/src/trexsql/target/release/trex /usr/bin/

# ── pg_trex extension ──
COPY --from=pg-trex-builder /usr/src/pg_trex/target/release/libpg_trex.so /usr/lib/postgresql/17/lib/pg_trex.so
COPY plugins/pg_trex/pg_trex.control /usr/share/postgresql/17/extension/
COPY plugins/pg_trex/sql/pg_trex--0.1.0.sql /usr/share/postgresql/17/extension/

# ── trexsql extensions (npm + CI override) ──
COPY --from=npm-deps /usr/lib/trexsql/extensions/ /usr/lib/trexsql/extensions/

# ── DuckDB extensions (for both postgres user and root) ──
ENV DUCKDB_VERSION=1.4.4
COPY --from=npm-deps /root/.duckdb/extensions/ /var/lib/postgresql/.duckdb/extensions/
COPY --from=npm-deps /root/.duckdb/extensions/ /root/.duckdb/extensions/
RUN chown -R postgres:postgres /var/lib/postgresql/.duckdb

# ── Shinylive ──
ARG SHINYLIVE_VERSION=0.10.7
RUN cd /usr/src && \
    curl -sLO https://github.com/posit-dev/shinylive/releases/download/v${SHINYLIVE_VERSION}/shinylive-${SHINYLIVE_VERSION}.tar.gz && \
    tar -xzf shinylive-${SHINYLIVE_VERSION}.tar.gz && \
    mv shinylive-${SHINYLIVE_VERSION} shinylive && \
    rm shinylive-${SHINYLIVE_VERSION}.tar.gz

# ── Application files ──
WORKDIR /usr/src
COPY core/ ./core/
COPY functions/ ./functions/
COPY --from=docs-builder /usr/src/docs/build ./docs/build
RUN mkdir -p ./plugins

# ── Pre-initialize the database to avoid the initdb→restart cycle.
# The entrypoint detects existing PGDATA and skips initdb+init scripts,
# which prevents services (gossip, flight, trexas) from starting and
# needing to be torn down during the init phase shutdown.
ENV POSTGRES_PASSWORD=mypass
ENV POSTGRES_HOST_AUTH_METHOD=trust
RUN mkdir -p /var/lib/postgresql/data \
    && chown postgres:postgres /var/lib/postgresql/data \
    && su postgres -c "initdb -D /var/lib/postgresql/data --auth=trust --no-sync" \
    && su postgres -c "pg_ctl -D /var/lib/postgresql/data -o '-c shared_preload_libraries=pg_trex' -w start" \
    && su postgres -c "psql -c \"CREATE EXTENSION pg_trex;\"" \
    && su postgres -c "createdb testdb" \
    && su postgres -c "psql -d testdb -c \"CREATE EXTENSION pg_trex;\"" \
    && echo "host all all 0.0.0.0/0 trust" >> /var/lib/postgresql/data/pg_hba.conf \
    && su postgres -c "pg_ctl -D /var/lib/postgresql/data -m immediate stop"
ENV PGDATA=/var/lib/postgresql/data

ENV SCHEMA_DIR=/usr/src/core/schema
