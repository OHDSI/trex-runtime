# syntax=docker/dockerfile:1.4

FROM rust:1.84.0-bookworm AS ort
ENV ONNXRUNTIME_VERSION=1.20.1
ENV DENO_VERSION=2.1.4
ENV BASEPATH=${TREXBASEPATH:-"."}

RUN mkdir -p ./scripts

COPY $BASEPATH/scripts/install_onnx.sh ./scripts

RUN ./scripts/install_onnx.sh $ONNXRUNTIME_VERSION linux linux/amd64 /root/onnxruntime

FROM node:24-bookworm AS base

COPY --from=ort /root/onnxruntime/lib/libonnxruntime.so* /usr/lib/

RUN echo "deb http://deb.debian.org/debian testing main" >> /etc/apt/sources.list && \
apt-get update && apt-get install -y \
    curl clang libgomp1 unzip wget libssl-dev default-jre-headless mesa-vulkan-drivers libopenblas-dev \
    && apt-get install -y -t testing libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

ARG TREXSQL_VERSION=v1.4.4-trex
RUN mkdir -p /usr/local/lib && \
    wget -O /tmp/libtrexsql.zip \
      https://github.com/p-hoffmann/trexsql-rs/releases/download/${TREXSQL_VERSION}/libtrexsql-linux-amd64.zip && \
    unzip /tmp/libtrexsql.zip -d /tmp/trexsql && \
    cp /tmp/trexsql/libtrexsql.so /usr/local/lib/ && \
    cp /tmp/trexsql/duckdb.h /usr/local/include/ && \
    cp /tmp/trexsql/duckdb.hpp /usr/local/include/ && \
    rm -rf /tmp/libtrexsql.zip /tmp/trexsql && \
    ldconfig

RUN curl -sL https://lib.chdb.io | bash

RUN mkdir -p /usr/src

WORKDIR /usr/src

COPY $BASEPATH/deno.json $BASEPATH/package.json $BASEPATH/package-lock.json $BASEPATH/.npmrc $BASEPATH/nbb.edn ./

RUN npm install -g nbb

RUN npm install

COPY $BASEPATH/src ./src
COPY $BASEPATH/main ./main
COPY $BASEPATH/event-worker ./event-worker

RUN mkdir -p ./cert

ENV TREXAS_HOST=0.0.0.0
ENV TREXAS_PORT=9876
ENV PGWIRE_HOST=0.0.0.0
ENV PGWIRE_PORT=5433
ENV TLS_CERT_PATH=""
ENV TLS_KEY_PATH=""
ENV TLS_PORT=9443
ENV MAIN_PATH=/usr/src/main

EXPOSE 33001 5432 33000

#HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
#    CMD curl -f http://localhost:33001/_internal/health || exit 1

CMD ["bun", "run", "start:docker"]
