ARG RUST_VERSION=1.95.0
ARG CARGOFLAGS=""
ARG RUSTFLAGS="-C target-feature=+crt-static -L /usr/lib -l static=crypto -l static=ssl"
ARG TARGET=x86_64-unknown-linux-musl

FROM rust:${RUST_VERSION}-alpine AS chef
RUN apk add --no-cache \
    git \
    openssh \
    libc-dev \
    bash \
    curl \
    make \
    cmake \
    mold \
    perl \
    musl-dev \
    pkgconfig \
    openssl-dev \
    openssl-libs-static \
    ca-certificates \
    zlib-dev \
    zlib-static \
    alpine-sdk \
    librdkafka-dev \
    clang \
    clang-dev \
    libc++ \
    libc++-dev \
    protobuf \
    protobuf-dev \
    python3

RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
# The recipe depends on manifests only; copying the tree costs nothing and means
# a new program crate needs no Dockerfile edit.
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG CARGOFLAGS
ARG RUSTFLAGS
ARG TARGET

ENV CARGOFLAGS=${CARGOFLAGS} \
    RUSTFLAGS=${RUSTFLAGS}

COPY rust-toolchain.toml ./
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --target ${TARGET} --release --recipe-path recipe.json

COPY . .
RUN cargo build --target ${TARGET} --release ${CARGOFLAGS}

FROM alpine:3.18 AS runtime
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/solana-sentinel* /usr/local/bin/
RUN rm -rf /usr/local/bin/*.d
