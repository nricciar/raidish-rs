# syntax=docker/dockerfile:1
# raidish in a container: see docker-compose.yml and "Try it with Docker" in
# the README.
#
#   docker build -t raidish .
#   docker build --build-arg RAIDISH_SKIP_UI=1 -t raidish .   # no dashboard, faster

FROM rust:1-bookworm AS build
ARG TARGETARCH
ARG TRUNK_VERSION=0.21.14
ARG RAIDISH_SKIP_UI=
WORKDIR /src

# The toolchain (nightly + the wasm target, from rust-toolchain.toml).
COPY rust-toolchain.toml .
RUN rustup toolchain install nightly --profile minimal --target wasm32-unknown-unknown

# trunk builds the dashboard; build.rs runs it during the release build.
RUN case "$TARGETARCH" in \
      arm64) arch=aarch64 ;; \
      *) arch=x86_64 ;; \
    esac \
 && curl -fsSL "https://github.com/trunk-rs/trunk/releases/download/v${TRUNK_VERSION}/trunk-${arch}-unknown-linux-gnu.tar.gz" \
    | tar -xz -C /usr/local/bin trunk

# BuildKit cache mounts keep the cargo registry, compiled dependencies
# (target/, including the dashboard's target/web) and trunk's downloaded
# tools (wasm-bindgen) between builds, so a source change only recompiles
# our own crates. They live outside the image: `docker builder prune`
# clears them.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/src/target \
    if [ -n "$RAIDISH_SKIP_UI" ]; then export RAIDISH_SKIP_UI; else unset RAIDISH_SKIP_UI; fi \
 && cargo build --release --locked --bin raidish \
 && cp target/release/raidish /usr/local/bin/raidish

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/raidish /usr/local/bin/raidish
COPY docker/entrypoint.sh /usr/local/bin/raidish-entrypoint
COPY docker/client.yaml /root/.raidish.yaml
VOLUME /data
ENTRYPOINT ["raidish-entrypoint"]
CMD ["--help"]
