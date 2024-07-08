FROM rust:1.79-bookworm as build

# For faster/easier installation of Rust binaries
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

RUN apt-get update \
  && apt-get install -y protobuf-compiler

# Compile and cache our dependencies in a layer
RUN cargo new /serve
WORKDIR /serve
COPY Cargo.toml .

RUN cargo new extras/upstreams
COPY extras/upstreams/Cargo.toml extras/upstreams/Cargo.toml

RUN cargo new servesink
COPY servesink/Cargo.toml servesink/Cargo.toml

RUN cargo new backoff
COPY backoff/Cargo.toml backoff/Cargo.toml

RUN cargo build --release

COPY ./ /serve
# update timestamps to force a new build
RUN touch src/main.rs servesink/src/main.rs extras/upstreams/src/main.rs 

RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --release

### Final

FROM debian:bookworm

USER root

RUN apt-get update \
  && apt-get install -y openssl

COPY --from=build /serve/target/release/serve .
COPY ./config config


ENTRYPOINT ["./serve"]
