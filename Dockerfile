ARG BASE_IMAGE=scratch
ARG ARCH=$TARGETARCH
####################################################################################################
# base (host-built Go binary + CA/tz data)
####################################################################################################
FROM alpine:3.17 AS base
ARG ARCH
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/numaflow-linux-${ARCH} /bin/numaflow
RUN chmod +x /bin/numaflow

####################################################################################################
# Rust binary
####################################################################################################
# Dependency reuse relies on BuildKit cache mounts below (not cargo-chef layer caching).
FROM rust:1.97.1-trixie AS rust-builder
ARG TARGETPLATFORM
WORKDIR /numaflow
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler cmake clang \
    && rm -rf /var/lib/apt/lists/*

# Install the pinned toolchain so source-only changes do not re-download it.
COPY rust/rust-toolchain.toml ./rust-toolchain.toml
RUN rustup show

COPY ./rust/ .

ARG ARCH
ARG VERSION=latest
ENV VERSION=$VERSION
ARG BUILD_DATE
ENV BUILD_DATE=$BUILD_DATE
ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT
ARG GIT_BRANCH
ENV GIT_BRANCH=$GIT_BRANCH
ARG GIT_TAG
ENV GIT_TAG=$GIT_TAG
ARG GIT_TREE_STATE
ENV GIT_TREE_STATE=$GIT_TREE_STATE
# Cargo profile: "release" (default, fat LTO) or "image-dev" (faster local builds).
ARG CARGO_PROFILE=release

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/numaflow/target \
    case ${TARGETPLATFORM} in \
        "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
        "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo build -p numaflow --profile ${CARGO_PROFILE} --target ${TARGET} && \
    case ${CARGO_PROFILE} in \
        "dev") OUT_DIR="debug" ;; \
        *) OUT_DIR="${CARGO_PROFILE}" ;; \
    esac && \
    cp -pv target/${TARGET}/${OUT_DIR}/numaflow /root/numaflow && \
    cp -pv target/${TARGET}/${OUT_DIR}/entrypoint /root/entrypoint

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS numaflow
ARG ARCH

COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/numaflow /bin/numaflow
COPY --from=rust-builder /root/numaflow /bin/numaflow-rs
COPY ui/build /ui/build

# TODO: remove this when we are ported everything to Rust
COPY --from=rust-builder /root/entrypoint /bin/entrypoint
ENTRYPOINT ["/bin/entrypoint"]

####################################################################################################
# testbase
####################################################################################################
FROM alpine:3.17 AS testbase
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/e2eapi /bin/e2eapi
RUN chmod +x /bin/e2eapi

####################################################################################################
# testapi
####################################################################################################
FROM scratch AS e2eapi
COPY --from=testbase /bin/e2eapi .
ENTRYPOINT ["/e2eapi"]
