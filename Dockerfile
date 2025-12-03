ARG BASE_IMAGE=scratch
ARG ARCH=$TARGETARCH
####################################################################################################
# base
####################################################################################################
FROM alpine:3.17 AS base
ARG ARCH
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/numaflow-linux-${ARCH} /bin/numaflow
COPY dist/numaflow-rs-linux-${ARCH} /bin/numaflow-rs

RUN chmod +x /bin/numaflow
RUN chmod +x /bin/numaflow-rs

# TODO: remove this when we are ported everything to Rust
COPY dist/entrypoint-linux-${ARCH} /bin/entrypoint
RUN chmod +x /bin/entrypoint

####################################################################################################
# Rust binary
####################################################################################################
FROM lukemathwalker/cargo-chef:latest-rust-1.90 AS chef
ARG TARGETPLATFORM
WORKDIR /numaflow
RUN apt-get update && apt-get install -y protobuf-compiler cmake clang

FROM chef AS planner
COPY ./rust/ .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS rust-builder

COPY --from=planner /numaflow/recipe.json recipe.json

# Build to cache dependencies
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    case ${TARGETPLATFORM} in \
        "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
        "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo chef cook --workspace --release --target ${TARGET} --recipe-path recipe.json

# Copy the actual source code files of the main project and the subprojects
COPY ./rust/ .

ARG TARGETPLATFORM
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
ARG GIT_TAG
ENV GIT_TAG=$GIT_TAG

# Build the real binaries
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    case ${TARGETPLATFORM} in \
        "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
        "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo build --workspace --all --release --target ${TARGET} && \
    cp -pv target/${TARGET}/release/numaflow /root/numaflow && \
    cp -pv target/${TARGET}/release/entrypoint /root/entrypoint

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS numaflow
ARG ARCH

COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/numaflow /bin/numaflow
COPY --from=base /bin/numaflow-rs /bin/numaflow-rs
COPY ui/build /ui/build

# TODO: remove this when we are ported everything to Rust
COPY --from=base /bin/entrypoint /bin/entrypoint
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
