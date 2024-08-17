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
ARG ARCH

COPY dist/numaflow-linux-${ARCH} /bin/numaflow

RUN chmod +x /bin/numaflow

####################################################################################################
# extension base
####################################################################################################
FROM rust:1.80-bookworm AS extension-base
ARG TARGETPLATFORM

RUN apt-get update && apt-get install protobuf-compiler -y

RUN cargo new numaflow
# Create a new empty shell project
WORKDIR /numaflow

RUN cargo new servesink
COPY ./rust/servesink/Cargo.toml ./servesink/

RUN cargo new backoff
COPY ./rust/backoff/Cargo.toml ./backoff/

RUN cargo new numaflow-models
COPY ./rust/numaflow-models/Cargo.toml ./numaflow-models/

RUN cargo new monovertex
COPY ./rust/monovertex/Cargo.toml ./monovertex/

RUN cargo new serving
COPY ./rust/serving/Cargo.toml ./serving/Cargo.toml

# Copy all Cargo.toml and Cargo.lock files for caching dependencies
COPY ./rust/Cargo.toml ./rust/Cargo.lock ./

# Build to cache dependencies
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    case ${TARGETPLATFORM} in \
        "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
        "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    mkdir -p src/bin && echo "fn main() {}" > src/bin/main.rs && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo build --workspace --all --release --target ${TARGET}

# Copy the actual source code files of the main project and the subprojects
COPY ./rust/src ./src
COPY ./rust/servesink/src ./servesink/src
COPY ./rust/backoff/src ./backoff/src
COPY ./rust/numaflow-models/src ./numaflow-models/src
COPY ./rust/serving/src ./serving/src
COPY ./rust/monovertex/src ./monovertex/src
COPY ./rust/monovertex/build.rs ./monovertex/build.rs
COPY ./rust/monovertex/proto ./monovertex/proto

# Build the real binaries
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    case ${TARGETPLATFORM} in \
        "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
        "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    touch src/bin/main.rs && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo build --workspace --all --release --target ${TARGET} && \
    cp -pv target/${TARGET}/release/numaflow /root/numaflow

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS numaflow

COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/numaflow /bin/numaflow
COPY ui/build /ui/build

COPY --from=extension-base /root/numaflow /bin/numaflow-rs
COPY ./rust/serving/config config

ENTRYPOINT [ "/bin/numaflow-rs" ]

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