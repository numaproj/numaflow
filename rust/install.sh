#!/bin/bash

set -euo pipefail

# Determine the host architecture
HOST_ARCH=$(uname -m)

if [ "$#" -eq 0 ]; then
    if [ "$HOST_ARCH" == "x86_64" ]; then
        set -- "amd64"
    else
        set -- "arm64"
    fi
fi

# Validate positional arguments
for ARCH in "$@"; do
    if [ "$ARCH" != "amd64" ] && [ "$ARCH" != "arm64" ]; then
        echo "Unsupported architecture: $ARCH. Supported values are: amd64, arm64."
        exit 1
    fi
done

# Update and install necessary packages
apt update && apt install -y build-essential curl protobuf-compiler

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env


echo "arch: $HOST_ARCH"

# if [ "$HOST_ARCH" == "aarch64" ]; then
#     apt install -y gcc-x86-64-linux-gnu
#     rustup target add x86_64-unknown-linux-gnu
#     RUSTFLAGS='-C linker=x86_64-linux-gnu-gcc -C target-feature=+crt-static' cargo build --target x86_64-unknown-linux-gnu
#     RUSTFLAGS='-C target-feature=+crt-static' cargo build --target aarch64-unknown-linux-gnu
# fi

# # Build for arm64
# if [ "$HOST_ARCH" == "x86_64" ]; then
#     apt install -y gcc-aarch64-linux-gnu
#     rustup target add aarch64-unknown-linux-gnu
#     RUSTFLAGS='-C linker=aarch64-linux-gnu-gcc -C target-feature=+crt-static' cargo build --target aarch64-unknown-linux-gnu
#     RUSTFLAGS='-C target-feature=+crt-static' cargo build --target x86_64-unknown-linux-gnu
# fi


for ARCH in "$@"; do
    case $ARCH in
        amd64)
            if [ "$HOST_ARCH" != "x86_64" ]; then
                apt install -y gcc-x86-64-linux-gnu
                rustup target add x86_64-unknown-linux-gnu
            fi
            echo "Building for target: x86_64-unknown-linux-gnu"
            RUSTFLAGS="-C linker=x86_64-linux-gnu-gcc -C target-feature=+crt-static" cargo build --target x86_64-unknown-linux-gnu --release
            ;;
        arm64)
            if [ "$HOST_ARCH" != "aarch64" ]; then
                apt install -y gcc-aarch64-linux-gnu
                rustup target add aarch64-unknown-linux-gnu
            fi
            echo "Building for target: aarch64-unknown-linux-gnu"
            RUSTFLAGS="-C linker=aarch64-linux-gnu-gcc -C target-feature=+crt-static" cargo build --target aarch64-unknown-linux-gnu --release
            ;;
        *)
            echo "Unsupported architecture: $ARCH"
            exit 1
            ;;
    esac
done