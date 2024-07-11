#!/bin/sh
set -e

usage() {
  this=$1
  cat <<EOF
$this: download and install protobuf

Usage: $this [-b <bindir>] [<version>]
  -b sets bindir or installation directory, defaults to ${HOME}/bin

EOF
  exit 2
}

parse_args() {
  # BINDIR is ./bin unless set be ENV
  # overridden by flag below

  BINDIR=${BINDIR:-${HOME}/bin}
  while getopts "b:h?x" arg; do
    case "$arg" in
      b) BINDIR="$OPTARG" ;;
      h | \?) usage "$0" ;;
      x) set -x ;;
    esac
  done
  shift $((OPTIND - 1))
  VERSION=$1
  if [ "$VERSION" = "" ]; then
      usage "$0"
  fi
  # if version starts with 'v', remove it
  VERSION=${VERSION#v}
}

source $(dirname $0)/library.sh

PB_REL="https://github.com/protocolbuffers/protobuf/releases"
OS=$(uname_os)
ARCH=$(uname_arch)

parse_args "$@"

echo "OS: $OS  ARCH: $ARCH"

tmp=$(mktemp -d)
trap 'rm -rf ${tmp}' EXIT

BINARY_URL=$PB_REL/download/v${VERSION}/protoc-${VERSION}-${OS}-${ARCH}.zip
if  [ "$OS" = "darwin" ]; then
  BINARY_URL=$PB_REL/download/v${VERSION}/protoc-${VERSION}-osx-universal_binary.zip
fi
echo $BINARY_URL

curl -sL -o ${tmp}/protoc-${VERSION}-${OS}-${ARCH}.zip $BINARY_URL
unzip ${tmp}/protoc-${VERSION}-${OS}-${ARCH}.zip -d ${tmp}
mv ${tmp}/bin/protoc $BINDIR
