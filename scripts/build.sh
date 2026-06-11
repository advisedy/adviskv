#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$REPO_ROOT"

BUILD_DIR="${BUILD_DIR:-build}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
GENERATOR="${GENERATOR:-Ninja}"
ADVISKV_DEPS_ROOT="${ADVISKV_DEPS_ROOT:-$REPO_ROOT/.adviskv_deps}"
VCPKG_INSTALL_ROOT="${VCPKG_INSTALL_ROOT:-$ADVISKV_DEPS_ROOT/vcpkg/installed}"
VCPKG_TOOLCHAIN_FILE="${VCPKG_TOOLCHAIN_FILE:-$REPO_ROOT/third_party/vcpkg/scripts/buildsystems/vcpkg.cmake}"
GENERATED_DIR="$BUILD_DIR/generated"

if [[ ! -d "$VCPKG_INSTALL_ROOT" ]]; then
  echo "missing vcpkg dependencies under $VCPKG_INSTALL_ROOT" >&2
  echo "run ./scripts/setup.sh once before building" >&2
  exit 1
fi

mkdir -p "$GENERATED_DIR"

PROTOC="$(find "$VCPKG_INSTALL_ROOT" -type f -path '*/tools/protobuf/protoc*' | sort | head -n 1)"
GRPC_CPP_PLUGIN="$(find "$VCPKG_INSTALL_ROOT" -type f -path '*/tools/grpc/grpc_cpp_plugin' | sort | head -n 1)"

if [[ -z "$PROTOC" || -z "$GRPC_CPP_PLUGIN" ]]; then
  echo "failed to find protoc or grpc_cpp_plugin under $VCPKG_INSTALL_ROOT" >&2
  echo "run ./scripts/setup.sh to install or refresh dependencies" >&2
  exit 1
fi

"$PROTOC" \
  -I proto \
  --cpp_out="$GENERATED_DIR" \
  --grpc_out="$GENERATED_DIR" \
  --plugin=protoc-gen-grpc="$GRPC_CPP_PLUGIN" \
  proto/common.proto \
  proto/storage.proto \
  proto/meta.proto \
  proto/sdm.proto

cmake \
  -S . \
  -B "$BUILD_DIR" \
  -G "$GENERATOR" \
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_TOOLCHAIN_FILE" \
  -DVCPKG_INSTALLED_DIR="$VCPKG_INSTALL_ROOT" \
  -DVCPKG_MANIFEST_INSTALL=OFF \
  "$@"

cmake --build "$BUILD_DIR" --parallel
