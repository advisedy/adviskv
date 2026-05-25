#!/usr/bin/env bash

set -euo pipefail


BUILD_DIR="${BUILD_DIR:-build}"

BUILD_TYPE="${BUILD_TYPE:-Release}"

GENERATOR="${GENERATOR:-Ninja}"

BUILD_TARGETS="${BUILD_TARGETS:-}"

VCPKG_TOOLCHAIN_FILE="${VCPKG_TOOLCHAIN_FILE:-third_party/vcpkg/scripts/buildsystems/vcpkg.cmake}"

GENERATED_DIR="$BUILD_DIR/generated"

VCPKG_EXE="${VCPKG_EXE:-third_party/vcpkg/vcpkg}"


if [[ ! -x "$VCPKG_EXE" ]]; then

  ./third_party/vcpkg/bootstrap-vcpkg.sh

fi


"$VCPKG_EXE" install --x-install-root="$PWD/$BUILD_DIR/vcpkg_installed"


mkdir -p "$GENERATED_DIR"


PROTOC="$(find "$BUILD_DIR/vcpkg_installed" -type f -path '*/tools/protobuf/protoc*' | sort | head -n 1)"

GRPC_CPP_PLUGIN="$(find "$BUILD_DIR/vcpkg_installed" -type f -path '*/tools/grpc/grpc_cpp_plugin' | sort | head -n 1)"


if [[ -z "$PROTOC" || -z "$GRPC_CPP_PLUGIN" ]]; then

  echo "failed to find protoc or grpc_cpp_plugin under $BUILD_DIR/vcpkg_installed" >&2

  exit 1

fi


PROTO_FILES=(
  proto/common.proto
  proto/storage.proto
  proto/meta.proto
  proto/sdm.proto
)


PROTOC_ARGS=(
  -I proto
  --cpp_out="$GENERATED_DIR"
  --grpc_out="$GENERATED_DIR"
  --plugin=protoc-gen-grpc="$GRPC_CPP_PLUGIN"
  "${PROTO_FILES[@]}"
)

"$PROTOC" "${PROTOC_ARGS[@]}"

CMAKE_ARGS=(
  -S .
  -B "$BUILD_DIR"
  -G "$GENERATOR"
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_TOOLCHAIN_FILE"
  "$@"
)

cmake "${CMAKE_ARGS[@]}"

if [[ -n "$BUILD_TARGETS" ]]; then

  for target in $BUILD_TARGETS; do

    cmake --build "$BUILD_DIR" --parallel --target "$target"

  done

else

  cmake --build "$BUILD_DIR" --parallel

fi

