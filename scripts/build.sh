#!/usr/bin/env bash
set -e

cmake -S . -B build \
  -DCMAKE_TOOLCHAIN_FILE=third_party/vcpkg/scripts/buildsystems/vcpkg.cmake \
  "$@"

cmake --build build