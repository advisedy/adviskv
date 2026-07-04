cmake_minimum_required(VERSION 3.20)

set(CMAKE_TOOLCHAIN_FILE
    "${CMAKE_CURRENT_LIST_DIR}/third_party/vcpkg/scripts/buildsystems/vcpkg.cmake"
    CACHE FILEPATH "vcpkg toolchain file")
set(VCPKG_INSTALLED_DIR
    "${CMAKE_CURRENT_LIST_DIR}/.adviskv_deps/vcpkg/installed"
    CACHE PATH "vcpkg installed directory")

#控制 vcpkg toolchain 是否在 CMake configure （cmake -S . -B build）阶段自动安装依赖，跑vcpkg install
set(VCPKG_MANIFEST_INSTALL OFF CACHE BOOL "Disable vcpkg install during CMake configure")

project(adviskv CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 关于开启覆盖率的
option(ADVISKV_ENABLE_COVERAGE "Enable coverage instrumentation" OFF)

#   -DADVISKV_SANITIZER=address  -> ASan + UBSan
set(ADVISKV_SANITIZER "" CACHE STRING "Sanitizer mode: empty or address")
set_property(CACHE ADVISKV_SANITIZER PROPERTY STRINGS "" address)

if(ADVISKV_SANITIZER)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        message(FATAL_ERROR "Sanitizers require GCC/Clang-compatible compilers")
    endif()
    if(ADVISKV_ENABLE_COVERAGE)
        message(FATAL_ERROR "ADVISKV_SANITIZER and ADVISKV_ENABLE_COVERAGE cannot be enabled together")
    endif()

    add_compile_options(-g -fno-omit-frame-pointer)
    if(ADVISKV_SANITIZER STREQUAL "address")
        add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=undefined)
        add_link_options(-fsanitize=address,undefined)
    else()
        message(FATAL_ERROR "Unknown ADVISKV_SANITIZER='${ADVISKV_SANITIZER}', expected empty or address")
    endif()
endif()

if(ADVISKV_ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        add_compile_options(-O0 -g --coverage)
        add_link_options(--coverage)
    else()
        message(FATAL_ERROR "Coverage is only supported with GCC/Clang-compatible compilers")
    endif()
endif()

# 关于开启warning
option(ADVISKV_ENABLE_WARNINGS "Enable project compiler warnings" ON)

if(ADVISKV_ENABLE_WARNINGS)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        add_compile_options(-Wall -Wextra -Wpedantic)
    endif()

    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
        add_compile_options(-Werror=reorder-init-list)
    endif()
endif()

find_package(fmt CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)

set(ADVISKV_GENERATED_DIR ${PROJECT_BINARY_DIR}/generated)
set(ADVISKV_PROTO_FILES
    ${CMAKE_SOURCE_DIR}/proto/common.proto
    ${CMAKE_SOURCE_DIR}/proto/storage.proto
    ${CMAKE_SOURCE_DIR}/proto/meta.proto
    ${CMAKE_SOURCE_DIR}/proto/sdm.proto
)

set(ADVISKV_PROTO_GENERATED_SRCS)
set(ADVISKV_PROTO_GENERATED_HDRS)

foreach(ADVISKV_PROTO_FILE ${ADVISKV_PROTO_FILES})
    # NAME_WE：file name without extension。
    get_filename_component(ADVISKV_PROTO_NAME ${ADVISKV_PROTO_FILE} NAME_WE)
    list(APPEND ADVISKV_PROTO_GENERATED_SRCS
        ${ADVISKV_GENERATED_DIR}/${ADVISKV_PROTO_NAME}.pb.cc
        ${ADVISKV_GENERATED_DIR}/${ADVISKV_PROTO_NAME}.grpc.pb.cc
    )
    list(APPEND ADVISKV_PROTO_GENERATED_HDRS
        ${ADVISKV_GENERATED_DIR}/${ADVISKV_PROTO_NAME}.pb.h
        ${ADVISKV_GENERATED_DIR}/${ADVISKV_PROTO_NAME}.grpc.pb.h
    )
endforeach()

set(ADVISKV_PROTO_GENERATED
    ${ADVISKV_PROTO_GENERATED_SRCS}
    ${ADVISKV_PROTO_GENERATED_HDRS}
)

add_custom_command(
    OUTPUT ${ADVISKV_PROTO_GENERATED}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${ADVISKV_GENERATED_DIR} # 创建生成目录
    COMMAND protobuf::protoc
    -I ${CMAKE_SOURCE_DIR}/proto # proto import 路径是 proto/ 目录
    --cpp_out=${ADVISKV_GENERATED_DIR}
    --grpc_out=${ADVISKV_GENERATED_DIR}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    ${ADVISKV_PROTO_FILES}
    DEPENDS ${ADVISKV_PROTO_FILES} protobuf::protoc gRPC::grpc_cpp_plugin
    VERBATIM
)

add_custom_target(adviskv_generate_proto DEPENDS ${ADVISKV_PROTO_GENERATED})

add_library(adviskv_proto STATIC
    ${ADVISKV_PROTO_GENERATED_SRCS}
    ${ADVISKV_PROTO_GENERATED_HDRS}
)
# 编译 adviskv_proto 之前，先执行 adviskv_generate_proto
add_dependencies(adviskv_proto adviskv_generate_proto)

target_include_directories(adviskv_proto PUBLIC
    ${ADVISKV_GENERATED_DIR}
)

target_link_libraries(adviskv_proto PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)

# adviskv_proto 的代码禁掉 warning
target_compile_options(adviskv_proto PRIVATE
    $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-nullability-extension>
)

enable_testing()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_subdirectory(src)
add_subdirectory(tools/cli)
add_subdirectory(tools/e2e)
add_subdirectory(tools/bench)
add_subdirectory(test)
