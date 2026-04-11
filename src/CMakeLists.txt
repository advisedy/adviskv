add_library(adviskv_common
    common/log.cpp
)

target_include_directories(adviskv_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(adviskv_common PUBLIC
    fmt::fmt
    spdlog::spdlog
    protobuf::libprotobuf
    yaml-cpp::yaml-cpp
    gRPC::grpc++
)

add_subdirectory(storage)
add_subdirectory(meta)
add_subdirectory(sdm)