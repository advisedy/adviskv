
# set 
#     ${PROJECT_BINARY_DIR}/generated/common.pb.cc


add_library(adviskv_common
    ${CMAKE_CURRENT_SOURCE_DIR}/common/log.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/metrics/metrics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/background_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/thread_pool.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/enum_convert.cpp
    ${PROJECT_BINARY_DIR}/generated/common.pb.cc
)

target_include_directories(adviskv_common PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/generated
)

target_compile_definitions(adviskv_common PUBLIC
    ADVISKV_PROJECT_ROOT="${CMAKE_SOURCE_DIR}"
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
add_subdirectory(sdk)
