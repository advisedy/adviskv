set(ADVISKV_SDK_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sdm_route_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/storage_client.cpp
    ${PROJECT_BINARY_DIR}/generated/common.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.grpc.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.grpc.pb.cc
)

add_library(adviskv_sdk
    ${ADVISKV_SDK_SOURCES}
)

target_link_libraries(adviskv_sdk PUBLIC
    adviskv_common
)

target_include_directories(adviskv_sdk PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/generated
)
