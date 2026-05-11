add_executable(storage_cli
    ${CMAKE_CURRENT_SOURCE_DIR}/src/storage_cli.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/direct_storage_client.cpp
    ${PROJECT_BINARY_DIR}/generated/storage.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.grpc.pb.cc
)

target_link_libraries(storage_cli PRIVATE
    adviskv_common
)

target_include_directories(storage_cli PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/generated
)

add_executable(meta_cli
    ${CMAKE_CURRENT_SOURCE_DIR}/src/meta_cli.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/direct_meta_client.cpp
    ${PROJECT_BINARY_DIR}/generated/meta.pb.cc
    ${PROJECT_BINARY_DIR}/generated/meta.grpc.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.grpc.pb.cc
)

target_link_libraries(meta_cli PRIVATE
    adviskv_common
)

target_include_directories(meta_cli PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/generated
)
