set(STORAGE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_node.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine/map_engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/utility/timer.cpp
    ${PROJECT_BINARY_DIR}/generated/common.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.grpc.pb.cc
)

add_executable(storage
    ${STORAGE_SOURCES}
)

target_link_libraries(storage PRIVATE
    adviskv_common
)

target_include_directories(storage PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/build/generated
)
