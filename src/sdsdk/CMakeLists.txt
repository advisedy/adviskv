set(SDSDK_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/node_agent.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/heartbeater.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica_controller.cpp
    ${PROJECT_BINARY_DIR}/generated/sdm.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.grpc.pb.cc
)

add_library(sdsdk
    ${SDSDK_SOURCES}
)

target_link_libraries(sdsdk PUBLIC
    adviskv_common
)

target_include_directories(sdsdk PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/build/generated
)
