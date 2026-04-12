set(SDM_SOURCES
    # current dir
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/manager/node_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/manager/route_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/manager/meta_cache_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/handler/sdm_service_impl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/route_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/placement_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/selector/node_selector/node_selector.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/selector/leader_selector/leader_selector.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operation/placedb_operation.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operation/placetable_operation.cpp

    # binary dir
    ${PROJECT_BINARY_DIR}/generated/common.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.grpc.pb.cc
)

add_executable(sdm
    ${SDM_SOURCES}
)

target_link_libraries(sdm PRIVATE
    adviskv_common
)

target_include_directories(sdm PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/build/generated
)
