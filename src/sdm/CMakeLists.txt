set(SDM_LIB_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/manager/node_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/manager/route_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/manager/meta_cache_manager.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/handler/sdm_service_impl.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/service/route_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/placement_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/node_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/table_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/heartbeat_service.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/selector/node_selector/node_selector.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/selector/leader_selector/leader_selector.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/utility/pb_convert.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/model/i_sdm_metastore.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/model/sdm_store.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/model/sdm_runtime_index.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/sdm_persist_engine.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/background/heartbeat_check_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/background/routeupdate_check_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/background/table_reconciler.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/client/storage_client.cpp

    # binary dir
    ${PROJECT_BINARY_DIR}/generated/sdm.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.grpc.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.grpc.pb.cc
    
)

add_library(adviskv_sdm_lib
    ${SDM_LIB_SOURCES}
)

target_link_libraries(adviskv_sdm_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_sdm_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/build/generated
)

add_executable(sdm
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)

target_link_libraries(sdm PRIVATE
    adviskv_sdm_lib
)
