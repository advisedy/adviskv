set(TOPO_LIB_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/handler/topo_service_impl.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/service/route_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/node_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/table_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/replica_group_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/service_manager.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/selector/node_selector/node_selector.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/store/memory_metastore.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/persistent_metastore.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/runtime_store.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/topo_store.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/topo_store_txn.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/runtime_index.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/topo_persist_engine.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/background/background.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_group_membership_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_group_plan_reconciler.cpp
)

add_executable(topo
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)

target_link_libraries(topo PRIVATE
    adviskv_topo_lib
)

add_executable(topo_test
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)

target_link_libraries(topo_test PRIVATE
    adviskv_topo_test_lib
)

add_library(adviskv_topo_lib
    ${TOPO_LIB_SOURCES}
)

target_link_libraries(adviskv_topo_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_topo_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

add_library(adviskv_topo_test_lib
    ${TOPO_LIB_SOURCES}
)

target_link_libraries(adviskv_topo_test_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_topo_test_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

target_compile_definitions(adviskv_topo_test_lib PUBLIC
    ADVISKV_ENABLE_CRASH_TEST
)
