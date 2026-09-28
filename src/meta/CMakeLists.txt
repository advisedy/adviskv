find_package(nlohmann_json CONFIG REQUIRED)

set(META_LIB_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/handler/meta_service_impl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/ddl_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/route_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/node_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/replica_group_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/service_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/placement/node_selector.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/meta_store.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/meta_store_txn.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/meta_index.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/meta_persist_engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/background/background.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/node_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_group_membership_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_group_plan_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_group_reconcile_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_group_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_phase_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/table_reconciler.cpp
)

add_executable(meta
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)
target_link_libraries(meta PRIVATE adviskv_meta_lib)

add_executable(meta_test
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)
target_link_libraries(meta_test PRIVATE adviskv_meta_test_lib)

add_library(adviskv_meta_lib ${META_LIB_SOURCES})
target_link_libraries(adviskv_meta_lib PUBLIC
    adviskv_common
    nlohmann_json::nlohmann_json
)
target_include_directories(adviskv_meta_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

add_library(adviskv_meta_test_lib ${META_LIB_SOURCES})
target_link_libraries(adviskv_meta_test_lib PUBLIC
    adviskv_common
    nlohmann_json::nlohmann_json
)
target_include_directories(adviskv_meta_test_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)
target_compile_definitions(adviskv_meta_test_lib PUBLIC
    ADVISKV_ENABLE_CRASH_TEST
)
