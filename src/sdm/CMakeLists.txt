set(SDM_LIB_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/handler/sdm_service_impl.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/service/route_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/node_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/table_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/replica_group_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/service_manager.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/selector/node_selector/node_selector.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/store/memory_metastore.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/persistent_metastore.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/runtime_store.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/sdm_store.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/sdm_store_txn.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/runtime_index.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/sdm_persist_engine.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/background/background.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_group_membership_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile/replica_group_plan_reconciler.cpp
)

add_executable(sdm
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)

target_link_libraries(sdm PRIVATE
    adviskv_sdm_lib
)

add_executable(sdm_test
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)

target_link_libraries(sdm_test PRIVATE
    adviskv_sdm_test_lib
)

add_library(adviskv_sdm_lib
    ${SDM_LIB_SOURCES}
)

target_link_libraries(adviskv_sdm_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_sdm_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

add_library(adviskv_sdm_test_lib
    ${SDM_LIB_SOURCES}
)

target_link_libraries(adviskv_sdm_test_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_sdm_test_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

target_compile_definitions(adviskv_sdm_test_lib PUBLIC
    ADVISKV_ENABLE_CRASH_TEST
)
