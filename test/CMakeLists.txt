include(GoogleTest)

function(add_adviskv_storage_gtest target_name)
    add_executable(${target_name}
        ${CMAKE_SOURCE_DIR}/test/test_main.cpp
        ${ARGN}
    )
    set_target_properties(${target_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test"
    )
    target_link_libraries(${target_name} PRIVATE
        adviskv_storage_test_lib
        GTest::gtest
    )
    target_include_directories(${target_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/test
        ${PROJECT_BINARY_DIR}/generated
    )
    gtest_discover_tests(${target_name}
        DISCOVERY_TIMEOUT 30
    )
endfunction()

function(add_adviskv_sdm_gtest target_name)
    add_executable(${target_name}
        ${CMAKE_SOURCE_DIR}/test/test_main.cpp
        ${ARGN}
    )
    set_target_properties(${target_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test"
    )
    target_link_libraries(${target_name} PRIVATE
        adviskv_sdm_test_lib
        GTest::gtest
    )
    target_include_directories(${target_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/test
        ${PROJECT_BINARY_DIR}/generated
    )
    gtest_discover_tests(${target_name}
        DISCOVERY_TIMEOUT 30
    )
endfunction()

function(add_adviskv_meta_gtest target_name)
    add_executable(${target_name}
        ${CMAKE_SOURCE_DIR}/test/test_main.cpp
        ${ARGN}
    )
    set_target_properties(${target_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test"
    )
    target_link_libraries(${target_name} PRIVATE
        adviskv_meta_test_lib
        GTest::gtest
    )
    target_include_directories(${target_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/test
        ${PROJECT_BINARY_DIR}/generated
    )
    gtest_discover_tests(${target_name}
        DISCOVERY_TIMEOUT 30
    )
endfunction()

add_adviskv_storage_gtest(map_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/engine/map_engine_test.cpp
)

add_adviskv_storage_gtest(persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/persist_engine_test.cpp
)

add_executable(wal_crash_runner
    ${CMAKE_SOURCE_DIR}/test/storage/persist/wal_crash_runner.cpp
)
set_target_properties(wal_crash_runner PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test"
)
target_link_libraries(wal_crash_runner PRIVATE
    adviskv_storage_test_lib
)
target_include_directories(wal_crash_runner PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/generated
)

add_adviskv_storage_gtest(wal_crash_injection_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/wal_crash_injection_test.cpp
)
target_compile_definitions(wal_crash_injection_test PRIVATE
    ADVISKV_CRASH_RUNNER_PATH="$<TARGET_FILE:wal_crash_runner>"
)

add_adviskv_storage_gtest(replica_meta_persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/replica_meta_persist_engine_test.cpp
)

add_adviskv_storage_gtest(kv_state_machine_test
    ${CMAKE_SOURCE_DIR}/test/storage/state_machine/kv_state_machine_test.cpp
)

add_adviskv_storage_gtest(replica_test
    ${CMAKE_SOURCE_DIR}/test/storage/replica/replica_test.cpp
)

add_adviskv_storage_gtest(replica_manager_test
    ${CMAKE_SOURCE_DIR}/test/storage/replica/replica_manager_test.cpp
)

add_adviskv_storage_gtest(node_agent_test
    ${CMAKE_SOURCE_DIR}/test/storage/node_agent/node_agent_test.cpp
)

add_adviskv_storage_gtest(raft_node_test
    ${CMAKE_SOURCE_DIR}/test/storage/raft/raft_node_test.cpp
)

add_adviskv_storage_gtest(raft_cluster_test
    ${CMAKE_SOURCE_DIR}/test/storage/raft/raft_cluster_test.cpp
)

add_adviskv_sdm_gtest(node_service_test
    ${CMAKE_SOURCE_DIR}/test/sdm/service/node_service_test.cpp
)

add_adviskv_sdm_gtest(table_service_test
    ${CMAKE_SOURCE_DIR}/test/sdm/service/table_service_test.cpp
)

add_adviskv_sdm_gtest(route_service_test
    ${CMAKE_SOURCE_DIR}/test/sdm/service/route_service_test.cpp
)

add_adviskv_sdm_gtest(heartbeat_service_test
    ${CMAKE_SOURCE_DIR}/test/sdm/service/heartbeat_service_test.cpp
)

add_adviskv_sdm_gtest(heartbeat_check_task_test
    ${CMAKE_SOURCE_DIR}/test/sdm/background/heartbeat_check_task_test.cpp
)

add_adviskv_sdm_gtest(routeupdate_check_task_test
    ${CMAKE_SOURCE_DIR}/test/sdm/background/routeupdate_check_task_test.cpp
)

add_adviskv_sdm_gtest(table_reconciler_test
    ${CMAKE_SOURCE_DIR}/test/sdm/background/table_reconciler_test.cpp
)

add_adviskv_sdm_gtest(sdm_runtime_index_test
    ${CMAKE_SOURCE_DIR}/test/sdm/model/sdm_runtime_index_test.cpp
)

add_adviskv_sdm_gtest(node_selector_test
    ${CMAKE_SOURCE_DIR}/test/sdm/selector/node_selector_test.cpp
)

add_adviskv_sdm_gtest(memory_meta_store_test
    ${CMAKE_SOURCE_DIR}/test/sdm/model/memory_meta_store_test.cpp
)

add_adviskv_sdm_gtest(persistent_meta_store_test
    ${CMAKE_SOURCE_DIR}/test/sdm/model/persistent_meta_store_test.cpp
)

add_adviskv_sdm_gtest(sdm_persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/sdm/persist/sdm_persist_engine_test.cpp
)

add_adviskv_sdm_gtest(sdm_store_test
    ${CMAKE_SOURCE_DIR}/test/sdm/model/sdm_store_test.cpp
)

add_adviskv_meta_gtest(meta_persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/meta/persist/meta_persist_engine_test.cpp
)

add_adviskv_meta_gtest(catalog_manager_test
    ${CMAKE_SOURCE_DIR}/test/meta/catalog/catalog_manager_test.cpp
)

add_adviskv_meta_gtest(ddl_service_test
    ${CMAKE_SOURCE_DIR}/test/meta/ddl_service/ddl_service_test.cpp
)

add_adviskv_meta_gtest(table_ddl_reconciler_test
    ${CMAKE_SOURCE_DIR}/test/meta/background/table_ddl_reconciler_test.cpp
)
