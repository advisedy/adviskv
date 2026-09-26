include(GoogleTest)

function(add_adviskv_common_gtest target_name)
    add_executable(${target_name}
        ${CMAKE_SOURCE_DIR}/test/test_main.cpp
        ${ARGN}
    )
    set_target_properties(${target_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/test"
    )
    target_link_libraries(${target_name} PRIVATE
        adviskv_common
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

add_adviskv_common_gtest(metrics_test
    ${CMAKE_SOURCE_DIR}/test/common/metrics/metrics_test.cpp
)

add_adviskv_common_gtest(shard_range_test
    ${CMAKE_SOURCE_DIR}/test/common/shard_range_test.cpp
)

add_adviskv_storage_gtest(map_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/engine/map_engine_test.cpp
)

add_adviskv_storage_gtest(persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/persist_engine_test.cpp
)

add_adviskv_storage_gtest(wal_crash_injection_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/wal_crash_injection_test.cpp
)

add_adviskv_storage_gtest(replica_meta_persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/replica_meta_persist_engine_test.cpp
)

add_adviskv_storage_gtest(kv_state_machine_test
    ${CMAKE_SOURCE_DIR}/test/storage/raft/state_machine/kv_state_machine_test.cpp
)

add_adviskv_storage_gtest(replica_test
    ${CMAKE_SOURCE_DIR}/test/storage/replica/replica_test.cpp
)

add_adviskv_storage_gtest(replica_manager_test
    ${CMAKE_SOURCE_DIR}/test/storage/replica/replica_manager_test.cpp
)

add_adviskv_storage_gtest(storage_service_test
    ${CMAKE_SOURCE_DIR}/test/storage/handler/storage_service_test.cpp
)

add_adviskv_storage_gtest(node_agent_test
    ${CMAKE_SOURCE_DIR}/test/storage/node_agent/node_agent_test.cpp
)

add_adviskv_storage_gtest(raft_node_test
    ${CMAKE_SOURCE_DIR}/test/storage/raft/core/raft_node_test.cpp
)

add_adviskv_storage_gtest(raft_sender_test
    ${CMAKE_SOURCE_DIR}/test/storage/raft/raft_sender_test.cpp
)

add_adviskv_storage_gtest(raft_cluster_test
    ${CMAKE_SOURCE_DIR}/test/storage/raft/core/raft_cluster_test.cpp
)

add_adviskv_meta_gtest(meta_route_service_test
    ${CMAKE_SOURCE_DIR}/test/meta/service/route_service_test.cpp
)
add_adviskv_meta_gtest(meta_node_service_test
    ${CMAKE_SOURCE_DIR}/test/meta/service/node_service_test.cpp
)
add_adviskv_meta_gtest(meta_persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/meta/persist/meta_persist_engine_test.cpp
)
add_adviskv_meta_gtest(meta_store_test
    ${CMAKE_SOURCE_DIR}/test/meta/store/meta_store_test.cpp
)
add_adviskv_meta_gtest(meta_ddl_service_test
    ${CMAKE_SOURCE_DIR}/test/meta/service/ddl_service_test.cpp
)
add_adviskv_meta_gtest(meta_node_heartbeat_service_test
    ${CMAKE_SOURCE_DIR}/test/meta/service/node_heartbeat_service_test.cpp
)
add_adviskv_meta_gtest(meta_node_reconcile_service_test
    ${CMAKE_SOURCE_DIR}/test/meta/service/node_reconcile_service_test.cpp
)
add_adviskv_meta_gtest(meta_route_reconcile_service_test
    ${CMAKE_SOURCE_DIR}/test/meta/service/route_reconcile_service_test.cpp
)
add_adviskv_meta_gtest(meta_table_reconcile_service_test
    ${CMAKE_SOURCE_DIR}/test/meta/service/table_reconcile_service_test.cpp
)
add_adviskv_meta_gtest(meta_node_selector_test
    ${CMAKE_SOURCE_DIR}/test/meta/selector/node_selector/node_selector_test.cpp
)
add_adviskv_meta_gtest(meta_index_test
    ${CMAKE_SOURCE_DIR}/test/meta/store/meta_index_test.cpp
)
