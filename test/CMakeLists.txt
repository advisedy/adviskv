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
        adviskv_storage_lib
        GTest::gtest
    )
    target_include_directories(${target_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/test
        ${CMAKE_SOURCE_DIR}/build/generated
    )
    gtest_discover_tests(${target_name})
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
        adviskv_sdm_lib
        GTest::gtest
    )
    target_include_directories(${target_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/test
        ${CMAKE_SOURCE_DIR}/build/generated
    )
    gtest_discover_tests(${target_name})
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
        adviskv_meta_lib
        GTest::gtest
    )
    target_include_directories(${target_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/test
        ${CMAKE_SOURCE_DIR}/build/generated
    )
    gtest_discover_tests(${target_name})
endfunction()

add_adviskv_storage_gtest(map_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/engine/map_engine_test.cpp
)

add_adviskv_storage_gtest(persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/persist_engine_test.cpp
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

add_adviskv_sdm_gtest(placetable_workflow_test
    ${CMAKE_SOURCE_DIR}/test/sdm/workflow/placetable_workflow_test.cpp
)

add_adviskv_sdm_gtest(sdm_service_test
    ${CMAKE_SOURCE_DIR}/test/sdm/service/sdm_service_test.cpp
)

add_adviskv_sdm_gtest(sdm_runtime_index_test
    ${CMAKE_SOURCE_DIR}/test/sdm/model/sdm_runtime_index_test.cpp
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
