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

add_adviskv_storage_gtest(map_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/engine/map_engine_test.cpp
)

add_adviskv_storage_gtest(persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/persist_engine_test.cpp
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

add_adviskv_storage_gtest(raft_node_test
    ${CMAKE_SOURCE_DIR}/test/storage/raft/raft_node_test.cpp
)

add_adviskv_sdm_gtest(placetable_workflow_test
    ${CMAKE_SOURCE_DIR}/test/sdm/workflow/placetable_workflow_test.cpp
)
