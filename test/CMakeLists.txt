include(GoogleTest)

function(add_adviskv_gtest target_name)
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
        ${CMAKE_SOURCE_DIR}/build/generated
    )
    gtest_discover_tests(${target_name})
endfunction()

add_adviskv_gtest(map_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/engine/map_engine_test.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/engine/map_engine.cpp
)

add_adviskv_gtest(persist_engine_test
    ${CMAKE_SOURCE_DIR}/test/storage/persist/persist_engine_test.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/persist/persist_engine.cpp
)

add_adviskv_gtest(kv_state_machine_test
    ${CMAKE_SOURCE_DIR}/test/storage/state_machine/kv_state_machine_test.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/raft/state_machine/kv_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/engine/map_engine.cpp
)

add_adviskv_gtest(replica_test
    ${CMAKE_SOURCE_DIR}/test/storage/replica/replica_test.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/replica/replica.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/replica/replica_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/raft/raft_node.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/raft/raft_background_task.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/raft/state_machine/kv_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/engine/map_engine.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/persist/persist_engine.cpp
    ${PROJECT_BINARY_DIR}/generated/storage.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.grpc.pb.cc
)

add_adviskv_gtest(replica_manager_test
    ${CMAKE_SOURCE_DIR}/test/storage/replica/replica_manager_test.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/replica/replica_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/replica/replica.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/raft/raft_node.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/raft/raft_background_task.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/raft/state_machine/kv_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/engine/map_engine.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/persist/persist_engine.cpp
    ${PROJECT_BINARY_DIR}/generated/storage.pb.cc
    ${PROJECT_BINARY_DIR}/generated/storage.grpc.pb.cc
)
