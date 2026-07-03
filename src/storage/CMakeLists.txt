set(STORAGE_LIB_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/node_agent/node_agent.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica_applier.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica_loop.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica_message_dispatcher.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica_read_index_checker.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/replica/replica_snapshot_coordinator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_apply.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_log.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_membership.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_election.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_replication.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_peer_progress.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_sender.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/grpc_raft_rpc_transport.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/raft_background_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/core/raft_core.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/core/raft_core_election.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/core/raft_core_append.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/core/raft_core_snapshot.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/core/raft_core_recovery.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/core/raft_core_read.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/state_machine/kv_state_machine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/engine/map_engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/persist_engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/replica_meta_persist_engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/handler/storage_service.cpp
)

add_executable(storage
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)

target_link_libraries(storage PRIVATE
    adviskv_storage_lib
)


add_library(adviskv_storage_lib
    ${STORAGE_LIB_SOURCES}
)

target_link_libraries(adviskv_storage_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_storage_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

add_library(adviskv_storage_test_lib
    ${STORAGE_LIB_SOURCES}
)

target_link_libraries(adviskv_storage_test_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_storage_test_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

target_compile_definitions(adviskv_storage_test_lib PRIVATE
    ADVISKV_ENABLE_CRASH_TEST
)
