find_package(nlohmann_json CONFIG REQUIRED)

add_executable(maelstrom_raft_node
    ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/maelstrom_node.cpp
)

target_include_directories(maelstrom_raft_node PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(maelstrom_raft_node PRIVATE
    adviskv_storage_lib
    nlohmann_json::nlohmann_json
)
