add_executable(e2e_client
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e_main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e_assert.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e_context.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e_case_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e_kv_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e_route_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e_storage_test_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e_table_util.cpp
)

target_link_libraries(e2e_client PRIVATE
    adviskv_sdk
)

target_include_directories(e2e_client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
)