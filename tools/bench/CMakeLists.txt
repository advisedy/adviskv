add_executable(bench_client
    ${CMAKE_CURRENT_SOURCE_DIR}/bench_client.cpp
    ${CMAKE_SOURCE_DIR}/tools/e2e/e2e_assert.cpp
    ${CMAKE_SOURCE_DIR}/tools/e2e/e2e_context.cpp
    ${CMAKE_SOURCE_DIR}/tools/e2e/e2e_table_util.cpp
)

target_link_libraries(bench_client PRIVATE
    adviskv_sdk
)

target_include_directories(bench_client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/tools/e2e
    ${CMAKE_SOURCE_DIR}/src
)
