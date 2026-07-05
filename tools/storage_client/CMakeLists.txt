add_library(adviskv_tools_storage_client
    ${CMAKE_CURRENT_SOURCE_DIR}/storage_client.cpp
)

target_link_libraries(adviskv_tools_storage_client PUBLIC
    adviskv_common
)

target_include_directories(adviskv_tools_storage_client PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
)
