add_executable(adviskvctl
    ${CMAKE_CURRENT_SOURCE_DIR}/src/adviskvctl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/direct_meta_client.cpp
)

target_link_libraries(adviskvctl PRIVATE
    adviskv_sdk
)

target_include_directories(adviskvctl PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src
)