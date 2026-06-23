add_library(adviskv_common
    ${CMAKE_CURRENT_SOURCE_DIR}/common/log.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/metrics/metrics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/background_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/serial_task_runner.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/thread_pool.cpp
)

target_include_directories(adviskv_common PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

target_compile_definitions(adviskv_common PUBLIC
    ADVISKV_PROJECT_ROOT="${CMAKE_SOURCE_DIR}"
)


target_link_libraries(adviskv_common PUBLIC
    adviskv_proto
    fmt::fmt
    spdlog::spdlog
    yaml-cpp::yaml-cpp
)



add_subdirectory(storage)
add_subdirectory(meta)
add_subdirectory(sdm)
add_subdirectory(sdk)
