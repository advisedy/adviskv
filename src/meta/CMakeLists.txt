find_package(nlohmann_json CONFIG REQUIRED)

set(META_LIB_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/store/meta_store.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/meta_store_txn.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/store/meta_index.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/meta_persist_engine.cpp
)

add_library(adviskv_meta_lib ${META_LIB_SOURCES})
target_link_libraries(adviskv_meta_lib PUBLIC
    adviskv_common
    nlohmann_json::nlohmann_json
)
target_include_directories(adviskv_meta_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)
