set(CATALOG_LIB_SOURCES
    # current dir
    ${CMAKE_CURRENT_SOURCE_DIR}/store/catalog_store.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/background/table_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/catalog_persist_engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/ddl_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/topo_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/handler/catalog_service_impl.cpp
)

add_executable(catalog
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)

target_link_libraries(catalog PRIVATE
    adviskv_catalog_lib
)

add_library(adviskv_catalog_lib
    ${CATALOG_LIB_SOURCES}
)

target_link_libraries(adviskv_catalog_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_catalog_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

add_library(adviskv_catalog_test_lib
    ${CATALOG_LIB_SOURCES}
)

target_link_libraries(adviskv_catalog_test_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_catalog_test_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

target_compile_definitions(adviskv_catalog_test_lib PUBLIC
    ADVISKV_ENABLE_CRASH_TEST
)
