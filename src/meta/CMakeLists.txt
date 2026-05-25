set(META_LIB_SOURCES
    # current dir
    ${CMAKE_CURRENT_SOURCE_DIR}/catalog/catalog_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/background/table_ddl_reconciler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/persist/meta_persist_engine.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/ddl_service.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/service/sdm_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/handler/meta_service_impl.cpp

    # binary dir
    ${PROJECT_BINARY_DIR}/generated/common.pb.cc
    ${PROJECT_BINARY_DIR}/generated/meta.pb.cc
    ${PROJECT_BINARY_DIR}/generated/meta.grpc.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.pb.cc
    ${PROJECT_BINARY_DIR}/generated/sdm.grpc.pb.cc

)

add_executable(meta
    ${CMAKE_CURRENT_SOURCE_DIR}/main/main.cpp
)

target_link_libraries(meta PRIVATE
    adviskv_meta_lib
)

add_library(adviskv_meta_lib
    ${META_LIB_SOURCES}
)

target_link_libraries(adviskv_meta_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_meta_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/generated
)

add_library(adviskv_meta_test_lib
    ${META_LIB_SOURCES}
)

target_link_libraries(adviskv_meta_test_lib PUBLIC
    adviskv_common
)

target_include_directories(adviskv_meta_test_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/generated
)

target_compile_definitions(adviskv_meta_test_lib PRIVATE
    ADVISKV_ENABLE_CRASH_TEST
)