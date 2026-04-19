set(${SDSDK_SOURCES}
${CMAKE_CURRENT_SOURCE_DIR}/node_agent.cpp
${CMAKE_CURRENT_SOURCE_DIR}/heartbeater.cpp
${CMAKE_CURRENT_SOURCE_DIR}/replica_controller.cpp

${PROJECT_BINARY_DIR}/generated/common.pb.cc 
)

add_library(sdsdk )


target_link_libraries(storage PRIVATE
    adviskv_common
)

target_include_directories(storage PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/build/generated
)
