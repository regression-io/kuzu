include_directories(${CMAKE_CURRENT_BINARY_DIR})
# Avoid the import annotation when building on windows
# Really this should be set per target,
# but the targets are split among many files and only the object files are linked against here
add_compile_definitions(kuzu_shared_EXPORTS)

add_subdirectory(binder)
add_subdirectory(c_api)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(expression_evaluator)
add_subdirectory(function)
add_subdirectory(main)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(planner)
add_subdirectory(processor)
add_subdirectory(storage)
add_subdirectory(transaction)

add_library(kuzu STATIC ${ALL_OBJECT_FILES})
add_library(kuzu_interface INTERFACE)
add_library(kuzu_shared SHARED ${ALL_OBJECT_FILES})

target_link_libraries(kuzu_interface INTERFACE ${ALL_OBJECT_FILES})

set(KUZU_LIBRARIES antlr4_cypher antlr4_runtime fast_float utf8proc re2 serd Threads::Threads fastpfor miniparquet zstd miniz)
target_link_libraries(kuzu PUBLIC ${KUZU_LIBRARIES})
target_link_libraries(kuzu_interface INTERFACE ${KUZU_LIBRARIES})
target_link_libraries(kuzu_shared PUBLIC ${KUZU_LIBRARIES})
unset(KUZU_LIBRARIES)

set(KUZU_INCLUDES $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}> ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(kuzu PUBLIC ${KUZU_INCLUDES})
target_include_directories(kuzu_interface INTERFACE ${KUZU_INCLUDES})
target_include_directories(kuzu_shared PUBLIC ${KUZU_INCLUDES})
unset(KUZU_INCLUDES)

if(NOT WIN32)
        set_target_properties(kuzu_shared PROPERTIES OUTPUT_NAME kuzu)
endif()

include(GenerateExportHeader)
target_compile_definitions(kuzu PUBLIC KUZU_STATIC_DEFINE)

install(TARGETS kuzu kuzu_shared)

# Create a command to generate kuzu.hpp, and then create a target that is
# always built that depends on it. This allows our generator to detect when
# exactly to build kuzu.hpp, while still building the target by default.
add_custom_command(
        OUTPUT kuzu.hpp
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/../scripts/collect-single-file-header.py
        DEPENDS
                ${CMAKE_CURRENT_SOURCE_DIR}/../scripts/collect-single-file-header.py
                ${CMAKE_CURRENT_SOURCE_DIR}/include/main/kuzu.h)
add_custom_target(single_file_header ALL DEPENDS kuzu.hpp)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/kuzu.hpp ${CMAKE_CURRENT_SOURCE_DIR}/include/c_api/kuzu.h TYPE INCLUDE)
