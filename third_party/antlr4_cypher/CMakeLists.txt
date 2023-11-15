add_custom_command(
        OUTPUT
                ${CMAKE_CURRENT_SOURCE_DIR}/cypher_lexer.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/cypher_parser.cpp
        COMMAND ./generate_grammar.sh
        DEPENDS ../../src/antlr4/Cypher.g4
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/antlr4)

add_library(antlr4_cypher
        STATIC
        cypher_lexer.cpp
        cypher_parser.cpp)

target_include_directories(antlr4_cypher
        PRIVATE ../antlr4_runtime/src)

target_link_libraries(antlr4_cypher PRIVATE antlr4_runtime)
