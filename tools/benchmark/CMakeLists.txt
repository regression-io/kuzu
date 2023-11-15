include_directories(
        ../../src/include
        ./include
        ../../test/include/test_helper)

add_executable(kuzu_benchmark
        benchmark.cpp
        benchmark_runner.cpp
        main.cpp)

target_link_libraries(kuzu_benchmark kuzu_interface test_helper)
