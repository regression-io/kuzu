add_kuzu_test(main_test
        arrow_test.cpp
        config_test.cpp
        connection_test.cpp
        csv_output_test.cpp
        prepare_test.cpp
        result_value_test.cpp
        storage_driver_test.cpp
        udf_test.cpp)
add_kuzu_test(test_locking test_locking.cpp)
add_kuzu_test(test_read_only test_read_only.cpp)
