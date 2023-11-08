add_library(kuzu_storage_index_hash
        OBJECT
        base_hash_index_basic.cpp
        base_hash_index_detail.cpp
        bucket_operations.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_index_hash>
        PARENT_SCOPE)
