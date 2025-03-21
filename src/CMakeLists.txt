include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

set(SOURCE_FILES
        algorithm/join_operations.hpp
        algorithm/min_hash_set_operations.hpp
        algorithm/omni_sketch_operations.hpp

        hash.hpp
        min_hash_sketch.cpp
        min_hash_sketch.hpp
        omni_sketch.hpp
        omni_sketch_cell.hpp
)

add_library(omnisketch STATIC ${SOURCE_FILES})
