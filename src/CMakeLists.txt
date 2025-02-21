include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

set(SOURCE_FILES
        omni_sketch.cpp
        omni_sketch_cell.cpp
        min_hash_sketch.cpp
)

add_library(omnisketch STATIC ${SOURCE_FILES})
