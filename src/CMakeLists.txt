include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

set(SOURCE_FILES
        min_hash_sketch.cpp
)

add_library(omnisketch STATIC ${SOURCE_FILES})
