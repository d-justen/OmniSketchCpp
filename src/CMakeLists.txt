include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

set(SOURCE_FILES
        algorithm/set_membership.hpp

        combinator.cpp
        combinator.hpp
        hash.hpp
        min_hash_sketch.cpp
        min_hash_sketch.hpp
        omni_sketch.hpp
        omni_sketch.tpp
        omni_sketch_cell.cpp
        omni_sketch_cell.hpp
)

add_library(omnisketch STATIC ${SOURCE_FILES})
