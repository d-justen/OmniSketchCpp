include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

add_executable(min_hash_sketch_benchmark min_hash_sketch_benchmark.cpp)
target_link_libraries(min_hash_sketch_benchmark omnisketch benchmark::benchmark)

add_executable(omni_sketch_benchmark omni_sketch_benchmark.cpp)
target_link_libraries(omni_sketch_benchmark omnisketch benchmark::benchmark)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    target_link_libraries(min_hash_sketch_benchmark Shlwapi)
    target_link_libraries(omni_sketch_benchmark Shlwapi)
endif()