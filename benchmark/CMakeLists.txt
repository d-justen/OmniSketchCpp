include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

add_executable(micro_benchmarks benchmark.cpp)

target_link_libraries(micro_benchmarks omnisketch benchmark::benchmark)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    target_link_libraries(micro_benchmarks Shlwapi)
endif()

add_executable(min_hash_sketch_benchmark min_hash_sketch_benchmark.cpp)

target_link_libraries(min_hash_sketch_benchmark omnisketch benchmark::benchmark)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    target_link_libraries(micro_benchmarks Shlwapi)
endif()