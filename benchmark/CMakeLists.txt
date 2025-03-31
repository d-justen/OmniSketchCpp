include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

set(BENCHMARK_ENABLE_TESTING NO)

include(FetchContent)
FetchContent_Declare(
        googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG origin/main
)

FetchContent_MakeAvailable(googlebenchmark)

add_executable(min_hash_sketch_benchmark min_hash_sketch_benchmark.cpp)
target_link_libraries(min_hash_sketch_benchmark omnisketch benchmark::benchmark)

add_executable(omni_sketch_benchmark omni_sketch_benchmark.cpp)
target_link_libraries(omni_sketch_benchmark omnisketch benchmark::benchmark)

add_executable(join_benchmark join_benchmark.cpp)
target_link_libraries(join_benchmark omnisketch benchmark::benchmark)

add_executable(individual_probe_errors_benchmark individual_probe_errors_benchmark.cpp)
target_link_libraries(individual_probe_errors_benchmark omnisketch benchmark::benchmark)


if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    target_link_libraries(min_hash_sketch_benchmark Shlwapi)
    target_link_libraries(omni_sketch_benchmark Shlwapi)
    target_link_libraries(join_benchmark Shlwapi)
    target_link_libraries(individual_probe_errors_benchmark Shlwapi)
endif()