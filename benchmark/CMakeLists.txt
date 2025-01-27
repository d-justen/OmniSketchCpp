include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

set(BENCHMARK_SOURCES
        benchmark.cpp
)

add_executable(micro_benchmarks ${BENCHMARK_SOURCES})

target_link_libraries(micro_benchmarks omnisketch benchmark::benchmark)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    target_link_libraries(micro_benchmarks Shlwapi)
endif()