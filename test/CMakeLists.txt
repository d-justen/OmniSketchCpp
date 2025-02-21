include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

enable_testing()

set(TEST_SOURCES
        algorithm_test.cpp
        min_hash_sketch_test.cpp
        omni_sketch_test.cpp
)

add_executable(unit_tests ${TEST_SOURCES})

target_link_libraries(unit_tests GTest::gtest_main omnisketch)

include(GoogleTest)
gtest_discover_tests(unit_tests)
