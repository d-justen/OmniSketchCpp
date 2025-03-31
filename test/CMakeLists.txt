include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

set(TEST_SOURCES
        combinator_test.cpp
        min_hash_sketch_test.cpp
        min_hash_sketch_test.hpp
        omni_sketch_test.cpp
)

add_executable(unit_tests ${TEST_SOURCES})

target_link_libraries(unit_tests GTest::gtest_main omnisketch)

include(GoogleTest)
gtest_discover_tests(unit_tests)
