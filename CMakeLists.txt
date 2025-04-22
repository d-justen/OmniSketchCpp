cmake_minimum_required(VERSION 3.14)
project(omnisketch
        VERSION 0.0.1
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "-Werror -Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

option(OMNISKETCH_ENABLE_TESTS "Enable tests" OFF)
option(OMNISKETCH_ENABLE_BENCHMARKS "Enable benchmarks" OFF)

if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    cmake_policy(SET CMP0135 NEW)
endif()

set(SOURCE_FILES
        src/include/combinator.hpp
        src/include/csv_importer.hpp
        src/include/hash.hpp
        src/include/min_hash_sketch.hpp
        src/include/omni_sketch.hpp
        src/include/omni_sketch_cell.hpp
        src/include/plan_generator.hpp
        src/include/registry.hpp
        src/include/set_membership.hpp
        src/include/value.hpp

        src/combinator.cpp
        src/csv_importer.cpp
        src/min_hash_sketch.cpp
        src/omni_sketch.cpp
        src/omni_sketch_cell.cpp
        src/plan_generator.cpp
        src/registry.cpp
)

add_library(omnisketch STATIC ${SOURCE_FILES})

target_compile_features(omnisketch PUBLIC cxx_std_14)
target_include_directories(omnisketch PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/include>
        $<INSTALL_INTERFACE:src/include>
)

include(GNUInstallDirs)

install(TARGETS omnisketch EXPORT omnisketch-targets)
install(DIRECTORY src/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT omnisketch-targets
        FILE omnisketch-targets.cmake
        NAMESPACE omnisketch::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/omnisketch)

include(CMakePackageConfigHelpers)

write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/omnisketch-config-version.cmake"
        COMPATIBILITY SameMajorVersion
)

configure_file(cmake/omnisketch-config.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/omnisketch-config.cmake" @ONLY)

install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/omnisketch-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/omnisketch-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/omnisketch
)

# Export package
export(EXPORT omnisketch-targets FILE "${CMAKE_CURRENT_BINARY_DIR}/omnisketch-targets.cmake" NAMESPACE omnisketch::)

if(OMNISKETCH_ENABLE_BENCHMARKS)
    add_subdirectory(benchmark)
endif()

if(OMNISKETCH_ENABLE_TESTS)
    add_subdirectory(test)
endif()
