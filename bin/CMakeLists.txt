include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

add_executable(build_sketches build_sketches.cpp)
target_link_libraries(build_sketches omnisketch)

add_executable(run_dp_size run_dp_size.cpp)
target_link_libraries(run_dp_size omnisketch)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    target_link_libraries(build_sketches Shlwapi)
    target_link_libraries(run_dp_size Shlwapi)
endif()
