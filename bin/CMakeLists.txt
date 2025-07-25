include_directories(
        ${PROJECT_SOURCE_DIR}/src
)

add_executable(build_sketches build_sketches.cpp)
target_link_libraries(build_sketches omnisketch)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    target_link_libraries(build_sketches Shlwapi)
endif()
