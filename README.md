# OmniSketchCpp

This is a C++ OmniSketch library

## Compilation

For development:
```shell
mkdir build && cd build
cmake .. -DOMNISKETCH_ENABLE_TESTS=ON -DOMNISKETCH_ENABLE_BENCHMARKS=ON
make
```

In CLion, insert into CMake Arguments field in build settings:
```
-G Ninja -DOMNISKETCH_ENABLE_TESTS=ON -DOMNISKETCH_ENABLE_BENCHMARKS=ON
```

