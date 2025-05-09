#!/bin/bash

stamp=$(date +%Y%m%d_%H%M%S)_$(git rev-parse --short HEAD)
mkdir -p benchmark_results/omni_insert
mkdir -p benchmark_results/omni_probe
mkdir -p benchmark_results/min_hash_intersect
mkdir -p benchmark_results/min_hash_union
mkdir -p benchmark_results/ssb
mkdir -p benchmark_results/ssb_skew
mkdir -p benchmark_results/ssb_corr
mkdir -p benchmark_results/ssb_skew_corr

cd build || exit
cmake ..
make

./benchmark/omni_sketch_benchmark --benchmark_filter=AddRecords --benchmark_out_format=json --benchmark_out=../benchmark_results/omni_insert/"$stamp".json
./benchmark/omni_sketch_benchmark --benchmark_filter=PointQuery --benchmark_out_format=json --benchmark_out=../benchmark_results/omni_probe/"$stamp".json
./benchmark/min_hash_sketch_benchmark --benchmark_filter=MultiwayIntersect --benchmark_out_format=json --benchmark_out=../benchmark_results/min_hash_intersect/"$stamp".json
./benchmark/min_hash_sketch_benchmark --benchmark_filter=MultiwayUnion --benchmark_out_format=json --benchmark_out=../benchmark_results/min_hash_union/"$stamp".json
./benchmark/join_benchmark --benchmark_filter=SSBE --benchmark_out_format=json --benchmark_out=../benchmark_results/ssb/"$stamp".json
./benchmark/join_benchmark --benchmark_filter=SSBSkewSubE --benchmark_out_format=json --benchmark_out=../benchmark_results/ssb_skew/"$stamp".json
./benchmark/join_benchmark --benchmark_filter=SSBCE --benchmark_out_format=json --benchmark_out=../benchmark_results/ssb_corr/"$stamp".json
./benchmark/join_benchmark --benchmark_filter=SSBSkewSubCE --benchmark_out_format=json --benchmark_out=../benchmark_results/ssb_skew_corr/"$stamp".json
