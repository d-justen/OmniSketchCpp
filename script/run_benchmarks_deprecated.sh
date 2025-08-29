#!/bin/bash

stamp=$(date +%Y%m%d_%H%M%S)_$(git rev-parse --short HEAD)
mkdir -p benchmark_results/omni_insert
mkdir -p benchmark_results/omni_probe
mkdir -p benchmark_results/min_hash_intersect
mkdir -p benchmark_results/min_hash_union
mkdir -p benchmark_results/ssb/baselines
mkdir -p benchmark_results/ssb_skew/baselines
mkdir -p benchmark_results/ssb_corr/baselines
mkdir -p benchmark_results/ssb_skew_corr/baselines

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

cd ..
source venv/bin/activate
python3 script/duckdb_benchmarks.py data/ssb/definition.csv data/ssb/queries.csv benchmark_results/ssb/baselines/duckdb.json
cp benchmark_results/ssb/baselines/duckdb.json benchmark_results/ssb_corr/baselines/duckdb.json
python3 script/duckdb_benchmarks.py data/ssb-skew-sf1/definition.csv "data/ssb-skew-sf1/sub_queries_*" benchmark_results/ssb_skew/baselines/duckdb.json
cp benchmark_results/ssb_skew/baselines/duckdb.json benchmark_results/ssb_skew_corr/baselines/duckdb.json

python3 script/postgres_benchmarks.py data/ssb/definition.csv data/ssb/queries.csv benchmark_results/ssb/baselines/postgres.json
cp benchmark_results/ssb/baselines/postgres.json benchmark_results/ssb_corr/baselines/postgres.json
python3 script/postgres_benchmarks.py data/ssb-skew-sf1/definition.csv "data/ssb-skew-sf1/sub_queries_*" benchmark_results/ssb_skew/baselines/postgres.json
cp benchmark_results/ssb_skew/baselines/postgres.json benchmark_results/ssb_skew_corr/baselines/postgres.json
