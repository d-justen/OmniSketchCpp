#include <benchmark/benchmark.h>

#include "min_hash_sketch_fixture.hpp"

constexpr size_t MAX_SAMPLE_SIZE_LARGE = 1024;
constexpr size_t MAX_SAMPLE_SIZE_SMALL = 16;
constexpr size_t MATCH_COUNT = 32;

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect64Tree, MAX_SAMPLE_SIZE_LARGE, MATCH_COUNT)
(benchmark::State &state) {
	IntersectTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect64Vector, MAX_SAMPLE_SIZE_LARGE, MATCH_COUNT)
(benchmark::State &state) {
	IntersectVectors(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion64Tree, MAX_SAMPLE_SIZE_SMALL, 0)
(benchmark::State &state) {
	UnionTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion64Vector, MAX_SAMPLE_SIZE_SMALL, 0)
(benchmark::State &state) {
	UnionVectors(state);
}

BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect64Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect64Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion64Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion64Vector)->RangeMultiplier(2)->Range(2, 4096);

BENCHMARK_MAIN();