#include <benchmark/benchmark.h>

#include "min_hash_sketch_fixture.hpp"

constexpr size_t kMaxSampleSizeLarge = 1024;
constexpr size_t kMaxSampleSizeSmall = 16;
constexpr size_t kMatchCount = 32;

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect64Tree, kMaxSampleSizeLarge, kMatchCount)
(benchmark::State &state) {
	IntersectTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect64Vector, kMaxSampleSizeLarge, kMatchCount)
(benchmark::State &state) {
	IntersectVectors(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion64Tree, kMaxSampleSizeSmall, 0)
(benchmark::State &state) {
	UnionTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion64Vector, kMaxSampleSizeSmall, 0)
(benchmark::State &state) {
	UnionVectors(state);
}

BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect64Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect64Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion64Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion64Vector)->RangeMultiplier(2)->Range(2, 4096);

BENCHMARK_MAIN();
