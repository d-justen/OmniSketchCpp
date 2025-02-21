#include <benchmark/benchmark.h>

#include "min_hash_sketch_fixture.hpp"

constexpr size_t MAX_SAMPLE_SIZE_LARGE = 1024;
constexpr size_t MAX_SAMPLE_SIZE_SMALL = 16;
constexpr size_t MATCH_COUNT = 32;

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect16Tree, std::set<uint16_t>, MAX_SAMPLE_SIZE_LARGE,
                            MATCH_COUNT)
(benchmark::State &state) {
	IntersectTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect32Tree, std::set<uint32_t>, MAX_SAMPLE_SIZE_LARGE,
                            MATCH_COUNT)
(benchmark::State &state) {
	IntersectTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect64Tree, std::set<uint64_t>, MAX_SAMPLE_SIZE_LARGE,
                            MATCH_COUNT)
(benchmark::State &state) {
	IntersectTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect16Vector, std::vector<uint16_t>,
                            MAX_SAMPLE_SIZE_LARGE, MATCH_COUNT)
(benchmark::State &state) {
	IntersectVectors(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect32Vector, std::vector<uint32_t>,
                            MAX_SAMPLE_SIZE_LARGE, MATCH_COUNT)
(benchmark::State &state) {
	IntersectVectors(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect64Vector, std::vector<uint64_t>,
                            MAX_SAMPLE_SIZE_LARGE, MATCH_COUNT)
(benchmark::State &state) {
	IntersectVectors(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion16Tree, std::set<uint16_t>, MAX_SAMPLE_SIZE_SMALL, 0)
(benchmark::State &state) {
	UnionTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion32Tree, std::set<uint32_t>, MAX_SAMPLE_SIZE_SMALL, 0)
(benchmark::State &state) {
	UnionTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion64Tree, std::set<uint64_t>, MAX_SAMPLE_SIZE_SMALL, 0)
(benchmark::State &state) {
	UnionTrees(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion16Vector, std::vector<uint16_t>, MAX_SAMPLE_SIZE_SMALL,
                            0)
(benchmark::State &state) {
	UnionVectors(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion32Vector, std::vector<uint32_t>, MAX_SAMPLE_SIZE_SMALL,
                            0)
(benchmark::State &state) {
	UnionVectors(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayUnion64Vector, std::vector<uint64_t>, MAX_SAMPLE_SIZE_SMALL,
                            0)
(benchmark::State &state) {
	UnionVectors(state);
}

BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect16Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect32Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect64Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect16Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect32Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect64Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion16Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion32Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion64Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion16Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion32Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayUnion64Vector)->RangeMultiplier(2)->Range(2, 4096);

BENCHMARK_MAIN();