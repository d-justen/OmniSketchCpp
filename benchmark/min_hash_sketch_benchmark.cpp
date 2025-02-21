#include <benchmark/benchmark.h>

#include "min_hash_sketch_fixture.hpp"

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect16Tree, std::set<uint16_t>)(benchmark::State &state) {
    ExecuteTree(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect32Tree, std::set<uint32_t>)(benchmark::State &state) {
	ExecuteTree(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect64Tree, std::set<uint64_t>)(benchmark::State &state) {
	ExecuteTree(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect16Vector, std::vector<uint16_t>)(benchmark::State &state) {
	ExecuteVector(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect32Vector, std::vector<uint32_t>)(benchmark::State &state) {
	ExecuteVector(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(MinHashSketchFixture, MultiwayIntersect64Vector, std::vector<uint64_t>)(benchmark::State &state) {
	ExecuteVector(state);
}

BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect16Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect32Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect64Tree)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect16Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect32Vector)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(MinHashSketchFixture, MultiwayIntersect64Vector)->RangeMultiplier(2)->Range(2, 4096);


BENCHMARK_MAIN();