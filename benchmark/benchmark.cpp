#include <benchmark/benchmark.h>
#include "omni_sketch.hpp"

static void BM_OmniSketchInsert(benchmark::State &state) {
	omnisketch::OmniSketch sketch(4096, 4, 8);
	for (auto _ : state) {
		for (size_t i = 0; i < 6000000; i++) {
			sketch.AddRecord(i % 50000, i);
		}
	}
}

BENCHMARK(BM_OmniSketchInsert);

static void BM_OmniSketchInsertVectorized(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(4096, 4, 8);
	for (auto _ : state) {
		sketch.AddRecords(keys.data(), rids.data(), 6000000);
	}
}

BENCHMARK(BM_OmniSketchInsertVectorized);

static void BM_OmniSketchIntersect(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(4096, 3, 8);
	sketch.AddRecords(keys.data(), rids.data(), 6000000);
	std::vector<size_t> search_keys(keys.begin(), std::next(keys.begin(), 1024));

	for (auto _ : state) {
		auto card = sketch.EstimateCardinality(search_keys.data(), 1024);
	}
}

BENCHMARK(BM_OmniSketchIntersect);

BENCHMARK_MAIN();