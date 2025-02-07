#include <benchmark/benchmark.h>
#include "omni_sketch.hpp"

static void BM_OmniSketchInsert(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(4096, 4, 8);
	for (auto _ : state) {
		for (size_t i = 0; i < 6000000; i++) {
			sketch.AddRecord(keys[i], rids[i]);
		}
	}
}

BENCHMARK(BM_OmniSketchInsert);

static void BM_OmniSketchInsertBatched(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(4096, 4, 8);
	sketch.InitializeBuffers(6000000);
	for (auto _ : state) {
		sketch.AddRecords(keys.data(), rids.data(), 6000000);
	}
}

BENCHMARK(BM_OmniSketchInsertBatched);

static void BM_OmniSketchIntersect(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(4096, 3, 8);
	sketch.InitializeBuffers(6000000);
	sketch.AddRecords(keys.data(), rids.data(), 6000000);
	std::vector<size_t> search_keys(keys.begin(), std::next(keys.begin(), 1024));

	for (auto _ : state) {
		for (size_t i = 0; i < 1024; i++) {
			auto card = sketch.EstimateCardinality(search_keys[i]);
		}
	}
}

BENCHMARK(BM_OmniSketchIntersect);

static void BM_OmniSketchIntersectBatched(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(4096, 3, 8);
	sketch.InitializeBuffers(6000000);
	sketch.AddRecords(keys.data(), rids.data(), 6000000);
	std::vector<size_t> search_keys(keys.begin(), std::next(keys.begin(), 1024));

	for (auto _ : state) {
		auto card = sketch.EstimateCardinality(search_keys.data(), 1024, sketch.MinHashSampleCount());
	}
}

BENCHMARK(BM_OmniSketchIntersectBatched);

BENCHMARK_MAIN();