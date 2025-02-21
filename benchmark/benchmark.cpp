#include <benchmark/benchmark.h>
#include "algorithm/algorithm.hpp"
#include "omni_sketch.hpp"

/*
static void BM_MinHashIntersect(benchmark::State &state) {
	const size_t sketch_size = 128;
	const size_t match_count = (1.0/16) * 128;
	std::vector<omnisketch::MinHashSketch> sketches(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		for (size_t j = 0; j < match_count; j++) {
			auto hash = omnisketch::Hash(j);
			sketches[i].AddRecord(hash, sketch_size);
		}
		for (size_t j = match_count; j < sketch_size; j++) {
			auto hash = omnisketch::Hash(i * sketch_size + j);
			sketches[i].AddRecord(hash, sketch_size);
		}
	}

	std::vector<const omnisketch::MinHashSketch*> sketch_ptrs(sketches.size());
	for (size_t i = 0; i < sketch_ptrs.size(); i++) {
		sketch_ptrs[i] = &sketches[i];
	}
	std::vector<std::set<uint64_t>::iterator> offsets(sketch_ptrs.size());

	size_t result_count = 0;
	for (auto _ : state) {
		auto result = omnisketch::MinHashSketch::Intersect(sketch_ptrs, offsets);
		result_count = result.Size();
	}

	state.counters["MatchCount"] = result_count;
}

BENCHMARK(BM_MinHashIntersect)->RangeMultiplier(2)->Range(2, 4096);

static void BM_MinHashUnion(benchmark::State &state) {
	const size_t sketch_size = 128;
	const size_t match_count = (1.0/16) * 128;
	std::vector<omnisketch::MinHashSketch> sketches(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		for (size_t j = 0; j < match_count; j++) {
			auto hash = omnisketch::Hash(j);
			sketches[i].AddRecord(hash, sketch_size);
		}
		for (size_t j = match_count; j < sketch_size; j++) {
			auto hash = omnisketch::Hash(i * sketch_size + j);
			sketches[i].AddRecord(hash, sketch_size);
		}
	}

	std::vector<const omnisketch::MinHashSketch*> sketch_ptrs(sketches.size());
	for (size_t i = 0; i < sketch_ptrs.size(); i++) {
		sketch_ptrs[i] = &sketches[i];
	}

	size_t result_count = 0;
	for (auto _ : state) {
		auto result = omnisketch::MinHashSketch::Union(sketch_ptrs, sketch_size);
		result_count = result.Size();
	}

	state.counters["MatchCount"] = result_count;
}

BENCHMARK(BM_MinHashUnion)->RangeMultiplier(2)->Range(2, 4096);

static void BM_MinHashIntersectUnion(benchmark::State &state) {
	const size_t sketch_size = 128;
	const size_t match_count = (1.0/16) * 128;
	const size_t intersection_count = 3;

	std::vector<std::vector<omnisketch::MinHashSketch>> sketches(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		for (size_t k = 0; k < intersection_count; k++) {
			sketches[i].resize(intersection_count);

			for (size_t j = 0; j < match_count; j++) {
				auto hash = omnisketch::Hash(k * intersection_count + j);
				sketches[i][k].AddRecord(hash, sketch_size);
			}
			for (size_t j = match_count; j < sketch_size; j++) {
				auto hash = omnisketch::Hash(j * k * intersection_count + i * sketch_size + j);
				sketches[i][k].AddRecord(hash, sketch_size);
			}
		}
	}

	std::vector<std::vector<const omnisketch::MinHashSketch*>> sketch_ptrs(sketches.size());
	for (size_t i = 0; i < sketch_ptrs.size(); i++) {
		for (size_t j = 0; j < intersection_count; j++) {
			sketch_ptrs[i].push_back(&sketches[i][j]);
		}
	}

	size_t result_count = 0;
	for (auto _ : state) {
		std::vector<std::set<uint64_t>::iterator> offsets(intersection_count);
		std::vector<omnisketch::MinHashSketch> intersect_results(sketches.size());
		std::vector<const omnisketch::MinHashSketch*> intersect_result_ptrs(sketches.size());

		for (size_t i = 0; i < sketches.size(); i++) {
			intersect_results[i] = omnisketch::MinHashSketch::Intersect(sketch_ptrs[i], offsets);
			intersect_result_ptrs[i] = &intersect_results[i];
		}

		auto result = omnisketch::MinHashSketch::Union(intersect_result_ptrs, sketch_size);
		result_count = result.Size();
	}

	state.counters["MatchCount"] = result_count;
}

BENCHMARK(BM_MinHashIntersectUnion)->RangeMultiplier(2)->Range(2, 4096);

static void BM_MinHashIntersectUnionEarlyExit(benchmark::State &state) {
	const size_t sketch_size = 128;
	const size_t match_count = (1.0/16) * 128;
	const size_t intersection_count = 3;

	std::vector<std::vector<omnisketch::MinHashSketch>> sketches(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		for (size_t k = 0; k < intersection_count; k++) {
			sketches[i].resize(intersection_count);

			for (size_t j = 0; j < match_count; j++) {
				auto hash = omnisketch::Hash(k * intersection_count + j);
				sketches[i][k].AddRecord(hash, sketch_size);
			}
			for (size_t j = match_count; j < sketch_size; j++) {
				auto hash = omnisketch::Hash(j * k * intersection_count + i * sketch_size + j);
				sketches[i][k].AddRecord(hash, sketch_size);
			}
		}
	}

	std::vector<std::vector<const omnisketch::MinHashSketch*>> sketch_ptrs(sketches.size());
	for (size_t i = 0; i < sketch_ptrs.size(); i++) {
		for (size_t j = 0; j < intersection_count; j++) {
			sketch_ptrs[i].push_back(&sketches[i][j]);
		}
	}

	size_t result_count = 0;
	for (auto _ : state) {
		std::vector<std::set<uint64_t>::iterator> offsets(intersection_count);
		std::vector<omnisketch::MinHashSketch> intersect_results(sketches.size());
		std::vector<const omnisketch::MinHashSketch*> intersect_result_ptrs(sketches.size());

		for (size_t i = 0; i < sketches.size(); i++) {
			intersect_results[i] = omnisketch::MinHashSketch::Intersect(sketch_ptrs[i], offsets);
			intersect_result_ptrs[i] = &intersect_results[i];
		}

		auto result = omnisketch::MinHashSketch::Union(intersect_result_ptrs, sketch_size);
		result_count = result.Size();
	}

	state.counters["MatchCount"] = result_count;
}

BENCHMARK(BM_MinHashIntersectUnionEarlyExit)->RangeMultiplier(2)->Range(2, 4096);

static void BM_ConjunctionLargeOmniSketch(benchmark::State &state) {
	omnisketch::OmniSketch sketch_1(512, 3, 1024);
	omnisketch::OmniSketch sketch_2(512, 3, 1024);
	omnisketch::OmniSketch sketch_3(512, 3, 1024);

	for (size_t i = 0; i < 1000000; i++) {
		sketch_1.AddRecord(i % 2000, i);
		sketch_2.AddRecord(i % 10000, i);
		sketch_3.AddRecord(i % 100000, i);
	}

	std::vector<const omnisketch::OmniSketch *> sketches {&sketch_1, &sketch_2, &sketch_3};
	std::vector<uint64_t> hashes(3);

	omnisketch::CardEstResult result;
	for (auto _ : state) {
		for (auto &hash : hashes) {
			hash = omnisketch::Hash(1337);
		}

		result = omnisketch::Algorithm::Conjunction(sketches, hashes);
	}


	double q_error = std::max(result.cardinality, 10.0) / std::min(result.cardinality, 10.0);
	if (result.cardinality < 10) {
		q_error *= -1;
	}
	state.counters["Q-Error"] = q_error;
}

BENCHMARK(BM_ConjunctionLargeOmniSketch)->Unit(benchmark::kMillisecond);

static void BM_DisjunctionLargeOmniSketch(benchmark::State &state) {
	omnisketch::OmniSketch sketch_3(512, 3, 1024);

	for (size_t i = 0; i < 1000000; i++) {
		sketch_3.AddRecord(i % 100000, i);
	}

	std::vector<const omnisketch::OmniSketch *> sketches(state.range(), &sketch_3);
	std::vector<uint64_t> hashes(state.range());

	omnisketch::CardEstResult result;
	for (auto _ : state) {
		for (size_t i = 0; i < hashes.size(); i++) {
			hashes[i] = omnisketch::Hash(1337 + i);
		}
		result = omnisketch::Algorithm::Disjunction(sketches, hashes);
	}

	double actual_card = state.range() * 10;
	double q_error = std::max(result.cardinality, actual_card) / std::min(result.cardinality, actual_card);
	if (result.cardinality < actual_card) {
		q_error *= -1;
	}
	state.counters["Q-Error"] = q_error;
}

BENCHMARK(BM_DisjunctionLargeOmniSketch)->RangeMultiplier(2)->Range(2, 4096)->Unit(benchmark::kMillisecond);

static void BM_DisjunctionSmallOmniSketch(benchmark::State &state) {
	omnisketch::OmniSketch sketch(512, 3, 16);

	for (size_t i = 0; i < 1000000; i++) {
		sketch.AddRecord(i % 100000, i);
	}

	std::vector<uint64_t> hashes(state.range());
	std::vector<const omnisketch::OmniSketch *> sketches(state.range(), &sketch);

	omnisketch::CardEstResult result;
	for (auto _ : state) {
		for (size_t i = 0; i < state.range(); i++) {
			hashes[i] = omnisketch::Hash(1337 + i);
		}
		result = omnisketch::Algorithm::Disjunction(sketches, hashes);
	}

	double actual_card = state.range() * 10;
	double q_error = std::max(result.cardinality, actual_card) / std::min(result.cardinality, actual_card);
	if (result.cardinality < actual_card) {
		q_error *= -1;
	}
	state.counters["Q-Error"] = q_error;
	state.counters["MatchCount"] = result.min_hash_sketch.Size();
}

BENCHMARK(BM_DisjunctionSmallOmniSketch)->RangeMultiplier(2)->Range(2, 4096)->Unit(benchmark::kMillisecond);

static void BM_PartialIntersectUnion(benchmark::State &state) {
	omnisketch::OmniSketch sketch(512, 3, 16);

	for (size_t i = 0; i < 1000000; i++) {
		sketch.AddRecord(i % 100000, i);
	}

	std::vector<uint64_t> hashes(state.range());

	omnisketch::CardEstResult result;
	for (auto _ : state) {
		for (size_t i = 0; i < state.range(); i++) {
			hashes[i] = omnisketch::Hash(1337 + i);
		}
		result = omnisketch::Algorithm::PartialIntersectUnion(sketch, hashes);
	}

	double actual_card = state.range() * 10;
	double q_error = std::max(result.cardinality, actual_card) / std::min(result.cardinality, actual_card);
	if (result.cardinality < actual_card) {
		q_error *= -1;
	}
	state.counters["Q-Error"] = q_error;
	state.counters["MatchCount"] = result.min_hash_sketch.Size();
}

BENCHMARK(BM_PartialIntersectUnion)->RangeMultiplier(2)->Range(2, 65536)->Unit(benchmark::kMillisecond);

static void BM_JoinAsUnionByRow(benchmark::State &state) {
	omnisketch::OmniSketch sketch(512, 3, 16);

	for (size_t i = 0; i < 1000000; i++) {
		sketch.AddRecord(i % 100000, i);
	}

	std::vector<uint64_t> hashes(state.range());

	omnisketch::CardEstResult result;
	for (auto _ : state) {
		for (size_t i = 0; i < state.range(); i++) {
			hashes[i] = omnisketch::Hash(1337 + i);
		}
		result = omnisketch::Algorithm::UnionByRow(sketch, hashes);
	}

	double actual_card = state.range() * 10;
	double q_error = std::max(result.cardinality, actual_card) / std::min(result.cardinality, actual_card);
	if (result.cardinality < actual_card) {
		q_error *= -1;
	}
	state.counters["Q-Error"] = q_error;
}

//BENCHMARK(BM_JoinAsUnionByRow)->RangeMultiplier(2)->Range(64, 4096);

static void BM_JoinAsSegmentedUnionByRow(benchmark::State &state) {
	omnisketch::OmniSketch sketch(512, 3, 16);

	for (size_t i = 0; i < 1000000; i++) {
		sketch.AddRecord(i % 100000, i);
	}

	std::vector<uint64_t> hashes(state.range());

	omnisketch::CardEstResult result;
	for (auto _ : state) {
		for (size_t i = 0; i < state.range(); i++) {
			hashes[i] = omnisketch::Hash(1337 + i);
		}
		result = omnisketch::Algorithm::SegmentedUnionByRow(sketch, hashes);
	}

	double actual_card = state.range() * 10;
	double q_error = std::max(result.cardinality, actual_card) / std::min(result.cardinality, actual_card);
	if (result.cardinality < actual_card) {
		q_error *= -1;
	}
	state.counters["Q-Error"] = q_error;
}

// BENCHMARK(BM_JoinAsSegmentedUnionByRow)->RangeMultiplier(2)->Range(1024, 4096);

static void BM_JoinAsDisjunctionEarlyExit(benchmark::State &state) {
	omnisketch::OmniSketch sketch(512, 3, 1024);

	for (size_t i = 0; i < 1000000; i++) {
		sketch.AddRecord(i % 100000, i);
	}

	std::vector<uint64_t> hashes(state.range());
	std::vector<const omnisketch::OmniSketch *> sketches(state.range(), &sketch);

	omnisketch::CardEstResult result;
	for (auto _ : state) {
		for (size_t i = 0; i < state.range(); i++) {
			hashes[i] = omnisketch::Hash(1337 + i);
		}
		result = omnisketch::Algorithm::DisjunctionEarlyExit(sketches, hashes);
	}

	double actual_card = state.range() * 10;
	double q_error = std::max(result.cardinality, actual_card) / std::min(result.cardinality, actual_card);
	if (result.cardinality < actual_card) {
		q_error *= -1;
	}
	state.counters["Q-Error"] = q_error;
}

BENCHMARK(BM_JoinAsDisjunctionEarlyExit)->RangeMultiplier(2)->Range(2, 4096)->Unit(benchmark::kMillisecond);

static void BM_OmniSketchInsert(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(512, 4, state.range());
	for (auto _ : state) {
		for (size_t i = 0; i < 6000000; i++) {
			sketch.AddRecord(keys[i], rids[i]);
		}
	}
}

BENCHMARK(BM_OmniSketchInsert)->DenseRange(8, 1024, 1016)->Unit(benchmark::kMillisecond);

static void BM_OmniSketchInsertBatched(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(512, 4, state.range());
	sketch.InitializeBuffers(6000000);
	for (auto _ : state) {
		sketch.AddRecords(keys.data(), rids.data(), 6000000);
	}
}

BENCHMARK(BM_OmniSketchInsertBatched)->DenseRange(8, 1024, 1016)->Unit(benchmark::kMillisecond);

static void BM_OmniSketchIntersect(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(512, 3, state.range());
	sketch.InitializeBuffers(6000000);
	sketch.AddRecords(keys.data(), rids.data(), 6000000);
	std::vector<size_t> search_keys(keys.begin(), std::next(keys.begin(), 1024));

	for (auto _ : state) {
		for (size_t i = 0; i < 1024; i++) {
			auto card = sketch.EstimateCardinality(search_keys[i]);
		}
	}
}

BENCHMARK(BM_OmniSketchIntersect)->DenseRange(8, 1024, 1016)->Unit(benchmark::kMillisecond);

static void BM_OmniSketchIntersectBatched(benchmark::State &state) {
	std::vector<size_t> keys(6000000);
	std::vector<size_t> rids(6000000);

	for (size_t i = 0; i < 6000000; i++) {
		keys[i] = i % 50000;
		rids[i] = i;
	}

	omnisketch::OmniSketch sketch(512, 3, state.range());
	sketch.InitializeBuffers(6000000);
	sketch.AddRecords(keys.data(), rids.data(), 6000000);
	std::vector<size_t> search_keys(keys.begin(), std::next(keys.begin(), 1024));

	omnisketch::CardEstResult result;
	for (auto _ : state) {
		result = sketch.EstimateCardinality(search_keys.data(), 1024, sketch.MinHashSampleCount());
	}

	double q_error = std::max(result.cardinality, 122880.0) / std::min(result.cardinality, 122880.0);
	if (result.cardinality < 122880.0) {
		q_error *= -1;
	}
	state.counters["Q-Error"] = q_error;
}

BENCHMARK(BM_OmniSketchIntersectBatched)->DenseRange(8, 1024, 1016)->Unit(benchmark::kMillisecond);
*/
BENCHMARK_MAIN();