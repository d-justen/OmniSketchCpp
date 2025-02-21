#pragma once

#include "algorithm/min_hash_set_intersection.hpp"
#include "min_hash_sketch.hpp"
#include "hash.hpp"

constexpr size_t MAX_HASH_COUNT = 1024;
constexpr size_t MATCH_COUNT = 32;

template <typename T>
class MinHashSketchFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &state) override {
		const size_t sketch_count = state.range();

		sketches = std::vector<omnisketch::MinHashSketch<std::set<typename T::value_type>>>(
		    sketch_count, omnisketch::MinHashSketch<std::set<typename T::value_type>>(MAX_HASH_COUNT));

		for (size_t i = 0; i < sketch_count; i++) {
			for (size_t j = 0; j < MATCH_COUNT; j++) {
				typename T::value_type hash = omnisketch::Hash(j);
				sketches[i].AddRecord(hash);
			}
			for (size_t j = MATCH_COUNT; j < MAX_HASH_COUNT; j++) {
				typename T::value_type hash = omnisketch::Hash(i * MAX_HASH_COUNT + j);
				sketches[i].AddRecord(hash);
			}
		}

		sketch_ptrs = std::vector<const omnisketch::MinHashSketch<std::set<typename T::value_type>> *>(sketches.size());
		for (size_t i = 0; i < sketch_ptrs.size(); i++) {
			sketch_ptrs[i] = &sketches[i];
		}
	}

	void Flatten() {
		sketches_flattened.reserve(sketches.size());
		for (auto& sketch : sketches) {
			sketches_flattened.push_back(sketch.Flatten());
		}

		sketch_ptrs_flattened.resize(sketches_flattened.size());
		for (size_t i = 0; i < sketch_ptrs_flattened.size(); i++) {
			sketch_ptrs_flattened[i] = &sketches_flattened[i];
		}

		sketch_ptrs.clear();
		sketches.clear();
	}

	void ExecuteTree(::benchmark::State &state) {
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = omnisketch::Algorithm::MultiwayIntersection(sketch_ptrs);
			result_count = result.Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void ExecuteVector(::benchmark::State &state) {
		Flatten();
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = omnisketch::Algorithm::MultiwayIntersection(sketch_ptrs_flattened);
			result_count = result.Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	std::vector<omnisketch::MinHashSketch<std::set<typename T::value_type>>> sketches;
	std::vector<const omnisketch::MinHashSketch<std::set<typename T::value_type>> *> sketch_ptrs;
	std::vector<omnisketch::MinHashSketch<T>> sketches_flattened;
	std::vector<const omnisketch::MinHashSketch<T> *> sketch_ptrs_flattened;
};