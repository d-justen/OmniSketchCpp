#pragma once

#include "algorithm/min_hash_set_operations.hpp"
#include "min_hash_sketch.hpp"
#include "hash.hpp"

template <typename T, unsigned int MaxSampleSize, unsigned int MatchCount>
class MinHashSketchFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &state) override {
		const size_t sketch_count = state.range();

		sketches = std::vector<omnisketch::MinHashSketch<std::set<typename T::value_type>>>(
		    sketch_count, omnisketch::MinHashSketch<std::set<typename T::value_type>>(MaxSampleSize));

		for (size_t i = 0; i < sketch_count; i++) {
			for (size_t j = 0; j < MatchCount; j++) {
				typename T::value_type hash = omnisketch::Hash(j);
				sketches[i].AddRecord(hash);
			}
			for (size_t j = MatchCount; j < MaxSampleSize; j++) {
				typename T::value_type hash = omnisketch::Hash(i * MaxSampleSize + j);
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
		for (auto &sketch : sketches) {
			sketches_flattened.push_back(sketch.Flatten());
		}

		sketch_ptrs_flattened.resize(sketches_flattened.size());
		for (size_t i = 0; i < sketch_ptrs_flattened.size(); i++) {
			sketch_ptrs_flattened[i] = &sketches_flattened[i];
		}

		sketch_ptrs.clear();
		sketches.clear();
	}

	void IntersectTrees(::benchmark::State &state) {
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = omnisketch::Algorithm::MultiwayIntersection(sketch_ptrs);
			result_count = result.Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void IntersectVectors(::benchmark::State &state) {
		Flatten();
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = omnisketch::Algorithm::MultiwayIntersection(sketch_ptrs_flattened);
			result_count = result.Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void UnionTrees(::benchmark::State &state) {
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = omnisketch::Algorithm::MultiwayUnion(sketch_ptrs);
			result_count = result.Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void UnionVectors(::benchmark::State &state) {
		Flatten();
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = omnisketch::Algorithm::MultiwayUnion(sketch_ptrs_flattened);
			result_count = result.Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void TearDown(::benchmark::State &state) override {
		sketches.clear();
		sketch_ptrs.clear();
		sketches_flattened.clear();
		sketch_ptrs_flattened.clear();
	}

	std::vector<omnisketch::MinHashSketch<std::set<typename T::value_type>>> sketches;
	std::vector<const omnisketch::MinHashSketch<std::set<typename T::value_type>> *> sketch_ptrs;
	std::vector<omnisketch::MinHashSketch<T>> sketches_flattened;
	std::vector<const omnisketch::MinHashSketch<T> *> sketch_ptrs_flattened;
};