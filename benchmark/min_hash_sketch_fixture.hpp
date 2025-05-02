#pragma once

#include "min_hash_sketch/min_hash_sketch_set.hpp"
#include "util/hash.hpp"

template <unsigned int MaxSampleSize, unsigned int MatchCount>
class MinHashSketchFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &state) override {
		const size_t sketch_count = state.range();

		sketches = std::vector<std::shared_ptr<omnisketch::MinHashSketch>>(sketch_count);
		auto hf = std::make_shared<omnisketch::MurmurHashFunction<size_t>>();

		for (size_t i = 0; i < sketch_count; i++) {
			sketches[i] = std::make_shared<omnisketch::MinHashSketchSet>(MaxSampleSize);
			for (size_t j = 0; j < MatchCount; j++) {
				const uint64_t hash = hf->HashRid(j);
				sketches[i]->AddRecord(hash);
			}
			for (size_t j = MatchCount; j < MaxSampleSize; j++) {
				const uint64_t hash = hf->HashRid(i * MaxSampleSize + j);
				sketches[i]->AddRecord(hash);
			}
		}
	}

	void Flatten() {
		sketches_flattened.reserve(sketches.size());
		for (auto &sketch : sketches) {
			sketches_flattened.push_back(sketch->Flatten());
		}

		sketches.clear();
	}

	void IntersectTrees(::benchmark::State &state) {
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = sketches.front()->Intersect(sketches);
			result_count = result->Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void IntersectVectors(::benchmark::State &state) {
		Flatten();
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = sketches_flattened.front()->Intersect(sketches_flattened);
			result_count = result->Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void UnionTrees(::benchmark::State &state) {
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = sketches.front()->Combine(sketches);
			result_count = result->Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void UnionVectors(::benchmark::State &state) {
		Flatten();
		size_t result_count = 0;
		for (auto _ : state) {
			auto result = sketches_flattened.front()->Combine(sketches_flattened);
			result_count = result->Size();
		}

		state.counters["MatchCount"] = result_count;
	}

	void TearDown(::benchmark::State &) override {
		sketches.clear();
		sketches_flattened.clear();
	}

	std::vector<std::shared_ptr<omnisketch::MinHashSketch>> sketches;
	std::vector<std::shared_ptr<omnisketch::MinHashSketch>> sketches_flattened;
};