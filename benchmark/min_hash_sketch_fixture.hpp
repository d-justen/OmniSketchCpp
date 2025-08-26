#pragma once

#include "min_hash_sketch/min_hash_sketch_set.hpp"
#include "util/hash.hpp"

template <unsigned int MaxSampleSize, unsigned int MatchCount>
class MinHashSketchFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &state) override {
		const size_t sketchCount = static_cast<size_t>(state.range());

		sketches.reserve(sketchCount);
		auto hashFunction = std::make_shared<omnisketch::MurmurHashFunction<size_t>>();

		for (size_t i = 0; i < sketchCount; ++i) {
			auto sketch = std::make_shared<omnisketch::MinHashSketchSet>(MaxSampleSize);
			
			// Add matching records
			for (size_t j = 0; j < MatchCount; ++j) {
				const uint64_t hash = hashFunction->HashRid(j);
				sketch->AddRecord(hash);
			}
			
			// Add unique records for this sketch
			for (size_t j = MatchCount; j < MaxSampleSize; ++j) {
				const uint64_t hash = hashFunction->HashRid(i * MaxSampleSize + j);
				sketch->AddRecord(hash);
			}
			
			sketches.emplace_back(std::move(sketch));
		}
	}

	void Flatten() {
		sketchesFlattened.reserve(sketches.size());
		for (const auto &sketch : sketches) {
			sketchesFlattened.emplace_back(sketch->Flatten());
		}
		sketches.clear();
	}

	void IntersectTrees(::benchmark::State &state) {
		size_t resultCount = 0;
		for (auto _ : state) {
			auto result = sketches.front()->Intersect(sketches);
			resultCount = result->Size();
		}
		state.counters["MatchCount"] = static_cast<double>(resultCount);
	}

	void IntersectVectors(::benchmark::State &state) {
		Flatten();
		size_t resultCount = 0;
		for (auto _ : state) {
			auto result = sketchesFlattened.front()->Intersect(sketchesFlattened);
			resultCount = result->Size();
		}
		state.counters["MatchCount"] = static_cast<double>(resultCount);
	}

	void UnionTrees(::benchmark::State &state) {
		size_t resultCount = 0;
		for (auto _ : state) {
			auto result = sketches.front()->Combine(sketches);
			resultCount = result->Size();
		}
		state.counters["MatchCount"] = static_cast<double>(resultCount);
	}

	void UnionVectors(::benchmark::State &state) {
		Flatten();
		size_t resultCount = 0;
		for (auto _ : state) {
			auto result = sketchesFlattened.front()->Combine(sketchesFlattened);
			resultCount = result->Size();
		}
		state.counters["MatchCount"] = static_cast<double>(resultCount);
	}

	void TearDown(::benchmark::State &) override {
		sketches.clear();
		sketchesFlattened.clear();
	}

private:
	std::vector<std::shared_ptr<omnisketch::MinHashSketch>> sketches;
	std::vector<std::shared_ptr<omnisketch::MinHashSketch>> sketchesFlattened;
};
