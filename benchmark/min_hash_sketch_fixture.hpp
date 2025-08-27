#pragma once

#include "min_hash_sketch/min_hash_sketch_set.hpp"
#include "util/hash.hpp"

template <unsigned int MaxSampleSize, unsigned int MatchCount>
class MinHashSketchFixture : public benchmark::Fixture {
public:
    void SetUp(::benchmark::State& state) override {
        const size_t sketch_count = static_cast<size_t>(state.range());

        sketches.reserve(sketch_count);
        auto hash_function = std::make_shared<omnisketch::MurmurHashFunction<size_t>>();

        for (size_t i = 0; i < sketch_count; ++i) {
            auto sketch = std::make_shared<omnisketch::MinHashSketchSet>(MaxSampleSize);

            // Add matching records
            for (size_t j = 0; j < MatchCount; ++j) {
                const uint64_t hash = hash_function->HashRid(j);
                sketch->AddRecord(hash);
            }

            // Add unique records for this sketch
            for (size_t j = MatchCount; j < MaxSampleSize; ++j) {
                const uint64_t hash = hash_function->HashRid(i * MaxSampleSize + j);
                sketch->AddRecord(hash);
            }

            sketches.emplace_back(std::move(sketch));
        }
    }

    void Flatten() {
        sketches_flattened.reserve(sketches.size());
        for (const auto& sketch : sketches) {
            sketches_flattened.emplace_back(sketch->Flatten());
        }
        sketches.clear();
    }

    void IntersectTrees(::benchmark::State& state) {
        size_t result_count = 0;
        for (auto _ : state) {
            auto result = sketches.front()->Intersect(sketches);
            result_count = result->Size();
        }
        state.counters["MatchCount"] = static_cast<double>(result_count);
    }

    void IntersectVectors(::benchmark::State& state) {
        Flatten();
        size_t result_count = 0;
        for (auto _ : state) {
            auto result = sketches_flattened.front()->Intersect(sketches_flattened);
            result_count = result->Size();
        }
        state.counters["MatchCount"] = static_cast<double>(result_count);
    }

    void UnionTrees(::benchmark::State& state) {
        size_t result_count = 0;
        for (auto _ : state) {
            auto result = sketches.front()->Combine(sketches);
            result_count = result->Size();
        }
        state.counters["MatchCount"] = static_cast<double>(result_count);
    }

    void UnionVectors(::benchmark::State& state) {
        Flatten();
        size_t result_count = 0;
        for (auto _ : state) {
            auto result = sketches_flattened.front()->Combine(sketches_flattened);
            result_count = result->Size();
        }
        state.counters["MatchCount"] = static_cast<double>(result_count);
    }

    void TearDown(::benchmark::State&) override {
        sketches.clear();
        sketches_flattened.clear();
    }

private:
    std::vector<std::shared_ptr<omnisketch::MinHashSketch>> sketches;
    std::vector<std::shared_ptr<omnisketch::MinHashSketch>> sketches_flattened;
};
