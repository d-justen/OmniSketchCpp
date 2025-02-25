#pragma once

#include <benchmark/benchmark.h>

#include "omni_sketch.hpp"

template <unsigned int WIDTH, unsigned int DEPTH, unsigned int SAMPLE_COUNT>
class OmniSketchFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &state) override {
		omni_sketch =
		    std::make_unique<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>>(WIDTH, DEPTH, SAMPLE_COUNT);
	}

	void FillOmniSketch(size_t value_count, size_t multiplicity,
	                    omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>> *sketch = nullptr) {
		if (!sketch) {
			sketch = &*omni_sketch;
		}
		const size_t rid_count = value_count * multiplicity;

		for (size_t i = 0; i < rid_count; i++) {
			sketch->AddRecord(i % value_count, i);
		}
	}

	void TearDown(::benchmark::State &state) override {
		omni_sketch = nullptr;
	}

	std::unique_ptr<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>> omni_sketch;
};
