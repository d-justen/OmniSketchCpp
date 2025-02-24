#pragma once

#include <benchmark/benchmark.h>

#include "omni_sketch.hpp"

template <unsigned int WIDTH, unsigned int DEPTH, unsigned int SAMPLE_COUNT>
class OmniSketchFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &state) override {
		omni_sketch = std::make_unique<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>>(WIDTH, DEPTH, SAMPLE_COUNT);
	}

	void FillOmniSketch(double fullness) {
		const size_t record_count = WIDTH * SAMPLE_COUNT * fullness;
		for (size_t i = 0; i < record_count; i++) {
			omni_sketch->AddRecord(i, i);
		}
	}

	void TearDown(::benchmark::State &state) override {
		omni_sketch = nullptr;
	}

	std::unique_ptr<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>> omni_sketch;
};
