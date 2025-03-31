#pragma once

#include <benchmark/benchmark.h>

#include "include/omni_sketch.hpp"

template <unsigned int WIDTH, unsigned int DEPTH, unsigned int SAMPLE_COUNT>
class OmniSketchFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &state) override {
		omni_sketch = std::make_shared<omnisketch::PointOmniSketch<size_t>>(WIDTH, DEPTH, SAMPLE_COUNT);
	}

	void FillOmniSketch(size_t value_count, size_t multiplicity,
	                    omnisketch::PointOmniSketch<size_t> *sketch = nullptr) {
		if (!sketch) {
			sketch = omni_sketch.get();
		}
		const size_t rid_count = value_count * multiplicity;

		for (size_t i = 0; i < rid_count; i++) {
			sketch->AddRecord(i % value_count, i);
		}
	}

	void TearDown(::benchmark::State &state) override {
		omni_sketch = nullptr;
	}

	std::shared_ptr<omnisketch::PointOmniSketch<size_t>> omni_sketch;
};
