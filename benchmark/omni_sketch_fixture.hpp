#pragma once

#include <benchmark/benchmark.h>

#include "omni_sketch/standard_omni_sketch.hpp"
#include "registry.hpp"

template <unsigned int WIDTH, unsigned int DEPTH, unsigned int SAMPLE_COUNT>
class OmniSketchFixture : public benchmark::Fixture {
public:
	OmniSketchFixture() {
		omnisketch::OmniSketchConfig config;
		config.sample_count = SAMPLE_COUNT;
		config.SetWidth(WIDTH);
		config.depth = DEPTH;
		config.hash_processor = std::make_shared<omnisketch::BarrettModSplitHashMapper>(WIDTH);
		omni_sketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
		    config.width, config.depth, config.sample_count, std::make_shared<omnisketch::MurmurHashFunction<size_t>>(),
		    config.set_membership_algo, config.hash_processor);
	}

	void FillOmniSketch(size_t value_count, size_t multiplicity,
	                    omnisketch::TypedPointOmniSketch<size_t> *sketch = nullptr) {
		if (!sketch) {
			sketch = omni_sketch.get();
		}
		const size_t rid_count = value_count * multiplicity;

		for (size_t i = 0; i < rid_count; i++) {
			sketch->AddRecord(i % value_count, i);
		}
	}

	std::shared_ptr<omnisketch::TypedPointOmniSketch<size_t>> omni_sketch;
	size_t current_repetition = 0;
};
