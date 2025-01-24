#pragma once

#include "min_hash_sketch.hpp"

#include <cstddef>
#include <cstdint>
#include <set>

namespace omnisketch {

class OmniSketchCell {
public:
	OmniSketchCell();
	void AddRecord(uint64_t hash, size_t max_sample_size);
	size_t RecordCount() const;
	size_t SampleCount() const;
	const MinHashSketch &GetMinHashSketch() const;

protected:
	size_t record_count;
	MinHashSketch min_hash_sketch;
};

} // namespace omnisketch
