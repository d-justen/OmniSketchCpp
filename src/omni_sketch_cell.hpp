#pragma once

#include "min_hash_sketch.hpp"

#include <cstddef>
#include <cstdint>
#include <set>

namespace omnisketch {

class OmniSketchCell {
public:
	OmniSketchCell();
	explicit OmniSketchCell(size_t max_count_p);
	void AddRecord(uint64_t hash, size_t max_sample_size);
	size_t RecordCount() const;
	size_t SampleCount() const;
	void Combine(const OmniSketchCell &other, size_t max_sample_size);
	const MinHashSketch<std::set<uint64_t>> &GetMinHashSketch() const;

protected:
	size_t record_count;
	MinHashSketch<std::set<uint64_t>> min_hash_sketch;
};

} // namespace omnisketch
