#include "omni_sketch_cell.hpp"

namespace omnisketch {

OmniSketchCell::OmniSketchCell() : record_count(0) {
}

void OmniSketchCell::AddRecord(const uint64_t hash, const size_t max_sample_size) {
	min_hash_sketch.AddRecord(hash, max_sample_size);
	record_count++;
}

size_t OmniSketchCell::RecordCount() const {
	return record_count;
}

size_t OmniSketchCell::SampleCount() const {
	return min_hash_sketch.Size();
}

const MinHashSketch &OmniSketchCell::GetMinHashSketch() const {
	return min_hash_sketch;
}

} // namespace omnisketch
