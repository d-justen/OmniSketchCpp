#include "omni_sketch_cell.hpp"

namespace omnisketch {

OmniSketchCell::OmniSketchCell() : record_count(0) {
}

OmniSketchCell::OmniSketchCell(size_t max_count) : min_hash_sketch(max_count), record_count(0) {
}

void OmniSketchCell::AddRecord(const uint64_t hash, const size_t max_sample_size) {
	min_hash_sketch.AddRecord(hash);
	record_count++;
}

size_t OmniSketchCell::RecordCount() const {
	return record_count;
}

size_t OmniSketchCell::SampleCount() const {
	return min_hash_sketch.Size();
}

const MinHashSketch<std::set<uint64_t>> &OmniSketchCell::GetMinHashSketch() const {
	return min_hash_sketch;
}

void OmniSketchCell::Combine(const OmniSketchCell &other, size_t max_sample_size) {
	record_count += other.record_count;
	min_hash_sketch.Union(other.min_hash_sketch);
}

} // namespace omnisketch
