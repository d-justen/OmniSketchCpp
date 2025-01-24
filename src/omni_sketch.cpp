#include "omni_sketch.hpp"

namespace omnisketch {

OmniSketch::OmniSketch(size_t width_p, size_t depth_p, size_t min_hash_sample_count_p)
    : width(width_p), depth(depth_p), min_hash_sample_count(min_hash_sample_count_p), record_count(0) {
	cells.resize(depth);
	for (auto &row : cells) {
		row.resize(width);
	}
}

size_t OmniSketch::RecordCount() const {
	return record_count;
}

void OmniSketch::AddRecordInternal(uint64_t value_hash, uint64_t rid_hash) {
	uint32_t value_hash_1 = value_hash;
	uint32_t value_hash_2 = value_hash >> 32;

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		size_t col_idx = ((uint64_t)value_hash_1 + row_idx * value_hash_2) % width;
		cells[row_idx][col_idx].AddRecord(rid_hash, min_hash_sample_count);
	}
	record_count++;
}

double OmniSketch::EstimateCardinalityInternal(uint64_t value_hash) const {
	uint32_t value_hash_1 = value_hash;
	uint32_t value_hash_2 = value_hash >> 32;

	std::vector<const MinHashSketch *> min_hash_sketches;
	min_hash_sketches.reserve(depth);
	size_t n_max = 0;

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		size_t col_idx = ((uint64_t)value_hash_1 + row_idx * value_hash_2) % width;
		const auto &cell = cells[row_idx][col_idx];
		min_hash_sketches.push_back(&cell.GetMinHashSketch());
		n_max = std::max(n_max, cell.RecordCount());
	}

	const auto intersection = MinHashSketch::Intersect(min_hash_sketches);
	const double card_est = ((double)n_max / (double)min_hash_sample_count) * intersection.Size();
	return card_est;
}

} // namespace omnisketch
