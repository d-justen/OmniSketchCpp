#include "omni_sketch.hpp"

#include <cassert>

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

void OmniSketch::AddRecordInternal(const uint64_t value_hash, const uint64_t rid_hash) {
	uint32_t value_hash_1 = value_hash;
	uint32_t value_hash_2 = value_hash >> 32;

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		size_t col_idx = ComputeCellIdx(value_hash_1, value_hash_2, row_idx, width);
		cells[row_idx][col_idx].AddRecord(rid_hash, min_hash_sample_count);
	}
	record_count++;
}

CardEstResult OmniSketch::EstimateCardinalityInternal(const std::vector<size_t> &cell_idxs) const {
	assert(cell_idxs.size() == depth);
	std::vector<const MinHashSketch *> min_hash_sketches;
	min_hash_sketches.reserve(cell_idxs.size());
	size_t n_max = 0;

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		const size_t cell_idx = cell_idxs[row_idx];
		const auto &cell = cells[row_idx][cell_idx];
		min_hash_sketches.push_back(&cell.GetMinHashSketch());
		n_max = std::max(n_max, cell.RecordCount());
	}

	size_t sample_count = std::min(n_max, min_hash_sample_count);

	CardEstResult result;
	result.min_hash_sketch = MinHashSketch::Intersect(min_hash_sketches);
	result.cardinality = ((double)n_max / (double)sample_count) * (double)result.min_hash_sketch.Size();
	result.max_sample_size = min_hash_sample_count;
	return result;
}

void OmniSketch::FindCellsInternal(const uint64_t value_hash, std::vector<size_t> &result) const {
	uint32_t value_hash_1 = value_hash;
	uint32_t value_hash_2 = value_hash >> 32;

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		size_t col_idx = ComputeCellIdx(value_hash_1, value_hash_2, row_idx, width);
		result.push_back(col_idx);
	}
}
size_t OmniSketch::Width() const {
	return width;
}

size_t OmniSketch::Depth() const {
	return depth;
}
size_t OmniSketch::MinHashSampleCount() const {
	return min_hash_sample_count;
}

} // namespace omnisketch
