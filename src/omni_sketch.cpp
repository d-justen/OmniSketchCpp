#include "omni_sketch.hpp"

#include "algorithm/min_hash_set_intersection.hpp"

namespace omnisketch {

OmniSketch::OmniSketch(size_t width_p, size_t depth_p, size_t min_hash_sample_count_p)
    : width(width_p), depth(depth_p), min_hash_sample_count(min_hash_sample_count_p), record_count(0) {
	cells.resize(depth);
	for (auto &row : cells) {
		row.resize(width);
	}
}

void OmniSketch::InitializeBuffers(size_t size) {
	if (size > 0) {
		// Use old-school initialization for backwards compatibility with C++ 11
		value_hashes = std::unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>(size));
		rid_hashes = std::unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>(size));
	}
}

size_t OmniSketch::RecordCount() const {
	return record_count;
}

void OmniSketch::AddRecordInternal(const uint64_t value_hash, const uint64_t rid_hash) {
	auto hash_pair = SplitHash(value_hash);

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		size_t col_idx = ComputeCellIdx(hash_pair.first, hash_pair.second, row_idx, width);
		cells[row_idx][col_idx].AddRecord(rid_hash, min_hash_sample_count);
	}
	record_count++;
}

CardEstResult OmniSketch::EstimateCardinalityHashed(const uint64_t *values, size_t count,
                                                    size_t output_sample_count) const {
	CardEstResult result(output_sample_count);

	for (size_t value_idx = 0; value_idx < count; value_idx++) {
		std::vector<size_t> cell_idxs;
		cell_idxs.reserve(depth);
		AppendCellIndicesInternal(values[value_idx], cell_idxs);
		auto intermediate_result = EstimateCardinalityInternal(cell_idxs);
		result.min_hash_sketch.Union(intermediate_result.min_hash_sketch);
		result.cardinality += intermediate_result.cardinality;
	}

	result.cardinality = std::min(result.cardinality, (double)record_count);
	return result;
}

CardEstResult OmniSketch::EstimateCardinalityInternal(const std::vector<size_t> &cell_idxs) const {
	assert(cell_idxs.size() == depth);
	std::vector<const MinHashSketch<std::set<uint64_t>> *> min_hash_sketches;
	min_hash_sketches.reserve(cell_idxs.size());
	size_t n_max = 0;

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		const size_t cell_idx = cell_idxs[row_idx];
		const auto &cell = cells[row_idx][cell_idx];
		min_hash_sketches.push_back(&cell.GetMinHashSketch());
		n_max = std::max(n_max, cell.RecordCount());
	}

	if (n_max == 0) {
		// OmniSketch does not contain key
		CardEstResult result(min_hash_sample_count);
		return result;
	}

	size_t sample_count = std::min(n_max, min_hash_sample_count);

	CardEstResult result(min_hash_sample_count);
	result.min_hash_sketch = Algorithm::MultiwayIntersection(min_hash_sketches);
	result.cardinality = EstimateCardinality(result.min_hash_sketch, n_max, sample_count);
	return result;
}

void OmniSketch::AppendCellIndicesInternal(const uint64_t value_hash, std::vector<size_t> &result) const {
	auto hash_pair = SplitHash(value_hash);

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		size_t col_idx = ComputeCellIdx(hash_pair.first, hash_pair.second, row_idx, width);
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

size_t OmniSketch::EstimateByteSize() const {
	size_t size = sizeof(cells) + sizeof(*cells.begin()) * cells.capacity();
	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		size += cells[row_idx].capacity() * sizeof(OmniSketchCell);
		for (size_t col_idx = 0; col_idx < width; col_idx++) {
			const auto &cell = cells[row_idx][col_idx];
			size += cell.GetMinHashSketch().Size() * (32 + sizeof(uint64_t));
		}
	}
	return size;
}

void OmniSketch::Combine(const OmniSketch &other) {
	assert(other.depth == depth);
	assert(other.width == width);
	assert(other.min_hash_sample_count == min_hash_sample_count);

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		for (size_t col_idx = 0; col_idx < width; col_idx++) {
			cells[row_idx][col_idx].Combine(other.cells[row_idx][col_idx], min_hash_sample_count);
		}
	}

	record_count += other.record_count;
}

void OmniSketch::GetCells(uint64_t value_hash, std::vector<const OmniSketchCell *> &result_cells) const {
	auto hash_pair = SplitHash(value_hash);

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		size_t col_idx = ComputeCellIdx(hash_pair.first, hash_pair.second, row_idx, width);
		result_cells.push_back(&cells[row_idx][col_idx]);
	}
}

} // namespace omnisketch
