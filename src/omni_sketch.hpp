#pragma once

#include "hash.hpp"
#include "omni_sketch_cell.hpp"

#include <cstddef>
#include <cstdint>

namespace omnisketch {

using OmniSketchCellVector = std::vector<OmniSketchCell>;

struct CardEstResult {
	double cardinality = 0;
	MinHashSketch min_hash_sketch;
	size_t max_sample_size = 0;
};

class OmniSketch {
public:
	OmniSketch(size_t width, size_t depth, size_t min_hash_sample_count_p);

	template <class T, class U>
	void AddRecord(const T &value, const U &rid) {
		AddRecordInternal(Hash(value), Hash(rid));
	}

	template <class T, class U>
	void AddRecords(T *values, U *rids, size_t count) {
		std::vector<uint64_t> value_hashes(count);
		std::vector<uint64_t> rid_hashes(count);

		for (size_t record_idx = 0; record_idx < count; record_idx++) {
			value_hashes[record_idx] = Hash(values[record_idx]);
			rid_hashes[record_idx] = Hash(rids[record_idx]);
		}

		for (size_t record_idx = 0; record_idx < count; record_idx++) {
			AddRecordInternal(value_hashes[record_idx], rid_hashes[record_idx]);
		}
	}

	template <class T>
	void FindCells(const T &value, std::vector<size_t> &result) const {
		FindCellsInternal(Hash(value), result);
	}

	template <class T>
	CardEstResult EstimateCardinality(const T &value) const {
		std::vector<size_t> cell_idxs;
		cell_idxs.reserve(depth);
		FindCells(value, cell_idxs);
		return EstimateCardinalityInternal(cell_idxs);
	}

	template <class T>
	CardEstResult EstimateCardinality(const T *values, size_t count) const {
		std::vector<uint64_t> value_hashes(count);
		for (size_t value_idx = 0; value_idx < count; value_idx++) {
			value_hashes[value_idx] = Hash(values[value_idx]);
		}

		CardEstResult result;
		result.max_sample_size = min_hash_sample_count;

		for (size_t value_idx = 0; value_idx < count; value_idx++) {
			std::vector<size_t> cell_idxs;
			cell_idxs.reserve(depth);
			FindCellsInternal(value_hashes[value_idx], cell_idxs);
			auto intermediate_result = EstimateCardinalityInternal(cell_idxs);
			result.min_hash_sketch.Combine(intermediate_result.min_hash_sketch, min_hash_sample_count);
			result.cardinality += intermediate_result.cardinality;
		}

		result.cardinality = std::min(result.cardinality, (double)record_count);
		return result;
	}

	size_t RecordCount() const;
	size_t MinHashSampleCount() const;
	size_t Width() const;
	size_t Depth() const;

protected:
	void AddRecordInternal(uint64_t value_hash, uint64_t rid_hash);
	CardEstResult EstimateCardinalityInternal(const std::vector<size_t> &cell_idxs) const;
	void FindCellsInternal(uint64_t value_hash, std::vector<size_t> &cell_idxs) const;

	const size_t width;
	const size_t depth;
	const size_t min_hash_sample_count;
	size_t record_count;
	std::vector<OmniSketchCellVector> cells;
};

} // namespace omnisketch
