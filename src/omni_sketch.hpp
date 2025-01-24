#pragma once

#include "hash.hpp"
#include "omni_sketch_cell.hpp"

#include <cstddef>
#include <cstdint>

namespace omnisketch {

using OmniSketchCellVector = std::vector<OmniSketchCell>;

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
	double EstimateCardinality(const T &value) const {
		return EstimateCardinalityInternal(Hash(value));
	}

	size_t RecordCount() const;

protected:
	void AddRecordInternal(uint64_t value_hash, uint64_t rid_hash);
	double EstimateCardinalityInternal(uint64_t value_hash) const;

	const size_t width;
	const size_t depth;
	const size_t min_hash_sample_count;
	size_t record_count;
	std::vector<OmniSketchCellVector> cells;
};

} // namespace omnisketch
