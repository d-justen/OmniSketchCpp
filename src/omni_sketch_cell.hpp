#pragma once

#include "min_hash_sketch.hpp"

#include <cstddef>
#include <cstdint>
#include <set>

namespace omnisketch {

template <typename ContainerType>
class OmniSketchCell {
public:
	OmniSketchCell() : record_count(0) {
	}

	explicit OmniSketchCell(size_t max_count_p)
	    : min_hash_sketch(MinHashSketch<ContainerType>(max_count_p)), record_count(0) {
	}

	void AddRecord(typename ContainerType::value_type hash) {
		min_hash_sketch.AddRecord(hash);
		record_count++;
	}

	inline size_t RecordCount() const {
		return record_count;
	}

	inline size_t SampleCount() const {
		return min_hash_sketch.Size();
	}

	inline size_t MaxSampleCount() const {
		return min_hash_sketch.max_count;
	}

	void Combine(const OmniSketchCell &other, size_t max_sample_size) {
		record_count += other.record_count;
		min_hash_sketch.Union(other.min_hash_sketch);
	}

	const MinHashSketch<ContainerType> &GetMinHashSketch() const {
		return min_hash_sketch;
	}

	MinHashSketch<ContainerType> &GetMinHashSketch() {
		return min_hash_sketch;
	}

	OmniSketchCell<std::vector<typename ContainerType::value_type>> Flatten() {
		OmniSketchCell<std::vector<typename ContainerType::value_type>> result;
		result.SetRecordCount(record_count);
		result.SetMinHashSketch(min_hash_sketch.Flatten());
		return result;
	}

	void SetRecordCount(size_t count) {
		record_count = count;
	}

	void SetMinHashSketch(MinHashSketch<ContainerType> sketch) {
		min_hash_sketch = std::move(sketch);
	}

protected:
	size_t record_count;
	MinHashSketch<ContainerType> min_hash_sketch;
};

} // namespace omnisketch
