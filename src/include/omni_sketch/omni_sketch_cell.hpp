#pragma once

#include "min_hash_sketch/min_hash_sketch.hpp"

#include <cstddef>
#include <cstdint>
#include <set>

namespace omnisketch {

class OmniSketchCell {
public:
	OmniSketchCell(std::shared_ptr<MinHashSketch> min_hash_sketch_p, size_t record_count_p);
	explicit OmniSketchCell(std::shared_ptr<MinHashSketch> min_hash_sketch_p);
	explicit OmniSketchCell(size_t max_sample_count);
	OmniSketchCell();

	void AddRecord(uint64_t hash);
	size_t RecordCount() const;
	size_t SampleCount() const;
	size_t MaxSampleCount() const;
	double SamplingProbability() const;
	void Combine(const OmniSketchCell &other);
	const std::shared_ptr<MinHashSketch> &GetMinHashSketch() const;
	std::shared_ptr<MinHashSketch> &GetMinHashSketch();
	std::shared_ptr<OmniSketchCell> Flatten();
	void SetRecordCount(size_t count);
	void SetMinHashSketch(std::shared_ptr<MinHashSketch> sketch);
	size_t EstimateByteSize() const;
	static std::shared_ptr<OmniSketchCell> Intersect(const std::vector<std::shared_ptr<OmniSketchCell>> &cells,
	                                                 size_t max_samples = 0);
	static std::shared_ptr<OmniSketchCell> Combine(const std::vector<std::shared_ptr<OmniSketchCell>> &cells);

protected:
	std::shared_ptr<MinHashSketch> min_hash_sketch;
	size_t record_count;
};

} // namespace omnisketch
