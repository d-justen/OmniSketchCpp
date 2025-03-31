#pragma once

#include "set_membership.hpp"
#include "hash.hpp"
#include "omni_sketch_cell.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace omnisketch {

class OmniSketchBase {
public:
	virtual size_t RecordCount() const = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeHash(uint64_t hash,
	                                                  std::vector<std::shared_ptr<OmniSketchCell>> &matches) const = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeSet(const std::shared_ptr<MinHashSketch> &values) const = 0;
	virtual void Flatten() = 0;
	virtual size_t EstimateByteSize() const = 0;
	virtual size_t Depth() const = 0;
	virtual size_t Width() const = 0;
	virtual size_t MinHashSketchSize() const = 0;
	virtual void Combine(const std::shared_ptr<OmniSketchBase> &other) = 0;
	virtual const OmniSketchCell &GetCell(size_t row_idx, size_t col_idx) const = 0;
};

template <typename T>
class OmniSketch : public OmniSketchBase {
public:
	virtual void AddRecord(const T &value, uint64_t record_id) = 0;
	virtual std::shared_ptr<OmniSketchCell> Probe(const T &value) const = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeSet(const T *values, size_t count) const = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeRange(const T &lower_bound, const T &upper_bound) const = 0;
};

template <typename T>
class PointOmniSketch : public OmniSketch<T> {
public:
	PointOmniSketch(size_t width_p, size_t depth_p, std::shared_ptr<MinHashSketchFactory> min_hash_sketch_factory_p,
	                std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
	                std::shared_ptr<HashProcessor> hash_processor_p);
	PointOmniSketch(size_t width, size_t depth, size_t max_sample_count_p);

	void AddRecord(const T &value, uint64_t record_id) override;
	size_t RecordCount() const override;
	std::shared_ptr<OmniSketchCell> Probe(const T &value) const override;
	std::shared_ptr<OmniSketchCell> ProbeHash(uint64_t hash,
	                                          std::vector<std::shared_ptr<OmniSketchCell>> &matches) const override;
	std::shared_ptr<OmniSketchCell> ProbeSet(const std::shared_ptr<MinHashSketch> &values) const override;
	std::shared_ptr<OmniSketchCell> ProbeSet(const T *values, size_t count) const override;
	std::shared_ptr<OmniSketchCell> ProbeRange(const T &lower_bound, const T &upper_bound) const override;
	void Flatten() override;
	size_t EstimateByteSize() const override;
	size_t Depth() const override;
	size_t Width() const override;
	size_t MinHashSketchSize() const override;
	void Combine(const std::shared_ptr<OmniSketchBase> &other) override;
	const OmniSketchCell &GetCell(size_t row_idx, size_t col_idx) const override;

protected:
	size_t width;
	size_t depth;
	std::shared_ptr<MinHashSketchFactory> min_hash_sketch_factory;
	std::shared_ptr<SetMembershipAlgorithm> set_membership_algo;
	std::shared_ptr<HashProcessor> hash_processor;

	std::vector<std::vector<std::shared_ptr<OmniSketchCell>>> cells;
	size_t record_count = 0;
};

} // namespace omnisketch

#include "omni_sketch.tpp"
