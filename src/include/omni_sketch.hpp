#pragma once

#include "set_membership.hpp"
#include "hash.hpp"
#include "omni_sketch_cell.hpp"
#include "value.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace omnisketch {

class OmniSketch {
public:
	virtual ~OmniSketch() = default;
	virtual size_t RecordCount() const = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeHash(uint64_t hash,
	                                                  std::vector<std::shared_ptr<OmniSketchCell>> &matches) const = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeHashedSet(const std::shared_ptr<MinHashSketch> &values) const = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeHashedSet(const std::shared_ptr<OmniSketchCell> &values) const = 0;
	virtual void AddValueRecord(const Value &value, uint64_t record_id) = 0;
	virtual void AddRecordHashed(uint64_t value_hash, uint64_t record_id_hash) = 0;
	virtual void AddNullValues(size_t count) = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeValue(const Value &value) const = 0;
	virtual std::shared_ptr<OmniSketchCell> ProbeValueSet(const ValueSet &values) const = 0;
	virtual void Flatten() = 0;
	virtual size_t EstimateByteSize() const = 0;
	virtual size_t Depth() const = 0;
	virtual size_t Width() const = 0;
	virtual size_t MinHashSketchSize() const = 0;
	virtual std::shared_ptr<OmniSketchCell> GetRids() const = 0;
	virtual void Combine(const std::shared_ptr<OmniSketch> &other) = 0;
	virtual const OmniSketchCell &GetCell(size_t row_idx, size_t col_idx) const = 0;
};

class PointOmniSketch : public OmniSketch {
public:
	virtual ~PointOmniSketch() = default;
	PointOmniSketch(size_t width_p, size_t depth_p, std::shared_ptr<MinHashSketchFactory> min_hash_sketch_factory_p,
	                std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
	                std::shared_ptr<CellIdxMapper> hash_processor_p);
	PointOmniSketch(size_t width, size_t depth, size_t max_sample_count_p);

	void AddValueRecord(const Value &value, uint64_t record_id) override;
	void AddRecordHashed(uint64_t value_hash, uint64_t record_id_hash) override;
	void AddNullValues(size_t count) override;
	size_t RecordCount() const override;
	std::shared_ptr<OmniSketchCell> ProbeValue(const Value &value) const override;
	std::shared_ptr<OmniSketchCell> ProbeHash(uint64_t hash,
	                                          std::vector<std::shared_ptr<OmniSketchCell>> &matches) const override;
	std::shared_ptr<OmniSketchCell> ProbeHashedSet(const std::shared_ptr<MinHashSketch> &values) const override;
	std::shared_ptr<OmniSketchCell> ProbeHashedSet(const std::shared_ptr<OmniSketchCell> &values) const override;
	std::shared_ptr<OmniSketchCell> ProbeValueSet(const ValueSet &values) const override;
	void Flatten() override;
	size_t EstimateByteSize() const override;
	size_t Depth() const override;
	size_t Width() const override;
	size_t MinHashSketchSize() const override;
	std::shared_ptr<OmniSketchCell> GetRids() const override;
	void Combine(const std::shared_ptr<OmniSketch> &other) override;
	const OmniSketchCell &GetCell(size_t row_idx, size_t col_idx) const override;

protected:
	size_t width;
	size_t depth;
	std::shared_ptr<MinHashSketchFactory> min_hash_sketch_factory;
	std::shared_ptr<SetMembershipAlgorithm> set_membership_algo;
	std::shared_ptr<CellIdxMapper> hash_processor;

	std::vector<std::vector<std::shared_ptr<OmniSketchCell>>> cells;
	size_t record_count = 0;
};

template <typename T>
class TypedPointOmniSketch : public PointOmniSketch {
public:
	TypedPointOmniSketch(size_t width_p, size_t depth_p, std::shared_ptr<HashFunction<T>> hash_function_p,
	                     std::shared_ptr<MinHashSketchFactory> min_hash_sketch_factory_p,
	                     std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
	                     std::shared_ptr<CellIdxMapper> hash_processor_p)
	    : PointOmniSketch(width_p, depth_p, std::move(min_hash_sketch_factory_p), std::move(set_membership_algo_p),
	                      std::move(hash_processor_p)),
	      hf(std::move(hash_function_p)) {
	}

	TypedPointOmniSketch(size_t width, size_t depth, size_t max_sample_count_p)
	    : TypedPointOmniSketch(width, depth, std::make_shared<MurmurHashFunction<T>>(),
	                           std::make_shared<MinHashSketchSetFactory>(max_sample_count_p),
	                           std::make_shared<ProbeAllSum>(), std::make_shared<BarrettModSplitHashMapper>(width)) {
	}

	void AddRecord(const T &value, uint64_t record_id) {
		min = std::min(min, value);
		max = std::max(max, value);
		PointOmniSketch::AddRecordHashed(hf->Hash(value), hf->HashRid(record_id));
	}

	std::shared_ptr<OmniSketchCell> Probe(const T &value) const {
		std::vector<std::shared_ptr<OmniSketchCell>> matches(depth);
		return PointOmniSketch::ProbeHash(hf->Hash(value), matches);
	}

	std::shared_ptr<OmniSketchCell> ProbeSet(const T *values, size_t count) const {
		std::vector<uint64_t> hashes;
		hashes.reserve(count);

		for (size_t value_idx = 0; value_idx < count; value_idx++) {
			hashes.push_back(hf->Hash(values[value_idx]));
		}

		return PointOmniSketch::ProbeHashedSet(std::make_shared<MinHashSketchVector>(hashes));
	}

	std::shared_ptr<OmniSketchCell> ProbeRange(const T &lower_bound, const T &upper_bound) const {
		std::vector<uint64_t> hashes;
		hashes.reserve((upper_bound - lower_bound) + 1);
		for (T value = lower_bound; value <= upper_bound; value++) {
			hashes.push_back(hf->Hash(value));
		}

		return PointOmniSketch::ProbeHashedSet(std::make_shared<MinHashSketchVector>(hashes));
	}

	T GetMin() const {
		return min;
	}

	T GetMax() const {
		return max;
	}

protected:
	std::shared_ptr<HashFunction<T>> hf;
	T min = std::numeric_limits<T>::max();
	T max = std::numeric_limits<T>::min();
};

} // namespace omnisketch
