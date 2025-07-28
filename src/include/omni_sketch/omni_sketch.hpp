#pragma once

#include "min_hash_sketch/min_hash_sketch_set.hpp"
#include "omni_sketch_cell.hpp"
#include "set_membership.hpp"
#include "util/hash.hpp"
#include "util/value.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace omnisketch {

enum class OmniSketchType { STANDARD, PRE_JOINED };

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
	virtual size_t CountNulls() const = 0;
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
	virtual OmniSketchType Type() const = 0;
};

class PointOmniSketch : public OmniSketch {
public:
	virtual ~PointOmniSketch() = default;
	PointOmniSketch(size_t width_p, size_t depth_p, size_t max_sample_count_p,
	                std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
	                std::shared_ptr<CellIdxMapper> hash_processor_p,
	                const std::shared_ptr<MinHashSketch::SketchFactory> &factory =
	                    std::make_shared<MinHashSketchSet::SketchFactory>());
	PointOmniSketch(size_t width, size_t depth, size_t max_sample_count_p);

	virtual void AddValueRecord(const Value &value, uint64_t record_id) override;
	virtual void AddRecordHashed(uint64_t value_hash, uint64_t record_id_hash) override;
	void AddNullValues(size_t count) override;
	size_t CountNulls() const override;
	size_t RecordCount() const override;
	void SetRecordCount(size_t record_count_p);
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
	void SetCell(size_t row_idx, size_t col_idx, std::shared_ptr<OmniSketchCell> cell);

protected:
	size_t width;
	size_t depth;
	size_t max_sample_count;
	std::shared_ptr<SetMembershipAlgorithm> set_membership_algo;
	std::shared_ptr<CellIdxMapper> hash_processor;

	std::vector<std::vector<std::shared_ptr<OmniSketchCell>>> cells;
	size_t record_count = 0;
	size_t null_count = 0;
};

} // namespace omnisketch
